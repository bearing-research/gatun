"""Payload arena for true zero-copy Arrow transfer.

This module provides a bump allocator for Arrow buffers in shared memory,
enabling true zero-copy data transfer between Python and Java.

The arena uses a simple reset strategy: allocate buffers sequentially,
then reset the entire arena when done processing. This works well for
batch processing patterns.

Example:
    arena = PayloadArena.create(Path("~/gatun_payload.shm"), 64 * 1024 * 1024)

    # Allocate buffers for Arrow data
    info1 = arena.allocate(1024)
    info2 = arena.allocate(2048)

    # When done processing, reset for next batch
    arena.reset()

    arena.close()

For scoped transfers with automatic cleanup, use ScopedArenaContext:

    with client.arrow_context() as ctx:
        for table in tables:
            ctx.send(table)  # Auto-resets arena between sends
        result = ctx.receive()  # Get data back from Java
    # Arena automatically closed and reset on exit
"""

from __future__ import annotations

import ctypes
import io
import mmap
from collections.abc import Iterator
from pathlib import Path
from typing import TYPE_CHECKING, NamedTuple, Self

import pyarrow as pa

if TYPE_CHECKING:
    from gatun.async_client import AsyncGatunClient
    from gatun.client import GatunClient


class BufferInfo(NamedTuple):
    """Information about an allocated buffer."""

    offset: int  # Offset from start of arena
    length: int  # Buffer size in bytes
    buffer: (
        pa.Buffer | None
    )  # PyArrow buffer backed by shared memory (None for zero-length)


class PayloadArena:
    """Manages Arrow buffer allocation in payload shared memory.

    Uses a simple bump allocator with arena-wide reset. Buffers are allocated
    sequentially with 64-byte alignment (Arrow's requirement).

    This arena is separate from the control shared memory used for commands
    and responses, allowing large Arrow data to be sized independently.
    """

    # Arrow requires 64-byte alignment for SIMD operations
    ALIGNMENT = 64

    # Instance attributes with optional types for from_mmap factory
    path: Path | None
    _file: io.BufferedRandom | None
    _mmap: mmap.mmap | None
    _base_offset: int

    def __init__(self, path: Path | str, size: int):
        """Initialize the payload arena from an existing file.

        Args:
            path: Path to the shared memory file (must exist)
            size: Size of the shared memory region in bytes

        Use PayloadArena.create() to create a new arena file.
        """
        self.path = Path(path).expanduser()
        self.size = size
        self.offset = 0  # Current allocation offset (bump pointer)
        self._base_offset = 0  # No offset for direct file access

        # Open and map the shared memory file
        # Held open for the life of the mapping; released in close()
        self._file = open(self.path, "r+b")  # noqa: SIM115
        self._mmap = mmap.mmap(self._file.fileno(), size)

        # Get the base address for foreign_buffer
        # We need to use ctypes to get the actual memory address
        self._base_address = ctypes.addressof(ctypes.c_char.from_buffer(self._mmap))

    @classmethod
    def create(cls, path: Path | str, size: int) -> PayloadArena:
        """Create a new payload arena file.

        Args:
            path: Path for the shared memory file (will be created/truncated)
            size: Size of the shared memory region in bytes

        Returns:
            PayloadArena instance backed by the new file
        """
        path = Path(path).expanduser()

        # Create the file with the specified size
        with open(path, "wb") as f:
            f.truncate(size)

        return cls(path, size)

    @classmethod
    def from_mmap(
        cls, mmap_obj: mmap.mmap, base_offset: int, size: int
    ) -> PayloadArena:
        """Create a PayloadArena from an existing mmap.

        This is used to create an arena view into a slice of an existing
        shared memory region (e.g., the payload zone of the control shm).

        Args:
            mmap_obj: Existing mmap object
            base_offset: Offset within the mmap where the arena starts
            size: Size of the arena region in bytes

        Returns:
            PayloadArena instance backed by the mmap slice
        """
        arena = object.__new__(cls)
        arena.path = None
        arena.size = size
        arena.offset = 0
        arena._file = None  # No separate file - using existing mmap
        arena._mmap = mmap_obj
        arena._base_offset = base_offset  # Track offset within parent mmap

        # Get the base address for foreign_buffer
        base_address = ctypes.addressof(ctypes.c_char.from_buffer(mmap_obj))
        arena._base_address = base_address + base_offset

        return arena

    def allocate(self, size: int, alignment: int | None = None) -> BufferInfo:
        """Allocate a buffer in the arena.

        Args:
            size: Size of the buffer in bytes
            alignment: Alignment requirement (default: 64 bytes for Arrow)

        Returns:
            BufferInfo with offset, length, and PyArrow buffer

        Raises:
            MemoryError: If arena doesn't have enough space
        """
        if alignment is None:
            alignment = self.ALIGNMENT

        # Align the offset
        aligned_offset = (self.offset + alignment - 1) & ~(alignment - 1)

        if aligned_offset + size > self.size:
            raise MemoryError(
                f"Payload arena full: need {aligned_offset + size} bytes, "
                f"have {self.size} bytes (current offset: {self.offset})"
            )

        # Create an Arrow buffer backed by the shared memory region
        # foreign_buffer creates a zero-copy view into existing memory
        address = self._base_address + aligned_offset
        buffer = pa.foreign_buffer(address, size, base=self._mmap)

        # Update the bump pointer
        self.offset = aligned_offset + size

        return BufferInfo(offset=aligned_offset, length=size, buffer=buffer)

    def allocate_and_copy(self, data: bytes | pa.Buffer) -> BufferInfo:
        """Allocate a buffer and copy data into it.

        Uses a single memcpy/memmove to copy data directly into shared memory,
        avoiding intermediate Python bytes objects for Arrow buffers.

        Args:
            data: Data to copy (bytes or PyArrow Buffer)

        Returns:
            BufferInfo with the data copied into shared memory
        """
        if isinstance(data, pa.Buffer):
            size = data.size
        else:
            size = len(data)

        if size == 0:
            return BufferInfo(offset=0, length=0, buffer=None)

        info = self.allocate(size)

        # Copy data into the shared memory buffer using a single memcpy
        base_offset = getattr(self, "_base_offset", 0)
        dst_addr = self._base_address + info.offset

        if isinstance(data, pa.Buffer):
            # Direct copy from Arrow buffer's native memory - single memcpy
            # Arrow buffers expose their address via .address property
            ctypes.memmove(dst_addr, data.address, size)
        else:
            # For Python bytes, use mmap.write (still efficient)
            self._mmap.seek(base_offset + info.offset)
            self._mmap.write(data)

        return info

    def reset(self):
        """Reset the arena for the next batch.

        This simply resets the bump pointer to the beginning.
        All previously allocated buffers become invalid.
        """
        self.offset = 0

    def bytes_used(self) -> int:
        """Return the number of bytes currently allocated."""
        return self.offset

    def bytes_available(self) -> int:
        """Return the number of bytes available for allocation."""
        return self.size - self.offset

    def close(self):
        """Close the arena and release resources.

        If this arena is a view into an existing mmap (created via from_mmap),
        this only clears the reference without closing the underlying mmap.
        """
        # Only close mmap if we own it (created via __init__ or create)
        if self._file is not None:
            if self._mmap:
                self._mmap.close()
            self._file.close()
            self._file = None
        self._mmap = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False


# Dictionary type needs special handling (not yet supported)
_UNSUPPORTED_TYPES = (pa.DictionaryType,)


class UnsupportedArrowTypeError(TypeError):
    """Raised when an Arrow schema contains unsupported types."""


def _iter_types(
    schema_or_type: pa.Schema | pa.DataType, path: str = ""
) -> Iterator[tuple[str, pa.DataType]]:
    """Yield (path, type) for every type in a schema or type, parents first.

    An extension type is reported as its storage type, which is what its arrays
    are made of and what the schema says on the wire.
    """
    if isinstance(schema_or_type, pa.Schema):
        for field in schema_or_type:
            field_path = field.name if not path else f"{path}.{field.name}"
            yield from _iter_types(field.type, field_path)
        return

    arrow_type = schema_or_type
    while isinstance(arrow_type, pa.BaseExtensionType):
        arrow_type = arrow_type.storage_type
    yield path, arrow_type

    if isinstance(arrow_type, (pa.ListType, pa.LargeListType, pa.FixedSizeListType)):
        yield from _iter_types(arrow_type.value_type, f"{path}[]")
    elif isinstance(arrow_type, pa.MapType):
        yield from _iter_types(arrow_type.key_type, f"{path}.key")
        yield from _iter_types(arrow_type.item_type, f"{path}.value")
    elif isinstance(arrow_type, (pa.StructType, pa.UnionType)):
        for i in range(arrow_type.num_fields):
            field = arrow_type.field(i)
            field_path = f"{path}.{field.name}" if path else field.name
            yield from _iter_types(field.type, field_path)


def _has_known_layout(arrow_type: pa.DataType) -> bool:
    """Whether the buffer transfer knows which buffers an array of this type has.

    Java checks a batch against a fixed number of buffers per type and the
    descriptor has no room for anything else, so types whose buffer count varies
    from array to array (string_view, binary_view) cannot be carried, and nor can
    types whose values live outside the array (dictionary) or that this module
    does not know how to walk (list_view, run_end_encoded).
    """
    types = pa.types
    return bool(
        types.is_null(arrow_type)
        or types.is_primitive(arrow_type)  # booleans, numbers, dates and times
        or types.is_decimal(arrow_type)
        or types.is_fixed_size_binary(arrow_type)
        or types.is_string(arrow_type)
        or types.is_large_string(arrow_type)
        or types.is_binary(arrow_type)
        or types.is_large_binary(arrow_type)
        or types.is_list(arrow_type)
        or types.is_large_list(arrow_type)
        or types.is_fixed_size_list(arrow_type)
        or types.is_struct(arrow_type)
        or types.is_map(arrow_type)
        or types.is_union(arrow_type)
    )


def _validate_supported_schema(schema: pa.Schema) -> None:
    """Validate that the buffer transfer can carry every type in a schema.

    Supported:
    - Primitive types: null, bool, int, float, decimal, string, binary and their
      large and fixed-size variants
    - Date/time types: date32, date64, timestamp, time32, time64, duration, interval
    - Nested types: list, large_list, fixed_size_list, struct, map, union
    - Extension types over any of the above (sent as their storage)

    Not supported:
    - Dictionary (requires separate dictionary/indices handling)
    - string_view, binary_view (a varying number of buffers per array)
    - list_view, large_list_view, run_end_encoded

    Args:
        schema: Arrow schema to validate

    Raises:
        UnsupportedArrowTypeError: If schema contains unsupported types
    """
    unsupported = [
        f"{path}: {arrow_type}"
        for path, arrow_type in _iter_types(schema)
        if not _has_known_layout(arrow_type)
    ]

    if unsupported:
        raise UnsupportedArrowTypeError(
            f"Arrow zero-copy transfer does not support these types: "
            f"{', '.join(unsupported)}. "
            f"Use send_arrow_table() for tables with unsupported types."
        )


def _check_type_recursive(
    schema_or_type: pa.Schema | pa.DataType, unsupported: list[str], path: str = ""
) -> None:
    """Collect the dictionary-encoded types in a schema, which no transfer supports."""
    unsupported.extend(
        f"{type_path}: {arrow_type}"
        for type_path, arrow_type in _iter_types(schema_or_type, path)
        if isinstance(arrow_type, _UNSUPPORTED_TYPES)
    )


# Array.nbytes counts the bytes an array refers to, slice or not, but leaves out the
# final entry of each offsets buffer. One 64-bit offset per buffer covers that.
_OFFSET_SLACK = 8


def _storage(array: pa.Array) -> pa.Array:
    """The array that holds an extension array's buffers and children."""
    while isinstance(array, pa.ExtensionArray):
        array = array.storage
    return array


def _child_arrays(array: pa.Array) -> list[pa.Array]:
    """The arrays that follow this one in Arrow's depth-first order.

    Each child's field node and buffers are emitted as it is returned here, so
    this is also where an offset in a child has to be looked for.
    """
    arr_type = array.type
    if isinstance(
        arr_type, (pa.ListType, pa.LargeListType, pa.FixedSizeListType, pa.MapType)
    ):
        # One child. For a map it is the struct of entries, which in turn has the
        # key and value arrays as its children.
        return [array.values]
    if isinstance(arr_type, (pa.StructType, pa.UnionType)):
        return [array.field(i) for i in range(arr_type.num_fields)]
    return []


def _has_offset(array: pa.Array) -> bool:
    """Whether this array, or any array nested in it, starts part-way into its buffers.

    Java reads every buffer from its first byte, so an array with an offset has to
    be rebuilt before it is copied. A parent's offset says nothing about its
    children: ``StructArray.from_arrays([child.slice(2)])`` has offset 0 itself.
    """
    array = _storage(array)
    return array.offset != 0 or any(_has_offset(c) for c in _child_arrays(array))


def _is_slice(array: pa.Array) -> bool:
    """Whether the array's buffers hold rows that the array itself does not cover.

    True for any slice of a larger array, including one that starts at row 0, such
    as the first batch of ``table.to_batches()``.
    """
    if _has_offset(array):
        return True
    # With no offset anywhere, Java reads the right rows whatever this returns:
    # rows past the end are only copied for nothing. Comparing byte counts can
    # miss a few of those, and that is all it can get wrong.
    buffer_count = sum(buf is not None for buf in array.buffers())
    unused = array.get_total_buffer_size() - array.nbytes
    return bool(unused > _OFFSET_SLACK * buffer_count)


def _get_own_buffers(array: pa.Array) -> list[pa.Buffer | None]:
    """Get only this array's own buffers (not children's buffers), as Java expects them.

    ``array.buffers()`` lists this array's buffers followed by its children's.
    ``DataType.num_buffers`` says how many are its own, and for most types that is
    also what Arrow Java's TypeLayout has:
    - Fixed width (int, float, bool, decimal, ...): [validity, data]
    - String/Binary and their large variants: [validity, offsets, data]
    - List, LargeList, Map: [validity, offsets]
    - FixedSizeList, Struct: [validity]

    Two types differ, because Arrow C++ keeps a slot that the IPC format and Java
    do not have:
    - Null: C++ has one buffer that is always absent; Java has none
    - Union: C++ has an always-absent validity slot first; Java has [type_ids] for
      a sparse union and [type_ids, offsets] for a dense one
    """
    arr_type = array.type
    own: list[pa.Buffer | None] = array.buffers()[: arr_type.num_buffers]
    if pa.types.is_null(arr_type):
        return []
    if pa.types.is_union(arr_type):
        return own[1:]
    return own


def _compact_dense_union(array: pa.UnionArray) -> pa.UnionArray:
    """Rebuild a dense union so that each child holds exactly the values in use.

    Arrow allows a child to hold values that no slot of the union points at, and
    slicing a dense union leaves such values behind. Arrow Java does not: it sets
    each child's length to the number of slots of that child's type
    (DenseUnionVector.setValueCount), which cuts a longer child short and leaves
    offsets pointing past its end. So the children are reduced to the values the
    slots use, in slot order, and the offsets renumbered to match.

    The array must have no offset, which holds for everything that reaches
    _extract_field_nodes_and_buffers_recursive.
    """
    # Only unions need compute functions, and importing them is not free
    import pyarrow.compute as pc

    type_ids = array.type_codes
    offsets = array.offsets
    compact_offsets = offsets
    slots_of_child: list[pa.BooleanArray] = []
    # A plain 1 would widen the result to int64; union offsets are int32
    one = pa.scalar(1, pa.int32())
    for code in array.type.type_codes:
        slots = pc.equal(type_ids, code)
        position = pc.subtract(pc.cumulative_sum(pc.cast(slots, pa.int32())), one)
        compact_offsets = pc.if_else(slots, position, compact_offsets)
        slots_of_child.append(slots)

    if compact_offsets.equals(offsets) and all(
        len(array.field(i)) == slots.true_count
        for i, slots in enumerate(slots_of_child)
    ):
        return array

    children = [
        array.field(i).take(offsets.filter(slots))
        for i, slots in enumerate(slots_of_child)
    ]
    union_type = array.type
    return pa.UnionArray.from_dense(
        type_ids,
        compact_offsets,
        children,
        [union_type.field(i).name for i in range(union_type.num_fields)],
        union_type.type_codes,
    )


def _extract_field_nodes_and_buffers_recursive(
    array: pa.Array,
    field_nodes: list[tuple[int, int]],
    buffers: list[pa.Buffer | None],
) -> None:
    """Extract field nodes and buffers from an array in depth-first pre-order.

    Arrow IPC format requires both field nodes and buffers in depth-first pre-order
    traversal. For nested types, we emit the parent's data first, then recurse into
    children.

    This function ensures that:
    1. Field nodes are in DFS order: parent before children
    2. Buffers are in DFS order: parent's buffers before children's buffers
    3. Buffer counts match what VectorLoader expects

    Args:
        array: The Arrow array to extract from
        field_nodes: List to append (length, null_count) tuples to
        buffers: List to append buffers to
    """
    # The schema describes an extension column by its storage type, so that is
    # the layout Java expects
    array = _storage(array)
    if pa.types.is_union(array.type) and array.type.mode == "dense":
        array = _compact_dense_union(array)

    field_nodes.append((len(array), array.null_count))
    buffers.extend(_get_own_buffers(array))

    for child in _child_arrays(array):
        _extract_field_nodes_and_buffers_recursive(child, field_nodes, buffers)


def copy_arrow_table_to_arena(
    table: pa.Table, arena: PayloadArena
) -> tuple[list[BufferInfo], list[tuple[int, int]]]:
    """Copy an Arrow table's buffers into the payload arena.

    This is the key function for zero-copy transfer. It:
    1. Validates schema contains only supported types
    2. Combines chunked columns into single arrays (required for protocol)
    3. Extracts field nodes and buffers in DFS order (matching Arrow IPC format)
    4. Copies each buffer into the arena
    5. Returns buffer descriptors for the protocol message

    IMPORTANT: Both field nodes and buffers MUST be in depth-first pre-order
    traversal to match Arrow IPC/VectorLoader expectations. For nested types:
    - Field nodes: parent before children
    - Buffers: parent's buffers before children's buffers

    Note: Tables with chunked columns are combined into single chunks before
    copying. This ensures a 1:1 mapping between schema fields and field nodes,
    which simplifies the protocol and reconstruction logic.

    Args:
        table: PyArrow Table to copy
        arena: PayloadArena to allocate into

    Returns:
        Tuple of:
        - List of BufferInfo for each buffer
        - List of (length, null_count) tuples for each field node

    Raises:
        UnsupportedArrowTypeError: If table contains unsupported types (e.g., dictionary)
    """
    # Validate schema before processing
    _validate_supported_schema(table.schema)

    field_nodes: list[tuple[int, int]] = []
    raw_buffers: list[pa.Buffer | None] = []

    # Combine chunks to ensure one chunk per column
    # This is required because our protocol assumes 1:1 field-to-node mapping
    table = table.combine_chunks()

    for column in table.columns:
        # After combine_chunks() a column has one chunk, or none at all if the
        # table was built from no batches; that one still needs its (empty) buffers.
        # pa.nulls can build every type, unions included, which pa.array cannot.
        chunk = column.chunks[0] if column.num_chunks else pa.nulls(0, column.type)

        # Compact sliced arrays. A slice shares its parent's buffers, so they hold
        # rows before the slice (which must not be read as its own) and after it
        # (which would be copied for nothing). concat_arrays builds a new array
        # whose buffers hold only its rows and that has offset 0 at every level
        # of nesting.
        if _is_slice(chunk):
            chunk = pa.concat_arrays([chunk])

        # Extract field nodes AND buffers together in DFS order
        # This ensures correct ordering for nested types (list, struct, map, union)
        _extract_field_nodes_and_buffers_recursive(chunk, field_nodes, raw_buffers)

    # Copy buffers to arena
    # Note: Absent buffers (e.g., no validity bitmap when no nulls) are represented
    # as zero-length buffers (offset=0, length=0). This is required because Arrow's
    # VectorLoader expects the exact buffer count from TypeLayout and doesn't handle
    # null/absent buffers well. See: https://issues.apache.org/jira/browse/ARROW-8803
    buffer_infos: list[BufferInfo] = []
    for buf in raw_buffers:
        if buf is None:
            # Absent buffer - use zero-length placeholder
            info = BufferInfo(offset=0, length=0, buffer=None)
        else:
            info = arena.allocate_and_copy(buf)
        buffer_infos.append(info)

    return buffer_infos, field_nodes


def compute_schema_hash(schema: pa.Schema) -> int:
    """Compute a hash of an Arrow schema for caching.

    The hash is based on the serialized schema bytes.
    """
    schema_bytes = schema.serialize().to_pybytes()

    # Use a simple hash (Python's hash is not stable across runs,
    # so we use a simple FNV-1a style hash)
    h = 0xCBF29CE484222325  # FNV offset basis
    for b in schema_bytes:
        h ^= b
        h = (h * 0x100000001B3) & 0xFFFFFFFFFFFFFFFF  # FNV prime, 64-bit
    return h


def serialize_schema(schema: pa.Schema) -> bytes:
    """Serialize an Arrow schema for transmission."""
    result: bytes = schema.serialize().to_pybytes()
    return result


def deserialize_schema(schema_bytes: bytes) -> pa.Schema:
    """Deserialize an Arrow schema from IPC format bytes."""
    return pa.ipc.read_schema(pa.BufferReader(schema_bytes))


def estimate_arrow_size(table: pa.Table | pa.RecordBatch) -> int:
    """Estimate buffer size needed for an Arrow table (conservative estimate).

    This function estimates the total buffer size required to store a table
    in the payload arena, including alignment padding. The estimate is
    conservative (may overestimate) to ensure allocations don't fail.

    A slice of a larger table, such as a batch from ``table.to_batches()``, is
    sized by its own rows, because only those are copied.

    Args:
        table: PyArrow Table or RecordBatch to estimate size for

    Returns:
        Estimated size in bytes including alignment padding
    """
    total = 0
    for column in table.columns:
        chunks = column.chunks if isinstance(column, pa.ChunkedArray) else [column]
        for chunk in chunks:
            buffer_sizes = [buf.size for buf in chunk.buffers() if buf is not None]
            if _is_slice(chunk):
                size = chunk.nbytes + _OFFSET_SLACK * len(buffer_sizes)
            else:
                size = sum(buffer_sizes)
            # Add maximum alignment padding (64 bytes) per buffer
            total += size + PayloadArena.ALIGNMENT * len(buffer_sizes)
    return total


def _copy_table(table: pa.Table) -> pa.Table:
    """Copy a table into memory that pyarrow owns, keeping its schema exactly.

    Going through Python objects instead would let pyarrow infer the types again:
    int32 comes back as int64, timestamp[s] as timestamp[us], an all-null string
    column as null. The IPC format carries the schema itself and copies each
    buffer once.
    """
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return pa.ipc.open_stream(sink.getvalue()).read_all()


class ScopedArenaContext:
    """Context manager for scoped Arrow transfers with automatic cleanup.

    Provides a convenient way to send and receive Arrow data with automatic
    arena lifecycle management. The arena is reset between sends and cleaned
    up on context exit.

    Example:
        with client.arrow_context() as ctx:
            # Send multiple tables (arena auto-resets between sends)
            ctx.send(table1)
            ctx.send(table2)

            # Receive data back from Java (copies to ensure validity after exit)
            result = ctx.receive()

        # Arena is automatically closed and reset on exit

    Attributes:
        client: The GatunClient instance
        schema_cache: Schema cache shared across all sends in this context
    """

    def __init__(self, client: GatunClient):
        """Initialize the scoped arena context.

        Args:
            client: GatunClient instance to use for transfers
        """
        self._client = client
        self._arena: PayloadArena | None = None
        self._schema_cache: dict[int, bool] = {}
        self._entered = False

    def __enter__(self) -> Self:
        """Enter the context and acquire the arena."""
        if self._entered:
            raise RuntimeError("ScopedArenaContext is not reentrant")
        self._arena = self._client.get_payload_arena()
        self._entered = True
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Exit the context, reset and close the arena."""
        if self._arena:
            try:
                # Reset arena and notify server
                self._arena.reset()
                self._client.reset_payload_arena()
            finally:
                self._arena.close()
                self._arena = None
        self._entered = False

    def send(self, table: pa.Table) -> str:
        """Send an Arrow table to Java, auto-resetting arena first.

        This method resets the arena before each send, allowing the same
        arena to be reused for multiple sequential transfers.

        Args:
            table: PyArrow Table to send

        Returns:
            Response string from Java (e.g., "Received N rows")

        Raises:
            RuntimeError: If called outside of context manager
            PayloadTooLargeError: If table is too large for arena
        """
        if not self._entered or self._arena is None:
            raise RuntimeError("send() must be called within a context manager")

        # Reset arena for this transfer
        self._arena.reset()

        # Send using the client's method
        result: str = self._client.send_arrow_buffers(
            table, self._arena, self._schema_cache
        )
        return result

    def receive(self) -> pa.Table:
        """Receive Arrow data from Java, returning a copy.

        The returned table is a copy of the data, ensuring it remains valid
        after the context exits (when the arena is reset).

        Returns:
            PyArrow Table with data from Java (owned copy)

        Raises:
            RuntimeError: If called outside of context manager
        """
        if not self._entered or self._arena is None:
            raise RuntimeError("receive() must be called within a context manager")

        # The view points into shared memory that the next transfer reuses
        return _copy_table(self._client.get_arrow_data().table)

    @property
    def arena(self) -> PayloadArena | None:
        """Get the underlying arena (for advanced use)."""
        return self._arena

    @property
    def schema_cache(self) -> dict[int, bool]:
        """Get the schema cache (shared across sends)."""
        return self._schema_cache


class AsyncScopedArenaContext:
    """Async context manager for scoped Arrow transfers with automatic cleanup.

    Similar to ScopedArenaContext but for use with AsyncGatunClient.
    Uses send_arrow_table() for async transfers (IPC format).

    Example:
        async with client.arrow_context() as ctx:
            # Send multiple tables
            await ctx.send(table1)
            await ctx.send(table2)

        # Resources automatically cleaned up on exit

    Attributes:
        client: The AsyncGatunClient instance
    """

    def __init__(self, client: AsyncGatunClient):
        """Initialize the async scoped arena context.

        Args:
            client: AsyncGatunClient instance to use for transfers
        """
        self._client = client
        self._entered = False

    async def __aenter__(self) -> Self:
        """Enter the context."""
        if self._entered:
            raise RuntimeError("AsyncScopedArenaContext is not reentrant")
        self._entered = True
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Exit the context."""
        self._entered = False

    async def send(self, table: pa.Table) -> str:
        """Send an Arrow table to Java.

        Uses the async client's send_arrow_table() method which uses
        Arrow IPC format for transfer.

        Args:
            table: PyArrow Table to send

        Returns:
            Response string from Java (e.g., "Received N rows")

        Raises:
            RuntimeError: If called outside of context manager
            PayloadTooLargeError: If table is too large for shared memory
        """
        if not self._entered:
            raise RuntimeError("send() must be called within a context manager")

        result: str = await self._client.send_arrow_table(table)
        return result
