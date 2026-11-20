from __future__ import annotations

import array
import calendar
import contextvars
import datetime
import functools
import inspect
import logging
import math
import mmap
import os
import socket
import struct
import threading
import time
import weakref
from collections import OrderedDict, deque
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, Literal, Self, cast, overload

import flatbuffers
import pyarrow as pa

if TYPE_CHECKING:
    from gatun.arena import PayloadArena, ScopedArenaContext

from gatun.callback_channel import (
    MAX_FRAME_SIZE,
    CallbackChannel,
    CallMemory,
    ChannelCall,
)
from gatun.generated.org.gatun.protocol import Action as Act
from gatun.generated.org.gatun.protocol import (
    Argument,
    ArrayVal,
    ArrowBatchDescriptor,
    BatchCommand,
    BatchResponse,
    BoolVal,
    BufferDescriptor,
    CharVal,
    CreateObjectsRequest,
    DoubleVal,
    ElementType,
    FieldNode,
    GetFieldsRequest,
    IntVal,
    InvokeMethodsRequest,
    ListVal,
    MapEntry,
    MapVal,
    MethodCall,
    ObjectRef,
    ObjectSpec,
    Response,
    StringVal,
    TemporalKind,
    TemporalVal,
    Value,
)
from gatun.generated.org.gatun.protocol import Command as Cmd

logger = logging.getLogger(__name__)


# Object IDs released per FreeObject command. Each ID costs about 40 bytes, so a full
# chunk stays well inside the 64KB command zone.
_FREE_CHUNK_SIZE = 512

_EPOCH_ORDINAL = datetime.date(1970, 1, 1).toordinal()


def _take_free_chunk(pending: deque[int]) -> list[int]:
    """Remove and return the next object IDs to release, at most one command's worth.

    Finalizers append to the queue from any thread while this runs, so it pops
    one at a time instead of copying and clearing.
    """
    ids: list[int] = []
    try:
        while len(ids) < _FREE_CHUNK_SIZE:
            ids.append(pending.popleft())
    except IndexError:
        pass  # queue drained
    return ids


def _build_free_command(
    builder: flatbuffers.Builder, ids: list[int], answers_sent: int = 0
) -> bytes:
    """Build a FreeObject command that releases every ID in one round trip.

    `answers_sent` is for a command that goes on the client's own channel: the
    number of callback answers sent on the callback connection so far. One of
    them may name an object released here, and the server reads that many
    answers before it releases anything. Read the count after taking the IDs
    off the queue, never before.
    """
    builder.Clear()
    arg_offsets = []
    for object_id in ids:
        IntVal.Start(builder)
        IntVal.AddV(builder, object_id)
        val_off = IntVal.End(builder)
        Argument.Start(builder)
        Argument.AddValType(builder, Value.Value.IntVal)
        Argument.AddVal(builder, val_off)
        arg_offsets.append(Argument.End(builder))
    Cmd.CommandStartArgsVector(builder, len(arg_offsets))
    for offset in reversed(arg_offsets):
        builder.PrependUOffsetTRelative(offset)
    args_vec = builder.EndVector()
    Cmd.CommandStart(builder)
    Cmd.CommandAddAction(builder, Act.Action.FreeObject)
    Cmd.CommandAddArgs(builder, args_vec)
    if answers_sent:
        Cmd.CommandAddAnswersSent(builder, answers_sent)
    builder.Finish(Cmd.CommandEnd(builder))
    return bytes(builder.Output())


def _encode_temporal(value: Any) -> tuple[int, int] | None:
    """Encode a datetime/date/time as (TemporalKind, integer) for a TemporalVal.

    Returns None for any other type. The server builds the matching java.sql
    object, so no Java call is needed on this side.
    """
    # datetime is a subclass of date, so it has to be tested first
    if isinstance(value, datetime.datetime):
        if value.tzinfo is not None:
            epoch_secs = calendar.timegm(value.utctimetuple())
        else:
            # Naive datetime: local time
            epoch_secs = int(time.mktime(value.timetuple()))
        micros = epoch_secs * 1_000_000 + value.microsecond
        return TemporalKind.TemporalKind.Timestamp, micros
    if isinstance(value, datetime.date):
        return TemporalKind.TemporalKind.Date, value.toordinal() - _EPOCH_ORDINAL
    if isinstance(value, datetime.time):
        # java.sql.Time has whole-second resolution
        seconds = value.hour * 3600 + value.minute * 60 + value.second
        return TemporalKind.TemporalKind.Time, seconds
    return None


def _create_typed_vector(builder, data: bytes, itemsize: int, num_elements: int):
    """Create a typed FlatBuffers vector from raw bytes.

    This mimics flatbuffers.Builder.CreateNumpyVector but without numpy.
    It creates a properly-typed vector that can be read with typed accessors
    like IntValues(i), DoubleValues(i), etc.

    Args:
        builder: FlatBuffers Builder instance
        data: Raw bytes in little-endian format
        itemsize: Size of each element in bytes (4 for int32, 8 for int64/double, etc.)
        num_elements: Number of elements in the vector

    Returns:
        Vector offset for use with FlatBuffers Add*Values methods
    """
    from flatbuffers.builder import UOffsetTFlags

    builder.StartVector(itemsize, num_elements, itemsize)  # alignment = itemsize

    # Calculate total length
    length = UOffsetTFlags.py_type(itemsize * num_elements)
    builder.head = UOffsetTFlags.py_type(builder.Head() - length)

    # Copy bytes directly into buffer
    builder.Bytes[builder.Head() : builder.Head() + length] = data

    builder.vectorNumElems = num_elements
    return builder.EndVector()


# Protocol version - must match the server version
# Version 2: Per-client shared memory (handshake includes SHM path)
PROTOCOL_VERSION = 4

# Memory zone sizes - must match GatunServer.java constants
COMMAND_ZONE_SIZE = 65536  # 64KB for commands
RESPONSE_ZONE_SIZE = 65536  # 64KB for responses

# Maximum size for callback error messages to prevent exceeding command zone.
# Leave headroom for FlatBuffers overhead and other command fields.
MAX_CALLBACK_ERROR_SIZE = COMMAND_ZONE_SIZE // 16  # 4KB

# Default timeout for callback execution in seconds.
# Prevents hung callbacks from deadlocking the protocol.
# Can be overridden per-client via callback_timeout parameter.
DEFAULT_CALLBACK_TIMEOUT = 30.0

# Default timeout for socket read operations in seconds.
# Used for handshake, response reads, etc. to prevent indefinite hangs.
# None means no timeout (blocking) - only used when explicitly needed.
DEFAULT_SOCKET_TIMEOUT = 30.0

# Handshake timeout - shorter since handshake should be fast
HANDSHAKE_TIMEOUT = 10.0

# FlatBuffer field offsets for ArrayVal - derived from generated code (ArrayVal.py)
# These correspond to vtable slot numbers: (slot_index + 2) * 2
# See: https://flatbuffers.dev/flatbuffers_internals.html
_ARRAYVAL_INT_VALUES_OFFSET = 6  # slot 1: int_values
_ARRAYVAL_LONG_VALUES_OFFSET = 8  # slot 2: long_values
_ARRAYVAL_DOUBLE_VALUES_OFFSET = 10  # slot 3: double_values
_ARRAYVAL_BOOL_VALUES_OFFSET = 12  # slot 4: bool_values
_ARRAYVAL_BYTE_VALUES_OFFSET = 14  # slot 5: byte_values

# FlatBuffer field offset for ArrowBatchDescriptor.schema_bytes
_ARROW_BATCH_SCHEMA_BYTES_OFFSET = 6  # slot 1: schema_bytes


def _unpack_flatbuffer_vector(tab, field_offset, length, fmt_char, elem_size):
    """Unpack a primitive vector directly from FlatBuffer bytes.

    This is a fast alternative to element-by-element access, using direct
    buffer slicing and struct.unpack for O(1) bulk unpacking.

    Args:
        tab: FlatBuffer table object (has Offset, Vector, Bytes)
        field_offset: VTable offset for the field (e.g., 6, 8, 10)
        length: Number of elements in the vector
        fmt_char: struct format character ('i' for int32, 'q' for int64, etc.)
        elem_size: Size of each element in bytes

    Returns:
        List of unpacked values, or empty list if vector is absent
    """
    o = flatbuffers.number_types.UOffsetTFlags.py_type(tab.Offset(field_offset))
    if o == 0 or length == 0:
        return []
    start = tab.Vector(o)
    buf = tab.Bytes[start : start + length * elem_size]
    return list(struct.unpack(f"<{length}{fmt_char}", buf))


class PayloadTooLargeError(Exception):
    """Raised when a payload exceeds the available shared memory space."""

    def __init__(self, payload_size: int, max_size: int, zone: str):
        self.payload_size = payload_size
        self.max_size = max_size
        self.zone = zone
        # Only the payload zone grows with the memory setting; commands and
        # responses have 64KB each whatever it is
        if zone in ("Command", "Response"):
            advice = (
                f"{zone}s are limited to this size whatever the memory setting. "
                "Split the call, or move bulk data as Arrow."
            )
        else:
            advice = (
                "Consider increasing memory size when connecting "
                "(e.g., gatun.connect(memory='64MB'))"
            )
        super().__init__(
            f"{zone} payload too large: {payload_size:,} bytes exceeds "
            f"maximum {max_size:,} bytes. {advice}"
        )


# --- Java Exception Hierarchy ---
# These exceptions mirror common Java exceptions for better error handling


class JavaException(Exception):
    """Base class for all Java exceptions.

    Attributes:
        java_class: The fully qualified Java exception class name
        message: The exception message
        stack_trace: The full Java stack trace as a string
    """

    def __init__(self, java_class: str, message: str, stack_trace: str):
        self.java_class = java_class
        self.message = message
        self.stack_trace = stack_trace
        super().__init__(stack_trace)


class JavaSecurityException(JavaException):
    """Raised when a security violation occurs (e.g., accessing blocked class)."""


class JavaIllegalArgumentException(JavaException):
    """Raised when an illegal argument is passed to a Java method."""


class JavaNoSuchMethodException(JavaException):
    """Raised when a method cannot be found."""


class JavaNoSuchFieldException(JavaException):
    """Raised when a field cannot be found."""


class JavaClassNotFoundException(JavaException):
    """Raised when a class cannot be found."""


class JavaNullPointerException(JavaException):
    """Raised when null is dereferenced in Java."""


class JavaIndexOutOfBoundsException(JavaException):
    """Raised when an index is out of bounds."""


class JavaNumberFormatException(JavaException):
    """Raised when a string cannot be parsed as a number."""


class JavaRuntimeException(JavaException):
    """Raised for generic Java runtime exceptions."""


class CancelledException(Exception):
    """Raised when a request is cancelled.

    `request_id` is set only when the server named a request; a request stopped
    by `client.cancel()` is not numbered.
    """

    def __init__(self, request_id: int | None = None):
        self.request_id = request_id
        if request_id is None:
            super().__init__("The request was cancelled")
        else:
            super().__init__(f"Request {request_id} was cancelled")


class ReentrancyError(Exception):
    """Raised when a nested Java call is attempted from within a callback.

    Gatun does not support reentrant calls - you cannot call Java from Python
    while inside a callback that Java invoked. This would deadlock because:
    1. The original request is waiting for a response
    2. The callback tries to send a new request on the same socket
    3. Both sides would wait forever for the other

    To work around this:
    - Queue work for later instead of calling Java immediately
    - Use the async client with proper concurrency patterns
    - Restructure code to avoid nested calls
    """


class CallbackTimeoutError(Exception):
    """Raised when a callback execution exceeds the timeout.

    This prevents hung callbacks from deadlocking the protocol. When a callback
    times out, an error response is sent to Java so the original request can
    complete (with an error) rather than hanging indefinitely.
    """


class ProtocolDesyncError(Exception):
    """Raised when the client and server are out of sync.

    This typically indicates:
    - Corrupted data on the socket
    - Server crashed mid-response
    - Response size exceeds valid bounds

    The connection should be closed and re-established.
    """

    def __init__(self, message: str, response_size: int | None = None):
        self.response_size = response_size
        super().__init__(message)


class DeadConnectionError(Exception):
    """Raised when attempting to use a dead connection.

    After a ProtocolDesyncError or SocketTimeoutError, the client is marked
    as dead and cannot be used. Create a new client to continue.
    """


class TypeHint:
    """Wrapper to provide an explicit Java type hint for overload resolution.

    Use this when Python's automatic type inference doesn't select the correct
    Java method overload. This is common with Spark/Scala APIs that have many
    overloaded methods.

    Examples:
        # Force long instead of int
        client.invoke_method(obj, "method", TypeHint(42, "long"))

        # Use interface type instead of implementation
        client.invoke_method(obj, "method", TypeHint(my_list, "java.util.List"))

        # Scala collection types
        client.invoke_method(obj, "method", TypeHint(my_list, "scala.collection.Seq"))
    """

    __slots__ = ("java_type", "value")

    def __init__(self, value, java_type: str):
        """Create a type-hinted value.

        Args:
            value: The Python value to pass
            java_type: The Java type to use for overload resolution.
                       Can be a primitive ("int", "long", "double"), a boxed type
                       ("Integer", "Long"), a full class name ("java.util.List"),
                       or a Scala type ("scala.collection.Seq").
        """
        self.value = value
        self.java_type = java_type

    def __repr__(self):
        return f"TypeHint({self.value!r}, {self.java_type!r})"


# Mapping from Java exception class names to Python exception classes
_JAVA_EXCEPTION_MAP: dict[str, type[Exception]] = {
    "java.lang.SecurityException": JavaSecurityException,
    "java.lang.IllegalArgumentException": JavaIllegalArgumentException,
    "java.lang.NoSuchMethodException": JavaNoSuchMethodException,
    "java.lang.NoSuchFieldException": JavaNoSuchFieldException,
    "java.lang.ClassNotFoundException": JavaClassNotFoundException,
    "java.lang.NullPointerException": JavaNullPointerException,
    "java.lang.IndexOutOfBoundsException": JavaIndexOutOfBoundsException,
    "java.lang.ArrayIndexOutOfBoundsException": JavaIndexOutOfBoundsException,
    "java.lang.StringIndexOutOfBoundsException": JavaIndexOutOfBoundsException,
    "java.lang.NumberFormatException": JavaNumberFormatException,
    "java.lang.RuntimeException": JavaRuntimeException,
    "org.gatun.PayloadTooLargeException": PayloadTooLargeError,
    "java.lang.InterruptedException": CancelledException,
}


def _raise_java_exception_impl(error_type: str, error_msg: str) -> None:
    """Raise the appropriate Python exception for a Java error.

    This is the core implementation used by both sync and async clients.

    Args:
        error_type: The fully qualified Java exception class name
        error_msg: The full error message including stack trace
    """
    import re

    # Handle PayloadTooLargeException specially - parse sizes from message
    if error_type == "org.gatun.PayloadTooLargeException":
        # Message format: "Response too large: X bytes exceeds Y byte limit"
        match = re.search(r"(\d+) bytes exceeds (\d+) byte", error_msg)
        if match:
            payload_size = int(match.group(1))
            max_size = int(match.group(2))
        else:
            payload_size = 0
            max_size = 0
        raise PayloadTooLargeError(payload_size, max_size, "Response")

    # Handle InterruptedException specially - extract request ID
    if error_type == "java.lang.InterruptedException":
        # Message format: "Request X was cancelled"
        match = re.search(r"Request (\d+) was cancelled", error_msg)
        raise CancelledException(int(match.group(1)) if match else None)

    # Get the exception class, defaulting to JavaException
    exc_class = _JAVA_EXCEPTION_MAP.get(error_type, JavaException)

    # Extract just the message (first line before stack trace)
    message = error_msg.split("\n")[0] if error_msg else ""
    if ": " in message:
        message = message.split(": ", 1)[1]

    # Create and raise the exception
    raise exc_class(error_type, message, error_msg)


class SocketTimeoutError(Exception):
    """Raised when a socket read times out."""


def _usable_timeout(timeout: float | None) -> float | None:
    """A timeout as the socket and thread calls want it; None means no limit.

    Zero, a negative number and infinity all mean "wait as long as it takes": a
    zero deadline fails before reading anything, and the calls that take a
    timeout reject infinity.
    """
    if timeout is None or timeout <= 0 or math.isinf(timeout):
        return None
    return timeout


def _recv_exactly(sock, n, timeout: float | None = None):
    """Receive exactly n bytes from socket, handling partial reads.

    Args:
        sock: The socket to read from
        n: Number of bytes to receive
        timeout: Optional timeout in seconds. If None, uses the socket's
                 current timeout setting without modification.

    Raises:
        SocketTimeoutError: If the timeout expires before receiving all bytes
        RuntimeError: If the socket closes unexpectedly
    """
    timeout = _usable_timeout(timeout)

    # Fast path: no timeout specified, use socket's existing timeout
    # This avoids gettimeout/settimeout overhead in the common case
    if timeout is None:
        data = bytearray()
        while len(data) < n:
            try:
                chunk = sock.recv(n - len(data))
                if not chunk:
                    raise RuntimeError("Socket closed unexpectedly")
                data.extend(chunk)
            except TimeoutError:
                raise SocketTimeoutError(
                    f"Timeout after receiving {len(data)}/{n} bytes"
                )
        return bytes(data)

    # Slow path: caller specified explicit timeout
    # Use deadline-based approach to handle partial reads correctly
    deadline = time.monotonic() + timeout
    original_timeout = sock.gettimeout()

    try:
        data = bytearray()
        while len(data) < n:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise SocketTimeoutError(
                    f"Timeout after receiving {len(data)}/{n} bytes"
                )
            # Set timeout for this recv to remaining time
            sock.settimeout(remaining)
            try:
                chunk = sock.recv(n - len(data))
                if not chunk:
                    raise RuntimeError("Socket closed unexpectedly")
                data.extend(chunk)
            except TimeoutError:
                raise SocketTimeoutError(
                    f"Timeout after receiving {len(data)}/{n} bytes"
                )
        return bytes(data)
    finally:
        # Restore original timeout
        sock.settimeout(original_timeout)


class StaleArenaError(RuntimeError):
    """Raised when accessing Arrow data from a stale arena epoch.

    This error occurs when trying to use a table returned from get_arrow_data()
    after reset_payload_arena() has been called or the client has been closed.
    The underlying shared memory buffers may have been overwritten or unmapped.
    """

    def __init__(self, table_epoch: int, current_epoch: int, reason: str | None = None):
        if reason is not None:
            msg = f"Arrow table from epoch {table_epoch} is invalid: {reason}"
        elif current_epoch == -1:
            # Client was closed - shm is unmapped
            msg = (
                f"Arrow table from epoch {table_epoch} is invalid: client was closed. "
                f"The shared memory buffer has been unmapped."
            )
        else:
            msg = (
                f"Arrow table from epoch {table_epoch} is stale (current epoch: {current_epoch}). "
                f"Tables become invalid after reset_payload_arena() is called."
            )
        super().__init__(msg)
        self.table_epoch = table_epoch
        self.current_epoch = current_epoch


class ArrowTableView:
    """A wrapper around pa.Table that validates arena epoch on access.

    Tables returned from get_arrow_data() are backed by shared memory that
    can be overwritten after reset_payload_arena(). This wrapper tracks the
    epoch when the table was created and raises StaleArenaError if accessed
    after the arena has been reset.

    For most use cases, immediately copy the data you need from the table
    or call table.to_pandas() / table.to_pydict() before resetting the arena.

    A view from arrow_from_java(copy=False) can lie anywhere in the payload
    zone, so it ends with the next Arrow transfer of any kind (any_write).
    One from get_arrow_data() lies in the second half of the zone and outlives
    transfers that stay in the first.

    The methods that copy the data out (to_pandas, to_pydict, to_pylist) hold
    the client's channel while they do, so a transfer on another thread cannot
    change the bytes halfway through. The table and its columns, once handed
    out, are read without that protection.
    """

    def __init__(
        self,
        table: pa.Table,
        epoch: int,
        client: GatunClient | CallMemory,
        *,
        any_write: bool = False,
    ):
        self._table = table
        self._epoch = epoch
        self._client = client
        self._any_write = any_write
        self._written = self._writes_seen()

    def _writes_seen(self) -> int:
        """The client's count of the writes that end this kind of view."""
        if self._any_write:
            return self._client._payload_writes
        return self._client._payload_generation

    def _check_epoch(self):
        """Raise StaleArenaError if epoch changed or client closed."""
        # Check if client's shm was closed - buffer pointers would be invalid
        if self._client.shm is None:
            raise StaleArenaError(
                self._epoch,
                -1,  # Sentinel indicating client closed
            )
        current = self._client._arena_epoch
        if self._epoch != current:
            raise StaleArenaError(self._epoch, current)
        if self._written != self._writes_seen():
            raise StaleArenaError(
                self._epoch,
                current,
                reason="a later Arrow transfer reused the shared memory it "
                "pointed at. Copy the data out first.",
            )

    @property
    def table(self) -> pa.Table:
        """Get the underlying table, validating epoch first."""
        self._check_epoch()
        return self._table

    def to_pandas(self, **kwargs):
        """Convert to pandas DataFrame (copies data, safe after reset)."""
        with self._client._exchange_lock:
            self._check_epoch()
            return self._table.to_pandas(**kwargs)

    def to_pydict(self):
        """Convert to Python dict (copies data, safe after reset)."""
        with self._client._exchange_lock:
            self._check_epoch()
            return self._table.to_pydict()

    def to_pylist(self):
        """Convert to list of dicts (copies data, safe after reset)."""
        with self._client._exchange_lock:
            self._check_epoch()
            return self._table.to_pylist()

    @property
    def num_rows(self) -> int:
        """Get row count (safe, doesn't access buffer data)."""
        return cast(int, self._table.num_rows)

    @property
    def num_columns(self) -> int:
        """Get column count (safe, doesn't access buffer data)."""
        return cast(int, self._table.num_columns)

    @property
    def schema(self) -> pa.Schema:
        """Get schema (safe, doesn't access buffer data)."""
        return self._table.schema

    @property
    def column_names(self) -> list[str]:
        """Get column names (safe, doesn't access buffer data)."""
        return cast(list[str], self._table.column_names)

    def column(self, name: str):
        """Get a column by name."""
        self._check_epoch()
        return self._table.column(name)

    def __repr__(self):
        try:
            self._check_epoch()
            epoch_status = "valid"
        except StaleArenaError:
            epoch_status = "STALE"
        return f"<ArrowTableView rows={self.num_rows} cols={self.num_columns} epoch={self._epoch} ({epoch_status})>"


class JavaList(list):
    """A Python list that also exposes Java List-like methods.

    This allows Gatun to auto-convert Java Lists to Python lists while
    still supporting code that expects Java method names like size(), isEmpty(), etc.
    """

    def size(self):
        """Java List.size() - returns the number of elements."""
        return len(self)

    def isEmpty(self):
        """Java List.isEmpty() - returns true if list is empty."""
        return len(self) == 0

    def get(self, index):
        """Java List.get(index) - returns element at index."""
        return self[index]

    def contains(self, item):
        """Java List.contains(item) - returns true if item is in list."""
        return item in self


class JavaArray(list):
    """A Python list that represents a Java array.

    When passed back to Java, this is serialized as an ArrayVal (Java array)
    rather than a ListVal (Java ArrayList). This preserves the array semantics
    when round-tripping through Gatun.

    The element_type attribute stores the original Java array element type.
    """

    ELEMENT_TYPES = frozenset(
        {
            "Int",
            "Long",
            "Double",
            "Float",
            "Bool",
            "Byte",
            "Short",
            "Char",
            "String",
            "Object",
        }
    )

    def __init__(self, iterable=(), element_type: str = "Object"):
        # A misspelt type ("int") used to be sent as Object[] without a word
        if element_type not in self.ELEMENT_TYPES:
            raise ValueError(
                f"Unknown JavaArray element_type {element_type!r}; "
                f"expected one of {sorted(self.ELEMENT_TYPES)}"
            )
        super().__init__(iterable)
        self.element_type = element_type

    def size(self):
        """Java-style length access."""
        return len(self)

    @property
    def length(self):
        """Java array length property."""
        return len(self)


def _get_own_buffer_count(arrow_type: pa.DataType) -> int:
    """Get the number of buffers owned directly by this Arrow type (not children).

    Arrow types use different numbers of buffers for their own data:
    - Fixed-width primitives (int, float, bool): 2 (validity + data)
    - Variable-width (string, binary): 3 (validity + offsets + data)
    - Null type: 0 (no buffers)
    - List types: 2 (validity + offsets) - child buffers are separate
    - Struct types: 1 (validity only) - child buffers are separate
    - Map types: 2 (validity + offsets) - child buffers are separate

    This is needed to correctly partition buffers when reconstructing arrays.
    """
    if pa.types.is_null(arrow_type):
        return 0  # Null arrays have no buffers
    elif pa.types.is_boolean(arrow_type):
        return 2  # validity + packed bits
    elif pa.types.is_primitive(arrow_type):
        return 2  # validity + data
    elif (
        pa.types.is_string(arrow_type)
        or pa.types.is_large_string(arrow_type)
        or pa.types.is_binary(arrow_type)
        or pa.types.is_large_binary(arrow_type)
    ):
        return 3  # validity + offsets + data
    elif pa.types.is_fixed_size_binary(arrow_type):
        return 2  # validity + data
    elif pa.types.is_list(arrow_type) or pa.types.is_large_list(arrow_type):
        return 2  # validity + offsets (child buffers handled via recursion)
    elif pa.types.is_fixed_size_list(arrow_type) or pa.types.is_struct(arrow_type):
        return 1  # validity only (child buffers handled via recursion)
    elif pa.types.is_map(arrow_type):
        return 2  # validity + offsets (child buffers handled via recursion)
    elif pa.types.is_union(arrow_type):
        if arrow_type.mode == "sparse":
            return 1  # type_ids only
        else:
            return 2  # type_ids + offsets
    elif pa.types.is_dictionary(arrow_type):
        return 2  # validity + indices (dictionary handled separately)
    else:
        # Default assumption for unknown types
        return 2


_EMPTY_BUFFER = pa.py_buffer(b"")


def _reconstruct_array_recursive(
    arrow_type: pa.DataType,
    buffers: list,
    field_nodes: list[tuple[int, int]],
    buffer_idx: int,
    node_idx: int,
) -> tuple[pa.Array, int, int]:
    """Recursively reconstruct an Arrow array from buffers and field nodes.

    This handles nested types by recursively building child arrays first,
    then constructing the parent array with the children.

    Args:
        arrow_type: The Arrow data type to reconstruct
        buffers: List of all PyArrow buffers (flattened)
        field_nodes: List of (length, null_count) tuples
        buffer_idx: Current position in buffers list
        node_idx: Current position in field_nodes list

    Returns:
        Tuple of (reconstructed array, next buffer index, next node index)
    """
    if isinstance(arrow_type, pa.BaseExtensionType):
        # The buffers are laid out as the storage type; the extension type only wraps them
        storage, buffer_idx, node_idx = _reconstruct_array_recursive(
            arrow_type.storage_type, buffers, field_nodes, buffer_idx, node_idx
        )
        return (
            pa.ExtensionArray.from_storage(arrow_type, storage),
            buffer_idx,
            node_idx,
        )

    # Get this array's field node
    length, null_count = field_nodes[node_idx]
    node_idx += 1

    # Get this type's own buffers
    own_buffer_count = _get_own_buffer_count(arrow_type)
    own_buffers = buffers[buffer_idx : buffer_idx + own_buffer_count]
    buffer_idx += own_buffer_count

    # A zero-length buffer arrives as None. That is right for a validity bitmap,
    # which may be absent, but pyarrow requires every other buffer to exist: a
    # string column with no characters in it (no rows, or only "" and nulls) has
    # an empty data buffer, not a missing one. A union's buffers have no validity
    # bitmap among them.
    first_data = 0 if pa.types.is_union(arrow_type) else 1
    own_buffers = [
        _EMPTY_BUFFER if buf is None and i >= first_data else buf
        for i, buf in enumerate(own_buffers)
    ]

    # Handle nested types by recursively building children
    if pa.types.is_list(arrow_type) or pa.types.is_large_list(arrow_type):
        # List has one child: the values array
        value_type = arrow_type.value_type
        child_array, buffer_idx, node_idx = _reconstruct_array_recursive(
            value_type, buffers, field_nodes, buffer_idx, node_idx
        )
        arr = pa.Array.from_buffers(
            arrow_type, length, own_buffers, null_count, children=[child_array]
        )

    elif pa.types.is_fixed_size_list(arrow_type):
        # Fixed-size list has one child: the values array
        value_type = arrow_type.value_type
        child_array, buffer_idx, node_idx = _reconstruct_array_recursive(
            value_type, buffers, field_nodes, buffer_idx, node_idx
        )
        arr = pa.Array.from_buffers(
            arrow_type, length, own_buffers, null_count, children=[child_array]
        )

    elif pa.types.is_struct(arrow_type):
        # Struct has multiple children: one per field
        children = []
        for i in range(arrow_type.num_fields):
            field = arrow_type.field(i)
            child_array, buffer_idx, node_idx = _reconstruct_array_recursive(
                field.type, buffers, field_nodes, buffer_idx, node_idx
            )
            children.append(child_array)
        arr = pa.Array.from_buffers(
            arrow_type, length, own_buffers, null_count, children=children
        )

    elif pa.types.is_map(arrow_type):
        # Map is internally list<struct<key, value>>
        # We need to reconstruct the struct child which contains key and value
        # The struct type is struct<key: key_type, value: item_type>
        struct_type = pa.struct(
            [
                pa.field("key", arrow_type.key_type, nullable=False),
                pa.field("value", arrow_type.item_type),
            ]
        )
        struct_array, buffer_idx, node_idx = _reconstruct_array_recursive(
            struct_type, buffers, field_nodes, buffer_idx, node_idx
        )
        arr = pa.Array.from_buffers(
            arrow_type, length, own_buffers, null_count, children=[struct_array]
        )

    elif pa.types.is_union(arrow_type):
        # Union has multiple children: one per type code
        children = []
        for i in range(arrow_type.num_fields):
            field = arrow_type.field(i)
            child_array, buffer_idx, node_idx = _reconstruct_array_recursive(
                field.type, buffers, field_nodes, buffer_idx, node_idx
            )
            children.append(child_array)
        # pyarrow lists a union's buffers with a leading validity slot that is
        # always empty; Arrow's layout, which Java sends, does not have it
        arr = pa.Array.from_buffers(
            arrow_type, length, [None, *own_buffers], null_count, children=children
        )

    elif pa.types.is_null(arrow_type):
        # Java sends no buffers for a null column; there is nothing to wrap
        arr = pa.nulls(length)

    else:
        # Primitive type (no children)
        arr = pa.Array.from_buffers(arrow_type, length, own_buffers, null_count)

    return arr, buffer_idx, node_idx


class JavaObject:
    def __init__(self, client, object_id):
        self.client = client
        self.object_id = object_id
        # When this object is GC'd, queue its ID to be freed on the Java side
        self._finalizer = weakref.finalize(self, client._queue_free, object_id)

    def detach(self):
        """Prevents automatic freeing on GC (useful for manual testing)."""
        self._finalizer.detach()

    def __getattr__(self, name):
        """obj.method(args) -> client.invoke_method(id, method, args)"""

        def method_proxy(*args):
            return self.client.invoke_method(self.object_id, name, *args)

        return method_proxy

    def __str__(self):
        try:
            return self.client.invoke_method(self.object_id, "toString")
        except Exception:  # noqa: BLE001 - __str__ must not raise, whatever the transport does
            return f"<Dead JavaObject id={self.object_id}>"

    def __eq__(self, other):
        """Compare with another JavaObject using Java's equals() method."""
        if not isinstance(other, JavaObject):
            return False
        # If same object_id, they're definitely equal
        if self.object_id == other.object_id:
            return True
        # Otherwise, call Java's equals() method
        try:
            return self.client.invoke_method(self.object_id, "equals", other)
        except Exception:  # noqa: BLE001 - __eq__ must not raise; unreachable means unequal
            return False

    def __hash__(self):
        """Return Java's hashCode() for this object."""
        try:
            return self.client.invoke_method(self.object_id, "hashCode")
        except Exception:  # noqa: BLE001 - __hash__ must not raise; fall back to the id
            # Fallback to object_id if we can't get hashCode
            return hash(self.object_id)

    def __len__(self):
        """Return length of Java array or collection.

        For arrays, uses Array.getLength().
        For collections, calls size().
        Raises TypeError if object doesn't support length.
        """
        # Try array length first
        try:
            return self.client.invoke_static_method(
                "java.lang.reflect.Array", "getLength", self
            )
        except JavaException:
            pass

        # Try collection size()
        try:
            return self.client.invoke_method(self.object_id, "size")
        except JavaException:
            pass

        # Object doesn't support length
        raise TypeError("object has no len()")

    def __bool__(self):
        """Return True if this is a valid Java object reference.

        This prevents bool() from calling __len__() on objects that
        don't support length (like JavaSparkContext).
        """
        # A JavaObject with a valid object_id is truthy
        return self.object_id is not None

    def __iter__(self):
        """Iterate over Java array or collection.

        For arrays, uses Array.get() with index.
        For Iterable objects, calls iterator().
        """
        try:
            # Try array iteration first
            length = self.client.invoke_static_method(
                "java.lang.reflect.Array", "getLength", self
            )
            for i in range(length):
                yield self.client.invoke_static_method(
                    "java.lang.reflect.Array", "get", self, i
                )
        except JavaException:
            # Fall back to Iterable.iterator()
            iterator = self.client.invoke_method(self.object_id, "iterator")
            while self.client.invoke_method(iterator.object_id, "hasNext"):
                yield self.client.invoke_method(iterator.object_id, "next")

    def __getitem__(self, index):
        """Get element at index from Java array or list.

        For arrays, uses Array.get().
        For lists, calls get(index).
        """
        try:
            # Try array access first
            return self.client.invoke_static_method(
                "java.lang.reflect.Array", "get", self, index
            )
        except JavaException:
            # Fall back to list.get(index)
            return self.client.invoke_method(self.object_id, "get", index)

    def __setitem__(self, index, value):
        """Set element at index in Java array or list.

        For arrays, uses Array.set().
        For lists, calls set(index, value).
        """
        try:
            # Try array access first
            self.client.invoke_static_method(
                "java.lang.reflect.Array", "set", self, index, value
            )
        except JavaException:
            # Fall back to list.set(index, value)
            self.client.invoke_method(self.object_id, "set", index, value)


def java_import(jvm_view: JVMView, import_path: str) -> None:
    """Import Java classes into the JVM view's namespace.

    This is a convenience function similar to Py4J's java_import.
    It allows accessing classes without their full package path.

    Args:
        jvm_view: A JVMView instance (typically client.jvm)
        import_path: Package path with optional wildcard.
                    Examples: "java.util.*", "java.util.ArrayList"

    Example:
        from gatun import java_import

        java_import(client.jvm, "java.util.*")
        # Now you can access:
        arr = client.jvm.ArrayList()  # instead of client.jvm.java.util.ArrayList()

        java_import(client.jvm, "java.lang.Math")
        result = client.jvm.Math.max(1, 2)

    Note:
        Unlike Py4J, this doesn't actually import into a Python namespace.
        It registers shortcuts on the JVM view so that class names can be
        accessed directly without the full package path.
    """
    if not hasattr(jvm_view, "_imports"):
        jvm_view._imports = {}

    if import_path.endswith(".*"):
        # Wildcard import - store the package prefix
        package = import_path[:-2]  # Remove ".*"
        jvm_view._imports[package] = True
    else:
        # Single class import
        # Extract class name from full path
        last_dot = import_path.rfind(".")
        if last_dot != -1:
            class_name = import_path[last_dot + 1 :]
            jvm_view._imports[class_name] = import_path


class JavaClass:
    """Proxy for a Java class. Supports instantiation and static method calls."""

    def __init__(self, client, class_name):
        self._client = client
        self._class_name = class_name

    def __call__(self, *args):
        """Instantiate the class: ArrayList() -> client.create_object('java.util.ArrayList')"""
        return self._client.create_object(self._class_name, *args)

    def __getattr__(self, name):
        """Access static methods: Integer.parseInt(...) -> invoke_static_method(...)"""

        def static_method_proxy(*args):
            return self._client.invoke_static_method(self._class_name, name, *args)

        return static_method_proxy

    def __repr__(self):
        return f"<JavaClass {self._class_name}>"


class JVMView:
    """Navigate Java packages using attribute access.

    Example:
        jvm = client.jvm
        ArrayList = jvm.java.util.ArrayList
        my_list = ArrayList()  # creates instance
        result = jvm.java.lang.Integer.parseInt("123")  # static method

    With java_import:
        from gatun import java_import
        java_import(jvm, "java.util.*")
        my_list = jvm.ArrayList()  # shortcut access
    """

    def __init__(self, client, package_path=""):
        self._client = client
        self._package_path = package_path
        self._imports: dict[str, str | bool] = {}

    def __getattr__(self, name):
        """Navigate deeper into package hierarchy or return a JavaClass."""
        # Check for imported classes first (only at root level)
        if not self._package_path and hasattr(self, "_imports"):
            # Check for direct class import (e.g., java_import(jvm, "java.util.ArrayList"))
            if name in self._imports and isinstance(self._imports[name], str):
                return _JVMNode(self._client, self._imports[name])

            # Check for wildcard imports (e.g., java_import(jvm, "java.util.*"))
            # Only apply wildcards for uppercase names (class names per Java convention)
            # We need to check all wildcard packages and return the first one that
            # actually contains the class, since multiple packages may be imported.
            if name and name[0].isupper():
                for package, is_wildcard in self._imports.items():
                    if is_wildcard is True:
                        # Try the package.name path - verify it exists first
                        full_path = f"{package}.{name}"
                        # Use reflection to check if this class exists
                        cached_type = self._client._reflect_cached(full_path)
                        if cached_type in ("class", "method", "field"):
                            return _JVMNode(self._client, full_path)

        if self._package_path:
            new_path = f"{self._package_path}.{name}"
        else:
            new_path = name

        # Return a new JVMView that can be either a package or a class
        # We use a hybrid object that acts as both
        return _JVMNode(self._client, new_path)

    def __repr__(self):
        if self._package_path:
            return f"<JVMView {self._package_path}>"
        return "<JVMView>"


# Maximum size for LRU caches to prevent unbounded memory growth
_CACHE_MAX_SIZE = 10000


class _LRUCache[V]:
    """Simple LRU cache with bounded size.

    Uses OrderedDict to maintain insertion order. When the cache is full,
    the oldest entry is evicted. Access (get) moves the entry to the end.
    """

    def __init__(self, maxsize: int = _CACHE_MAX_SIZE):
        self._cache: OrderedDict[str, V] = OrderedDict()
        self._maxsize = maxsize

    def get(self, key: str) -> V | None:
        """Get a value from the cache, moving it to the end (most recent)."""
        # Threads sharing a client share this cache. Each step is atomic, but
        # another thread may evict the key between two of them.
        try:
            self._cache.move_to_end(key)
            return self._cache[key]
        except KeyError:
            return None

    def set(self, key: str, value: V) -> None:
        """Set a value in the cache, evicting oldest if full."""
        try:
            self._cache.move_to_end(key)
        except KeyError:
            if len(self._cache) >= self._maxsize:
                try:
                    self._cache.popitem(last=False)  # Remove oldest
                except KeyError:
                    pass  # another thread emptied it
        self._cache[key] = value

    def __contains__(self, key: str) -> bool:
        return key in self._cache

    def __getitem__(self, key: str) -> V:
        """Get a value from the cache (without moving it to the end)."""
        return self._cache[key]

    def pop(self, key: str, default: V | None = None) -> V | None:
        """Remove and return a value from the cache."""
        return self._cache.pop(key, default)

    def __len__(self) -> int:
        return len(self._cache)


class _JVMNode:
    """Hybrid node that can act as both a package path and a Java class.

    - Accessing attributes navigates deeper (jvm.java.util -> jvm.java.util.ArrayList)
    - Calling it instantiates a class (ArrayList() -> new ArrayList())
    - Accessing a method and calling it invokes static method (Integer.parseInt(...))

    This implementation uses Java reflection to correctly identify:
    - Regular Java classes (e.g., java.util.ArrayList)
    - Scala objects (e.g., org.apache.spark.sql.functions)
    - Static methods on classes
    - Static fields on classes

    Results are cached per client: what a name means depends on the classpath of
    the server that client is connected to.
    """

    def __init__(self, client, path):
        self._client = client
        self._path = path

    def _get_type(self) -> str:
        """Get the type of this path via reflection (cached)."""
        return cast(str, self._client._reflect_cached(self._path))

    def __getattr__(self, name):
        """Navigate deeper or access static method/field."""
        if name.startswith("_"):
            # Python probing for a protocol (copy, pickle, IPython's _repr_html_),
            # not a Java name. Each would otherwise be a question to the server.
            raise AttributeError(name)
        new_path = f"{self._path}.{name}"

        # A static field's value is wanted now; anything else is navigated to and
        # resolved when it is called. Only a member of a class can be a field, so
        # the server is asked only when the parent is spelled like a class; the
        # spelling of the name itself says nothing (Encoders.STRING is a method,
        # System.out a field).
        last_segment = self._path.rsplit(".", 1)[-1]
        if (
            last_segment[:1].isupper()
            and self._client._reflect_cached(new_path) == "field"
        ):
            return self._client.get_static_field(self._path, name)

        return _JVMNode(self._client, new_path)

    def __call__(self, *args):
        """Instantiate as a class or invoke as a static method."""
        # Use reflection to determine if this is a class or method
        node_type = self._get_type()

        if node_type == "class":
            # This is a constructor call: java.util.ArrayList()
            return self._client.create_object(self._path, *args)
        elif node_type == "method":
            # This is a static method call: java.lang.Integer.parseInt("42")
            # or Scala object method: functions.col("name")
            last_dot = self._path.rfind(".")
            if last_dot == -1:
                raise ValueError(f"Invalid method path: {self._path}")
            class_name = self._path[:last_dot]
            method_name = self._path[last_dot + 1 :]
            return self._client.invoke_static_method(class_name, method_name, *args)
        elif node_type == "field":
            # This is a static field access (being called as function - error)
            raise TypeError(f"{self._path} is a static field, not a method")
        else:
            # Unknown type - use heuristics to decide whether this is a class or method
            # If the path has a dot and the parent segment looks like a class name
            # (starts with uppercase), treat the last segment as a method call.
            # Example: org.apache.spark.api.python.PythonSQLUtils.explainString
            #          -> PythonSQLUtils is the class, explainString is the method
            last_dot = self._path.rfind(".")
            if last_dot != -1:
                parent_path = self._path[:last_dot]
                method_name = self._path[last_dot + 1 :]
                parent_last_segment = parent_path.rsplit(".", 1)[-1]

                # If parent looks like a class (starts with uppercase) and
                # method name looks like a method (starts with lowercase),
                # try as static method first
                if (
                    parent_last_segment
                    and parent_last_segment[0].isupper()
                    and method_name
                    and method_name[0].islower()
                ):
                    return self._client.invoke_static_method(
                        parent_path, method_name, *args
                    )

            # Fallback: try as class instantiation (backward compatibility)
            # This handles packages that haven't been loaded yet
            return self._client.create_object(self._path, *args)

    def __repr__(self):
        return f"<JVM {self._path}>"


def _unpack_value_impl(
    val_type: int, val_table: Any, client: Any, ref_class: Any
) -> Any:
    """Unpack a FlatBuffer Value union to a Python object.

    Shared by the sync and async clients. Object references are wrapped as
    ref_class(client, object_id).
    """
    if val_type == Value.Value.NullVal:
        return None
    elif val_type == Value.Value.StringVal:
        str_obj = StringVal.StringVal()
        str_obj.Init(val_table.Bytes, val_table.Pos)
        return str_obj.V().decode("utf-8")
    elif val_type == Value.Value.IntVal:
        int_obj = IntVal.IntVal()
        int_obj.Init(val_table.Bytes, val_table.Pos)
        return int_obj.V()
    elif val_type == Value.Value.DoubleVal:
        double_obj = DoubleVal.DoubleVal()
        double_obj.Init(val_table.Bytes, val_table.Pos)
        return double_obj.V()
    elif val_type == Value.Value.BoolVal:
        bool_obj = BoolVal.BoolVal()
        bool_obj.Init(val_table.Bytes, val_table.Pos)
        return bool_obj.V()
    elif val_type == Value.Value.CharVal:
        char_obj = CharVal.CharVal()
        char_obj.Init(val_table.Bytes, val_table.Pos)
        return chr(char_obj.V())  # Convert to Python str (single char)
    elif val_type == Value.Value.ObjectRef:
        ref_obj = ObjectRef.ObjectRef()
        ref_obj.Init(val_table.Bytes, val_table.Pos)
        return ref_class(client, ref_obj.Id())
    elif val_type == Value.Value.ListVal:
        list_obj = ListVal.ListVal()
        list_obj.Init(val_table.Bytes, val_table.Pos)
        list_result = JavaList()
        for i in range(list_obj.ItemsLength()):
            item = list_obj.Items(i)
            list_result.append(
                _unpack_value_impl(item.ValType(), item.Val(), client, ref_class)
            )
        return list_result
    elif val_type == Value.Value.MapVal:
        map_obj = MapVal.MapVal()
        map_obj.Init(val_table.Bytes, val_table.Pos)
        map_result: dict[Any, Any] = {}
        for i in range(map_obj.EntriesLength()):
            entry = map_obj.Entries(i)
            key_arg = entry.Key()
            val_arg = entry.Value()
            key = _unpack_value_impl(
                key_arg.ValType(), key_arg.Val(), client, ref_class
            )
            val = _unpack_value_impl(
                val_arg.ValType(), val_arg.Val(), client, ref_class
            )
            if isinstance(key, list):
                key = tuple(key)  # a Java List key; lists are not hashable
            map_result[key] = val
        return map_result
    elif val_type == Value.Value.ArrayVal:
        arr_obj = ArrayVal.ArrayVal()
        arr_obj.Init(val_table.Bytes, val_table.Pos)
        return _unpack_array_impl(arr_obj, client, ref_class)

    return None


def _release_unread_response(client: Any, reply: bytes) -> None:
    """Queue the objects named in a response that nobody will read for release.

    The request that asked for it stopped waiting (it timed out), but Java
    registered the objects in its result for this client, and only the client
    can release them before the session ends.
    """
    ids: list[int] = []
    try:
        resp = Response.Response.GetRootAsResponse(reply, 0)
        if resp.IsError() or resp.IsCallback():
            return
        batch = resp.Batch()
        results = (
            [resp]
            if batch is None
            else [batch.Responses(i) for i in range(batch.ResponsesLength())]
        )
        for result in results:
            if result.IsError():
                continue
            # No wrappers, which would be Java objects to hash as a map's keys
            _unpack_value_impl(
                result.ReturnValType(),
                result.ReturnVal(),
                client,
                lambda _client, object_id: ids.append(object_id),
            )
    except Exception as e:  # noqa: BLE001 - whatever it is, there is nobody to tell
        logger.debug("Could not read a response nobody waited for: %s", e)
    for object_id in ids:
        client._queue_free(object_id)


def _unpack_array_impl(array_val, client: Any, ref_class: Any):
    """Unpack an ArrayVal FlatBuffer to a JavaArray (preserves array semantics).

    Uses direct buffer slicing with struct.unpack for primitive types,
    which is much faster than element-by-element access.
    """
    elem_type = array_val.ElementType()
    tab = array_val._tab

    if elem_type == ElementType.ElementType.Int:
        length = array_val.IntValuesLength()
        values = _unpack_flatbuffer_vector(
            tab, _ARRAYVAL_INT_VALUES_OFFSET, length, "i", 4
        )
        return JavaArray(values, element_type="Int")
    elif elem_type == ElementType.ElementType.Long:
        length = array_val.LongValuesLength()
        values = _unpack_flatbuffer_vector(
            tab, _ARRAYVAL_LONG_VALUES_OFFSET, length, "q", 8
        )
        return JavaArray(values, element_type="Long")
    elif elem_type == ElementType.ElementType.Double:
        length = array_val.DoubleValuesLength()
        values = _unpack_flatbuffer_vector(
            tab, _ARRAYVAL_DOUBLE_VALUES_OFFSET, length, "d", 8
        )
        return JavaArray(values, element_type="Double")
    elif elem_type == ElementType.ElementType.Float:
        # Float was widened to double
        length = array_val.DoubleValuesLength()
        values = _unpack_flatbuffer_vector(
            tab, _ARRAYVAL_DOUBLE_VALUES_OFFSET, length, "d", 8
        )
        return JavaArray(values, element_type="Float")
    elif elem_type == ElementType.ElementType.Bool:
        length = array_val.BoolValuesLength()
        values = _unpack_flatbuffer_vector(
            tab, _ARRAYVAL_BOOL_VALUES_OFFSET, length, "?", 1
        )
        return JavaArray(values, element_type="Bool")
    elif elem_type == ElementType.ElementType.Byte:
        length = array_val.ByteValuesLength()
        # Direct buffer slice for bytes
        o = flatbuffers.number_types.UOffsetTFlags.py_type(
            tab.Offset(_ARRAYVAL_BYTE_VALUES_OFFSET)
        )
        if o == 0 or length == 0:
            return b""
        start = tab.Vector(o)
        return bytes(tab.Bytes[start : start + length])
    elif elem_type == ElementType.ElementType.Short:
        # Short was widened to int
        length = array_val.IntValuesLength()
        values = _unpack_flatbuffer_vector(
            tab, _ARRAYVAL_INT_VALUES_OFFSET, length, "i", 4
        )
        return JavaArray(values, element_type="Short")
    elif elem_type == ElementType.ElementType.Char:
        # Char was widened to int; each element becomes a one-character string
        length = array_val.IntValuesLength()
        values = _unpack_flatbuffer_vector(
            tab, _ARRAYVAL_INT_VALUES_OFFSET, length, "i", 4
        )
        return JavaArray(map(chr, values), element_type="Char")
    elif elem_type == ElementType.ElementType.String:
        result = JavaArray(element_type="String")
        for i in range(array_val.ObjectValuesLength()):
            item = array_val.ObjectValues(i)
            result.append(
                _unpack_value_impl(item.ValType(), item.Val(), client, ref_class)
            )
        return result
    else:
        # Object array
        result = JavaArray(element_type="Object")
        for i in range(array_val.ObjectValuesLength()):
            item = array_val.ObjectValues(i)
            result.append(
                _unpack_value_impl(item.ValType(), item.Val(), client, ref_class)
            )
        return result


def _arrow_table_to_send(table: pa.Table | pa.RecordBatch) -> pa.Table:
    """Check a table for arrow_to_java() and return it in the form it is sent in."""
    from gatun.arena import UnsupportedArrowTypeError, _check_type_recursive

    if isinstance(table, pa.RecordBatch):
        table = pa.Table.from_batches([table])
    elif not isinstance(table, pa.Table):
        raise TypeError(
            f"Expected a pyarrow Table or RecordBatch, got {type(table).__name__}"
        )

    dictionary_columns: list[str] = []
    _check_type_recursive(table.schema, dictionary_columns)
    if dictionary_columns:
        raise UnsupportedArrowTypeError(
            "Dictionary-encoded columns cannot be turned into a Java table: "
            f"{', '.join(dictionary_columns)}. Decode them first, for example with "
            "pyarrow.compute.dictionary_decode()."
        )

    # One chunk per column, so the table arrives as a single record batch and
    # Java can keep its buffers as they are instead of stitching batches together.
    return table.combine_chunks()


def _arrow_stream_size(table: pa.Table) -> int:
    """The length of the table's Arrow IPC stream, without writing its data."""
    sink = pa.MockOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return cast(int, sink.size())


def _require_payload_room(
    client: Any, size: int, offset: int, zone: str = "Arrow table"
) -> None:
    """Raise PayloadTooLargeError unless `size` bytes fit in the payload zone at `offset`."""
    available = client.response_offset - client.payload_offset - offset
    if size > available:
        raise PayloadTooLargeError(size, max(available, 0), zone)


def _write_arrow_table(
    client: Any, table: pa.Table, offset: int, size: int, zone: str = "Arrow table"
) -> None:
    """Write a table as an Arrow IPC stream into the payload zone at the given offset.

    The stream is written where Java reads it. Building it in a buffer of its
    own and copying that across would pass over the data twice. `size` is the
    stream's length, from _arrow_stream_size().

    Raises PayloadTooLargeError when it does not fit between the offset and the
    response zone; nothing is written then. Any other failure may leave part of
    a stream behind, so a client that hands out views of the zone must count
    the write before calling this.
    """
    assert client.shm is not None, "Not connected"
    _require_payload_room(client, size, offset, zone)

    start = client.payload_offset + offset
    buffer = pa.py_buffer(memoryview(client.shm)[start : start + size])
    if not buffer.is_mutable:
        # The writer would abort the process on this, rather than raise
        raise RuntimeError("The shared memory is mapped read-only")
    sink = pa.FixedSizeBufferWriter(buffer)
    writer = None
    try:
        with pa.ipc.new_stream(sink, table.schema) as writer:
            writer.write_table(table)
        written = sink.tell()
    finally:
        # Each of these holds the mapping open, and so would a traceback that
        # kept them: close() could not unmap the shared memory
        del writer, sink, buffer
    if written != size:
        raise RuntimeError(
            f"Arrow stream took {written:,} bytes where {size:,} were expected"
        )


def _finish_arrow_to_object(builder: Any, offset: int, length: int) -> Any:
    """Build the ArrowToObject command for a stream already in the payload zone."""
    BufferDescriptor.Start(builder)
    BufferDescriptor.AddOffset(builder, offset)
    BufferDescriptor.AddLength(builder, length)
    stream = BufferDescriptor.End(builder)

    Cmd.CommandStart(builder)
    Cmd.CommandAddAction(builder, Act.Action.ArrowToObject)
    Cmd.CommandAddArrowStream(builder, stream)
    builder.Finish(Cmd.CommandEnd(builder))
    return builder.Output()


def _finish_arrow_from_object(builder: Any, object_id: int) -> Any:
    """Build the ArrowFromObject command."""
    Cmd.CommandStart(builder)
    Cmd.CommandAddAction(builder, Act.Action.ArrowFromObject)
    Cmd.CommandAddTargetId(builder, object_id)
    builder.Finish(Cmd.CommandEnd(builder))
    return builder.Output()


def _read_arrow_stream(
    client: Any, location: tuple[int, int], *, in_place: bool = False
) -> pa.Table:
    """Read the IPC stream the server wrote into the payload zone.

    By default the bytes are copied out of shared memory first, so the table
    owns its data and stays valid whatever happens to the zone or the connection
    afterwards. Read in place, the table's columns are the bytes in the zone:
    nothing is copied, and the next write there changes the table. It holds the
    mapping open, so it never reads unmapped memory.
    """
    offset, length = location
    assert client.shm is not None, "Not connected"
    start = client.payload_offset + offset
    if offset < 0 or length < 0 or start + length > client.response_offset:
        raise ProtocolDesyncError(
            f"Arrow stream at offset {offset} with length {length} is outside "
            "the payload zone"
        )
    if in_place:
        data = pa.py_buffer(memoryview(client.shm)[start : start + length])
    else:
        # The copy goes into Arrow's memory pool, which (mimalloc, jemalloc)
        # hands back the memory of a table that was dropped. A bytes object of
        # 32 MiB or more is new memory from the OS every time, and so is the
        # pool's with the system allocator behind it; the page faults of
        # touching new memory cost several times the copy.
        data = pa.allocate_buffer(length)
        target = memoryview(data).cast("B")
        source = memoryview(client.shm)[start : start + length]
        try:
            target[:] = source
        finally:
            # A view left behind would keep close() from unmapping the memory
            source.release()
            target.release()
    return pa.ipc.open_stream(data).read_all()


def _close_shared_memory(client: Any) -> None:
    """Release a client's mapping of the shared memory file, best effort.

    mmap.close() raises BufferError while something still holds a buffer export
    of the mapping (a memoryview, for example); the mapping then lives until
    that is released. The file handle is closed either way.
    """
    shm, client.shm = client.shm, None
    shm_file, client.shm_file = client.shm_file, None
    for resource in (shm, shm_file):
        if resource is None:
            continue
        try:
            resource.close()
        except (OSError, ValueError, BufferError):
            pass


def _remember_reflection(cache: _LRUCache[str], full_name: str, kind: str) -> None:
    """Cache what the server said a dotted name is.

    "none" is kept only for a name inside a known class: a class's members are
    settled, while a name that is not a class yet may become one (a JAR added
    later), and asking again is how that is noticed.
    """
    parent = full_name.rpartition(".")[0]
    if kind == "none":
        if parent and cache.get(parent) == "class":
            cache.set(full_name, kind)
        return
    cache.set(full_name, kind)
    if kind != "class" and parent and cache.get(parent) is None:
        # Something with a method or a field is a class
        cache.set(parent, "class")


class _CallbackScope:
    """One running callback of one client.

    `call` is None for a callback made in band, by the thread running the
    client's request: Java calls from inside it are refused. For a callback
    that came on the callback connection it is the call Java is waiting on,
    which is also the route such calls take.
    """

    __slots__ = ("active", "call", "client")

    def __init__(self, client: Any, call: Any = None):
        self.client = client
        self.call = call
        self.active = True


# The callbacks running in the current context. Tasks, and threads started with
# a copy of the context (asyncio.to_thread), inherit it from the callback that
# started them, which is how a Java call a callback hands to a helper is still
# recognised as reentrant. A helper that outlives its callback is free to call
# Java, hence the scope's `active` flag rather than the mere presence of a scope.
_callback_scopes: contextvars.ContextVar[tuple[_CallbackScope, ...]] = (
    contextvars.ContextVar("gatun_callback_scopes", default=())
)


def _enter_callback(
    client: Any, call: Any = None
) -> tuple[_CallbackScope, contextvars.Token]:
    scope = _CallbackScope(client, call)
    return scope, _callback_scopes.set((*_callback_scopes.get(), scope))


def _exit_callback(scope: _CallbackScope, token: contextvars.Token) -> None:
    scope.active = False
    _callback_scopes.reset(token)


def _in_callback_of(client: Any) -> bool:
    """Whether the current context is inside a running in-band callback of `client`."""
    for scope in _callback_scopes.get():
        if scope.active and scope.client is client and scope.call is None:
            return True
    return False


def _channel_call_of(client: Any) -> Any:
    """The callback-connection call of `client` the current context is running
    in and Java is still waiting on, or None."""
    for scope in reversed(_callback_scopes.get()):
        if scope.call is not None and scope.client is client and scope.call.active:
            return scope.call
    return None


# What a callback may send on the callback connection. The rest needs the
# client's shared memory: every Arrow transfer but the two that go through the
# call's own memory.
_NESTABLE_ACTIONS = frozenset(
    {
        Act.Action.Ping,
        Act.Action.Reflect,
        Act.Action.CreateObject,
        Act.Action.FreeObject,
        Act.Action.InvokeMethod,
        Act.Action.InvokeStaticMethod,
        Act.Action.GetField,
        Act.Action.SetField,
        Act.Action.IsInstanceOf,
        Act.Action.GetStaticField,
        Act.Action.SetStaticField,
        Act.Action.RegisterCallback,
        Act.Action.UnregisterCallback,
        Act.Action.GetFields,
        Act.Action.InvokeMethods,
        Act.Action.CreateObjects,
        Act.Action.Batch,
        Act.Action.GetMetrics,
        Act.Action.OpenCallMemory,
        Act.Action.ArrowToObject,
        Act.Action.ArrowFromObject,
    }
)
# The error type of the response to a command sent for a call that Java has
# stopped waiting on. Must match GatunServer.CALL_ENDED.
_CALL_ENDED = b"org.gatun.server.CallbackCallEnded"
_ACTION_NAMES = {
    value: name for name, value in vars(Act.Action).items() if isinstance(value, int)
}


def _not_inside_channel_callback(what: str) -> ReentrancyError:
    return ReentrancyError(
        f"{what} cannot be used inside a callback that Java made from another "
        "thread. Such a callback reaches Java over the callback connection, "
        "and this goes through the client's own shared memory, which the "
        "request waiting for the callback may be using. Move tables with "
        "arrow_to_java() or arrow_from_java() there, or do it after the "
        "callback has returned."
    )


def _nested_command(call_id: int, data: bytes | bytearray) -> bytearray:
    """Wrap a command for the callback connection, or refuse it."""
    action = Cmd.Command.GetRootAsCommand(data, 0).Action()
    if action not in _NESTABLE_ACTIONS:
        raise _not_inside_channel_callback(
            _ACTION_NAMES.get(action, f"Action {action}")
        )
    builder = flatbuffers.Builder(len(data) + 64)
    nested = builder.CreateByteVector(bytes(data))
    Cmd.CommandStart(builder)
    Cmd.CommandAddAction(builder, Act.Action.NestedCommand)
    Cmd.CommandAddCallId(builder, call_id)
    Cmd.CommandAddNestedCommand(builder, nested)
    builder.Finish(Cmd.CommandEnd(builder))
    frame = cast(bytearray, builder.Output())
    if len(frame) > MAX_FRAME_SIZE:
        raise PayloadTooLargeError(len(frame), MAX_FRAME_SIZE, "Command")
    return frame


def _callback_target(handler: Any, method: bytes | None) -> Callable[..., Any]:
    """What answers a call that Java made on a callback's proxy.

    A callable answers every method of its interface. Anything else answers
    with its own method of the same name, which is how an interface with
    several methods (a listener) is implemented.
    """
    if callable(handler):
        return cast(Callable[..., Any], handler)
    name = method.decode("utf-8") if method else ""
    target = getattr(handler, name, None)
    if not callable(target):
        raise TypeError(
            f"{type(handler).__name__} has no method {name}() to answer the "
            "Java call with"
        )
    return cast(Callable[..., Any], target)


def _callback_methods(handler: Any) -> list[str]:
    """The names of the methods a callback object answers Java's calls with.

    Registration sends them: a default method of the interface that the object
    has a method for is the object's to answer, and the others run in Java.
    A callable has none. It answers the abstract methods only.
    """
    if callable(handler):
        return []
    names = []
    for name in dir(handler):
        if name.startswith("_"):
            continue
        # Without running a property to find out what it is
        attribute = inspect.getattr_static(handler, name, None)
        if callable(attribute) or isinstance(attribute, (classmethod, staticmethod)):
            names.append(name)
    if not names:
        raise TypeError(
            f"{type(handler).__name__} cannot be a callback: it is not callable "
            "and has no public method for Java to call"
        )
    return names


def _finish_register_callback(
    builder: flatbuffers.Builder, interface_name: str, handler: Any
) -> Any:
    interface_off = builder.CreateString(interface_name)
    names = [builder.CreateString(name) for name in _callback_methods(handler)]
    Cmd.CommandStartCallbackMethodsVector(builder, len(names))
    for name in reversed(names):
        builder.PrependUOffsetTRelative(name)
    names_off = builder.EndVector()

    Cmd.CommandStart(builder)
    Cmd.CommandAddAction(builder, Act.Action.RegisterCallback)
    Cmd.CommandAddTargetName(builder, interface_off)
    Cmd.CommandAddCallbackMethods(builder, names_off)
    builder.Finish(Cmd.CommandEnd(builder))
    return builder.Output()


def _build_open_call_memory() -> bytes:
    builder = flatbuffers.Builder(32)
    Cmd.CommandStart(builder)
    Cmd.CommandAddAction(builder, Act.Action.OpenCallMemory)
    builder.Finish(Cmd.CommandEnd(builder))
    return bytes(builder.Output())


_OPEN_CALL_MEMORY = _build_open_call_memory()


def _holding_channel(method):
    """Run a client method with the channel held from start to finish.

    For methods that use the payload zone around their request: the zone is
    shared by every thread using the client, so writing it, sending the request
    that refers to it and reading the answer out of it are one step.
    """

    @functools.wraps(method)
    def wrapper(self, *args, **kwargs):
        # The zone belongs to the client's own channel, which may be busy with
        # the very request that is waiting for this callback
        if _channel_call_of(self) is not None:
            raise _not_inside_channel_callback(f"{method.__name__}()")
        # Before waiting for the channel: the request that holds it is the one
        # waiting for this callback
        self._check_reentrancy()
        with self._exchange_lock:
            return method(self, *args, **kwargs)

    return wrapper


class _ValueEncoder:
    """Builds FlatBuffers values from Python arguments.

    Shared by the sync and async clients. Nothing here may talk to the server:
    the command these values belong to is still being built.
    """

    # Wrapper classes that are sent as object references
    _object_ref_types: tuple[type, ...] = (JavaObject,)

    def _create_string(self, builder: flatbuffers.Builder, s: str) -> int:
        return cast(int, builder.CreateString(s))

    def _build_argument(self, builder, value):
        """Convert a Python value to a FlatBuffer Argument.

        If value is a TypeHint wrapper, extracts the actual value and
        includes the type hint for Java overload resolution.
        """
        # Extract type hint if present
        type_hint = None
        if isinstance(value, TypeHint):
            type_hint = value.java_type
            value = value.value

        # Special case: char type hint with single-character string
        if type_hint in ("char", "Character", "java.lang.Character"):
            if isinstance(value, str) and len(value) == 1:
                CharVal.Start(builder)
                CharVal.AddV(builder, ord(value[0]))
                val_type, val_off = Value.Value.CharVal, CharVal.End(builder)
            elif isinstance(value, int):
                CharVal.Start(builder)
                CharVal.AddV(builder, value)
                val_type, val_off = Value.Value.CharVal, CharVal.End(builder)
            else:
                raise ValueError(
                    f"char type hint requires single-character string or int, got {type(value)}"
                )
        else:
            val_type, val_off = self._build_value(builder, value)

        # Build type hint string offset if present (must be before Start)
        type_hint_off = 0
        if type_hint:
            type_hint_off = self._create_string(builder, type_hint)

        Argument.Start(builder)
        Argument.AddValType(builder, val_type)
        Argument.AddVal(builder, val_off)
        if type_hint_off:
            Argument.AddTypeHint(builder, type_hint_off)
        return Argument.End(builder)

    def _build_value(self, builder, value):
        """Build a Value union and return (type, offset)."""
        if isinstance(value, str):
            str_off = self._create_string(builder, value)
            StringVal.Start(builder)
            StringVal.AddV(builder, str_off)
            return Value.Value.StringVal, StringVal.End(builder)
        elif isinstance(value, bool):  # Must check before int (bool is subclass of int)
            BoolVal.Start(builder)
            BoolVal.AddV(builder, value)
            return Value.Value.BoolVal, BoolVal.End(builder)
        elif isinstance(value, int):
            IntVal.Start(builder)
            IntVal.AddV(builder, value)
            return Value.Value.IntVal, IntVal.End(builder)
        elif isinstance(value, float):
            DoubleVal.Start(builder)
            DoubleVal.AddV(builder, value)
            return Value.Value.DoubleVal, DoubleVal.End(builder)
        elif isinstance(value, self._object_ref_types):
            ObjectRef.Start(builder)
            ObjectRef.AddId(builder, value.object_id)
            return Value.Value.ObjectRef, ObjectRef.End(builder)
        elif isinstance(value, JavaArray):
            # JavaArray came from Java and should go back as ArrayVal (Java array)
            return self._build_java_array(builder, value)
        elif isinstance(value, (list, tuple, range)):
            # Auto-convert Python list/tuple/range to ListVal (Java ArrayList)
            # These are treated the same as lists for Java interop
            items = list(value) if isinstance(value, range) else value
            item_offsets = []
            for item in items:
                item_offsets.append(self._build_argument(builder, item))
            ListVal.StartItemsVector(builder, len(item_offsets))
            for offset in reversed(item_offsets):
                builder.PrependUOffsetTRelative(offset)
            items_vec = builder.EndVector()
            ListVal.Start(builder)
            ListVal.AddItems(builder, items_vec)
            return Value.Value.ListVal, ListVal.End(builder)
        elif isinstance(value, dict):
            # Auto-convert Python dict to MapVal
            entry_offsets = []
            for k, v in value.items():
                key_arg = self._build_argument(builder, k)
                val_arg = self._build_argument(builder, v)
                MapEntry.Start(builder)
                MapEntry.AddKey(builder, key_arg)
                MapEntry.AddValue(builder, val_arg)
                entry_offsets.append(MapEntry.End(builder))
            MapVal.StartEntriesVector(builder, len(entry_offsets))
            for offset in reversed(entry_offsets):
                builder.PrependUOffsetTRelative(offset)
            entries_vec = builder.EndVector()
            MapVal.Start(builder)
            MapVal.AddEntries(builder, entries_vec)
            return Value.Value.MapVal, MapVal.End(builder)
        elif isinstance(value, (pa.Array, pa.ChunkedArray)):
            # Convert PyArrow array to ArrayVal
            if isinstance(value, pa.ChunkedArray):
                value = value.combine_chunks()
            return self._build_arrow_array(builder, value)
        elif isinstance(value, (bytes, bytearray)):
            # Convert bytes to byte array
            return self._build_byte_array(builder, value)
        elif isinstance(value, array.array):
            # Convert Python array.array to ArrayVal
            return self._build_typed_array(builder, value)
        elif value is None:
            from gatun.generated.org.gatun.protocol import NullVal

            NullVal.Start(builder)
            return Value.Value.NullVal, NullVal.End(builder)
        else:
            temporal = _encode_temporal(value)
            if temporal is not None:
                kind, temporal_value = temporal
                TemporalVal.Start(builder)
                TemporalVal.AddKind(builder, kind)
                TemporalVal.AddV(builder, temporal_value)
                return Value.Value.TemporalVal, TemporalVal.End(builder)
            else:
                raise TypeError(f"Unsupported argument type: {type(value)}")

    def _build_arrow_array(self, builder, arr: pa.Array):
        """Build an ArrayVal from a PyArrow Array.

        Uses Arrow's buffer directly for efficient serialization of primitive types.
        """
        # Mapping of Arrow types to (element_type, add_values_fn, itemsize, target_arrow_type)
        # itemsize is needed for creating properly-typed FlatBuffers vectors
        # target_arrow_type is used for casting when needed (e.g., float32 -> float64)
        primitive_types = {
            pa.int32(): (ElementType.ElementType.Int, ArrayVal.AddIntValues, 4, None),
            pa.int64(): (ElementType.ElementType.Long, ArrayVal.AddLongValues, 8, None),
            pa.float64(): (
                ElementType.ElementType.Double,
                ArrayVal.AddDoubleValues,
                8,
                None,
            ),
            pa.float32(): (
                ElementType.ElementType.Float,
                ArrayVal.AddDoubleValues,
                8,  # After widening to float64
                pa.float64(),  # Widen float32 to float64 for transmission
            ),
            # Note: bool handled specially below due to Arrow's bit-packing
            pa.int8(): (ElementType.ElementType.Byte, ArrayVal.AddByteValues, 1, None),
            pa.uint8(): (
                ElementType.ElementType.Byte,
                ArrayVal.AddByteValues,
                1,
                pa.int8(),
            ),
            pa.int16(): (
                ElementType.ElementType.Short,
                ArrayVal.AddIntValues,
                4,  # After widening to int32
                pa.int32(),  # Widen short to int for transmission
            ),
        }

        arrow_type = arr.type

        is_string = pa.types.is_string(arrow_type) or pa.types.is_large_string(
            arrow_type
        )
        if arr.null_count and (
            arrow_type == pa.bool_() or arrow_type in primitive_types
        ):
            raise ValueError(
                f"Cannot convert a pyarrow {arrow_type} array containing nulls to a "
                "Java primitive array. Fill or drop the nulls first."
            )

        # Special handling for bool arrays - Arrow uses bit-packing, Java expects byte-per-bool
        if arrow_type == pa.bool_():
            # Convert to byte array (1 byte per boolean)
            packed = struct.pack(f"<{len(arr)}B", *(1 if b.as_py() else 0 for b in arr))
            vec_off = _create_typed_vector(builder, packed, 1, len(arr))
            ArrayVal.Start(builder)
            ArrayVal.AddElementType(builder, ElementType.ElementType.Bool)
            ArrayVal.AddBoolValues(builder, vec_off)
            return Value.Value.ArrayVal, ArrayVal.End(builder)

        if arrow_type in primitive_types:
            elem_type, add_values_fn, itemsize, cast_type = primitive_types[arrow_type]
            if cast_type is not None:
                arr = arr.cast(cast_type)
            # Data buffer (index 1; index 0 is the validity bitmap). A sliced array
            # shares its parent's buffer, so take only this array's window.
            start = arr.offset * itemsize
            buf = arr.buffers()[1][start : start + len(arr) * itemsize].to_pybytes()
            vec_off = _create_typed_vector(builder, buf, itemsize, len(arr))
            ArrayVal.Start(builder)
            ArrayVal.AddElementType(builder, elem_type)
            add_values_fn(builder, vec_off)
            return Value.Value.ArrayVal, ArrayVal.End(builder)

        # String array - serialize each element
        if is_string:
            item_offsets = [
                self._build_argument(builder, arr[i].as_py()) for i in range(len(arr))
            ]
            ArrayVal.StartObjectValuesVector(builder, len(item_offsets))
            for offset in reversed(item_offsets):
                builder.PrependUOffsetTRelative(offset)
            vec_off = builder.EndVector()
            ArrayVal.Start(builder)
            ArrayVal.AddElementType(builder, ElementType.ElementType.String)
            ArrayVal.AddObjectValues(builder, vec_off)
            return Value.Value.ArrayVal, ArrayVal.End(builder)

        # Fallback: treat as object array, serialize each element
        item_offsets = [
            self._build_argument(builder, arr[i].as_py()) for i in range(len(arr))
        ]
        ArrayVal.StartObjectValuesVector(builder, len(item_offsets))
        for offset in reversed(item_offsets):
            builder.PrependUOffsetTRelative(offset)
        vec_off = builder.EndVector()
        ArrayVal.Start(builder)
        ArrayVal.AddElementType(builder, ElementType.ElementType.Object)
        ArrayVal.AddObjectValues(builder, vec_off)
        return Value.Value.ArrayVal, ArrayVal.End(builder)

    def _build_byte_array(self, builder, data):
        """Build an ArrayVal from bytes or bytearray."""
        vec_off = builder.CreateByteVector(data)
        ArrayVal.Start(builder)
        ArrayVal.AddElementType(builder, ElementType.ElementType.Byte)
        ArrayVal.AddByteValues(builder, vec_off)
        return Value.Value.ArrayVal, ArrayVal.End(builder)

    def _build_typed_array(self, builder, arr):
        """Build an ArrayVal from Python array.array.

        Uses array.tobytes() for direct byte conversion and _create_typed_vector
        for proper FlatBuffers vector creation.
        """
        typecode = arr.typecode
        n = len(arr)
        # array.array.tobytes() gives us the raw bytes directly
        if typecode == "i":  # int (usually 32-bit)
            vec_off = _create_typed_vector(builder, arr.tobytes(), 4, n)
            ArrayVal.Start(builder)
            ArrayVal.AddElementType(builder, ElementType.ElementType.Int)
            ArrayVal.AddIntValues(builder, vec_off)
            return Value.Value.ArrayVal, ArrayVal.End(builder)
        elif typecode == "l" or typecode == "q":  # long
            vec_off = _create_typed_vector(builder, arr.tobytes(), 8, n)
            ArrayVal.Start(builder)
            ArrayVal.AddElementType(builder, ElementType.ElementType.Long)
            ArrayVal.AddLongValues(builder, vec_off)
            return Value.Value.ArrayVal, ArrayVal.End(builder)
        elif typecode == "d":  # double
            vec_off = _create_typed_vector(builder, arr.tobytes(), 8, n)
            ArrayVal.Start(builder)
            ArrayVal.AddElementType(builder, ElementType.ElementType.Double)
            ArrayVal.AddDoubleValues(builder, vec_off)
            return Value.Value.ArrayVal, ArrayVal.End(builder)
        elif typecode == "f":  # float -> widen to double
            # Need to widen float32 to float64
            widened = struct.pack(f"<{n}d", *arr)
            vec_off = _create_typed_vector(builder, widened, 8, n)
            ArrayVal.Start(builder)
            ArrayVal.AddElementType(builder, ElementType.ElementType.Float)
            ArrayVal.AddDoubleValues(builder, vec_off)
            return Value.Value.ArrayVal, ArrayVal.End(builder)
        elif typecode == "b" or typecode == "B":  # byte
            vec_off = _create_typed_vector(builder, arr.tobytes(), 1, n)
            ArrayVal.Start(builder)
            ArrayVal.AddElementType(builder, ElementType.ElementType.Byte)
            ArrayVal.AddByteValues(builder, vec_off)
            return Value.Value.ArrayVal, ArrayVal.End(builder)
        elif typecode == "h" or typecode == "H":  # short -> widen to int
            # Need to widen int16 to int32
            widened = struct.pack(f"<{n}i", *arr)
            vec_off = _create_typed_vector(builder, widened, 4, n)
            ArrayVal.Start(builder)
            ArrayVal.AddElementType(builder, ElementType.ElementType.Short)
            ArrayVal.AddIntValues(builder, vec_off)
            return Value.Value.ArrayVal, ArrayVal.End(builder)
        else:
            raise TypeError(f"Unsupported array.array typecode: {typecode}")

    def _build_java_array(self, builder, java_array: JavaArray):
        """Build an ArrayVal from a JavaArray (preserving array semantics for round-trip).

        Uses struct.pack for efficient serialization and _create_typed_vector
        for proper FlatBuffers vector creation.
        """
        elem_type = java_array.element_type
        items = list(java_array)
        n = len(items)

        if elem_type == "Int":
            packed = struct.pack(f"<{n}i", *items)
            vec_off = _create_typed_vector(builder, packed, 4, n)
            ArrayVal.Start(builder)
            ArrayVal.AddElementType(builder, ElementType.ElementType.Int)
            ArrayVal.AddIntValues(builder, vec_off)
            return Value.Value.ArrayVal, ArrayVal.End(builder)
        elif elem_type == "Long":
            packed = struct.pack(f"<{n}q", *items)
            vec_off = _create_typed_vector(builder, packed, 8, n)
            ArrayVal.Start(builder)
            ArrayVal.AddElementType(builder, ElementType.ElementType.Long)
            ArrayVal.AddLongValues(builder, vec_off)
            return Value.Value.ArrayVal, ArrayVal.End(builder)
        elif elem_type == "Double":
            packed = struct.pack(f"<{n}d", *items)
            vec_off = _create_typed_vector(builder, packed, 8, n)
            ArrayVal.Start(builder)
            ArrayVal.AddElementType(builder, ElementType.ElementType.Double)
            ArrayVal.AddDoubleValues(builder, vec_off)
            return Value.Value.ArrayVal, ArrayVal.End(builder)
        elif elem_type == "Float":
            # Widen float to double for transmission
            packed = struct.pack(f"<{n}d", *items)
            vec_off = _create_typed_vector(builder, packed, 8, n)
            ArrayVal.Start(builder)
            ArrayVal.AddElementType(builder, ElementType.ElementType.Float)
            ArrayVal.AddDoubleValues(builder, vec_off)
            return Value.Value.ArrayVal, ArrayVal.End(builder)
        elif elem_type == "Bool":
            # Pack bools as bytes (0 or 1)
            packed = struct.pack(f"<{n}B", *(1 if b else 0 for b in items))
            vec_off = _create_typed_vector(builder, packed, 1, n)
            ArrayVal.Start(builder)
            ArrayVal.AddElementType(builder, ElementType.ElementType.Bool)
            ArrayVal.AddBoolValues(builder, vec_off)
            return Value.Value.ArrayVal, ArrayVal.End(builder)
        elif elem_type == "Byte":
            vec_off = _create_typed_vector(builder, bytes(items), 1, n)
            ArrayVal.Start(builder)
            ArrayVal.AddElementType(builder, ElementType.ElementType.Byte)
            ArrayVal.AddByteValues(builder, vec_off)
            return Value.Value.ArrayVal, ArrayVal.End(builder)
        elif elem_type == "Short":
            # Widen short to int for transmission
            packed = struct.pack(f"<{n}i", *items)
            vec_off = _create_typed_vector(builder, packed, 4, n)
            ArrayVal.Start(builder)
            ArrayVal.AddElementType(builder, ElementType.ElementType.Short)
            ArrayVal.AddIntValues(builder, vec_off)
            return Value.Value.ArrayVal, ArrayVal.End(builder)
        elif elem_type == "Char":
            # Widen char to int for transmission
            codes = [ord(c) if isinstance(c, str) else c for c in items]
            if any(not 0 <= code <= 0xFFFF for code in codes):
                raise ValueError(
                    "Char array elements must be single UTF-16 code units "
                    "(characters up to U+FFFF)"
                )
            packed = struct.pack(f"<{n}i", *codes)
            vec_off = _create_typed_vector(builder, packed, 4, n)
            ArrayVal.Start(builder)
            ArrayVal.AddElementType(builder, ElementType.ElementType.Char)
            ArrayVal.AddIntValues(builder, vec_off)
            return Value.Value.ArrayVal, ArrayVal.End(builder)
        elif elem_type == "String":
            # String array - need to build each as Argument
            item_offsets = []
            for item in items:
                item_offsets.append(self._build_argument(builder, item))
            ArrayVal.StartObjectValuesVector(builder, len(item_offsets))
            for offset in reversed(item_offsets):
                builder.PrependUOffsetTRelative(offset)
            vec_off = builder.EndVector()
            ArrayVal.Start(builder)
            ArrayVal.AddElementType(builder, ElementType.ElementType.String)
            ArrayVal.AddObjectValues(builder, vec_off)
            return Value.Value.ArrayVal, ArrayVal.End(builder)
        else:
            # Object array - need to build each as Argument
            item_offsets = []
            for item in items:
                item_offsets.append(self._build_argument(builder, item))
            ArrayVal.StartObjectValuesVector(builder, len(item_offsets))
            for offset in reversed(item_offsets):
                builder.PrependUOffsetTRelative(offset)
            vec_off = builder.EndVector()
            ArrayVal.Start(builder)
            ArrayVal.AddElementType(builder, ElementType.ElementType.Object)
            ArrayVal.AddObjectValues(builder, vec_off)
            return Value.Value.ArrayVal, ArrayVal.End(builder)


class GatunClient(_ValueEncoder):
    def __init__(
        self,
        socket_path=None,
        callback_timeout: float | None = DEFAULT_CALLBACK_TIMEOUT,
        socket_timeout: float | None = DEFAULT_SOCKET_TIMEOUT,
        handshake_timeout: float | None = HANDSHAKE_TIMEOUT,
    ):
        """Initialize a Gatun client.

        Args:
            socket_path: Path to the Unix domain socket for communication.
                        Defaults to ~/gatun.sock.
            callback_timeout: Timeout in seconds for callback execution.
                             Prevents hung callbacks from deadlocking the protocol.
                             Defaults to DEFAULT_CALLBACK_TIMEOUT (30s).
                             Set to None to disable timeout (not recommended).
            socket_timeout: Timeout in seconds for socket read operations.
                           Used for response reads to prevent indefinite hangs.
                           Defaults to DEFAULT_SOCKET_TIMEOUT (30s).
                           Set to None to disable timeout (not recommended).
            handshake_timeout: Timeout in seconds for the initial handshake.
                              Shorter than socket_timeout since handshake should be fast.
                              Defaults to HANDSHAKE_TIMEOUT (10s).
                              Set to None to disable timeout (not recommended).
        """
        if socket_path is None:
            socket_path = os.path.expanduser("~/gatun.sock")

        self.socket_path = socket_path
        self.memory_path: str | None = (
            None  # Set during connect() from server handshake
        )
        # None disables the corresponding timeout
        self.callback_timeout = _usable_timeout(callback_timeout)
        self.socket_timeout = _usable_timeout(socket_timeout)
        self.handshake_timeout = _usable_timeout(handshake_timeout)
        # Server process launched for this client by connect(); stopped on close()
        self._server_session: Any = None

        self.sock: socket.socket | None = socket.socket(
            socket.AF_UNIX, socket.SOCK_STREAM
        )
        self.shm_file: Any = None  # BufferedRandom when connected
        self.shm: mmap.mmap | None = None

        # These are set during connect()
        self.memory_size = 0
        self.command_offset = 0
        self.payload_offset = 65536  # 64KB - must match GatunServer.PAYLOAD_OFFSET
        self.response_offset = 0

        # JVM view for package-style access
        self._jvm: JVMView | None = None

        # Callback registry: callback_id -> callable
        self._callbacks: dict[int, Any] = {}

        # Where Java threads other than the one running a request call Python.
        # Opened with the first register_callback; see _ensure_callback_channel
        self._callback_channel: CallbackChannel | None = None
        self._callback_channel_tried = False

        # Arrow schema cache for Java -> Python transfers: hash -> Schema
        self._arrow_schema_cache: dict[int, pa.Schema] = {}

        # Arena epoch for lifetime safety - tracks current valid epoch
        self._arena_epoch: int = 0

        # Where the batch Java loaded in place (send_arrow_buffers) ends in the
        # payload zone; anything written before this would corrupt it
        self._arrow_loaded_end: int = 0

        # Bumped whenever something is written into the part of the payload
        # zone that get_arrow_data()'s ArrowTableViews point at; see
        # _note_payload_write
        self._payload_generation: int = 0

        # Bumped whenever anything is written into the payload zone at all. A
        # table read in place by arrow_from_java(copy=False) can lie anywhere in it
        self._payload_writes: int = 0

        # FlatBuffers builder pool - reuse builders to reduce allocation overhead.
        # One pool per thread: a command is built before the channel is taken,
        # so threads sharing a client would otherwise build into the same buffer.
        self._builders = threading.local()

        # String encoding cache - cache UTF-8 encoded bytes for commonly used strings
        # This avoids repeated str.encode() calls for class/method names
        # Bounded to prevent memory bloat in long-running applications
        self._string_cache: _LRUCache[bytes] = _LRUCache(_CACHE_MAX_SIZE)

        # Object IDs whose JavaObject wrappers have been garbage collected and still
        # need a FreeObject on the server. Finalizers only
        # append here; the IDs are sent just before the next request, from the
        # requesting thread. A finalizer can run at any point (in the middle of
        # another request, inside a callback, on another thread, at interpreter
        # exit), so it must never touch the socket or the command zone itself.
        self._pending_frees: deque[int] = deque()
        # Dedicated builder, so flushing frees never disturbs a command being built
        self._free_builder = flatbuffers.Builder(64)

        # One request/response exchange at a time. The channel carries a single
        # exchange, so threads sharing a client take turns. Re-entrant because
        # unpacking a response can itself make a call (hashing a JavaObject).
        self._exchange_lock = threading.RLock()

        # Threads that are running a callback. A Java call from one of them would
        # deadlock: Java is waiting for that callback's result on the only channel.
        # A callback that outlives its timeout keeps its entry until it returns, so
        # it can never send on a channel it no longer owns.
        self._callback_threads: set[int] = set()

        # Why the last connect() failed, for callers that report it
        self.connect_error: BaseException | None = None
        self._closed = False

        # What this client's server said each dotted path is: "class", "method"
        # or "field"
        self._reflect_cache: _LRUCache[str] = _LRUCache(_CACHE_MAX_SIZE)

        # Dead connection flag - set after protocol desync or timeout
        # Once dead, all operations will fail fast with DeadConnectionError
        self._dead: bool = False

    @property
    def _in_callback(self) -> bool:
        """Whether a callback is running, on any thread."""
        return bool(self._callback_threads)

    def _create_string(self, builder: flatbuffers.Builder, s: str) -> int:
        """Create a FlatBuffers string with caching of encoded bytes.

        Caches the UTF-8 encoded bytes of strings to avoid repeated encoding.
        Bounded to 10,000 entries to prevent memory bloat.
        """
        encoded = self._string_cache.get(s)
        if encoded is None:
            encoded = s.encode("utf-8")
            self._string_cache.set(s, encoded)
        return cast(int, builder.CreateString(encoded))

    def _get_builder(self, large: bool = False) -> flatbuffers.Builder:
        """Get a reusable FlatBuffers builder.

        Clears and returns an existing builder from the pool to avoid allocation.
        Use large=True for commands with arguments or data.

        Note: We cannot preserve vtables across clears because vtable entries
        contain absolute buffer offsets that become invalid after Clear().
        """
        pool = self._builders
        try:
            builder = pool.large if large else pool.small
        except AttributeError:
            # Small for simple commands, large for commands with arguments or data
            pool.small = flatbuffers.Builder(256)
            pool.large = flatbuffers.Builder(1024)
            builder = pool.large if large else pool.small
        builder.Clear()
        return builder

    @property
    def jvm(self):
        """Access Java classes via package navigation.

        Example:
            ArrayList = client.jvm.java.util.ArrayList
            my_list = ArrayList()
            result = client.jvm.java.lang.Integer.parseInt("123")
        """
        if self._jvm is None:
            self._jvm = JVMView(self)
        return self._jvm

    def connect(self):
        logger.debug("Connecting to %s", self.socket_path)
        try:
            self.sock.connect(self.socket_path)

            # 1. Handshake - read version, epoch, memory size, and SHM path
            # Format: [4 bytes: version] [4 bytes: arena_epoch] [8 bytes: memory size]
            #         [2 bytes: shm_path_length] [N bytes: shm_path (UTF-8)]
            # Use timeout to prevent indefinite hang if server doesn't respond
            handshake_header = _recv_exactly(
                self.sock, 18, timeout=self.handshake_timeout
            )
            server_version, arena_epoch, self.memory_size, shm_path_len = struct.unpack(
                "<IIQH", handshake_header
            )

            # Verify protocol version
            if server_version != PROTOCOL_VERSION:
                raise RuntimeError(
                    f"Protocol version mismatch: client={PROTOCOL_VERSION}, "
                    f"server={server_version}. Please update your client or server."
                )

            # Read SHM path
            shm_path_bytes = _recv_exactly(
                self.sock, shm_path_len, timeout=self.handshake_timeout
            )
            self.memory_path = shm_path_bytes.decode("utf-8")

            # Synchronize arena epoch with server
            self._arena_epoch = arena_epoch

            # 2. Configure Offsets
            # Response zone size must match GatunServer.RESPONSE_ZONE_SIZE (64KB)
            self.response_offset = self.memory_size - 65536

            # 3. Map Memory (session-specific SHM file created by server)
            assert self.memory_path is not None, "Memory path not set"
            # Held open for the life of the mapping; released in close()
            self.shm_file = open(self.memory_path, "r+b")  # noqa: SIM115
            self.shm = mmap.mmap(self.shm_file.fileno(), self.memory_size)
            logger.info(
                "Connected to %s (shared memory: %s, %.2f MB)",
                self.socket_path,
                self.memory_path,
                self.memory_size / 1024 / 1024,
            )

            self.connect_error = None
            return True
        except Exception as e:  # noqa: BLE001 - connect() reports every failure as False
            logger.error("Connection failed: %s", e)
            self.connect_error = e
            return False

    def get_payload_arena(self) -> PayloadArena:
        """Get a PayloadArena view into the client's shared memory.

        The returned arena writes to the payload zone of the client's shared
        memory, which is the area that Java reads from. Use this instead of
        creating a separate arena file when using send_arrow_buffers.

        Returns:
            PayloadArena backed by the client's shared memory payload zone.

        Example:
            arena = client.get_payload_arena()
            schema_cache = {}
            for table in tables:
                arena.reset()
                client.send_arrow_buffers(table, arena, schema_cache)
            arena.close()  # Just clears reference, doesn't close the shm
        """
        from gatun.arena import PayloadArena

        # Payload zone: from payload_offset to response_offset
        payload_size = self.response_offset - self.payload_offset
        assert self.shm is not None, "Not connected"
        arena = PayloadArena.from_mmap(self.shm, self.payload_offset, payload_size)
        # What is put in the arena lands on whatever views point at there. The
        # arena must not keep the client alive, so it gets a weak reference.
        client_ref = weakref.ref(self)

        def written(end: int) -> None:
            client = client_ref()
            if client is not None:
                client._note_payload_write(end)

        arena._on_allocate = written
        return arena

    def arrow_context(self) -> ScopedArenaContext:
        """Get a context manager for scoped Arrow transfers.

        Provides automatic arena lifecycle management with reset between sends
        and cleanup on context exit. This is the recommended way to transfer
        Arrow data when sending multiple tables.

        Returns:
            ScopedArenaContext for use in a with statement.

        Example:
            with client.arrow_context() as ctx:
                # Send multiple tables (arena auto-resets between sends)
                ctx.send(table1)
                ctx.send(table2)

                # Receive data back from Java (returns owned copy)
                result = ctx.receive()
            # Arena automatically cleaned up on exit
        """
        from gatun.arena import ScopedArenaContext

        return ScopedArenaContext(self)

    def _send_raw(self, data: bytes, expects_response: bool = True):
        """Write a command to SHM, signal Java and return the parsed response.

        With expects_response=False the command is one-way: Java sends no reply and
        nothing is read. That is only true of CallbackResponse.
        """
        # 0a. Fail fast on a connection that cannot be used
        self._require_usable()

        # 1. Validate command size
        max_command_size = self.payload_offset  # Command zone ends where payload starts
        if len(data) > max_command_size:
            raise PayloadTooLargeError(len(data), max_command_size, "Command")

        if not expects_response:
            # A CallbackResponse: part of an exchange that is already under way
            self._write_command(data)
            return None

        # 0b. Check for reentrancy - a call from a callback would deadlock
        self._check_reentrancy()

        # 0c. Inside a callback that came on the callback connection, the call
        # goes back the same way: the Java thread waiting for the callback runs
        # it, and the client's own channel is left alone
        call = _channel_call_of(self)
        if call is not None:
            routed, result = self._send_nested(call, data)
            if routed:
                return result

        with self._exchange_lock:
            # 2. Release objects whose wrappers were garbage collected since the
            # last request. This is a round trip of its own, so it has to finish
            # before our own command goes into shared memory.
            if self._pending_frees:
                self._flush_pending_frees()

            self._write_command(data)
            return self._read_response()

    def _require_usable(self) -> None:
        if self._closed:
            raise DeadConnectionError("Client is closed. Create a new client.")
        if self._dead:
            raise DeadConnectionError(
                "Connection is dead after protocol error. Create a new client."
            )

    def _send_nested(self, call: ChannelCall, data: bytes) -> tuple[bool, Any]:
        """Send a command on the callback connection, as part of `call`.

        Returns (False, None) if Java is no longer waiting for the callback,
        in which case nothing was sent and the command belongs on the client's
        own channel.
        """
        frame = _nested_command(call.call_id, data)
        # A callback that runs for hours would otherwise hold on to every
        # object it dropped until it returns
        if self._pending_frees:
            self._release_on_channel(call.channel)
        try:
            reply = call.request(frame, self.socket_timeout)
        except TimeoutError as e:
            raise SocketTimeoutError(
                f"A Java call made inside a callback got no answer: {e}"
            ) from None
        except OSError as e:
            raise DeadConnectionError(
                f"The callback connection closed while a callback was calling Java: {e}"
            ) from None
        if reply is None:
            return False, None
        resp = Response.Response.GetRootAsResponse(reply, 0)
        if resp.IsError() and resp.ErrorType() == _CALL_ENDED:
            # The Java thread gave up on the callback (it was interrupted) and
            # did not run the command
            call.abandon()
            return False, None
        return True, self._parse_response(resp)

    def _check_reentrancy(self) -> None:
        """Refuse a Java call made from inside a callback."""
        if threading.get_ident() in self._callback_threads or _in_callback_of(self):
            raise ReentrancyError(
                "Cannot call Java from within a callback. "
                "Nested calls would deadlock because the original request is still waiting. "
                "Queue work for later or restructure code to avoid nested calls."
            )

    def _write_command(self, data: bytes) -> None:
        """Write one command to shared memory and signal Java."""
        if self.shm is None:
            raise DeadConnectionError("Client is not connected")
        # No flush: both processes map the same pages, so the bytes are visible to
        # Java as soon as they are written, and the length sent on the socket
        # afterwards is what orders the two. flush() is an msync of the whole
        # mapping, a system call per command that buys nothing here.
        self.shm.seek(self.command_offset)
        self.shm.write(data)

        # Signal Java (Send Length)
        # Verify socket is open (check None first to avoid AttributeError)
        if not self.sock or self.sock.fileno() == -1:
            self._mark_dead()
            raise DeadConnectionError("Socket already closed")

        try:
            self.sock.sendall(struct.pack("<I", len(data)))
        except (OSError, BrokenPipeError, ConnectionResetError) as e:
            self._mark_dead()
            raise ProtocolDesyncError(f"Socket error sending command: {e}")
        except BaseException:
            # Interrupted (Ctrl-C) with the length possibly half sent
            self._mark_dead()
            raise

    def _queue_free(self, object_id: int) -> None:
        """Queue a Java object for release. Called by JavaObject finalizers.

        A finalizer can run at any moment (in the middle of another request, inside
        a callback, on another thread, at interpreter exit), so this must not touch
        the socket or the shared command zone. The next request sends the queue.
        """
        if not self._dead:
            self._pending_frees.append(object_id)

    def _release_unread_response(self, reply: bytes) -> None:
        _release_unread_response(self, reply)

    def _flush_pending_frees(self) -> None:
        """Send FreeObject for every queued object ID, many IDs per round trip.

        Runs on the requesting thread, between requests, so it cannot interleave
        with another command.
        """
        frees = self._pending_frees
        builder = self._free_builder
        while frees:
            ids = _take_free_chunk(frees)
            if not ids:
                break

            channel = self._callback_channel
            answers_sent = channel.answers_sent if channel is not None else 0
            self._write_command(_build_free_command(builder, ids, answers_sent))
            try:
                self._read_response()
            except JavaException as e:
                # The server ignores unknown IDs, so this is unexpected, and it
                # must not fail the caller's unrelated request.
                logger.debug("FreeObject for %d objects failed: %s", len(ids), e)

    def _mark_dead(self):
        """Mark client as dead after protocol desync.

        After a timeout or invalid response, the connection is in an unknown state.
        We must close the socket to prevent cascading failures from reading garbage.
        The SHM mapping is left intact so any existing JavaObject references don't
        crash, but the socket is closed so subsequent operations will fail cleanly.

        Once marked dead:
        - All future operations will immediately raise DeadConnectionError
        - The socket is closed (shutdown + close)
        - The client cannot be reused - create a new one
        """
        if self._dead:
            return  # Already dead

        self._dead = True
        self._pending_frees.clear()
        channel, self._callback_channel = self._callback_channel, None
        if channel is not None:
            channel.close()

        # Shutdown socket to unblock any pending reads, then close
        try:
            if self.sock:
                try:
                    self.sock.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass  # May fail if already disconnected
                self.sock.close()
                self.sock = None
        except OSError:
            pass

    def _unpack_value(self, val_type: int, val_table: Any) -> Any:
        """Unpack a FlatBuffer Value union to a Python object."""
        return _unpack_value_impl(val_type, val_table, self, JavaObject)

    def _read_response(self, timeout: float | None = None, _use_default: bool = True):
        """Read and parse a response from the server.

        Args:
            timeout: Socket read timeout in seconds. None for no timeout.
                    Default uses self.socket_timeout (30 seconds by default).
            _use_default: If True and timeout is None, use self.socket_timeout.

        Raises:
            SocketTimeoutError: If the socket read times out.
                               Connection is closed before raising.
            ProtocolDesyncError: If the response size is invalid.
                                Connection is closed before raising.
        """
        # Resolve to instance default if requested
        if _use_default and timeout is None:
            timeout = self.socket_timeout

        # What the last in-band callback answered with. Its wrappers must not die,
        # and queue their releases, before Java has read the answer: a release
        # can reach the server first, on the callback connection. Java's next
        # message on this channel shows it has.
        answered_with: list[Any] = []
        while True:
            # 1. Read Length with timeout
            try:
                sz_data = _recv_exactly(self.sock, 4, timeout=timeout)
            except SocketTimeoutError:
                # Timeout during read means connection is in unknown state
                self._mark_dead()
                raise
            except (RuntimeError, OSError, BrokenPipeError, ConnectionResetError) as e:
                # Socket closed or error - mark dead and convert to ProtocolDesyncError
                self._mark_dead()
                raise ProtocolDesyncError(f"Socket error reading response: {e}")
            except BaseException:
                # Ctrl-C, SystemExit or anything else while waiting: the response
                # is still on its way, and the next call would read it as its own
                self._mark_dead()
                raise
            sz = struct.unpack("<I", sz_data)[0]
            answered_with.clear()

            # 2. Validate response size before reading from SHM
            # This catches protocol desync (garbage data) early
            # On invalid size, close the connection to prevent cascading failures
            max_response_size = self.memory_size - self.response_offset
            if sz == 0:
                self._mark_dead()
                raise ProtocolDesyncError(
                    "Invalid response size: 0 bytes (empty response)", response_size=sz
                )
            if sz > RESPONSE_ZONE_SIZE:
                self._mark_dead()
                raise ProtocolDesyncError(
                    f"Response size {sz} exceeds RESPONSE_ZONE_SIZE ({RESPONSE_ZONE_SIZE})",
                    response_size=sz,
                )
            if sz > max_response_size:
                self._mark_dead()
                raise ProtocolDesyncError(
                    f"Response size {sz} exceeds available SHM space ({max_response_size})",
                    response_size=sz,
                )

            # 3. Read Data from SHM
            self.shm.seek(self.response_offset)
            resp_buf = self.shm.read(sz)

            # Validate we got all the bytes (mmap.read can return fewer)
            if len(resp_buf) != sz:
                self._mark_dead()
                raise ProtocolDesyncError(
                    f"SHM read returned {len(resp_buf)} bytes, expected {sz}",
                    response_size=sz,
                )

            # 4. Parse FlatBuffer
            resp = Response.Response.GetRootAsResponse(resp_buf, 0)

            # Check if this is a callback request from Java
            if resp.IsCallback():
                try:
                    answered_with.append(self._handle_callback(resp))
                except (OSError, BrokenPipeError, ConnectionResetError) as e:
                    # Socket error while sending callback response
                    self._mark_dead()
                    raise ProtocolDesyncError(f"Socket error during callback: {e}")
                except BaseException:
                    # Interrupted before the callback's result went out; Java is
                    # still waiting for it
                    self._mark_dead()
                    raise
                # After handling callback, continue reading for the actual response
                continue

            return self._parse_response(resp)

    def _parse_response(self, resp) -> Any:
        """What a response means to the caller: its value, or its exception."""
        if resp.IsError():
            error_msg = resp.ErrorMsg().decode("utf-8")
            error_type_bytes = resp.ErrorType()
            error_type = (
                error_type_bytes.decode("utf-8")
                if error_type_bytes
                else "java.lang.RuntimeException"
            )
            self._raise_java_exception(error_type, error_msg)

        # A batch's sub-responses are unpacked by BatchContext
        batch = resp.Batch()
        if batch is not None:
            return batch

        # Arrow responses
        arrow_stream = resp.ArrowStream()
        if arrow_stream is not None:
            return arrow_stream.Offset(), arrow_stream.Length()

        arrow_batch = resp.ArrowBatch()
        if arrow_batch is not None and arrow_batch.NumRows() >= 0:
            return self._unpack_arrow_batch(arrow_batch)

        return self._unpack_value(resp.ReturnValType(), resp.ReturnVal())

    def _raise_java_exception(self, error_type: str, error_msg: str) -> None:
        """Raise the appropriate Python exception for a Java error."""
        _raise_java_exception_impl(error_type, error_msg)

    def _handle_callback(
        self,
        resp,
        reply: Callable[[Any, bool, str | None], None] | None = None,
        channel_call: ChannelCall | None = None,
        handler: Any = None,
    ):
        """Handle a callback invocation request from Java.

        Without `reply` the request came in band: the client's own request is
        waiting on this callback, and the answer goes back on its channel. The
        thread that runs the callback is recorded for as long as it runs, so
        that a Java call made from it is refused instead of deadlocking.

        With `reply` (result, is_error, error message) the request came on the
        callback connection, from some other Java thread, and `channel_call`
        is the call that thread is waiting on. The callback may call Java:
        its calls travel on the callback connection as part of that call.

        If callback_timeout is set, the callback is executed in a separate thread
        with a timeout to prevent hung callbacks from deadlocking the protocol.

        Returns the result that was sent to Java, if one was.
        """
        callback_id = resp.CallbackId()
        in_band = reply is None

        def answer(result: Any, is_error: bool, error_msg: str | None) -> None:
            if reply is None:
                self._send_callback_response(callback_id, result, is_error, error_msg)
            else:
                reply(result, is_error, error_msg)

        callback_fn = (
            handler if handler is not None else self._callbacks.get(callback_id)
        )

        def call() -> Any:
            # Unpacked even when the callback is gone (unregistered while Java
            # was calling it): the server registered the objects among the
            # arguments for this client, and their wrappers are what frees them
            args = [
                self._unpack_value(arg.ValType(), arg.Val())
                for arg in (
                    resp.CallbackArgs(i) for i in range(resp.CallbackArgsLength())
                )
            ]
            if callback_fn is None:
                raise LookupError(f"Callback {callback_id} not found")
            return _callback_target(callback_fn, resp.CallbackMethod())(*args)

        def run() -> Any:
            if not in_band:
                # In whatever thread runs the callback, so that its Java calls
                # find their route
                scope, token = _enter_callback(self, channel_call)
                try:
                    return call()
                finally:
                    _exit_callback(scope, token)
            # Everything from unpacking the arguments on counts as "inside the
            # callback": unpacking a map keyed by Java objects hashes them, which
            # is a Java call, and Java is already waiting on this callback.
            ident = threading.get_ident()
            self._callback_threads.add(ident)
            scope, token = _enter_callback(self)
            try:
                return call()
            finally:
                _exit_callback(scope, token)
                self._callback_threads.discard(ident)

        try:
            if self.callback_timeout is None:
                # No timeout - execute directly (not recommended but supported)
                result = run()
            else:
                # Execute with timeout using a thread
                result_holder: dict[str, Any] = {
                    "result": None,
                    "error": None,
                    "done": False,
                }

                def run_callback() -> None:
                    try:
                        result_holder["result"] = run()
                    except BaseException as e:  # noqa: BLE001 - reported to Java below
                        # BaseException too: a callback that calls sys.exit() or is
                        # interrupted has not returned a result, and reporting
                        # None as its result would be a lie
                        result_holder["error"] = e
                    finally:
                        result_holder["done"] = True

                thread = threading.Thread(target=run_callback, daemon=True)
                thread.start()
                thread.join(timeout=self.callback_timeout)

                if not result_holder["done"]:
                    # Timeout - callback is still running (possibly hung)
                    # We can't kill the thread, but we must respond to Java
                    answer(
                        None,
                        True,
                        f"CallbackTimeoutError: Callback execution exceeded {self.callback_timeout}s timeout",
                    )
                    return
                if result_holder["error"] is not None:
                    raise cast(BaseException, result_holder["error"])
                result = result_holder["result"]
            answer(result, False, None)
            return result
        except ReentrancyError as e:
            # Propagate reentrancy error with clear message
            answer(None, True, f"ReentrancyError: {e}")
        except Exception as e:  # noqa: BLE001 - any callback failure is forwarded to Java
            answer(None, True, str(e))
        except BaseException as e:
            # Ctrl-C or sys.exit() inside the callback. Java gets an answer so it
            # stops waiting, and the exception carries on in this thread.
            answer(None, True, f"{type(e).__name__}: {e}")
            raise

    def _serve_channel_callback(
        self, channel: CallbackChannel, resp, handler: Any = None
    ) -> None:
        """Run one callback that arrived on the callback connection.

        Runs on a thread of its own, started by the connection's reader, which
        has looked up the callback's `handler`.
        """
        callback_id = resp.CallbackId()
        call_id = resp.CallId()
        channel_call = channel.open_call(call_id)
        answered = False

        def reply(result: Any, is_error: bool, error_msg: str | None) -> None:
            nonlocal answered
            answered = True
            channel_call.answer(
                self._build_callback_response(
                    callback_id, result, is_error, error_msg, call_id
                )
            )

        try:
            self._handle_callback(resp, reply, channel_call, handler)
        except OSError as e:
            # The connection closed under the callback; Java was told by that
            logger.debug("Callback %d could not be answered: %s", callback_id, e)
        except BaseException as e:
            # A Java thread is waiting for this call and nothing else ends its wait
            if not answered:
                try:
                    reply(None, True, f"{type(e).__name__}: {e}")
                except OSError:
                    pass
            raise
        finally:
            channel_call.finish()
            self._release_on_channel(channel)

    def _release_on_channel(self, channel: CallbackChannel) -> None:
        """Send the queued object releases on the callback connection.

        They normally ride ahead of the client's next request. A client that
        only receives callbacks, or sits in one long request, makes none, and
        the arguments of every call would stay registered on the server.
        """
        frees = self._pending_frees
        while frees:
            ids = _take_free_chunk(frees)
            if not ids:
                return
            try:
                channel.send(_build_free_command(flatbuffers.Builder(64), ids))
            except OSError:
                # The connection is gone; the next request can still send them
                frees.extend(ids)
                return

    def _ensure_callback_channel(self) -> None:
        """Open the callback connection, the first time a callback is registered.

        The server opens a socket for this session and says where; whoever
        connects to it is this session's callback connection. Without it
        callbacks still work on the thread that runs the request, so a failure
        to open it is logged and not raised: the server tells any other thread
        that calls a callback why it cannot.
        """
        # Before waiting for the channel: inside a callback the request that is
        # waiting for it holds the channel
        self._check_reentrancy()
        # Likewise: a callback that came on the connection runs while the
        # channel may be held by the request waiting for it
        if self._callback_channel is not None:
            return
        # Under the lock: threads registering callbacks at the same time open
        # it once
        with self._exchange_lock:
            if self._callback_channel_tried:
                return
            builder = self._get_builder()
            Cmd.CommandStart(builder)
            Cmd.CommandAddAction(builder, Act.Action.OpenCallbackChannel)
            builder.Finish(Cmd.CommandEnd(builder))
            command = builder.Output()
            # Only after this point: a request that could not be sent at all
            # (a call from inside a callback) is worth trying again
            path: str | None = None
            try:
                path = cast(str, self._send_raw(command))
                self._callback_channel_tried = True
                self._callback_channel = CallbackChannel(
                    self, path, self.handshake_timeout
                )
            except (JavaException, OSError) as e:
                self._callback_channel_tried = True
                logger.warning(
                    "Could not open the callback connection%s: %s. Callbacks will "
                    "work only on the Java thread that runs the request.",
                    f" at {path}" if path else "",
                    e,
                )

    def _unpack_arrow_batch(self, arrow_batch) -> ArrowTableView:
        """Unpack an ArrowBatchDescriptor from Java into an ArrowTableView.

        This reconstructs an Arrow table from buffer descriptors in the payload
        shared memory zone. The buffers are wrapped as zero-copy PyArrow buffers.

        The returned ArrowTableView wraps the table and validates the arena epoch
        on access, preventing use of stale data after reset_payload_arena().

        Args:
            arrow_batch: ArrowBatchDescriptor FlatBuffer object

        Returns:
            ArrowTableView wrapping the reconstructed table with epoch validation
        """

        from gatun.arena import _validate_supported_schema, deserialize_schema

        schema_hash = arrow_batch.SchemaHash()
        descriptor_epoch = arrow_batch.ArenaEpoch()

        # Validate epoch to prevent use-after-reset corruption
        # Python and Java epochs should be synchronized via reset_payload_arena()
        if descriptor_epoch != self._arena_epoch:
            raise StaleArenaError(descriptor_epoch, self._arena_epoch)

        # Get or deserialize schema
        schema = self._arrow_schema_cache.get(schema_hash)
        if schema is None:
            schema_bytes_len = arrow_batch.SchemaBytesLength()
            if schema_bytes_len == 0:
                raise RuntimeError(
                    f"No schema available for hash {schema_hash} and none cached"
                )
            # Direct buffer slice - much faster than element-by-element access
            # Access the underlying FlatBuffer and slice the vector directly
            o = flatbuffers.number_types.UOffsetTFlags.py_type(
                arrow_batch._tab.Offset(_ARROW_BATCH_SCHEMA_BYTES_OFFSET)
            )
            start = arrow_batch._tab.Vector(o)
            schema_bytes = bytes(
                arrow_batch._tab.Bytes[start : start + schema_bytes_len]
            )
            schema = deserialize_schema(schema_bytes)
            # Validate schema doesn't contain unsupported types (e.g., dictionary)
            _validate_supported_schema(schema)
            self._arrow_schema_cache[schema_hash] = schema

        # The buffers are slices of a memoryview of the mapping. Each one holds a
        # buffer export, so the mapping cannot be released while a table built on
        # them is alive: close() then leaves it mapped until the table is
        # collected. A buffer made from a raw address would be left pointing at
        # unmapped memory, and reading the table would crash the process.
        assert self.shm is not None, "Not connected"
        payload = memoryview(self.shm)[self.payload_offset : self.response_offset]
        payload_size = len(payload)

        # Build PyArrow buffers from buffer descriptors
        buffers: list[pa.Buffer | None] = []
        for i in range(arrow_batch.BuffersLength()):
            buf_desc = arrow_batch.Buffers(i)
            offset = buf_desc.Offset()
            length = buf_desc.Length()

            if length == 0:
                # Zero-length buffer (e.g., no validity bitmap when no nulls)
                buffers.append(None)
            else:
                if offset < 0 or length < 0 or offset + length > payload_size:
                    raise ProtocolDesyncError(
                        f"Arrow buffer {i} lies outside the payload zone "
                        f"(offset {offset}, length {length})"
                    )
                # Zero-copy buffer backed by shared memory
                buffers.append(pa.py_buffer(payload[offset : offset + length]))

        # Build field nodes as (length, null_count) tuples
        field_nodes = []
        for i in range(arrow_batch.NodesLength()):
            node = arrow_batch.Nodes(i)
            field_nodes.append((node.Length(), node.NullCount()))

        # Reconstruct arrays from buffers using schema (handles nested types recursively)
        arrays = []
        buffer_idx = 0
        node_idx = 0

        for field in schema:
            arr, buffer_idx, node_idx = _reconstruct_array_recursive(
                field.type, buffers, field_nodes, buffer_idx, node_idx
            )
            arrays.append(arr)

        # Build table from arrays and wrap in epoch-validating view
        table = pa.Table.from_arrays(arrays, schema=schema)
        return ArrowTableView(table, descriptor_epoch, self)

    def _send_callback_response(
        self, callback_id: int, result, is_error: bool, error_msg: str | None
    ):
        """Send the result of an in-band callback back to Java, on the channel of
        the request that is waiting for it."""
        data = self._build_callback_response(callback_id, result, is_error, error_msg)
        # Java reads this as the answer it is waiting for and sends nothing back
        self._send_raw(data, expects_response=False)

    def _build_callback_response(
        self,
        callback_id: int,
        result,
        is_error: bool,
        error_msg: str | None,
        call_id: int = 0,
    ) -> bytes:
        """Build the CallbackResponse command that answers a callback.

        call_id is the call's ID on the callback connection; 0 in band.
        """
        builder = self._get_builder(large=True)

        # Build arguments: [result, is_error]
        arg_offsets = []

        # First arg: the result value
        result_offset = self._build_argument(builder, result)
        arg_offsets.append(result_offset)

        # Second arg: is_error flag
        error_offset = self._build_argument(builder, is_error)
        arg_offsets.append(error_offset)

        # Build arguments vector
        Cmd.CommandStartArgsVector(builder, len(arg_offsets))
        for offset in reversed(arg_offsets):
            builder.PrependUOffsetTRelative(offset)
        args_vec = builder.EndVector()

        # Build target_name (error message if error)
        # Truncate large error messages to prevent exceeding command zone size.
        # During fuzzing, callback exceptions can have huge repr() or stack traces.
        # If we don't truncate, the answer cannot be sent and
        # Java will be stuck waiting for a CallbackResponse that never arrives.
        target_name_off = None
        if error_msg:
            if len(error_msg) > MAX_CALLBACK_ERROR_SIZE:
                error_msg = error_msg[:MAX_CALLBACK_ERROR_SIZE] + "...<truncated>"
            target_name_off = self._create_string(builder, error_msg)

        Cmd.CommandStart(builder)
        Cmd.CommandAddAction(builder, Act.Action.CallbackResponse)
        Cmd.CommandAddTargetId(builder, callback_id)
        if target_name_off:
            Cmd.CommandAddTargetName(builder, target_name_off)
        Cmd.CommandAddArgs(builder, args_vec)
        Cmd.CommandAddCallId(builder, call_id)
        cmd_offset = Cmd.CommandEnd(builder)
        builder.Finish(cmd_offset)

        # The command zone in band, a frame on the callback connection: the
        # same size
        data = bytes(builder.Output())
        if len(data) > self.payload_offset and not is_error:
            # Java is waiting; it has to hear something that fits
            return self._build_callback_response(
                callback_id,
                None,
                True,
                f"Callback result is too large to return: {len(data):,} bytes "
                f"exceeds the {self.payload_offset:,} bytes an answer can take",
                call_id,
            )
        return data

    def create_object(self, class_name, *args):
        builder = self._get_builder(large=True)
        cls_off = self._create_string(builder, class_name)

        # Build argument tables (must be done before Command)
        arg_offsets = []
        for arg in args:
            arg_offset = self._build_argument(builder, arg)
            arg_offsets.append(arg_offset)

        # Build arguments vector
        args_vec = None
        if arg_offsets:
            Cmd.CommandStartArgsVector(builder, len(arg_offsets))
            for offset in reversed(arg_offsets):
                builder.PrependUOffsetTRelative(offset)
            args_vec = builder.EndVector()

        Cmd.CommandStart(builder)
        Cmd.CommandAddAction(builder, Act.Action.CreateObject)
        Cmd.CommandAddTargetName(builder, cls_off)
        if args_vec:
            Cmd.CommandAddArgs(builder, args_vec)
        cmd_offset = Cmd.CommandEnd(builder)
        builder.Finish(cmd_offset)

        # Pass bytes!
        return self._send_raw(builder.Output())

    def invoke_method(self, obj_id, method_name, *args):
        builder = self._get_builder(large=True)
        meth_off = self._create_string(builder, method_name)

        # Build argument tables (must be done before Command)
        arg_offsets = []
        for arg in args:
            arg_offset = self._build_argument(builder, arg)
            arg_offsets.append(arg_offset)

        # Build arguments vector
        args_vec = None
        if arg_offsets:
            Cmd.CommandStartArgsVector(builder, len(arg_offsets))
            for offset in reversed(arg_offsets):
                builder.PrependUOffsetTRelative(offset)
            args_vec = builder.EndVector()

        # Build Command
        Cmd.CommandStart(builder)
        Cmd.CommandAddAction(builder, Act.Action.InvokeMethod)
        Cmd.CommandAddTargetId(builder, obj_id)
        Cmd.CommandAddTargetName(builder, meth_off)
        if args_vec:
            Cmd.CommandAddArgs(builder, args_vec)

        cmd = Cmd.CommandEnd(builder)
        builder.Finish(cmd)

        return self._send_raw(builder.Output())

    def invoke_static_method(
        self, class_name, method_name, *args, return_object_ref: bool = False
    ):
        """Invoke a static method on a class.

        Args:
            class_name: Fully qualified class name (e.g., "java.lang.Integer")
            method_name: Method name (e.g., "parseInt")
            *args: Arguments to pass to the method
            return_object_ref: If True, return result as ObjectRef (no auto-conversion)
        """
        builder = self._get_builder(large=True)
        # Format: "fully.qualified.ClassName.methodName"
        full_name = f"{class_name}.{method_name}"
        name_off = self._create_string(builder, full_name)

        # Build argument tables (must be done before Command)
        arg_offsets = []
        for arg in args:
            arg_offset = self._build_argument(builder, arg)
            arg_offsets.append(arg_offset)

        # Build arguments vector
        args_vec = None
        if arg_offsets:
            Cmd.CommandStartArgsVector(builder, len(arg_offsets))
            for offset in reversed(arg_offsets):
                builder.PrependUOffsetTRelative(offset)
            args_vec = builder.EndVector()

        # Build Command
        Cmd.CommandStart(builder)
        Cmd.CommandAddAction(builder, Act.Action.InvokeStaticMethod)
        Cmd.CommandAddTargetName(builder, name_off)
        if args_vec:
            Cmd.CommandAddArgs(builder, args_vec)
        if return_object_ref:
            Cmd.CommandAddReturnObjectRef(builder, True)

        cmd = Cmd.CommandEnd(builder)
        builder.Finish(cmd)

        return self._send_raw(builder.Output())

    def get_field(self, obj_id, field_name):
        """Get a field value from a Java object.

        Args:
            obj_id: Object ID (or JavaObject instance)
            field_name: Name of the field to get
        """
        if isinstance(obj_id, JavaObject):
            obj_id = obj_id.object_id

        builder = self._get_builder()
        name_off = self._create_string(builder, field_name)

        Cmd.CommandStart(builder)
        Cmd.CommandAddAction(builder, Act.Action.GetField)
        Cmd.CommandAddTargetId(builder, obj_id)
        Cmd.CommandAddTargetName(builder, name_off)
        cmd = Cmd.CommandEnd(builder)
        builder.Finish(cmd)

        return self._send_raw(builder.Output())

    def set_field(self, obj_id, field_name, value):
        """Set a field value on a Java object.

        Args:
            obj_id: Object ID (or JavaObject instance)
            field_name: Name of the field to set
            value: Value to set
        """
        if isinstance(obj_id, JavaObject):
            obj_id = obj_id.object_id

        builder = self._get_builder()
        name_off = self._create_string(builder, field_name)

        # Build argument for the value
        arg_offset = self._build_argument(builder, value)
        Cmd.CommandStartArgsVector(builder, 1)
        builder.PrependUOffsetTRelative(arg_offset)
        args_vec = builder.EndVector()

        Cmd.CommandStart(builder)
        Cmd.CommandAddAction(builder, Act.Action.SetField)
        Cmd.CommandAddTargetId(builder, obj_id)
        Cmd.CommandAddTargetName(builder, name_off)
        Cmd.CommandAddArgs(builder, args_vec)
        cmd = Cmd.CommandEnd(builder)
        builder.Finish(cmd)

        return self._send_raw(builder.Output())

    def get_static_field(self, class_name: str, field_name: str):
        """Get a static field value from a Java class.

        Args:
            class_name: Fully qualified class name (e.g., "java.lang.Integer")
            field_name: Name of the static field (e.g., "MAX_VALUE")

        Returns:
            The field value

        Example:
            >>> client.get_static_field("java.lang.Integer", "MAX_VALUE")
            2147483647
        """
        builder = self._get_builder()
        full_name = f"{class_name}.{field_name}"
        name_off = self._create_string(builder, full_name)

        Cmd.CommandStart(builder)
        Cmd.CommandAddAction(builder, Act.Action.GetStaticField)
        Cmd.CommandAddTargetName(builder, name_off)
        cmd = Cmd.CommandEnd(builder)
        builder.Finish(cmd)

        return self._send_raw(builder.Output())

    def set_static_field(self, class_name: str, field_name: str, value):
        """Set a static field value on a Java class.

        Args:
            class_name: Fully qualified class name
            field_name: Name of the static field
            value: Value to set
        """
        builder = self._get_builder()
        full_name = f"{class_name}.{field_name}"
        name_off = self._create_string(builder, full_name)

        # Build argument for the value
        arg_offset = self._build_argument(builder, value)
        Cmd.CommandStartArgsVector(builder, 1)
        builder.PrependUOffsetTRelative(arg_offset)
        args_vec = builder.EndVector()

        Cmd.CommandStart(builder)
        Cmd.CommandAddAction(builder, Act.Action.SetStaticField)
        Cmd.CommandAddTargetName(builder, name_off)
        Cmd.CommandAddArgs(builder, args_vec)
        cmd = Cmd.CommandEnd(builder)
        builder.Finish(cmd)

        return self._send_raw(builder.Output())

    def reflect(self, full_name: str) -> str:
        """Query Java to determine the type of a fully qualified name.

        This is used to distinguish between classes, methods, and fields
        when navigating the JVM via attribute access.

        Args:
            full_name: Fully qualified name to check
                      (e.g., "org.apache.spark.sql.functions" or
                       "org.apache.spark.sql.functions.col")

        Returns:
            One of:
            - "class" if the name refers to a loadable class (including Scala objects)
            - "method" if the last segment is a method on the parent class
            - "field" if the last segment is a field on the parent class
            - "none" if the name cannot be resolved

        Note:
            Results are typically cached by the caller for performance.
        """
        builder = self._get_builder()
        name_off = self._create_string(builder, full_name)

        Cmd.CommandStart(builder)
        Cmd.CommandAddAction(builder, Act.Action.Reflect)
        Cmd.CommandAddTargetName(builder, name_off)
        cmd = Cmd.CommandEnd(builder)
        builder.Finish(cmd)

        return cast(str, self._send_raw(builder.Output()))

    def _reflect_cached(self, full_name: str) -> str:
        """reflect(), remembering the answer.

        "none" is not remembered: it can mean a class that is not loadable yet.
        """
        cached = self._reflect_cache.get(full_name)
        if cached is None:
            cached = self.reflect(full_name)
            _remember_reflection(self._reflect_cache, full_name, cached)
        return cached

    def is_instance_of(self, obj, class_name: str) -> bool:
        """Check if a Java object is an instance of a class.

        This is equivalent to Java's `instanceof` operator. It checks if the
        object is an instance of the specified class or any of its subclasses.

        Args:
            obj: JavaObject instance or object ID
            class_name: Fully qualified Java class name
                       (e.g., "java.util.List", "java.util.ArrayList")

        Returns:
            True if the object is an instance of the specified class.

        Example:
            arr = client.create_object("java.util.ArrayList")
            client.is_instance_of(arr, "java.util.List")  # True
            client.is_instance_of(arr, "java.util.Map")   # False
        """
        if isinstance(obj, JavaObject):
            obj_id = obj.object_id
        else:
            obj_id = obj

        builder = self._get_builder()
        name_off = self._create_string(builder, class_name)

        Cmd.CommandStart(builder)
        Cmd.CommandAddAction(builder, Act.Action.IsInstanceOf)
        Cmd.CommandAddTargetId(builder, obj_id)
        Cmd.CommandAddTargetName(builder, name_off)
        cmd = Cmd.CommandEnd(builder)
        builder.Finish(cmd)

        return cast(bool, self._send_raw(builder.Output()))

    def ping(self) -> bool:
        """Check if the Java server is alive and responsive.

        This is a lightweight health check that sends a minimal request
        to the server and waits for a response. Useful for:
        - Verifying connection is still valid
        - Detecting stale connections before making expensive calls
        - Health checks in monitoring systems

        Returns:
            True if the server responded successfully.

        Raises:
            Various exceptions if the connection is dead or server unresponsive.

        Example:
            if client.ping():
                print("Server is alive")
        """
        builder = self._get_builder()

        Cmd.CommandStart(builder)
        Cmd.CommandAddAction(builder, Act.Action.Ping)
        cmd = Cmd.CommandEnd(builder)
        builder.Finish(cmd)

        return cast(bool, self._send_raw(builder.Output()))

    def get_metrics(self) -> str:
        """Get server metrics report.

        Returns a human-readable report of server performance metrics including:
        - Total request counts and error rates
        - Requests per second
        - Current and peak object counts
        - Arrow data transfer statistics
        - Callback statistics
        - Per-action latency percentiles (p50, p99)

        Returns:
            String containing the metrics report.

        Example:
            metrics = client.get_metrics()
            print(metrics)
        """
        builder = self._get_builder()

        Cmd.CommandStart(builder)
        Cmd.CommandAddAction(builder, Act.Action.GetMetrics)
        cmd = Cmd.CommandEnd(builder)
        builder.Finish(cmd)

        return cast(str, self._send_raw(builder.Output()))

    def get_fields(self, obj, field_names: list[str]) -> list[Any]:
        """Get multiple field values from a Java object in a single round-trip.

        This is more efficient than calling get_field() multiple times when
        you need to read several fields from the same object.

        Args:
            obj: JavaObject instance or object ID
            field_names: List of field names to read

        Returns:
            List of field values in the same order as field_names.

        Example:
            point = client.create_object("java.awt.Point", 10, 20)
            x, y = client.get_fields(point, ["x", "y"])  # Single round-trip
        """
        if isinstance(obj, JavaObject):
            obj_id = obj.object_id
        else:
            obj_id = obj

        builder = self._get_builder(large=True)

        # Build field names vector (strings must be created before the table)
        field_name_offsets = [
            self._create_string(builder, name) for name in field_names
        ]
        GetFieldsRequest.StartFieldNamesVector(builder, len(field_name_offsets))
        for offset in reversed(field_name_offsets):
            builder.PrependUOffsetTRelative(offset)
        field_names_vec = builder.EndVector()

        # Build GetFieldsRequest
        GetFieldsRequest.Start(builder)
        GetFieldsRequest.AddFieldNames(builder, field_names_vec)
        get_fields_off = GetFieldsRequest.End(builder)

        # Build Command
        Cmd.CommandStart(builder)
        Cmd.CommandAddAction(builder, Act.Action.GetFields)
        Cmd.CommandAddTargetId(builder, obj_id)
        Cmd.CommandAddGetFields(builder, get_fields_off)
        cmd = Cmd.CommandEnd(builder)
        builder.Finish(cmd)

        return cast(list[Any], self._send_raw(builder.Output()))

    def invoke_methods(
        self,
        obj,
        calls: list[tuple[str, tuple]],
        return_object_refs: bool | list[bool] = False,
    ) -> list[Any]:
        """Invoke multiple methods on the same Java object in a single round-trip.

        This is more efficient than calling invoke_method() multiple times when
        you need to call several methods on the same object.

        Args:
            obj: JavaObject instance or object ID
            calls: List of (method_name, args_tuple) pairs.
                   Each tuple is (method_name, (arg1, arg2, ...))
            return_object_refs: If True (or list of bools), return results as
                               ObjectRefs instead of auto-converting to Python.

        Returns:
            List of results in the same order as calls.

        Example:
            arr = client.create_object("java.util.ArrayList")
            # Add 3 items and get size in one round-trip
            results = client.invoke_methods(arr, [
                ("add", ("a",)),
                ("add", ("b",)),
                ("add", ("c",)),
                ("size", ()),
            ])
            # results = [True, True, True, 3]
        """
        if isinstance(obj, JavaObject):
            obj_id = obj.object_id
        else:
            obj_id = obj

        # Normalize return_object_refs to a list
        if isinstance(return_object_refs, bool):
            return_object_refs = [return_object_refs] * len(calls)

        builder = self._get_builder(large=True)

        # Build each MethodCall (must be done in reverse order for FlatBuffers)
        method_call_offsets = []
        for i, (method_name, args) in enumerate(calls):
            # Build method name string
            method_name_off = self._create_string(builder, method_name)

            # Build arguments
            arg_offsets = []
            for arg in args:
                arg_offset = self._build_argument(builder, arg)
                arg_offsets.append(arg_offset)

            args_vec = None
            if arg_offsets:
                MethodCall.StartArgsVector(builder, len(arg_offsets))
                for offset in reversed(arg_offsets):
                    builder.PrependUOffsetTRelative(offset)
                args_vec = builder.EndVector()

            # Build MethodCall
            MethodCall.Start(builder)
            MethodCall.AddMethodName(builder, method_name_off)
            if args_vec:
                MethodCall.AddArgs(builder, args_vec)
            if return_object_refs[i]:
                MethodCall.AddReturnObjectRef(builder, True)
            method_call_offsets.append(MethodCall.End(builder))

        # Build method calls vector
        InvokeMethodsRequest.StartMethodCallsVector(builder, len(method_call_offsets))
        for offset in reversed(method_call_offsets):
            builder.PrependUOffsetTRelative(offset)
        method_calls_vec = builder.EndVector()

        # Build InvokeMethodsRequest
        InvokeMethodsRequest.Start(builder)
        InvokeMethodsRequest.AddMethodCalls(builder, method_calls_vec)
        invoke_methods_off = InvokeMethodsRequest.End(builder)

        # Build Command
        Cmd.CommandStart(builder)
        Cmd.CommandAddAction(builder, Act.Action.InvokeMethods)
        Cmd.CommandAddTargetId(builder, obj_id)
        Cmd.CommandAddInvokeMethods(builder, invoke_methods_off)
        cmd = Cmd.CommandEnd(builder)
        builder.Finish(cmd)

        return cast(list[Any], self._send_raw(builder.Output()))

    def create_objects(self, specs: list[tuple[str, tuple]]) -> list[JavaObject]:
        """Create multiple Java objects in a single round-trip.

        This is more efficient than calling create_object() multiple times when
        you need to create several objects.

        Args:
            specs: List of (class_name, args_tuple) pairs.
                   Each tuple is (fully_qualified_class_name, (arg1, arg2, ...))

        Returns:
            List of JavaObject instances in the same order as specs.

        Example:
            # Create 3 ArrayLists in one round-trip
            lists = client.create_objects([
                ("java.util.ArrayList", ()),
                ("java.util.ArrayList", (100,)),  # with initial capacity
                ("java.util.HashMap", ()),
            ])
        """
        builder = self._get_builder(large=True)

        # Build each ObjectSpec
        object_spec_offsets = []
        for class_name, args in specs:
            # Build class name string
            class_name_off = self._create_string(builder, class_name)

            # Build arguments
            arg_offsets = []
            for arg in args:
                arg_offset = self._build_argument(builder, arg)
                arg_offsets.append(arg_offset)

            args_vec = None
            if arg_offsets:
                ObjectSpec.StartArgsVector(builder, len(arg_offsets))
                for offset in reversed(arg_offsets):
                    builder.PrependUOffsetTRelative(offset)
                args_vec = builder.EndVector()

            # Build ObjectSpec
            ObjectSpec.Start(builder)
            ObjectSpec.AddClassName(builder, class_name_off)
            if args_vec:
                ObjectSpec.AddArgs(builder, args_vec)
            object_spec_offsets.append(ObjectSpec.End(builder))

        # Build objects vector
        CreateObjectsRequest.StartObjectsVector(builder, len(object_spec_offsets))
        for offset in reversed(object_spec_offsets):
            builder.PrependUOffsetTRelative(offset)
        objects_vec = builder.EndVector()

        # Build CreateObjectsRequest
        CreateObjectsRequest.Start(builder)
        CreateObjectsRequest.AddObjects(builder, objects_vec)
        create_objects_off = CreateObjectsRequest.End(builder)

        # Build Command
        Cmd.CommandStart(builder)
        Cmd.CommandAddAction(builder, Act.Action.CreateObjects)
        Cmd.CommandAddCreateObjects(builder, create_objects_off)
        cmd = Cmd.CommandEnd(builder)
        builder.Finish(cmd)

        return cast(list["JavaObject"], self._send_raw(builder.Output()))

    def register_callback(self, callback_fn: Any, interface_name: str) -> JavaObject:
        """Register a Python callable, or object, as a Java interface implementation.

        This creates a Java dynamic proxy that implements the specified interface.
        When Java code calls methods on this proxy, the calls are forwarded to
        Python.

        Args:
            callback_fn: A callable, which receives the arguments of whichever
                        method of the interface Java calls and returns its
                        result. Or, for an interface with several methods, an
                        object that is not callable: each call goes to its
                        method of the same name (onQueryStarted, onQueryProgress).
            interface_name: Fully qualified Java interface name
                           (e.g., "java.util.function.Function")

        Returns:
            A JavaObject representing the proxy that implements the interface.
            The callback lasts until unregister_callback(), or until neither
            this handle nor any Java code refers to the proxy: keep the handle
            when what you give the proxy to holds it only weakly.

        Example:
            # Create a Comparator for sorting
            def my_compare(a, b):
                return a - b

            comparator = client.register_callback(my_compare, "java.util.Comparator")
            arr = client.create_object("java.util.ArrayList")
            arr.add(3)
            arr.add(1)
            arr.add(2)
            client.invoke_static_method("java.util.Collections", "sort", arr, comparator)
        """
        command = _finish_register_callback(
            self._get_builder(), interface_name, callback_fn
        )
        # This client calls the function and sends back what it returns: a
        # coroutine would go unawaited
        for target in (
            [callback_fn]
            if callable(callback_fn)
            else [getattr(callback_fn, name) for name in _callback_methods(callback_fn)]
        ):
            if inspect.iscoroutinefunction(target):
                raise TypeError(
                    f"{getattr(target, '__qualname__', target)} is a coroutine "
                    "function, which only the async client (aconnect) can run "
                    "as a callback"
                )

        # Java may call this callback from any thread from here on
        self._ensure_callback_channel()

        # Send command and get back the proxy object
        result: JavaObject = cast("JavaObject", self._send_raw(command))

        # Get the callback_id from the response - it's stored as the object_id
        # We need to also store our callback function
        # The Java side assigns callback_id = object_id for simplicity
        callback_id = result.object_id
        self._callbacks[callback_id] = callback_fn

        return result

    def unregister_callback(self, callback: JavaObject | int) -> None:
        """Unregister a previously registered callback.

        Args:
            callback: What register_callback() returned, or any other handle
                on the same Java object, such as one read back out of a Java
                collection. Or the object ID of one, while the handle is alive.
        """
        # `callback` stays referenced until the server has answered: a handle
        # that dies is queued for release, the queue is sent ahead of the next
        # command, and the server could not tell what a released ID had named
        callback_id = (
            callback.object_id if isinstance(callback, JavaObject) else callback
        )
        builder = self._get_builder()
        Cmd.CommandStart(builder)
        Cmd.CommandAddAction(builder, Act.Action.UnregisterCallback)
        Cmd.CommandAddTargetId(builder, callback_id)
        cmd = Cmd.CommandEnd(builder)
        builder.Finish(cmd)

        try:
            unregistered = self._send_raw(builder.Output())
        except BaseException:
            # A request that was refused must not leave Java calling a function
            # that is gone. On a client that is closed or dead the session is
            # over, and its callbacks with it.
            if self._closed or self._dead:
                self._callbacks.pop(callback_id, None)
            raise
        self._callbacks.pop(callback_id, None)
        # The callback's own ID, when it was named by another handle
        if isinstance(unregistered, int):
            self._callbacks.pop(unregistered, None)

    def free_object(self, object_id):
        """Release a Java object.

        Sent to the server straight away, except inside a callback made by the
        thread that runs the client's request, where it goes out with the next
        request. The server ignores an unknown or already-freed ID. JavaObject
        wrappers release themselves when garbage collected, so this is only
        needed for objects that were detach()ed.
        """
        if self._dead or self.sock is None or self.sock.fileno() == -1:
            return
        self._pending_frees.append(object_id)
        call = _channel_call_of(self)
        if call is not None:
            # The client's own channel may be held by the request that is
            # waiting for this callback
            self._release_on_channel(call.channel)
        elif threading.get_ident() not in self._callback_threads:
            try:
                with self._exchange_lock:
                    self._flush_pending_frees()
            except OSError:
                pass

    @_holding_channel
    def send_arrow_table(self, table):
        """Send an Arrow table to Java via shared memory.

        The data flow is:
        1. Python serializes the table as an Arrow IPC stream straight into
           shared memory (one pass over the data)
        2. Java reads the stream from shared memory

        Java only counts the rows. To hand the table to Java code use
        arrow_to_java(); to have Java read the buffers in place use
        send_arrow_buffers().
        """
        self._write_table_to_payload(table, 0, "Arrow batch")

        # Build Command
        builder = self._get_builder()
        Cmd.CommandStart(builder)
        Cmd.CommandAddAction(builder, Act.Action.SendArrowBatch)
        command = Cmd.CommandEnd(builder)
        builder.Finish(command)

        return self._send_raw(builder.Output())

    def send_arrow_table_batched(self, table, batch_size=None):
        """Send a large Arrow table in batches that fit in the payload zone.

        Args:
            table: PyArrow Table to send
            batch_size: Number of rows per batch. If None, automatically
                       calculated to fit within the payload zone.

        Returns:
            List of responses from each batch sent.

        Example:
            # Send a large table in batches
            responses = client.send_arrow_table_batched(large_table)

            # Or specify a custom batch size
            responses = client.send_arrow_table_batched(large_table, batch_size=10000)
        """
        max_payload_size = self.response_offset - self.payload_offset
        total_rows = table.num_rows

        if total_rows == 0:
            return [self.send_arrow_table(table)]

        if batch_size is None:
            # Sized from the whole table, not its first rows: rows differ in size,
            # and a sample of small ones gives batches too large to send
            bytes_per_row = max(1.0, table.nbytes / total_rows)
            batch_size = max(1, int(max_payload_size * 0.8 / bytes_per_row))

        responses = []
        start = 0
        while start < total_rows:
            count = min(batch_size, total_rows - start)
            while True:
                try:
                    responses.append(self.send_arrow_table(table.slice(start, count)))
                    break
                except PayloadTooLargeError:
                    # These rows are larger than the average. Nothing was sent
                    # (the size is checked first), so send fewer of them.
                    if count == 1:
                        raise
                    count //= 2
            start += count

        return responses

    @_holding_channel
    def send_arrow_buffers(
        self,
        table: pa.Table | pa.RecordBatch,
        arena: PayloadArena,
        schema_cache: dict[int, bool] | None = None,
    ):
        """Send an Arrow table or record batch via true zero-copy buffer transfer.

        This method copies Arrow buffers directly into the payload shared memory
        arena and sends buffer descriptors to Java, enabling Java to read the
        data in-place without any additional copies.

        Data flow:
        1. Python copies Arrow buffers into payload shm (one copy)
        2. Python sends buffer descriptors (offsets/lengths) to Java
        3. Java wraps the shm buffers directly as ArrowBuf (zero-copy read)

        Args:
            table: PyArrow Table to send
            arena: PayloadArena for buffer allocation
            schema_cache: Optional dict to track which schemas Java has seen.
                         If provided and schema hash is in cache, schema bytes
                         are omitted from the message.

        Returns:
            Response from Java after processing the batch.

        Raises:
            PayloadTooLargeError: If the table is too large for the arena's
                                 available space. The error includes the
                                 estimated size and available space.
            UnsupportedArrowTypeError: If the table contains unsupported types
                                      (e.g., dictionary encoding).

        Example:
            arena = client.get_payload_arena()
            schema_cache = {}

            # Send multiple tables, reusing the arena
            for table in tables:
                arena.reset()  # Reset for each batch
                client.send_arrow_buffers(table, arena, schema_cache)

            arena.close()
        """
        from gatun.arena import (
            compute_schema_hash,
            copy_arrow_table_to_arena,
            estimate_arrow_size,
            serialize_schema,
        )

        if getattr(arena, "_mmap", None) is not self.shm:
            # Java reads the buffers out of this client's shared memory, at the
            # offsets the arena hands out. Offsets into any other arena would be
            # read as whatever happens to be at those places.
            raise ValueError(
                "send_arrow_buffers needs the arena from client.get_payload_arena(); "
                "this arena is not part of the client's shared memory"
            )

        if isinstance(table, pa.RecordBatch):
            # Wrapping a batch in a Table shares its buffers; nothing is copied
            table = pa.Table.from_batches([table])

        # 0. Pre-check available space to provide informative error
        estimated = estimate_arrow_size(table)
        available = arena.bytes_available()
        if estimated > available:
            raise PayloadTooLargeError(
                estimated,
                available,
                f"Arrow table with {table.num_rows} rows. "
                f"Consider resetting arena or using streaming for large tables.",
            )

        # 1. Copy table buffers into the arena
        buffer_infos, field_nodes = copy_arrow_table_to_arena(table, arena)
        loaded_end = max(
            (info.offset + info.length for info in buffer_infos), default=0
        )
        self._note_payload_write(loaded_end)

        # 2. Compute schema hash and check cache
        schema_hash = compute_schema_hash(table.schema)
        include_schema = schema_cache is None or schema_hash not in schema_cache

        # 3. Build FlatBuffers command
        builder = self._get_builder(large=True)

        # Build schema bytes vector if needed
        schema_bytes_vec = None
        if include_schema:
            schema_bytes = serialize_schema(table.schema)
            schema_bytes_vec = builder.CreateByteVector(schema_bytes)

        # Build buffer descriptors
        buffer_offsets = []
        for info in buffer_infos:
            BufferDescriptor.Start(builder)
            BufferDescriptor.AddOffset(builder, info.offset)
            BufferDescriptor.AddLength(builder, info.length)
            buffer_offsets.append(BufferDescriptor.End(builder))

        # Build buffers vector
        ArrowBatchDescriptor.StartBuffersVector(builder, len(buffer_offsets))
        for offset in reversed(buffer_offsets):
            builder.PrependUOffsetTRelative(offset)
        buffers_vec = builder.EndVector()

        # Build field nodes
        node_offsets = []
        for length, null_count in field_nodes:
            FieldNode.Start(builder)
            FieldNode.AddLength(builder, length)
            FieldNode.AddNullCount(builder, null_count)
            node_offsets.append(FieldNode.End(builder))

        # Build nodes vector
        ArrowBatchDescriptor.StartNodesVector(builder, len(node_offsets))
        for offset in reversed(node_offsets):
            builder.PrependUOffsetTRelative(offset)
        nodes_vec = builder.EndVector()

        # Build ArrowBatchDescriptor
        # direction: 0 = Python->Java (default), 1 = Java->Python
        ArrowBatchDescriptor.Start(builder)
        ArrowBatchDescriptor.AddSchemaHash(builder, schema_hash)
        if schema_bytes_vec is not None:
            ArrowBatchDescriptor.AddSchemaBytes(builder, schema_bytes_vec)
        ArrowBatchDescriptor.AddNumRows(builder, table.num_rows)
        ArrowBatchDescriptor.AddNodes(builder, nodes_vec)
        ArrowBatchDescriptor.AddBuffers(builder, buffers_vec)
        ArrowBatchDescriptor.AddArenaEpoch(builder, self._arena_epoch)
        ArrowBatchDescriptor.AddDirection(builder, 0)  # Python -> Java
        batch_descriptor = ArrowBatchDescriptor.End(builder)

        # Build Command
        Cmd.CommandStart(builder)
        Cmd.CommandAddAction(builder, Act.Action.SendArrowBuffers)
        Cmd.CommandAddArrowBatch(builder, batch_descriptor)
        cmd = Cmd.CommandEnd(builder)
        builder.Finish(cmd)

        # 4. Send via standard path
        response = self._send_raw(builder.Output())

        # Only now does the server have the schema: a rejected send must not leave
        # the cache claiming it does, or every later send would omit it.
        if include_schema and schema_cache is not None:
            schema_cache[schema_hash] = True
        self._arrow_loaded_end = loaded_end
        return response

    @_holding_channel
    def reset_payload_arena(self):
        """Signal Java to reset its view of the payload arena.

        Call this after resetting the Python PayloadArena to ensure Java
        releases any references to the old buffer contents.

        WARNING: After calling this, any ArrowTableView objects from previous
        get_arrow_data() calls become invalid. Accessing them will raise
        StaleArenaError. Copy the data you need before resetting.
        """
        # Outstanding ArrowTableViews are invalid from here on, whatever happens
        self._end_views()

        builder = self._get_builder()

        Cmd.CommandStart(builder)
        Cmd.CommandAddAction(builder, Act.Action.ResetPayloadArena)
        cmd = Cmd.CommandEnd(builder)
        builder.Finish(cmd)

        response = self._send_raw(builder.Output())
        # Java has advanced its epoch and let go of the batch. Advancing ours
        # before knowing that would leave the two apart after a failed reset, and
        # every later send would be refused as stale.
        self._arena_epoch += 1
        self._arrow_loaded_end = 0
        return response

    def _note_payload_write(self, end: int) -> None:
        """Record that the payload zone is about to be overwritten up to offset `end`.

        Tables from get_arrow_data() point into the second half of the zone. A
        write that reaches it invalidates them, so that they raise
        StaleArenaError instead of returning whatever was written over them.
        Tables read in place by arrow_from_java(copy=False) end with any write.

        Call it before the first byte is written: a write that fails halfway
        has still changed the zone.
        """
        self._payload_writes += 1
        if end > (self.response_offset - self.payload_offset) // 2:
            self._payload_generation += 1

    def _end_views(self) -> None:
        """Invalidate every outstanding ArrowTableView, wherever it points."""
        self._payload_writes += 1
        self._payload_generation += 1

    def _write_table_to_payload(
        self, table: pa.Table, offset: int, zone: str = "Arrow table"
    ) -> int:
        """Write a table's IPC stream into the payload zone; returns its length.

        A table that does not fit is refused with nothing written and no view
        disturbed. Otherwise the views the stream lands on end before its first
        byte is written.
        """
        size = _arrow_stream_size(table)
        _require_payload_room(self, size, offset, zone)
        self._note_payload_write(offset + size)
        _write_arrow_table(self, table, offset, size, zone)
        return size

    @_holding_channel
    def get_arrow_data(self) -> ArrowTableView:
        """Retrieve Arrow data from Java via zero-copy transfer.

        This method requests the current Arrow data held by the Java server
        (e.g., data previously sent via send_arrow_buffers). Java writes the
        Arrow buffers to the payload shared memory zone and sends buffer
        descriptors back, which are used to reconstruct the table in Python.

        Returns:
            ArrowTableView wrapping the reconstructed table. The view validates
            the arena epoch on access - if reset_payload_arena() is called after
            receiving this table, accessing data will raise StaleArenaError.

            To safely use data after reset, copy it first:
            - table_view.to_pandas() -> pandas DataFrame
            - table_view.to_pydict() -> Python dict
            - table_view.to_pylist() -> list of dicts

        Raises:
            RuntimeError: If no Arrow data is available on the Java side.
            StaleArenaError: If accessing data after reset_payload_arena().

        Example:
            # Send data to Java
            client.send_arrow_buffers(table, arena, schema_cache)

            # Get it back (useful for testing or round-trip operations)
            table_view = client.get_arrow_data()

            # Safe: copy data before resetting
            data = table_view.to_pydict()
            arena.reset()
            client.reset_payload_arena()

            # Unsafe: would raise StaleArenaError after reset
            # table_view.column("x")  # Don't do this!
        """
        builder = self._get_builder()

        Cmd.CommandStart(builder)
        Cmd.CommandAddAction(builder, Act.Action.GetArrowData)
        cmd = Cmd.CommandEnd(builder)
        builder.Finish(cmd)

        # Java writes over the region earlier views point at as soon as it has
        # the command, whether or not its answer arrives. The view made from the
        # answer is the only current one.
        self._end_views()
        return cast(ArrowTableView, self._send_raw(builder.Output()))

    def arrow_to_java(self, table: pa.Table | pa.RecordBatch) -> JavaObject:
        """Copy an Arrow table into the JVM and return a handle to it.

        The handle refers to an org.apache.arrow.vector.VectorSchemaRoot that owns
        its memory. Pass it to any Java method that takes one, call its methods,
        or get a table back with arrow_from_java(). Java releases the table's
        memory when the handle is garbage collected (or the connection closes),
        so Java code that needs the data for longer must copy it.

        The table is sent in one piece and must fit in the payload zone; connect
        with more memory, or send it in slices, if it does not. It is written
        into shared memory as it is serialized, and Java copies it out from there.

        Inside a callback that Java made from another thread the table goes
        through shared memory of that call's own, as large as the client's, so
        it does not wait for the request the client may be running.

        Args:
            table: A pyarrow Table or RecordBatch.

        Returns:
            JavaObject wrapping the VectorSchemaRoot.

        Raises:
            PayloadTooLargeError: If the table does not fit in shared memory.
            UnsupportedArrowTypeError: If a column is dictionary-encoded.

        Example:
            root = client.arrow_to_java(pa.table({"x": [1, 2, 3]}))
            root.getRowCount()                      # 3
            client.jvm.com.example.Stats.sum(root)  # your own Java code
        """
        table = _arrow_table_to_send(table)
        call = _channel_call_of(self)
        if call is not None:
            routed, result = self._arrow_to_java_in_call(call, table)
            if routed:
                return cast(JavaObject, result)
        return cast(JavaObject, self._arrow_to_java_on_channel(table))

    @_holding_channel
    def _arrow_to_java_on_channel(self, table: pa.Table) -> JavaObject:
        # Stay clear of a batch Java is still reading in place
        offset = (self._arrow_loaded_end + 63) & ~63
        size = self._write_table_to_payload(table, offset)

        command = _finish_arrow_to_object(self._get_builder(), offset, size)
        return cast(JavaObject, self._send_raw(command))

    def _call_memory(self, call: ChannelCall) -> CallMemory | None:
        """The call's shared memory, which Java creates when first asked.

        None if Java is no longer waiting for the callback. The caller holds
        the call's transfer lock.
        """
        memory = call.memory
        if memory is None:
            routed, path = self._send_nested(call, _OPEN_CALL_MEMORY)
            if not routed:
                return None
            try:
                memory = CallMemory(path, call.transfer_lock)
            except FileNotFoundError:
                # Java stopped waiting for the callback after it replied, and
                # removed the file
                call.abandon()
                return None
            if call.active:
                call.memory = memory
        elif memory.removed():
            # Java stopped waiting for the callback (its thread was
            # interrupted) and this callback has not sent a command since
            call.abandon()
            call.memory = None
            return None
        return memory

    def _arrow_to_java_in_call(
        self, call: ChannelCall, table: pa.Table
    ) -> tuple[bool, Any]:
        """arrow_to_java() inside a callback that came on the callback connection.

        The table goes through the call's own memory. Returns (False, None),
        like _send_nested, if Java is no longer waiting for the callback: the
        table then belongs on the client's own channel.
        """
        self._require_usable()
        self._check_reentrancy()
        with call.transfer_lock:
            memory = self._call_memory(call)
            if memory is None:
                return False, None
            size = _arrow_stream_size(table)
            _require_payload_room(memory, size, 0)
            # Tables read in place there end before the first byte is written
            memory._payload_writes += 1
            _write_arrow_table(memory, table, 0, size)
            command = _finish_arrow_to_object(self._get_builder(), 0, size)
            return self._send_nested(call, command)

    def _arrow_from_java_in_call(
        self, call: ChannelCall, object_id: int, copy: bool
    ) -> tuple[bool, Any]:
        """arrow_from_java() inside a callback that came on the callback connection."""
        self._require_usable()
        self._check_reentrancy()
        with call.transfer_lock:
            memory = self._call_memory(call)
            if memory is None:
                return False, None
            # Java writes at the front of the memory as soon as it has the
            # command, so tables read in place there end now
            memory._payload_writes += 1
            command = _finish_arrow_from_object(self._get_builder(), object_id)
            routed, location = self._send_nested(call, command)
            if not routed:
                return False, None
            if copy:
                return True, _read_arrow_stream(memory, location)
            borrowed = _read_arrow_stream(memory, location, in_place=True)
            view = ArrowTableView(borrowed, memory._arena_epoch, memory, any_write=True)
            return True, view

    @overload
    def arrow_from_java(
        self, table: JavaObject | int, *, copy: Literal[True] = True
    ) -> pa.Table: ...

    @overload
    def arrow_from_java(
        self, table: JavaObject | int, *, copy: Literal[False]
    ) -> ArrowTableView: ...

    @overload
    def arrow_from_java(
        self, table: JavaObject | int, *, copy: bool
    ) -> pa.Table | ArrowTableView: ...

    def arrow_from_java(
        self, table: JavaObject | int, *, copy: bool = True
    ) -> pa.Table | ArrowTableView:
        """Fetch an Arrow table held by a Java object.

        Works for any org.apache.arrow.vector.VectorSchemaRoot the JVM holds:
        one made by arrow_to_java(), or one that Java code produced. The result
        is an ordinary pyarrow Table that owns its data.

        The Java table is left as it is. One that Java code created is that
        code's to release; call close() on the handle when you are done with it.

        With copy=False the table is not copied out of shared memory: you get
        an ArrowTableView whose table reads the bytes where Java wrote them.
        That saves a pass over the data, and the view is good only until this
        client's next Arrow transfer of any kind (or its close()); after that
        it raises StaleArenaError. Use it to read a result
        once, for example to aggregate it or to convert it with to_pandas(),
        and keep copy=True for a table you hold on to. A pyarrow Table taken
        from the view (view.table) is not checked: reading it after the next
        transfer returns whatever was written over it.

        Inside a callback that Java made from another thread the table comes
        through shared memory of that call's own, and a view of it is good
        until the callback's next Arrow transfer.

        Args:
            table: JavaObject (or object ID) of a VectorSchemaRoot.
            copy: False to read the table in place rather than copy it.

        Returns:
            A pyarrow Table, or an ArrowTableView with copy=False.

        Raises:
            PayloadTooLargeError: If the table does not fit in shared memory.
                Connect with more memory, or fetch root.slice(start, length)
                pieces.
            JavaIllegalArgumentException: If the object is not a VectorSchemaRoot.

        Example:
            result = client.jvm.com.example.Reports.build()  # a VectorSchemaRoot
            table = client.arrow_from_java(result)
            result.close()

            # Read once, in place
            total = pc.sum(client.arrow_from_java(result, copy=False).column("x"))
        """
        object_id = table.object_id if isinstance(table, JavaObject) else table
        call = _channel_call_of(self)
        if call is not None:
            routed, result = self._arrow_from_java_in_call(call, object_id, copy)
            if routed:
                return cast("pa.Table | ArrowTableView", result)
        return self._arrow_from_java_on_channel(object_id, copy)

    @_holding_channel
    def _arrow_from_java_on_channel(
        self, object_id: int, copy: bool
    ) -> pa.Table | ArrowTableView:
        command = _finish_arrow_from_object(self._get_builder(), object_id)
        # Java starts writing at the front of the free part of the zone as soon
        # as it has the command, so tables read in place there end now
        self._payload_writes += 1
        try:
            offset, length = self._send_raw(command)
        except BaseException:
            # No telling how far Java got: it fills what room there is before
            # finding a table too large, and a timeout leaves it writing
            self._end_views()
            raise
        self._note_payload_write(offset + length)
        if copy:
            return _read_arrow_stream(self, (offset, length))
        borrowed = _read_arrow_stream(self, (offset, length), in_place=True)
        return ArrowTableView(borrowed, self._arena_epoch, self, any_write=True)

    def cancel(self) -> bool:
        """Interrupt the request this client is running. Call it from another thread.

        The request's thread is interrupted on the server. Java code that is
        blocked in an interruptible call (sleeping, waiting, most I/O, a Spark
        job) stops there; code that polls ``GatunServer.checkCancelled()`` or
        ``Thread.interrupted()`` stops at its next check; code that does neither
        runs to completion. A cancelled request raises CancelledException in the
        thread that made it.

        The signal travels on a short-lived connection of its own, because the
        client's channel is occupied by the request being cancelled.

        Returns:
            True if a request was running and was signalled; False if there was
            nothing to cancel.

        Example:
            worker = threading.Thread(target=lambda: client.jvm.my.Job.run())
            worker.start()
            client.cancel()
        """
        if self._dead or self._closed or self.memory_path is None:
            return False
        control = GatunClient(
            self.socket_path,
            socket_timeout=self.handshake_timeout,
            handshake_timeout=self.handshake_timeout,
        )
        try:
            if not control.connect():
                return False
            builder = flatbuffers.Builder(256)
            session = builder.CreateString(self.memory_path)
            Cmd.CommandStart(builder)
            Cmd.CommandAddAction(builder, Act.Action.Cancel)
            Cmd.CommandAddTargetName(builder, session)
            builder.Finish(Cmd.CommandEnd(builder))
            return control._send_raw(builder.Output()) is True
        finally:
            control.close()

    def batch(self, stop_on_error: bool = False) -> BatchContext:
        """Create a batch context for executing multiple commands in one round-trip.

        Batching amortizes the per-call overhead (~100-150μs) across multiple
        operations. Commands are queued locally and sent together when the
        context exits or execute() is called.

        Args:
            stop_on_error: If True, stop executing on first error.
                          If False (default), continue and collect all results/errors.

        Returns:
            BatchContext that can be used as a context manager.

        Example:
            # Context manager (auto-execute on exit):
            with client.batch() as b:
                r1 = b.call(arr, "add", 1)
                r2 = b.call(arr, "add", 2)
                r3 = b.call(arr, "size")
            print(r3.get())  # 2

            # Manual execute:
            batch = client.batch()
            r1 = batch.call(arr, "add", 1)
            r2 = batch.call_static("java.lang.Math", "max", 10, 20)
            batch.execute()
            print(r2.get())  # 20

            # Stop on error:
            with client.batch(stop_on_error=True) as b:
                r1 = b.call(arr, "add", 1)
                r2 = b.call(invalid_obj, "bad_method")  # Will error
                r3 = b.call(arr, "size")  # Skipped if stop_on_error=True
        """
        return BatchContext(self, stop_on_error)

    def close(self):
        self._closed = True
        try:
            # The socket goes first and on its own: it is what ends the session on
            # the server, and a failure to release the mapping must not keep it open.
            sock, self.sock = self.sock, None
            if sock is not None:
                try:
                    sock.close()
                except OSError:
                    pass
            channel, self._callback_channel = self._callback_channel, None
            if channel is not None:
                channel.close()
            _close_shared_memory(self)
        finally:
            # A server started for this client by connect() goes away with it
            session, self._server_session = self._server_session, None
            if session is not None:
                session.stop()

    def __enter__(self):
        # Only connect if not already connected
        if self.shm is None and not self.connect():
            raise RuntimeError(
                f"Failed to connect to Gatun server at {self.socket_path}: "
                f"{self.connect_error}"
            ) from self.connect_error
        return self

    def __exit__(self, _exc_type, _exc_val, _exc_tb):
        self.close()
        return False


class BatchResult:
    """Lazy result placeholder that resolves after batch.execute().

    BatchResult objects are returned when queueing commands in a BatchContext.
    The actual result is not available until execute() is called.
    """

    __slots__ = ("_batch", "_index")

    def __init__(self, batch: BatchContext, index: int):
        self._batch = batch
        self._index = index

    def get(self):
        """Get the result value.

        Returns:
            The result of the command (same as calling the method directly).

        Raises:
            RuntimeError: If execute() has not been called yet.
            JavaException: If the command raised a Java exception.
        """
        if not self._batch._executed:
            raise RuntimeError("Must call batch.execute() before getting results")
        if self._batch._error is not None:
            raise RuntimeError(
                f"The batch failed as a whole: {self._batch._error}"
            ) from self._batch._error
        result = self._batch._results[self._index]
        if isinstance(result, Exception):
            raise result
        return result

    @property
    def is_error(self) -> bool:
        """Check if this result is an error.

        Raises:
            RuntimeError: If execute() has not been called yet.
        """
        if not self._batch._executed:
            raise RuntimeError("Must call batch.execute() before checking errors")
        if self._batch._error is not None:
            return True
        return isinstance(self._batch._results[self._index], Exception)


class BatchContext:
    """Context manager for batching multiple commands into a single round-trip.

    Commands queued in a batch are sent together when execute() is called
    (or automatically when exiting a context manager). This reduces per-call
    overhead from ~100-150μs to ~10μs per operation.

    Attributes:
        stop_on_error: If True, Java stops executing after first error.
    """

    def __init__(self, client: GatunClient, stop_on_error: bool = False):
        self._client = client
        self._stop_on_error = stop_on_error
        self._command_data: list[tuple[int, int, str | None, tuple]] = []
        # Targets of queued commands. Only their IDs go into _command_data, so
        # without this a target whose last reference is dropped inside the batch
        # would be freed before the batch runs.
        self._targets: list[JavaObject] = []
        self._results: list = []
        self._executed = False
        # Set when the batch was sent and no results came back
        self._error: BaseException | None = None

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None and not self._executed:
            self.execute()
        return False

    def create(self, class_name: str, *args) -> BatchResult:
        """Queue object creation.

        Args:
            class_name: Fully qualified Java class name.
            *args: Constructor arguments.

        Returns:
            BatchResult that will contain the JavaObject after execute().
        """
        idx = len(self._command_data)
        self._command_data.append((Act.Action.CreateObject, 0, class_name, args))
        return BatchResult(self, idx)

    def call(self, obj: JavaObject, method_name: str, *args) -> BatchResult:
        """Queue an instance method call.

        Args:
            obj: JavaObject to call method on.
            method_name: Name of the method to call.
            *args: Method arguments.

        Returns:
            BatchResult that will contain the return value after execute().
        """
        idx = len(self._command_data)
        self._targets.append(obj)
        self._command_data.append(
            (Act.Action.InvokeMethod, obj.object_id, method_name, args)
        )
        return BatchResult(self, idx)

    def call_static(self, class_name: str, method_name: str, *args) -> BatchResult:
        """Queue a static method call.

        Args:
            class_name: Fully qualified Java class name.
            method_name: Name of the static method to call.
            *args: Method arguments.

        Returns:
            BatchResult that will contain the return value after execute().
        """
        idx = len(self._command_data)
        full_name = f"{class_name}.{method_name}"
        self._command_data.append((Act.Action.InvokeStaticMethod, 0, full_name, args))
        return BatchResult(self, idx)

    def get_field(self, obj: JavaObject, field_name: str) -> BatchResult:
        """Queue a field get operation.

        Args:
            obj: JavaObject to get field from.
            field_name: Name of the field.

        Returns:
            BatchResult that will contain the field value after execute().
        """
        idx = len(self._command_data)
        self._targets.append(obj)
        self._command_data.append((Act.Action.GetField, obj.object_id, field_name, ()))
        return BatchResult(self, idx)

    def set_field(self, obj: JavaObject, field_name: str, value) -> BatchResult:
        """Queue a field set operation.

        Args:
            obj: JavaObject to set field on.
            field_name: Name of the field.
            value: Value to set.

        Returns:
            BatchResult (value will be None after execute()).
        """
        idx = len(self._command_data)
        self._targets.append(obj)
        self._command_data.append(
            (Act.Action.SetField, obj.object_id, field_name, (value,))
        )
        return BatchResult(self, idx)

    def execute(self) -> list:
        """Execute all queued commands in a single round-trip.

        Returns:
            List of results in the same order as commands were queued.
            Errors are stored as Exception objects in the list.

        Raises:
            RuntimeError: If execute() was already called.
        """
        if self._executed:
            return self._results

        if not self._command_data:
            self._executed = True
            return []

        # A batch is a request like any other: not allowed while Java is waiting
        # for a callback's result. Checked before building, which uses the
        # client's shared builder.
        self._client._check_reentrancy()

        # Build the batch command
        builder = self._client._get_builder(large=True)

        # Build all sub-commands first (must be done before BatchCommand)
        sub_cmd_offsets = []
        for action, target_id, target_name, args in self._command_data:
            # Build argument tables
            arg_offsets = []
            for arg in args:
                arg_offsets.append(self._client._build_argument(builder, arg))

            # Build arguments vector
            args_vec = None
            if arg_offsets:
                Cmd.CommandStartArgsVector(builder, len(arg_offsets))
                for offset in reversed(arg_offsets):
                    builder.PrependUOffsetTRelative(offset)
                args_vec = builder.EndVector()

            # Build target name string
            name_off = None
            if target_name:
                name_off = self._client._create_string(builder, target_name)

            # Build sub-command
            Cmd.CommandStart(builder)
            Cmd.CommandAddAction(builder, action)
            if target_id:
                Cmd.CommandAddTargetId(builder, target_id)
            if name_off:
                Cmd.CommandAddTargetName(builder, name_off)
            if args_vec:
                Cmd.CommandAddArgs(builder, args_vec)
            sub_cmd_offsets.append(Cmd.CommandEnd(builder))

        # Build commands vector for BatchCommand
        BatchCommand.StartCommandsVector(builder, len(sub_cmd_offsets))
        for offset in reversed(sub_cmd_offsets):
            builder.PrependUOffsetTRelative(offset)
        commands_vec = builder.EndVector()

        # Build BatchCommand
        BatchCommand.Start(builder)
        BatchCommand.AddCommands(builder, commands_vec)
        BatchCommand.AddStopOnError(builder, self._stop_on_error)
        batch_cmd_off = BatchCommand.End(builder)

        # Build outer Command with Action.Batch
        Cmd.CommandStart(builder)
        Cmd.CommandAddAction(builder, Act.Action.Batch)
        Cmd.CommandAddBatch(builder, batch_cmd_off)
        cmd = Cmd.CommandEnd(builder)
        builder.Finish(cmd)

        # Nothing has been sent up to here, so a failure above (an argument that
        # cannot be encoded, say) leaves the batch as it was. From here on it
        # counts as executed whatever happens: Java may have run some of it.
        data = builder.Output()
        if len(data) > self._client.payload_offset:
            raise PayloadTooLargeError(
                len(data), self._client.payload_offset, "Command"
            )
        call = _channel_call_of(self._client)
        if call is not None:
            # On the callback connection the command travels wrapped, in a
            # frame with a limit of its own: refused here, it was not sent
            _nested_command(call.call_id, data)

        self._executed = True
        try:
            # The ordinary exchange: it serves callbacks that the batched calls
            # trigger, and marks the connection dead if it is interrupted
            batch_resp = self._client._send_raw(data)
        except BaseException as e:
            self._error = e
            raise
        finally:
            self._targets.clear()
        if not isinstance(batch_resp, BatchResponse.BatchResponse):
            self._error = RuntimeError("Expected BatchResponse but got something else")
            raise self._error

        _ = batch_resp.ErrorIndex()  # Available for debugging
        response_count = batch_resp.ResponsesLength()

        # Unpack each sub-response
        for i in range(response_count):
            sub_resp = batch_resp.Responses(i)
            if sub_resp.IsError():
                error_msg = sub_resp.ErrorMsg().decode("utf-8")
                error_type_bytes = sub_resp.ErrorType()
                error_type = (
                    error_type_bytes.decode("utf-8")
                    if error_type_bytes
                    else "java.lang.RuntimeException"
                )
                # Store exception object instead of raising
                exc = self._make_exception(error_type, error_msg)
                self._results.append(exc)
            else:
                value = self._client._unpack_value(
                    sub_resp.ReturnValType(), sub_resp.ReturnVal()
                )
                self._results.append(value)

        # Fill remaining slots with None if stopped early
        while len(self._results) < len(self._command_data):
            self._results.append(None)

        return self._results

    def _make_exception(self, error_type: str, error_msg: str) -> Exception:
        """Create an exception object without raising it."""
        import re

        # Handle special exception types
        if error_type == "org.gatun.PayloadTooLargeException":
            match = re.search(r"(\d+) bytes exceeds (\d+) byte", error_msg)
            if match:
                return PayloadTooLargeError(
                    int(match.group(1)), int(match.group(2)), "Response"
                )
            return PayloadTooLargeError(0, 0, "Response")

        if error_type == "java.lang.InterruptedException":
            match = re.search(r"Request (\d+) was cancelled", error_msg)
            return CancelledException(int(match.group(1)) if match else None)

        # Get exception class
        exc_class = _JAVA_EXCEPTION_MAP.get(error_type, JavaException)

        # Extract message
        lines = error_msg.split("\n")
        message = lines[0] if lines else error_msg

        return exc_class(error_type, message, error_msg)
