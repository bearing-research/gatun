"""How Arrow arrays are laid out in the payload arena for the buffer transfer.

Java wraps the arena's buffers per Arrow Java's TypeLayout and reads each one from
its first byte, so what is written has to be exactly the rows of the table, in the
buffers Java expects. The first half of this file checks that with pyarrow alone;
the second half sends the same shapes through a server and reads them back.
"""

import uuid

import pyarrow as pa
import pytest

from gatun import UnsupportedArrowTypeError, estimate_arrow_size
from gatun.arena import (
    PayloadArena,
    _check_type_recursive,
    _compact_dense_union,
    _copy_table,
    copy_arrow_table_to_arena,
)


class _PointType(pa.ExtensionType):
    """An extension type with nested storage that pyarrow does not know by name."""

    def __init__(self):
        super().__init__(pa.struct([("x", pa.int32()), ("label", pa.string())]), "p")

    def __arrow_ext_serialize__(self):
        return b""

    @classmethod
    def __arrow_ext_deserialize__(cls, storage_type, serialized):
        return cls()


def _points() -> pa.ExtensionArray:
    storage = pa.array(
        [{"x": 1, "label": "a"}, {"x": 2, "label": "b"}, {"x": 3, "label": "c"}],
        type=_PointType().storage_type,
    )
    return pa.ExtensionArray.from_storage(_PointType(), storage)


def _dense_union() -> pa.UnionArray:
    """[1, "a", 2, "b"], with each child holding exactly the values in use."""
    return pa.UnionArray.from_dense(
        pa.array([0, 1, 0, 1], type=pa.int8()),
        pa.array([0, 0, 1, 1], type=pa.int32()),
        [pa.array([1, 2], type=pa.int64()), pa.array(["a", "b"])],
    )


def _sparse_union() -> pa.UnionArray:
    """[1, "b", 3]"""
    return pa.UnionArray.from_sparse(
        pa.array([0, 1, 0], type=pa.int8()),
        [pa.array([1, 2, 3], type=pa.int64()), pa.array(["a", "b", "c"])],
    )


def _struct_with_sliced_child() -> pa.StructArray:
    """A struct with offset 0 whose child starts two rows into its buffer."""
    child = pa.array([100, 200, 300, 400, 500], type=pa.int32()).slice(2, 3)
    return pa.StructArray.from_arrays([child], names=["c"])


def _list_with_sliced_values() -> pa.ListArray:
    """[[4, 5], [6, 7, 8, 9]], with the values starting four rows into their buffer."""
    values = pa.array(range(10), type=pa.int64()).slice(4, 6)
    return pa.ListArray.from_arrays(pa.array([0, 2, 6], type=pa.int32()), values)


# --- Reading the arena the way Java does ---


def _java_buffer_count(arrow_type: pa.DataType) -> int:
    """How many buffers Arrow Java's TypeLayout gives a type, children excluded."""
    types = pa.types
    if types.is_null(arrow_type):
        return 0
    if types.is_union(arrow_type):
        return 2 if arrow_type.mode == "dense" else 1
    if types.is_struct(arrow_type) or types.is_fixed_size_list(arrow_type):
        return 1
    if (
        types.is_list(arrow_type)
        or types.is_large_list(arrow_type)
        or types.is_map(arrow_type)
    ):
        return 2
    if (
        types.is_string(arrow_type)
        or types.is_large_string(arrow_type)
        or types.is_binary(arrow_type)
        or types.is_large_binary(arrow_type)
    ):
        return 3
    return 2


class _JavaReader:
    """Rebuilds arrays from the arena under the rules Arrow Java loads them by.

    A fixed number of buffers per type, field nodes and buffers in depth-first
    order, every buffer read from its first byte, and a dense union's children
    exactly as long as the number of slots that use them.
    """

    def __init__(self, arena: PayloadArena, infos, nodes):
        assert arena._mmap is not None
        self._buffers = [
            pa.py_buffer(arena._mmap[info.offset : info.offset + info.length])
            for info in infos
        ]
        self._nodes = list(nodes)

    def read(self, arrow_type: pa.DataType) -> pa.Array:
        if isinstance(arrow_type, pa.BaseExtensionType):
            storage = self.read(arrow_type.storage_type)
            return pa.ExtensionArray.from_storage(arrow_type, storage)

        length, null_count = self._nodes.pop(0)
        own: list[pa.Buffer | None] = [
            self._buffers.pop(0) for _ in range(_java_buffer_count(arrow_type))
        ]
        types = pa.types
        if types.is_null(arrow_type):
            return pa.nulls(length)

        if types.is_union(arrow_type):
            children = [
                self.read(arrow_type.field(i).type)
                for i in range(arrow_type.num_fields)
            ]
            if arrow_type.mode == "dense":
                type_ids = list(own[0].to_pybytes()[:length])
                for code, child in zip(arrow_type.type_codes, children):
                    assert len(child) == type_ids.count(code)
            # pyarrow keeps a validity slot for unions that the wire format lacks
            own = [None, *own]
        else:
            if own[0].size == 0:
                own[0] = None  # no validity bitmap: every row is valid
            if types.is_struct(arrow_type):
                child_types = [
                    arrow_type.field(i).type for i in range(arrow_type.num_fields)
                ]
            elif types.is_map(arrow_type):
                key = pa.field("key", arrow_type.key_type, nullable=False)
                child_types = [pa.struct([key, ("value", arrow_type.item_type)])]
            elif (
                types.is_list(arrow_type)
                or types.is_large_list(arrow_type)
                or types.is_fixed_size_list(arrow_type)
            ):
                child_types = [arrow_type.value_type]
            else:
                child_types = []
            children = [self.read(child_type) for child_type in child_types]

        return pa.Array.from_buffers(
            arrow_type, length, own, null_count, children=children
        )

    def exhausted(self) -> bool:
        return not self._buffers and not self._nodes


def _read_back(table: pa.Table, tmp_path) -> pa.Table:
    """Copy a table into an arena and rebuild it from what was written."""
    with PayloadArena.create(tmp_path / "arena.shm", 1 << 20) as arena:
        infos, nodes = copy_arrow_table_to_arena(table, arena)
        reader = _JavaReader(arena, infos, nodes)
        arrays = [reader.read(field.type) for field in table.schema]
        # Java refuses a batch with more or fewer buffers than its schema calls for
        assert reader.exhausted()
        return pa.Table.from_arrays(arrays, schema=table.schema)


# --- Offsets below the top level ---


def test_struct_child_with_an_offset_writes_only_its_rows(tmp_path):
    table = pa.table({"s": _struct_with_sliced_child()})

    with PayloadArena.create(tmp_path / "arena.shm", 1 << 20) as arena:
        infos, nodes = copy_arrow_table_to_arena(table, arena)

        assert nodes == [(3, 0), (3, 0)]
        data = infos[-1]
        assert arena._mmap is not None
        written = arena._mmap[data.offset : data.offset + data.length]
        assert pa.py_buffer(written).equals(
            pa.array([300, 400, 500], type=pa.int32()).buffers()[1]
        )


def _map_array() -> pa.MapArray:
    return pa.array(
        [[("a", 1)], [("b", 2), ("c", 3)], [], [("d", 4)]],
        type=pa.map_(pa.string(), pa.int32()),
    )


@pytest.mark.parametrize(
    "make_array",
    [
        pytest.param(_struct_with_sliced_child, id="struct_child"),
        pytest.param(_list_with_sliced_values, id="list_values"),
        pytest.param(
            lambda: pa.LargeListArray.from_arrays(
                pa.array([0, 1, 3], type=pa.int64()),
                pa.array(["u", "v", "w", "x"]).slice(1, 3),
            ),
            id="large_list_values",
        ),
        pytest.param(
            lambda: pa.FixedSizeListArray.from_arrays(
                pa.array(range(10), type=pa.int32()).slice(2, 6), 2
            ),
            id="fixed_size_list_values",
        ),
        pytest.param(
            lambda: pa.FixedSizeListArray.from_arrays(
                pa.array(range(10), type=pa.int32()), 2
            ).slice(1, 3),
            id="fixed_size_list_sliced",
        ),
        pytest.param(lambda: _map_array().slice(1, 2), id="map_sliced"),
        pytest.param(
            lambda: pa.StructArray.from_arrays(
                [_list_with_sliced_values(), pa.array(["p", None])], names=["l", "s"]
            ),
            id="struct_of_list_values",
        ),
        pytest.param(
            lambda: pa.ListArray.from_arrays(
                pa.array([0, 1, 3], type=pa.int32()), _struct_with_sliced_child()
            ),
            id="list_of_struct_child",
        ),
        pytest.param(
            lambda: pa.UnionArray.from_sparse(
                pa.array([0, 1, 0], type=pa.int8()),
                [
                    pa.array([9, 1, 2, 3], type=pa.int64()).slice(1, 3),
                    pa.array(["a", "b", "c"]),
                ],
            ),
            id="sparse_union_child",
        ),
        pytest.param(
            lambda: pa.UnionArray.from_dense(
                pa.array([0, 1, 0, 1], type=pa.int8()),
                pa.array([0, 0, 1, 1], type=pa.int32()),
                [
                    pa.array([7, 1, 2], type=pa.int64()).slice(1, 2),
                    pa.array(["z", "a", "b"]).slice(1, 2),
                ],
            ),
            id="dense_union_child",
        ),
        pytest.param(
            lambda: pa.StructArray.from_arrays([_points().slice(1, 2)], names=["e"]),
            id="struct_of_extension",
        ),
        pytest.param(
            lambda: pa.array([None, 1, None, 4, 5], type=pa.int32()).slice(1, 3),
            id="top_level_with_nulls",
        ),
    ],
)
def test_offsets_at_any_depth_are_removed(tmp_path, make_array):
    table = pa.table({"col": make_array()})

    assert _read_back(table, tmp_path).to_pylist() == table.to_pylist()


def test_a_slice_from_row_zero_still_fits_its_estimate(tmp_path):
    """Such a slice has no offset anywhere, only buffers longer than it needs."""
    child = pa.array(range(50_000), type=pa.int64())
    table = pa.table({"s": pa.StructArray.from_arrays([child], names=["c"])})
    head = table.slice(0, 10)

    with PayloadArena.create(tmp_path / "arena.shm", 1 << 20) as arena:
        copy_arrow_table_to_arena(head, arena)

        assert arena.bytes_used() <= estimate_arrow_size(head) < 1000


# --- Unions ---


def test_dense_union_writes_type_ids_then_offsets(tmp_path):
    union = _dense_union()
    table = pa.table({"u": union})

    with PayloadArena.create(tmp_path / "arena.shm", 1 << 20) as arena:
        infos, nodes = copy_arrow_table_to_arena(table, arena)

        assert arena._mmap is not None
        type_ids, offsets = (
            arena._mmap[info.offset : info.offset + info.length] for info in infos[:2]
        )
        assert type_ids == bytes([0, 1, 0, 1])
        assert pa.py_buffer(offsets).equals(union.offsets.buffers()[1])
        # union, int64 child, string child
        assert nodes == [(4, 0), (2, 0), (2, 0)]
        assert len(infos) == 2 + 2 + 3


def test_sparse_union_writes_type_ids(tmp_path):
    table = pa.table({"u": _sparse_union()})

    with PayloadArena.create(tmp_path / "arena.shm", 1 << 20) as arena:
        infos, nodes = copy_arrow_table_to_arena(table, arena)

        assert arena._mmap is not None
        type_ids = arena._mmap[infos[0].offset : infos[0].offset + infos[0].length]
        assert type_ids == bytes([0, 1, 0])
        assert nodes == [(3, 0), (3, 0), (3, 0)]
        assert len(infos) == 1 + 2 + 3


@pytest.mark.parametrize(
    "make_union",
    [
        pytest.param(_dense_union, id="dense"),
        pytest.param(_sparse_union, id="sparse"),
        pytest.param(lambda: _dense_union().slice(1, 2), id="dense_sliced"),
        pytest.param(lambda: _dense_union().slice(0, 1), id="dense_head"),
        pytest.param(lambda: _sparse_union().slice(1, 2), id="sparse_sliced"),
        pytest.param(
            # Children hold values no slot uses, and the slots use them out of order
            lambda: pa.UnionArray.from_dense(
                pa.array([0, 1, 0], type=pa.int8()),
                pa.array([3, 1, 0], type=pa.int32()),
                [pa.array([10, 11, 12, 13], type=pa.int64()), pa.array(["p", "q"])],
            ),
            id="dense_with_unused_values",
        ),
        pytest.param(
            lambda: pa.UnionArray.from_dense(
                pa.array([7, 3, 7], type=pa.int8()),
                pa.array([0, 0, 1], type=pa.int32()),
                [pa.array([1.5, 2.5]), pa.array([True])],
                ["f", "b"],
                [7, 3],
            ),
            id="dense_with_type_codes",
        ),
    ],
)
def test_union_values_survive(tmp_path, make_union):
    """The reader also holds a dense union's children to the length Java gives them."""
    table = pa.table({"u": make_union(), "x": pa.array(range(len(make_union())))})

    assert _read_back(table, tmp_path).to_pylist() == table.to_pylist()


def test_dense_union_nested_in_a_slice(tmp_path):
    union = _dense_union()
    struct = pa.StructArray.from_arrays([union, pa.array([1, 2, 3, 4])], ["u", "v"])
    table = pa.table({"s": struct.slice(2)})

    assert _read_back(table, tmp_path).to_pylist() == [
        {"s": {"u": 2, "v": 3}},
        {"s": {"u": "b", "v": 4}},
    ]


def test_dense_union_already_compact_is_not_rebuilt():
    union = _dense_union()

    assert _compact_dense_union(union) is union


# --- Null columns ---


def test_null_column_has_a_field_node_and_no_buffers(tmp_path):
    table = pa.table({"n": pa.nulls(3)})

    with PayloadArena.create(tmp_path / "arena.shm", 1 << 20) as arena:
        infos, nodes = copy_arrow_table_to_arena(table, arena)

    assert infos == []
    assert nodes == [(3, 3)]


def test_null_child_between_other_columns(tmp_path):
    struct = pa.StructArray.from_arrays(
        [pa.nulls(2), pa.array(["a", "b"])], names=["n", "s"]
    )
    table = pa.table({"before": [1, 2], "s": struct, "after": [3.0, 4.0]})

    assert _read_back(table, tmp_path).to_pylist() == table.to_pylist()


# --- Tables without chunks ---


@pytest.mark.parametrize(
    "arrow_type",
    [
        pa.int64(),
        pa.string(),
        pa.null(),
        pa.list_(pa.int32()),
        pa.struct([("a", pa.int32()), ("b", pa.string())]),
        pa.map_(pa.string(), pa.int32()),
        _dense_union().type,
        _sparse_union().type,
        pa.uuid(),
    ],
    ids=str,
)
def test_table_with_no_chunks(tmp_path, arrow_type):
    schema = pa.schema([("x", arrow_type), ("y", pa.float32())])
    table = pa.Table.from_batches([], schema)
    assert table.column("x").num_chunks == 0

    result = _read_back(table, tmp_path)

    assert result.num_rows == 0
    assert result.schema == schema


# --- Types the buffer transfer cannot carry ---


@pytest.mark.parametrize(
    ("arrow_type", "reported"),
    [
        (pa.string_view(), "col: string_view"),
        (pa.binary_view(), "col: binary_view"),
        (pa.list_(pa.string_view()), "col[]: string_view"),
        (pa.struct([("v", pa.binary_view())]), "col.v: binary_view"),
        (pa.list_view(pa.int32()), "col: list_view"),
        (pa.large_list_view(pa.int32()), "col: large_list_view"),
        (pa.run_end_encoded(pa.int32(), pa.string()), "col: run_end_encoded"),
        (pa.dictionary(pa.int32(), pa.string()), "col: dictionary"),
    ],
    ids=lambda value: str(value) if isinstance(value, pa.DataType) else "",
)
def test_unsupported_type_is_refused_before_anything_is_written(
    tmp_path, arrow_type, reported
):
    table = pa.Table.from_batches(
        [], pa.schema([("ok", pa.int64()), ("col", arrow_type)])
    )

    with PayloadArena.create(tmp_path / "arena.shm", 1 << 20) as arena:
        with pytest.raises(UnsupportedArrowTypeError) as raised:
            copy_arrow_table_to_arena(table, arena)

        assert reported in str(raised.value)
        assert "ok:" not in str(raised.value)
        assert arena.bytes_used() == 0


def test_string_view_data_is_refused(tmp_path):
    values = ["a", "bb", "a string longer than twelve bytes"]
    table = pa.table({"v": pa.array(values, type=pa.string_view())})

    with (
        PayloadArena.create(tmp_path / "arena.shm", 1 << 20) as arena,
        pytest.raises(UnsupportedArrowTypeError, match="v: string_view"),
    ):
        copy_arrow_table_to_arena(table, arena)


def test_dictionary_check_reports_only_dictionaries():
    """arrow_to_java() uses this check alone: the IPC stream it sends can carry
    view types, so they must not be reported here."""
    schema = pa.schema(
        [
            ("v", pa.string_view()),
            ("d", pa.dictionary(pa.int8(), pa.string())),
            ("l", pa.list_(pa.dictionary(pa.int8(), pa.string()))),
        ]
    )
    found: list[str] = []

    _check_type_recursive(schema, found)

    assert [entry.split(":")[0] for entry in found] == ["d", "l[]"]


# --- Extension types ---


def test_extension_column_is_written_as_its_storage(tmp_path):
    table = pa.table({"e": _points()})

    with PayloadArena.create(tmp_path / "arena.shm", 1 << 20) as arena:
        infos, nodes = copy_arrow_table_to_arena(table, arena)

    # struct, then its int32 and string children
    assert nodes == [(3, 0), (3, 0), (3, 0)]
    assert len(infos) == 1 + 2 + 3


@pytest.mark.parametrize(
    "make_array",
    [
        pytest.param(_points, id="struct_storage"),
        pytest.param(lambda: _points().slice(1, 2), id="struct_storage_sliced"),
        pytest.param(
            lambda: pa.array(['{"a": 1}', "[]", None], type=pa.json_()),
            id="string_storage",
        ),
        pytest.param(
            lambda: pa.array([uuid.UUID(int=1).bytes, None], type=pa.uuid()),
            id="fixed_size_binary_storage",
        ),
    ],
)
def test_extension_values_survive(tmp_path, make_array):
    table = pa.table({"e": make_array()})

    assert _read_back(table, tmp_path).to_pylist() == table.to_pylist()


def test_extension_over_an_unsupported_storage_is_refused(tmp_path):
    class Tagged(pa.ExtensionType):
        def __init__(self):
            super().__init__(pa.dictionary(pa.int8(), pa.string()), "tagged")

        def __arrow_ext_serialize__(self):
            return b""

        @classmethod
        def __arrow_ext_deserialize__(cls, storage_type, serialized):
            return cls()

    storage = pa.array(["a", "b", "a"]).dictionary_encode().cast(Tagged().storage_type)
    table = pa.table({"t": pa.ExtensionArray.from_storage(Tagged(), storage)})

    with (
        PayloadArena.create(tmp_path / "arena.shm", 1 << 20) as arena,
        pytest.raises(UnsupportedArrowTypeError, match="t: dictionary"),
    ):
        copy_arrow_table_to_arena(table, arena)


# --- Copying a received table ---


def test_copy_keeps_the_schema():
    schema = pa.schema(
        [
            pa.field("i", pa.int32(), nullable=False),
            ("f", pa.float32()),
            ("ts", pa.timestamp("s", tz="UTC")),
            ("all_null", pa.string()),
            ("l", pa.list_(pa.int16())),
        ],
        metadata={"origin": "test"},
    )
    table = pa.table(
        {
            "i": pa.array([1, 2], type=pa.int32()),
            "f": pa.array([1.5, None], type=pa.float32()),
            "ts": pa.array([1, 2], type=pa.timestamp("s", tz="UTC")),
            "all_null": pa.array([None, None], type=pa.string()),
            "l": pa.array([[1], []], type=pa.list_(pa.int16())),
        },
        schema=schema,
    )

    for source in (table, table.slice(0, 0)):
        copied = _copy_table(source)

        assert copied.schema.equals(schema, check_metadata=True)
        assert copied.to_pylist() == source.to_pylist()


def test_copy_does_not_share_memory_with_its_source():
    memory = bytearray(pa.array([1, 2, 3], type=pa.int32()).buffers()[1].to_pybytes())
    source = pa.table(
        {"x": pa.Array.from_buffers(pa.int32(), 3, [None, pa.py_buffer(memory)])}
    )

    copied = _copy_table(source)
    memory[0:4] = b"\xff\xff\xff\x7f"

    assert source.column("x")[0].as_py() == 2**31 - 1  # the source did change
    assert copied.to_pydict() == {"x": [1, 2, 3]}


# --- Through a server: Java loads the buffers and writes them back ---


def _round_trip(client, table: pa.Table) -> pa.Table:
    """Send a table through the buffer transfer and return what Java hands back."""
    arena = client.get_payload_arena()
    try:
        response = client.send_arrow_buffers(table, arena, {})
        assert f"Received {table.num_rows} rows" in response
        return _copy_table(client.get_arrow_data().table)
    finally:
        arena.close()


@pytest.mark.parametrize(
    "make_array",
    [
        pytest.param(_struct_with_sliced_child, id="struct_child"),
        pytest.param(_list_with_sliced_values, id="list_values"),
        pytest.param(lambda: _map_array().slice(1, 2), id="map_sliced"),
        pytest.param(
            lambda: pa.ListArray.from_arrays(
                pa.array([0, 1, 3], type=pa.int32()), _struct_with_sliced_child()
            ),
            id="list_of_struct_child",
        ),
    ],
)
def test_java_reads_nested_offsets(client, make_array):
    table = pa.table({"col": make_array()})

    received = _round_trip(client, table)

    assert received.schema == table.schema
    assert received.to_pylist() == table.to_pylist()


@pytest.mark.parametrize(
    "make_union",
    [
        pytest.param(_dense_union, id="dense"),
        pytest.param(_sparse_union, id="sparse"),
        pytest.param(lambda: _dense_union().slice(1, 2), id="dense_sliced"),
        pytest.param(lambda: _sparse_union().slice(1, 2), id="sparse_sliced"),
        pytest.param(
            lambda: pa.UnionArray.from_dense(
                pa.array([0, 1, 0], type=pa.int8()),
                pa.array([3, 1, 0], type=pa.int32()),
                [pa.array([10, 11, 12, 13], type=pa.int64()), pa.array(["p", "q"])],
            ),
            id="dense_with_unused_values",
        ),
        pytest.param(
            lambda: pa.StructArray.from_arrays(
                [_dense_union(), pa.array([1, 2, 3, 4])], ["u", "v"]
            ).slice(2),
            id="dense_in_sliced_struct",
        ),
    ],
)
def test_java_reads_unions(client, make_union):
    table = pa.table({"u": make_union()})

    received = _round_trip(client, table)

    assert received.schema == table.schema
    assert received.to_pylist() == table.to_pylist()


def test_java_accepts_a_null_column(client):
    struct = pa.StructArray.from_arrays([pa.nulls(3), pa.array([1, 2, 3])], ["n", "v"])
    table = pa.table({"n": pa.nulls(3), "x": [1, 2, 3], "s": struct})

    received = _round_trip(client, table)

    assert received.schema == table.schema
    assert received.to_pylist() == table.to_pylist()


def test_java_accepts_a_table_with_no_chunks(client):
    schema = pa.schema([("x", pa.int64()), ("y", pa.float32())])
    table = pa.Table.from_batches([], schema)

    received = _round_trip(client, table)

    assert received.schema == schema
    assert received.num_rows == 0


def test_java_reads_an_extension_column_by_its_storage(client):
    """pyarrow does not know this extension type by name, so what comes back is
    the storage with the extension's name in the field metadata."""
    table = pa.table({"e": _points().slice(1, 2)})

    received = _round_trip(client, table)

    field = received.schema.field("e")
    assert field.type == _PointType().storage_type
    assert field.metadata[b"ARROW:extension:name"] == b"p"
    assert received.column("e").to_pylist() == table.column("e").to_pylist()


def test_java_returns_a_known_extension_type(client):
    ids = [uuid.UUID(int=1).bytes, uuid.UUID(int=2).bytes]
    table = pa.table({"id": pa.array(ids, type=pa.uuid())})

    received = _round_trip(client, table)

    assert received.schema == table.schema
    assert received.to_pylist() == table.to_pylist()


def test_java_returns_a_known_extension_type_over_strings(client):
    table = pa.table({"doc": pa.array(['{"a": 1}', "[]"], type=pa.json_())})

    received = _round_trip(client, table)

    assert received.schema == table.schema
    assert received.to_pylist() == table.to_pylist()


def test_string_view_is_refused_and_the_session_stays_usable(client):
    table = pa.table({"v": pa.array(["a", "b"], type=pa.string_view())})
    arena = client.get_payload_arena()

    with pytest.raises(UnsupportedArrowTypeError, match="v: string_view"):
        client.send_arrow_buffers(table, arena, {})

    assert "Received 2 rows" in client.send_arrow_buffers(
        pa.table({"v": ["a", "b"]}), arena, {}
    )
    arena.close()


# --- ScopedArenaContext.receive() ---


def _narrow_types_table() -> pa.Table:
    """Types that pyarrow would widen if the values went through Python objects."""
    return pa.table(
        {
            "i": pa.array([1, None, 3], type=pa.int32()),
            "small": pa.array([1, 2, 3], type=pa.int8()),
            "f": pa.array([1.5, 2.5, None], type=pa.float32()),
            "ts": pa.array([1, 2, 3], type=pa.timestamp("s")),
            "all_null": pa.array([None, None, None], type=pa.int16()),
            "l": pa.array([[1], [], None], type=pa.list_(pa.int16())),
        }
    )


def test_receive_keeps_the_schema(client):
    table = _narrow_types_table()

    with client.arrow_context() as ctx:
        ctx.send(table)
        received = ctx.receive()

    assert received.schema == table.schema
    assert received.to_pylist() == table.to_pylist()


def test_receive_keeps_an_all_null_string_column(client):
    table = pa.table({"s": pa.array([None, None], type=pa.string()), "x": [1, 2]})

    with client.arrow_context() as ctx:
        ctx.send(table)
        received = ctx.receive()

    assert received.schema == table.schema
    assert received.to_pylist() == table.to_pylist()


def test_receive_keeps_the_schema_of_a_table_with_no_rows(client):
    table = _narrow_types_table().slice(0, 0)

    with client.arrow_context() as ctx:
        ctx.send(table)
        received = ctx.receive()

    assert received.schema == table.schema
    assert received.num_rows == 0


def test_receive_keeps_a_string_column_of_a_table_with_no_rows(client):
    table = pa.table({"s": pa.array([], type=pa.string())})

    with client.arrow_context() as ctx:
        ctx.send(table)
        received = ctx.receive()

    assert received.schema == table.schema
    assert received.num_rows == 0


def test_received_table_outlives_the_shared_memory_it_came_from(client):
    first = pa.table({"x": pa.array(range(1000), type=pa.int32())})
    second = pa.table({"x": pa.array(range(1000, 2000), type=pa.int32())})

    with client.arrow_context() as ctx:
        ctx.send(first)
        received = ctx.receive()
        # The second transfer goes through the same region of shared memory
        ctx.send(second)
        assert ctx.receive().equals(second)

    assert received.equals(first)
