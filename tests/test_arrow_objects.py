"""Tests for Arrow tables as Java objects: arrow_to_java() and arrow_from_java().

A table Python sends becomes a VectorSchemaRoot that Java methods can receive,
and any VectorSchemaRoot the JVM holds can be fetched as a pyarrow Table.
"""

import datetime
import decimal
import gc

import pyarrow as pa
import pytest

from gatun import (
    JavaException,
    JavaIllegalArgumentException,
    JavaObject,
    PayloadTooLargeError,
    StaleArenaError,
    UnsupportedArrowTypeError,
    aconnect,
    connect,
)
from gatun.client import _finish_arrow_to_object

ROOT_CLASS = "org.apache.arrow.vector.VectorSchemaRoot"
APPENDER = "org.apache.arrow.vector.util.VectorSchemaRootAppender"


@pytest.fixture(scope="module")
def arrow_client():
    """A server that may call Arrow's own table utilities as static methods."""
    client = connect(memory="32MB", allowed_classes=[APPENDER])
    yield client
    client.close()


def _every_type_table() -> pa.Table:
    return pa.table(
        {
            "int": pa.array([1, None, 3], type=pa.int64()),
            "small": pa.array([1, 2, None], type=pa.int8()),
            "float": [1.5, None, 3.5],
            "bool": [True, False, None],
            "text": ["a", "bb", None],
            "large_text": pa.array(["x", None, "zzz"], type=pa.large_string()),
            "binary": [b"\x00\x01", b"", None],
            "date": [datetime.date(2026, 1, 2), None, datetime.date(1969, 12, 31)],
            "timestamp": pa.array(
                [datetime.datetime(2026, 1, 2, 3, 4, 5, 6), None, None],
                type=pa.timestamp("us", tz="UTC"),
            ),
            "decimal": pa.array(
                [decimal.Decimal("1.23"), None, decimal.Decimal("-4.56")],
                type=pa.decimal128(10, 2),
            ),
            "list": [[1, 2], [], None],
            "struct": [{"a": 1, "b": "x"}, {"a": None, "b": "y"}, None],
            "map": pa.array(
                [[("k", 1)], [], None], type=pa.map_(pa.string(), pa.int32())
            ),
        }
    )


class TestSendingToJava:
    def test_handle_is_a_table_java_can_use(self, client):
        table = pa.table(
            {"x": pa.array([1, None, 3], type=pa.int64()), "s": list("abc")}
        )

        root = client.arrow_to_java(table)

        assert isinstance(root, JavaObject)
        assert client.is_instance_of(root, ROOT_CLASS)
        assert root.getRowCount() == 3
        assert root.getVector("x").getNullCount() == 1
        assert root.getVector("x").get(2) == 3
        assert root.contentToTSVString().splitlines() == [
            "x\ts",
            "1\ta",
            "null\tb",
            "3\tc",
        ]

    def test_java_method_receives_the_table(self, arrow_client):
        """A static Java method takes two sent tables and changes one of them."""
        target = arrow_client.arrow_to_java(pa.table({"x": [1, 2], "s": ["a", "b"]}))
        extra = arrow_client.arrow_to_java(pa.table({"x": [3], "s": ["c"]}))

        arrow_client.jvm.org.apache.arrow.vector.util.VectorSchemaRootAppender.append(
            target, extra
        )

        assert target.getRowCount() == 3
        assert arrow_client.arrow_from_java(target).to_pydict() == {
            "x": [1, 2, 3],
            "s": ["a", "b", "c"],
        }

    def test_every_type_survives_the_round_trip(self, client):
        table = _every_type_table()

        returned = client.arrow_from_java(client.arrow_to_java(table))

        assert returned.schema == table.schema
        assert returned.equals(table)

    def test_schema_metadata_is_kept(self, client):
        table = pa.table({"x": [1]}).replace_schema_metadata({"origin": "python"})

        returned = client.arrow_from_java(client.arrow_to_java(table))

        assert returned.schema.metadata == {b"origin": b"python"}

    def test_empty_table(self, client):
        table = _every_type_table().slice(0, 0)

        root = client.arrow_to_java(table)

        assert root.getRowCount() == 0
        assert client.arrow_from_java(root).equals(table)
        # Nothing was reserved for rows that are not there
        assert root.getVector("int").getValueCapacity() == 0
        assert root.getVector("text").getBufferSize() == 0

    def test_table_with_several_chunks_arrives_whole(self, client):
        table = pa.concat_tables([pa.table({"x": [1, 2]}), pa.table({"x": [3]})])
        assert table.column("x").num_chunks == 2

        root = client.arrow_to_java(table)

        assert root.getRowCount() == 3
        assert client.arrow_from_java(root).to_pydict() == {"x": [1, 2, 3]}

    def test_record_batch(self, client):
        batch = pa.record_batch({"x": [7, 8]})

        assert client.arrow_from_java(client.arrow_to_java(batch)).to_pydict() == {
            "x": [7, 8]
        }

    def test_dictionary_columns_are_refused(self, client):
        table = pa.table({"d": pa.array(["a", "b", "a"]).dictionary_encode()})

        with pytest.raises(UnsupportedArrowTypeError, match="d: dictionary"):
            client.arrow_to_java(table)
        assert client.ping() is True

    def test_not_a_table(self, client):
        with pytest.raises(TypeError, match="pyarrow Table or RecordBatch"):
            client.arrow_to_java({"x": [1, 2, 3]})


class TestFetchingFromJava:
    def test_table_produced_by_java(self, client):
        """slice() makes a new table on the Java side; Python never sent it."""
        table = pa.table({"x": [10, 20, 30, 40], "s": ["a", "b", "c", "d"]})
        root = client.arrow_to_java(table)

        produced = root.slice(1, 2)
        try:
            assert client.arrow_from_java(produced).to_pydict() == {
                "x": [20, 30],
                "s": ["b", "c"],
            }
        finally:
            produced.close()

    def test_table_passed_through_java_code(self, client):
        root = client.arrow_to_java(pa.table({"x": [1, 2, 3]}))
        holder = client.create_object("java.util.ArrayList")
        holder.add(root)

        assert client.arrow_from_java(holder.get(0)).to_pydict() == {"x": [1, 2, 3]}

    def test_by_object_id(self, client):
        root = client.arrow_to_java(pa.table({"x": [5]}))

        assert client.arrow_from_java(root.object_id).to_pydict() == {"x": [5]}

    def test_result_owns_its_data(self, make_client):
        """The table is a copy: later transfers and close() leave it alone."""
        client = make_client()
        first = client.arrow_from_java(client.arrow_to_java(pa.table({"x": [1, 2, 3]})))
        second = client.arrow_from_java(
            client.arrow_to_java(pa.table({"x": [9, 9, 9]}))
        )

        client.close()

        assert first.to_pydict() == {"x": [1, 2, 3]}
        assert second.to_pydict() == {"x": [9, 9, 9]}

    def test_object_that_is_not_a_table(self, client):
        items = client.create_object("java.util.ArrayList")

        with pytest.raises(JavaIllegalArgumentException, match=f"not an {ROOT_CLASS}"):
            client.arrow_from_java(items)
        assert client.ping() is True

    def test_unknown_object(self, client):
        with pytest.raises(JavaException, match="not found"):
            client.arrow_from_java(2**40)


class TestTableLifetime:
    def test_table_lives_while_any_handle_does(self, client):
        """The same table can be registered twice; it is closed with the last handle."""
        root = client.arrow_to_java(pa.table({"x": [1, 2, 3]}))
        holder = client.create_object("java.util.ArrayList")
        holder.add(root)
        alias = holder.get(0)

        del root
        gc.collect()
        client.ping()  # delivers the free
        assert client.arrow_from_java(alias).to_pydict() == {"x": [1, 2, 3]}
        assert alias.getVector("x").getValueCapacity() > 0

        del alias
        gc.collect()
        client.ping()
        # Closed with its last handle: the column's memory has been released
        assert holder.get(0).getVector("x").getValueCapacity() == 0

    def test_dropped_tables_give_their_memory_back(self):
        """Regression guard: handles own off-heap memory no garbage collector sees."""
        table = pa.table({"x": pa.array(range(20_000), type=pa.int64())})  # ~160KB
        client = connect(extra_jvm_flags=["-Dgatun.limits.max_arrow_bytes=1000000"])
        try:
            for _ in range(30):
                root = client.arrow_to_java(table)
                assert root.getRowCount() == 20_000

            held = [root]
            with pytest.raises(JavaException, match="Arrow buffer limit exceeded"):
                for _ in range(30):
                    held.append(client.arrow_to_java(table))
            assert 2 <= len(held) <= 7

            held.clear()
            del root
            gc.collect()
            assert client.arrow_to_java(table).getRowCount() == 20_000
        finally:
            client.close()

    def test_arena_reset_leaves_held_tables_alone(self, client):
        root = client.arrow_to_java(pa.table({"x": [1, 2, 3]}))

        client.reset_payload_arena()

        assert client.arrow_from_java(root).to_pydict() == {"x": [1, 2, 3]}

    def test_tables_count_against_the_object_limit(self):
        client = connect(extra_jvm_flags=["-Dgatun.limits.max_objects=3"])
        try:
            table = pa.table({"x": [1]})
            held = [client.arrow_to_java(table) for _ in range(3)]
            with pytest.raises(JavaException, match="Object limit exceeded"):
                client.arrow_to_java(table)
            held.pop()
            gc.collect()
            held.append(client.arrow_to_java(table))
        finally:
            client.close()


class TestSizeLimits:
    @pytest.fixture
    def small_client(self):
        client = connect(memory="2MB", allowed_classes=[APPENDER])
        yield client
        client.close()

    def test_table_too_large_to_send(self, small_client):
        table = pa.table({"x": pa.array(range(400_000), type=pa.int64())})  # 3.2MB

        with pytest.raises(PayloadTooLargeError) as exc_info:
            small_client.arrow_to_java(table)

        assert exc_info.value.payload_size > exc_info.value.max_size > 0
        assert small_client.ping() is True

    def test_table_too_large_to_fetch_can_be_fetched_in_slices(self, small_client):
        """Java can hold a table bigger than the payload zone."""
        half = pa.table({"x": pa.array(range(150_000), type=pa.int64())})  # 1.2MB
        grown = small_client.arrow_to_java(half)
        small_client.jvm.org.apache.arrow.vector.util.VectorSchemaRootAppender.append(
            grown, small_client.arrow_to_java(half)
        )
        assert grown.getRowCount() == 300_000

        with pytest.raises(PayloadTooLargeError) as exc_info:
            small_client.arrow_from_java(grown)
        assert exc_info.value.payload_size > exc_info.value.max_size > 0

        pieces = []
        for start in (0, 100_000, 200_000):
            piece = grown.slice(start, 100_000)
            pieces.append(small_client.arrow_from_java(piece))
            piece.close()
        assert pa.concat_tables(pieces).equals(pa.concat_tables([half, half]))


class TestAlongsideBufferTransfer:
    """send_arrow_buffers() leaves a batch in the payload zone that Java reads in place."""

    def test_loaded_batch_is_not_overwritten(self, client):
        in_place = pa.table({"x": pa.array(range(1000), type=pa.int64())})
        arena = client.get_payload_arena()
        try:
            client.send_arrow_buffers(in_place, arena, {})

            root = client.arrow_to_java(pa.table({"y": ["a", "b"]}))
            assert client.arrow_from_java(root).to_pydict() == {"y": ["a", "b"]}

            assert client.get_arrow_data().to_pydict() == in_place.to_pydict()
        finally:
            arena.close()

    def test_server_refuses_a_stream_over_the_loaded_batch(self, client):
        """A client that lost track of the loaded batch gets an error, not a
        table read from memory that two things claim."""
        arena = client.get_payload_arena()
        try:
            client.send_arrow_buffers(pa.table({"x": [1, 2, 3]}), arena, {})
            client._arrow_loaded_end = 0

            with pytest.raises(JavaException, match="overlaps the batch"):
                client.arrow_to_java(pa.table({"y": [1]}))
            assert client.ping() is True
        finally:
            arena.close()

    def test_reset_that_fails_keeps_the_batch_protected(self, client, monkeypatch):
        """Regression: the client forgot the loaded batch before the server had
        let go of it, so the next table was written over it."""
        arena = client.get_payload_arena()
        try:
            client.send_arrow_buffers(pa.table({"x": [1, 2, 3]}), arena, {})
            loaded_end = client._arrow_loaded_end
            assert loaded_end > 0

            def failing_send(*args, **kwargs):
                raise OSError("connection hiccup")

            with monkeypatch.context() as patch:
                patch.setattr(client, "_send_raw", failing_send)
                with pytest.raises(OSError, match="hiccup"):
                    client.reset_payload_arena()
            assert client._arrow_loaded_end == loaded_end

            client.reset_payload_arena()
            assert client._arrow_loaded_end == 0
        finally:
            arena.close()


class TestViewsOfSharedMemory:
    """A table from get_arrow_data() points into the second half of the payload
    zone. It must stop working when something else is written there."""

    @pytest.fixture
    def view_client(self):
        client = connect(memory="2MB")
        arena = client.get_payload_arena()
        client.send_arrow_buffers(pa.table({"x": [1, 2, 3]}), arena, {})
        yield client
        arena.close()
        client.close()

    # 1.2MB: placed after the loaded batch, it runs past the middle of the zone
    BIG = pa.table({"y": pa.array(range(150_000), type=pa.int64())})

    def test_small_transfers_leave_the_view_valid(self, view_client):
        view = view_client.get_arrow_data()

        root = view_client.arrow_to_java(pa.table({"y": [1]}))
        view_client.arrow_from_java(root)

        assert view.to_pydict() == {"x": [1, 2, 3]}

    def test_arrow_to_java_over_the_view(self, view_client):
        view = view_client.get_arrow_data()

        view_client.arrow_to_java(self.BIG)

        with pytest.raises(StaleArenaError, match="reused the shared memory"):
            view.to_pydict()

    def test_arrow_from_java_over_the_view(self, view_client):
        root = view_client.arrow_to_java(self.BIG)
        view = view_client.get_arrow_data()

        view_client.arrow_from_java(root)

        with pytest.raises(StaleArenaError, match="reused the shared memory"):
            view.to_pydict()

    def test_send_arrow_table_over_the_view(self, view_client):
        view = view_client.get_arrow_data()

        view_client.send_arrow_table(self.BIG)

        with pytest.raises(StaleArenaError, match="reused the shared memory"):
            view.to_pydict()

    def test_newer_view_replaces_the_older_one(self, view_client):
        older = view_client.get_arrow_data()
        newer = view_client.get_arrow_data()

        assert newer.to_pydict() == {"x": [1, 2, 3]}
        with pytest.raises(StaleArenaError, match="reused the shared memory"):
            older.to_pydict()


class TestMalformedRequests:
    @pytest.mark.parametrize(
        ("offset", "length"),
        [
            (1 << 40, 64),
            (0, 1 << 40),
            ((1 << 64) - 64, 64),
            (64, (1 << 64) - 1),
            (0, 0),
        ],
    )
    def test_stream_outside_the_payload_zone(self, client, offset, length):
        command = _finish_arrow_to_object(client._get_builder(), offset, length)

        with pytest.raises(
            JavaIllegalArgumentException, match="outside the payload zone"
        ):
            client._send_raw(command)
        assert client.ping() is True

    def test_stream_with_several_record_batches(self, client):
        """The Python client sends one batch per table; another client may not."""
        sink = pa.BufferOutputStream()
        schema = pa.schema([("x", pa.int64()), ("s", pa.string())])
        with pa.ipc.new_stream(sink, schema) as writer:
            writer.write_batch(
                pa.record_batch({"x": [1, 2], "s": ["a", "b"]}, schema=schema)
            )
            writer.write_batch(pa.record_batch({"x": [3], "s": [None]}, schema=schema))
            writer.write_batch(
                pa.record_batch({"x": [4, 5], "s": ["d", "e"]}, schema=schema)
            )
        stream = sink.getvalue().to_pybytes()
        start = client.payload_offset
        client.shm[start : start + len(stream)] = stream

        root = client._send_raw(
            _finish_arrow_to_object(client._get_builder(), 0, len(stream))
        )

        assert client.arrow_from_java(root).to_pydict() == {
            "x": [1, 2, 3, 4, 5],
            "s": ["a", "b", None, "d", "e"],
        }

    def test_bytes_that_are_not_a_stream_use_no_quota(self):
        client = connect(extra_jvm_flags=["-Dgatun.limits.max_arrow_bytes=4096"])
        try:
            start = client.payload_offset
            client.shm[start : start + 1024] = b"\xab" * 1024
            for _ in range(10):
                command = _finish_arrow_to_object(client._get_builder(), 0, 1024)
                with pytest.raises(JavaException):
                    client._send_raw(command)

            root = client.arrow_to_java(pa.table({"x": [1, 2, 3]}))
            assert root.getRowCount() == 3
        finally:
            client.close()


class TestAsyncClient:
    async def test_concurrent_transfers_do_not_mix(self):
        """Regression: the payload zone was written before the request had the
        channel, so tasks sharing a client overwrote each other's tables."""
        import asyncio

        client = await aconnect()
        try:

            async def round_trip(n: int) -> pa.Table:
                table = pa.table({"task": pa.array([n] * 2000, type=pa.int64())})
                async with await client.arrow_to_java(table) as root:
                    await asyncio.sleep(0)
                    return await client.arrow_from_java(root)

            results = await asyncio.gather(*(round_trip(n) for n in range(24)))

            for n, result in enumerate(results):
                assert result.column("task").unique().to_pylist() == [n]
        finally:
            await client.close()

    async def test_freeing_the_handle_releases_the_table(self):
        """Async handles are not garbage collected; they are freed explicitly."""
        table = pa.table({"x": pa.array(range(20_000), type=pa.int64())})  # ~160KB
        client = await aconnect(
            extra_jvm_flags=["-Dgatun.limits.max_arrow_bytes=1000000"]
        )
        try:
            for _ in range(30):
                async with await client.arrow_to_java(table) as root:
                    assert await root.getRowCount() == 20_000

            held = []
            with pytest.raises(JavaException, match="Arrow buffer limit exceeded"):
                for _ in range(30):
                    held.append(await client.arrow_to_java(table))
        finally:
            await client.close()

    async def test_round_trip(self):
        client = await aconnect()
        try:
            table = pa.table({"x": [1, 2, 3], "s": ["a", None, "c"]})

            root = await client.arrow_to_java(table)
            assert await root.getRowCount() == 3
            produced = await root.slice(1, 2)

            assert (await client.arrow_from_java(root)).equals(table)
            assert (await client.arrow_from_java(produced)).equals(table.slice(1, 2))
            await produced.close()
        finally:
            await client.close()
