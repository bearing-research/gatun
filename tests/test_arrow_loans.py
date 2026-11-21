"""Tests for the two ways an Arrow table reaches Java without a copy into
off-heap memory of the JVM's own.

arrow_to_java(copy=False) lends the table: Java reads it where Python wrote it,
until the loan ends. arrow_to_java_batches() hands over the record batches as
byte arrays, the form Spark takes Arrow data in.
"""

import datetime
import decimal
import gc
import glob
import time

import pyarrow as pa
import pytest

from gatun import (
    ArrowLoan,
    CancelledException,
    JavaException,
    JavaObject,
    PayloadTooLargeError,
    UnsupportedArrowTypeError,
    connect,
)
from gatun.async_client import AsyncGatunClient
from gatun.client import GatunClient, _finish_arrow_to_object
from gatun.generated.org.gatun.protocol import ArrowForm

ROOT_CLASS = "org.apache.arrow.vector.VectorSchemaRoot"
MARKER = 0x1122334455667788


def _table(rows: int = 1000) -> pa.Table:
    return pa.table(
        {
            "x": pa.array(range(rows), type=pa.int64()),
            "s": [f"row {i}" for i in range(rows)],
            "n": pa.array(
                [None if i % 3 == 0 else i / 2 for i in range(rows)], type=pa.float64()
            ),
        }
    )


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


def _zone_bytes(client, record) -> tuple[int, bytes]:
    """Where a loan lies in the client's shared memory, and its bytes."""
    start = client.payload_offset + record.start
    return start, client.shm[start : client.payload_offset + record.end]


@pytest.fixture
def small_client():
    """A payload zone of 896 KiB, where a table of a few hundred KiB matters."""
    client = connect(memory="1MB")
    yield client
    client.close()


def _eventually(check, timeout=5.0):
    deadline = time.monotonic() + timeout
    while True:
        try:
            return check()
        except AssertionError:
            if time.monotonic() > deadline:
                raise
            time.sleep(0.02)


class TestLending:
    def test_java_reads_the_table(self, client):
        table = _table()

        with client.arrow_to_java(table, copy=False) as root:
            assert isinstance(root, JavaObject)
            assert client.is_instance_of(root, ROOT_CLASS)
            assert root.getRowCount() == 1000
            assert root.getVector("x").get(999) == 999
            assert str(root.getVector("s").getObject(5)) == "row 5"
            assert root.getVector("n").getNullCount() == 334
            assert root.getVector("n").getObject(0) is None
            assert root.getVector("n").getObject(1) == 0.5

    def test_the_handle_is_a_loan(self, client):
        loan = client.arrow_to_java(pa.table({"x": [1, 2, 3]}), copy=False)

        assert isinstance(loan, ArrowLoan)
        assert loan.in_place
        assert not loan.closed
        assert loan.root.getRowCount() == 3
        loan.close()
        assert loan.closed

    def test_table_is_read_where_python_wrote_it(self, client):
        """Java sees a change Python makes to the bytes in shared memory."""
        table = pa.table({"x": pa.array([MARKER, 2, 3], type=pa.int64())})

        with client.arrow_to_java(table, copy=False) as root:
            start, written = _zone_bytes(client, client._loans[0])
            at = start + written.index(MARKER.to_bytes(8, "little"))
            assert root.getVector("x").get(0) == MARKER

            client.shm[at : at + 8] = (42).to_bytes(8, "little")

            assert root.getVector("x").get(0) == 42

    def test_no_jvm_memory_holds_the_data(self, client):
        with client.arrow_to_java(_table(), copy=False) as root:
            for name in ("x", "s", "n"):
                data = root.getVector(name).getDataBuffer()
                assert data.getReferenceManager().getAccountedSize() == 0

        owned = client.arrow_to_java(_table())
        data = owned.getVector("x").getDataBuffer()
        assert data.getReferenceManager().getAccountedSize() > 0

    def test_validity_of_a_column_without_nulls_is_charged(self):
        """A stream leaves that bitmap out and Arrow allocates one on loading.
        It is the one part of a lent table that takes memory in the JVM."""
        client = connect(extra_jvm_flags=["-Dgatun.limits.max_arrow_bytes=4096"])
        try:
            flags = pa.table({"b": pa.array([True] * 200_000)})  # a 25 KB bitmap
            with pytest.raises(JavaException, match="Arrow"):
                client.arrow_to_java(flags, copy=False)
            assert client._loans == []

            # With nulls the bitmap is in the stream, and lent like the data
            nullable = pa.table({"b": pa.array([True, None] * 100_000)})
            with client.arrow_to_java(nullable, copy=False) as root:
                assert root.getRowCount() == 200_000
            # What the first was charged while it was being refused is given back
            small = pa.table({"b": pa.array([True] * 8_000)})
            for _ in range(8):
                with client.arrow_to_java(small, copy=False) as root:
                    assert root.getRowCount() == 8_000
        finally:
            client.close()

    def test_every_type_reads_back_the_same(self, client):
        table = _every_type_table()

        with client.arrow_to_java(table, copy=False) as root:
            returned = client.arrow_from_java(root)

        assert returned.equals(table)

    def test_schema_metadata_is_kept(self, client):
        table = pa.table({"x": [1]}).replace_schema_metadata({"origin": "test"})

        with client.arrow_to_java(table, copy=False) as root:
            assert client.arrow_from_java(root).schema.metadata == {b"origin": b"test"}

    @pytest.mark.parametrize(
        "table",
        [
            pa.table(
                {"x": pa.array([], type=pa.int64()), "s": pa.array([], pa.string())}
            ),
            pa.Table.from_batches([], schema=pa.schema([("x", pa.int64())])),
        ],
        ids=["no rows", "no chunks"],
    )
    def test_empty_table(self, client, table):
        with client.arrow_to_java(table, copy=False) as root:
            assert root.getRowCount() == 0
            assert client.arrow_from_java(root).equals(table)

    def test_table_with_several_chunks_arrives_whole(self, client):
        table = pa.concat_tables([pa.table({"x": [1, 2]}), pa.table({"x": [3]})])

        with client.arrow_to_java(table, copy=False) as root:
            assert root.getRowCount() == 3
            assert client.arrow_from_java(root).to_pydict() == {"x": [1, 2, 3]}

    def test_record_batch(self, client):
        batch = pa.record_batch({"x": [1, 2, 3]})

        with client.arrow_to_java(batch, copy=False) as root:
            assert root.getRowCount() == 3

    def test_java_code_can_slice_and_transfer_it(self, client):
        """Arrow's own table operations work on a lent table, and what they
        make ends with the loan too."""
        with client.arrow_to_java(_table(), copy=False) as root:
            part = root.slice(10, 5)
            assert client.arrow_from_java(part).column("x").to_pylist() == [
                10,
                11,
                12,
                13,
                14,
            ]
            kept = part.getVector("x").getDataBuffer()

        with pytest.raises(JavaException, match="Ref count"):
            kept.getLong(0)
        part.close()

    def test_dictionary_columns_are_refused(self, client):
        table = pa.table({"d": pa.array(["a", "b", "a"]).dictionary_encode()})

        with pytest.raises(UnsupportedArrowTypeError):
            client.arrow_to_java(table, copy=False)
        assert client._loans == []

    def test_not_a_table(self, client):
        with pytest.raises(TypeError):
            client.arrow_to_java([1, 2, 3], copy=False)

    def test_lent_data_does_not_count_against_max_arrow_bytes(self):
        client = connect(extra_jvm_flags=["-Dgatun.limits.max_arrow_bytes=4096"])
        try:
            table = _table(5000)
            with pytest.raises(JavaException, match="max_arrow_bytes|Arrow"):
                client.arrow_to_java(table)

            with client.arrow_to_java(table, copy=False) as root:
                assert root.getRowCount() == 5000
        finally:
            client.close()

    def test_loans_count_against_the_object_limit(self):
        client = connect(extra_jvm_flags=["-Dgatun.limits.max_objects=2"])
        try:
            first = client.arrow_to_java(pa.table({"x": [1]}), copy=False)
            second = client.arrow_to_java(pa.table({"x": [2]}), copy=False)
            with pytest.raises(JavaException, match="limit"):
                client.arrow_to_java(pa.table({"x": [3]}), copy=False)

            first.close()
            with client.arrow_to_java(pa.table({"x": [3]}), copy=False) as root:
                assert root.getRowCount() == 1
            second.close()
        finally:
            client.close()


class TestEndOfALoan:
    def test_block_exit_takes_the_table_back(self, client):
        loan = client.arrow_to_java(pa.table({"x": [1, 2, 3]}), copy=False)

        with loan as root:
            vector = root.getVector("x")
            assert len(client._loans) == 1

        assert loan.closed
        assert client._loans == []
        with pytest.raises(JavaException, match="not found"):
            root.getRowCount()
        # What Java code got from the table before is empty now
        with pytest.raises(JavaException):
            vector.get(0)

    def test_buffer_java_kept_cannot_be_read(self, client):
        """Java code that held on to the data gets an exception, not whatever
        Python wrote there next."""
        with client.arrow_to_java(pa.table({"x": [7, 8, 9]}), copy=False) as root:
            data = root.getVector("x").getDataBuffer()
            manager = data.getReferenceManager()
            manager.retain()
            assert data.getLong(0) == 7

        with pytest.raises(JavaException, match="Ref count should be >= 1"):
            data.getLong(0)
        with pytest.raises(JavaException, match="lent by Python"):
            manager.retain()
        assert manager.release() is True

    def test_block_exit_on_an_exception(self, client):
        loan = client.arrow_to_java(pa.table({"x": [1]}), copy=False)

        with pytest.raises(ZeroDivisionError), loan:
            1 / 0  # noqa: B018

        assert loan.closed
        assert client._loans == []

    def test_close_twice(self, client):
        loan = client.arrow_to_java(pa.table({"x": [1]}), copy=False)

        loan.close()
        loan.close()

        assert client.ping() is True

    def test_closed_loan_cannot_be_entered(self, client):
        loan = client.arrow_to_java(pa.table({"x": [1]}), copy=False)
        loan.close()

        with pytest.raises(RuntimeError, match="ended"), loan:
            pass

    def test_dropped_loan_ends_with_the_next_request(self, client):
        loan = client.arrow_to_java(pa.table({"x": [1]}), copy=False)
        root = loan.root
        vector = root.getVector("x")

        del loan, root
        gc.collect()
        assert len(client._loans) == 1  # nothing was sent from the finalizer

        assert client.ping() is True
        assert client._loans == []
        with pytest.raises(JavaException):
            vector.get(0)

    def test_table_handle_keeps_the_loan_open(self, client):
        root = client.arrow_to_java(pa.table({"x": [1, 2]}), copy=False).root

        gc.collect()
        client.ping()

        assert root.getVector("x").get(1) == 2
        assert len(client._loans) == 1

    def test_freeing_the_object_ends_the_loan_on_the_server(self, client):
        loan = client.arrow_to_java(pa.table({"x": [1, 2]}), copy=False)
        data = loan.root.getVector("x").getDataBuffer()
        data.getReferenceManager().retain()

        client.free_object(loan.root.object_id)

        with pytest.raises(JavaException, match="Ref count"):
            data.getLong(0)
        data.getReferenceManager().release()
        loan.close()
        assert client._loans == []

    def test_reset_ends_every_loan(self, client):
        first = client.arrow_to_java(pa.table({"x": [1]}), copy=False)
        second = client.arrow_to_java(pa.table({"x": [2]}), copy=False)
        vector = second.root.getVector("x")

        client.reset_payload_arena()

        assert client._loans == []
        with pytest.raises(JavaException):
            vector.get(0)
        # The zone is free from its start again
        with client.arrow_to_java(pa.table({"x": [3]}), copy=False) as root:
            assert client._loans[0].start == 0
            assert root.getVector("x").get(0) == 3
        first.close()
        second.close()
        assert client.ping() is True

    def test_loan_ended_by_a_reset_is_closed(self, client):
        loan = client.arrow_to_java(pa.table({"x": [1, 2, 3]}), copy=False)
        assert not loan.closed

        client.reset_payload_arena()

        assert loan.closed
        with pytest.raises(RuntimeError, match="ended"), loan:
            pass
        # close() still releases the table's object
        loan.close()
        with pytest.raises(JavaException, match="not found"):
            loan.root.getRowCount()

    def test_a_table_whose_loan_ended_cannot_be_read_back(self, client):
        """Ending a loan closes the table, which keeps its row count over
        empty vectors: written out, that was a stream pyarrow refused."""
        loan = client.arrow_to_java(pa.table({"x": [1, 2, 3]}), copy=False)
        root = loan.root
        client.reset_payload_arena()

        with pytest.raises(JavaException, match="loan has ended"):
            client.arrow_from_java(root)
        with pytest.raises(JavaException, match="loan has ended"):
            client.arrow_from_java(root, copy=False)
        assert client.ping() is True
        loan.close()

    def test_a_closed_table_cannot_be_read_back(self, client):
        """The same for a table Java code closed: a closed table is no table."""
        root = client.arrow_to_java(pa.table({"x": [1, 2, 3]}))
        produced = root.slice(1, 2)
        produced.close()

        with pytest.raises(JavaException, match="has been closed"):
            client.arrow_from_java(produced)
        assert client.arrow_from_java(root).num_rows == 3

    def test_end_that_was_cancelled_is_tried_again(self, client, monkeypatch):
        """cancel() can catch the exchange that ends a loan. The server has
        not taken the table back then, and the client must not act as if."""
        loan = client.arrow_to_java(pa.table({"x": [1, 2, 3]}), copy=False)
        record = client._loans[0]
        read_response = client._read_response

        def cancelled(*args, **kwargs):
            monkeypatch.setattr(client, "_read_response", read_response)
            read_response(*args, **kwargs)
            raise CancelledException("Request was cancelled")

        monkeypatch.setattr(client, "_read_response", cancelled)

        with pytest.raises(CancelledException):
            loan.close()

        # Its room is still taken, until the next request has ended it
        assert client._loans == [record]
        assert not record.ended

        assert client.ping() is True
        assert client._loans == []
        assert record.ended

    def test_close_after_the_client_closed(self, make_client):
        client = make_client()
        loan = client.arrow_to_java(pa.table({"x": [1]}), copy=False)

        client.close()
        loan.close()

        assert loan.closed

    def test_shared_memory_is_free_to_close(self, make_client):
        client = make_client()
        shm = client.shm
        loan = client.arrow_to_java(_table(), copy=False)
        loan.close()

        client.close()

        assert shm.closed


class TestRoomInThePayloadZone:
    def test_later_transfers_are_written_after_an_open_loan(self, client):
        first = _table(500)
        with client.arrow_to_java(first, copy=False) as root:
            lent = client._loans[0]

            with client.arrow_to_java(pa.table({"y": [1, 2, 3]}), copy=False) as other:
                assert client._loans[1].start >= lent.end
                assert other.getRowCount() == 3
            owned = client.arrow_to_java(pa.table({"z": [4.5]}))
            fetched = client.arrow_from_java(owned)
            view = client.arrow_from_java(owned, copy=False)
            batches = client.arrow_to_java_batches(pa.table({"w": [1]}))

            assert fetched.to_pydict() == {"z": [4.5]}
            assert view.num_rows == 1
            assert batches.size() == 1
            # None of it landed on the table that is lent
            assert client.arrow_from_java(root).equals(first)

    def test_an_ended_loan_gives_its_room_back(self, small_client):
        big = pa.table({"x": pa.array(range(70_000), type=pa.int64())})  # 560 KB

        with small_client.arrow_to_java(big, copy=False) as root:
            with pytest.raises(PayloadTooLargeError):
                small_client.arrow_to_java(big)
            assert root.getRowCount() == 70_000

        assert small_client.arrow_to_java(big).getRowCount() == 70_000

    def test_dropped_loan_gives_its_room_back(self, small_client):
        big = pa.table({"x": pa.array(range(70_000), type=pa.int64())})
        loan = small_client.arrow_to_java(big, copy=False)

        del loan
        gc.collect()

        assert small_client.arrow_to_java(big).getRowCount() == 70_000

    def test_send_arrow_table_would_overwrite_a_loan(self, client):
        table = pa.table({"x": [1, 2, 3]})
        with client.arrow_to_java(table, copy=False) as root:
            with pytest.raises(RuntimeError, match="lent to Java"):
                client.send_arrow_table(table)

            assert client.arrow_from_java(root).equals(table)
        assert client.send_arrow_table(table) == "Received 3 rows"

    def test_the_arena_would_overwrite_a_loan(self, client):
        table = pa.table({"x": [1, 2, 3]})
        arena = client.get_payload_arena()
        with client.arrow_to_java(table, copy=False) as root:
            with pytest.raises(RuntimeError, match="lent to Java"):
                client.send_arrow_buffers(table, arena, {})
            assert arena.offset == 0

            assert client.arrow_from_java(root).equals(table)
        client.send_arrow_buffers(table, arena, {})
        arena.close()
        client.reset_payload_arena()

    def test_refused_arena_send_leaves_the_loaded_batch_alone(self, client):
        """Regression: the send was refused at the buffer that reached the
        loan, after the buffers before it had been copied over the batch Java
        held from the send before. get_arrow_data() then returned other rows."""
        loaded = pa.table({"x": pa.array(range(100), type=pa.int64())})
        arena = client.get_payload_arena()
        client.send_arrow_buffers(loaded, arena, {})
        wide = pa.table(
            {name: pa.array(range(50, 150), type=pa.int64()) for name in "abc"}
        )

        with client.arrow_to_java(pa.table({"y": [1.5]}), copy=False) as root:
            arena.reset()
            with pytest.raises(RuntimeError, match="lent to Java"):
                client.send_arrow_buffers(wide, arena, {})

            assert arena.offset == 0
            assert root.getVector("y").get(0) == 1.5
            assert client.get_arrow_data().to_pydict() == loaded.to_pydict()

        arena.close()
        client.reset_payload_arena()

    def test_arena_past_a_loan_is_free_to_use(self, client):
        arena = client.get_payload_arena()
        with client.arrow_to_java(pa.table({"y": [1.5]}), copy=False) as root:
            arena.offset = client._loans[0].end
            info = arena.allocate(64)

            assert info.offset >= client._loans[0].end
            assert root.getVector("y").get(0) == 1.5
        arena.close()

    def test_a_loan_goes_after_a_loaded_batch(self, client):
        loaded = pa.table({"x": list(range(100))})
        arena = client.get_payload_arena()
        client.send_arrow_buffers(loaded, arena, {})

        with client.arrow_to_java(pa.table({"y": [1.5]}), copy=False) as root:
            assert client._loans[0].start >= client._arrow_loaded_end > 0
            assert root.getVector("y").get(0) == 1.5
            # The arena may still be used below the loan
            arena.reset()
            client.send_arrow_buffers(pa.table({"x": [1]}), arena, {})
            assert root.getVector("y").get(0) == 1.5

        assert client.get_arrow_data().to_pydict() == {"x": [1]}
        arena.close()
        client.reset_payload_arena()

    def test_get_arrow_data_would_overwrite_a_loan(self, small_client):
        arena = small_client.get_payload_arena()
        small_client.send_arrow_buffers(pa.table({"x": [1, 2, 3]}), arena, {})
        big = pa.table({"x": pa.array(range(70_000), type=pa.int64())})

        with small_client.arrow_to_java(big, copy=False) as root:
            with pytest.raises(RuntimeError, match="lent to Java"):
                small_client.get_arrow_data()
            assert root.getVector("x").get(69_999) == 69_999

        assert small_client.get_arrow_data().to_pydict() == {"x": [1, 2, 3]}

    def test_server_refuses_a_stream_over_a_loan(self, client):
        """A client that lost track of its loans must not get Java to read a
        table that is being overwritten."""
        with client.arrow_to_java(_table(), copy=False) as root:
            record = client._loans[0]
            command = _finish_arrow_to_object(client._get_builder(), 0, record.end)

            with pytest.raises(JavaException, match="lent to Java"):
                client._send_raw(command)
            assert root.getRowCount() == 1000

    def test_server_refuses_a_counted_table_over_a_loan(self, client):
        table = pa.table({"x": [1, 2, 3]})
        with client.arrow_to_java(table, copy=False):
            record = client._loans.pop()  # the client forgets the loan
            try:
                with pytest.raises(JavaException, match="lent to Java"):
                    client.send_arrow_table(table)
            finally:
                client._loans.append(record)

    def test_server_refuses_returned_data_over_a_loan(self, small_client):
        arena = small_client.get_payload_arena()
        small_client.send_arrow_buffers(pa.table({"x": [1, 2, 3]}), arena, {})
        big = pa.table({"x": pa.array(range(70_000), type=pa.int64())})

        with small_client.arrow_to_java(big, copy=False):
            record = small_client._loans.pop()  # the client forgets the loan
            try:
                with pytest.raises(JavaException, match="lent to Java"):
                    small_client.get_arrow_data()
            finally:
                small_client._loans.append(record)


class TestMalformedStreams:
    def _send(self, client, stream: bytes, form: int = ArrowForm.ArrowForm.LentTable):
        start = client.payload_offset
        client.shm[start : start + len(stream)] = stream
        return client._send_raw(
            _finish_arrow_to_object(client._get_builder(), 0, len(stream), form)
        )

    def test_several_record_batches_cannot_be_lent(self, client):
        sink = pa.BufferOutputStream()
        schema = pa.schema([("x", pa.int64())])
        with pa.ipc.new_stream(sink, schema) as writer:
            writer.write_batch(pa.record_batch({"x": [1, 2]}, schema=schema))
            writer.write_batch(pa.record_batch({"x": [3]}, schema=schema))

        with pytest.raises(JavaException, match="one record batch"):
            self._send(client, sink.getvalue().to_pybytes())
        assert client.ping() is True

    def test_compressed_batch_cannot_be_lent(self, client):
        sink = pa.BufferOutputStream()
        table = pa.table({"x": list(range(1000))})
        options = pa.ipc.IpcWriteOptions(compression="lz4")
        with pa.ipc.new_stream(sink, table.schema, options=options) as writer:
            writer.write_table(table)

        with pytest.raises(JavaException, match="compressed"):
            self._send(client, sink.getvalue().to_pybytes())

    @pytest.mark.parametrize(
        "form", [ArrowForm.ArrowForm.LentTable, ArrowForm.ArrowForm.Batches]
    )
    def test_bytes_that_are_not_a_stream(self, client, form):
        for junk in (b"\xab" * 1024, b"\xff\xff\xff\xff" + b"\x10" * 60, b"\x00" * 64):
            with pytest.raises(JavaException, match="Not an Arrow IPC stream"):
                self._send(client, junk, form)
        assert client.ping() is True

    @pytest.mark.parametrize(
        "form", [ArrowForm.ArrowForm.LentTable, ArrowForm.ArrowForm.Batches]
    )
    def test_stream_cut_short(self, client, form):
        sink = pa.BufferOutputStream()
        table = pa.table({"x": list(range(1000))})
        with pa.ipc.new_stream(sink, table.schema) as writer:
            writer.write_table(table)
        stream = sink.getvalue().to_pybytes()

        with pytest.raises(JavaException, match="Not an Arrow IPC stream"):
            self._send(client, stream[: len(stream) - 4000], form)
        assert client.ping() is True

    def test_unknown_form(self, client):
        with pytest.raises(JavaException, match="arrow_form"):
            self._send(client, b"\x00" * 64, 99)


KEEPER_JAVA = """
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.util.MemoryUtil;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;

/** Java code that holds on to a table it was handed. */
public final class Keeper {
  private static volatile ArrowBuf kept;
  private static volatile String outcome;
  private static volatile long passes;

  private Keeper() {}

  public static void keep(VectorSchemaRoot root) {
    ArrowBuf data = root.getVector(0).getDataBuffer();
    data.getReferenceManager().retain();
    kept = data;
  }

  public static long read() {
    return kept.getLong(0);
  }

  /** Past Arrow's checks, as code that took the address would read. */
  public static long peek() {
    return MemoryUtil.getLong(kept.memoryAddress());
  }

  public static void release() {
    kept.getReferenceManager().release();
    kept = null;
  }

  /** Read the first column over and over on another thread, until that fails. */
  public static void readInBackground(VectorSchemaRoot root) {
    BigIntVector column = (BigIntVector) root.getVector(0);
    int rows = root.getRowCount();
    outcome = null;
    passes = 0;
    Thread reader = new Thread(() -> {
      try {
        while (true) {
          long sum = 0;
          for (int i = 0; i < rows; i++) {
            sum += column.get(i);
          }
          passes++;
        }
      } catch (Throwable t) {
        outcome = t.getClass().getName();
      }
    });
    reader.setDaemon(true);
    reader.start();
  }

  public static long passes() {
    return passes;
  }

  public static String outcome() {
    return outcome;
  }
}
"""


class TestJavaCodeThatKeepsTheTable:
    @pytest.fixture
    def owner(self, compile_java):
        classes = compile_java("Keeper", KEEPER_JAVA, with_server=True)
        client = connect(memory="16MB", classpath=[classes], allowed_classes=["Keeper"])
        yield client
        client.close()

    def test_thread_reading_when_the_loan_ends(self, owner):
        """The reader gets an exception; the server goes on."""
        table = pa.table({"x": pa.array(range(50_000), type=pa.int64())})
        keeper = owner.jvm.Keeper

        with owner.arrow_to_java(table, copy=False) as root:
            keeper.readInBackground(root)

            def reading():
                assert keeper.passes() > 3

            _eventually(reading)
            assert keeper.outcome() is None

        def stopped():
            assert keeper.outcome() is not None

        _eventually(stopped)
        assert keeper.outcome() in (
            "java.lang.IllegalStateException",
            "java.lang.IndexOutOfBoundsException",
        )
        assert owner.ping() is True

    def test_session_ends_while_java_holds_a_buffer(self, owner):
        """The session's memory stays mapped for as long as Java code can
        reach it: reading it must not bring the server down."""
        lender = GatunClient(owner.socket_path)
        assert lender.connect()
        keeper = owner.jvm.Keeper
        loan = lender.arrow_to_java(
            pa.table({"x": pa.array([MARKER, 2], type=pa.int64())}), copy=False
        )
        lender.jvm.Keeper.keep(loan.root)
        assert keeper.read() == MARKER
        files = f"{owner.socket_path}.*.shm"
        assert len(glob.glob(files)) == 2

        lender.close()

        def session_gone():
            assert len(glob.glob(files)) == 1

        _eventually(session_gone)
        with pytest.raises(JavaException, match="Ref count should be >= 1"):
            keeper.read()
        # The file is gone and the memory is still there
        assert keeper.peek() == MARKER
        keeper.release()
        assert owner.ping() is True

    def test_session_ends_with_a_loan_open(self, owner):
        lender = GatunClient(owner.socket_path)
        assert lender.connect()
        lender.arrow_to_java(_table(), copy=False)

        lender.close()

        with owner.arrow_to_java(_table(), copy=False) as root:
            assert root.getRowCount() == 1000


class TestBatches:
    @staticmethod
    def _decode(batches, schema: pa.Schema) -> pa.Table:
        """Read the byte arrays back the way a consumer of them would."""
        decoded = [
            pa.ipc.read_record_batch(pa.ipc.read_message(batches.get(i)), schema)
            for i in range(batches.size())
        ]
        return pa.Table.from_batches(decoded, schema=schema)

    def test_handle_is_a_list_of_byte_arrays(self, client):
        table = _table()

        batches = client.arrow_to_java_batches(table)

        assert isinstance(batches, JavaObject)
        assert client.is_instance_of(batches, "java.util.List")
        assert batches.size() == 1
        assert isinstance(batches.get(0), bytes)
        assert self._decode(batches, table.schema).equals(table)

    def test_each_chunk_is_a_batch(self, client):
        table = pa.concat_tables(
            [pa.table({"x": [1, 2]}), pa.table({"x": [3]}), pa.table({"x": [4, 5, 6]})]
        )

        batches = client.arrow_to_java_batches(table)

        assert batches.size() == 3
        assert self._decode(batches, table.schema).to_pydict() == {
            "x": [1, 2, 3, 4, 5, 6]
        }

    def test_max_rows_per_batch(self, client):
        table = _table(1000)

        batches = client.arrow_to_java_batches(table, max_rows_per_batch=300)

        assert batches.size() == 4
        assert self._decode(batches, table.schema).equals(table)

    def test_every_type(self, client):
        table = _every_type_table()

        batches = client.arrow_to_java_batches(table)

        assert self._decode(batches, table.schema).equals(table)

    def test_record_batch(self, client):
        batch = pa.record_batch({"x": [1, 2, 3]})

        batches = client.arrow_to_java_batches(batch)

        assert self._decode(batches, batch.schema).to_pydict() == {"x": [1, 2, 3]}

    def test_table_without_rows(self, client):
        table = pa.table({"x": pa.array([], type=pa.int64())})

        assert client.arrow_to_java_batches(table).size() == 0

    def test_nothing_is_held_in_arrow_memory(self):
        client = connect(extra_jvm_flags=["-Dgatun.limits.max_arrow_bytes=4096"])
        try:
            assert client.arrow_to_java_batches(_table(5000)).size() == 1
        finally:
            client.close()

    def test_batches_are_written_after_a_loaded_batch(self, client):
        loaded = pa.table({"x": list(range(100))})
        arena = client.get_payload_arena()
        client.send_arrow_buffers(loaded, arena, {})

        assert client.arrow_to_java_batches(_table()).size() == 1

        assert client.get_arrow_data().to_pydict() == loaded.to_pydict()
        arena.close()
        client.reset_payload_arena()

    def test_table_too_large(self, small_client):
        table = pa.table({"x": pa.array(range(200_000), type=pa.int64())})

        with pytest.raises(PayloadTooLargeError):
            small_client.arrow_to_java_batches(table)
        assert small_client.ping() is True

    def test_dictionary_columns_are_refused(self, client):
        table = pa.table({"d": pa.array(["a", "b", "a"]).dictionary_encode()})

        with pytest.raises(UnsupportedArrowTypeError):
            client.arrow_to_java_batches(table)

    def test_not_a_table(self, client):
        with pytest.raises(TypeError):
            client.arrow_to_java_batches([1, 2, 3])

    def test_batch_size_must_be_positive(self, client):
        with pytest.raises(ValueError, match="max_rows_per_batch"):
            client.arrow_to_java_batches(pa.table({"x": [1]}), max_rows_per_batch=0)

    def test_shared_memory_is_free_to_close(self, make_client):
        client = make_client()
        shm = client.shm
        client.arrow_to_java_batches(_table())

        client.close()

        assert shm.closed

    async def test_async_client(self, java_gateway):
        table = _table()
        async with AsyncGatunClient(java_gateway.socket_path) as client:
            batches = await client.arrow_to_java_batches(table, max_rows_per_batch=400)

            assert await batches.size() == 3
            first = pa.ipc.read_record_batch(
                pa.ipc.read_message(await batches.get(0)), table.schema
            )
            assert first.equals(table.slice(0, 400).to_batches()[0])
