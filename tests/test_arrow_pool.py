"""Tests for the server's pool of large Arrow blocks.

A table copied into the JVM needs a buffer of its size. The server keeps such
buffers when they are freed and hands them out again, because memory that is
new from the operating system costs several times the copy to touch. What it
keeps is limited in size and in time.
"""

import re
import time

import pyarrow as pa
import pytest

from gatun import JavaException, connect

MIB = 1024 * 1024


def _table(mib: float) -> pa.Table:
    """A table whose one column holds `mib` MiB of data."""
    return pa.table({"x": pa.array(range(int(mib * MIB) // 8), type=pa.int64())})


def _pool(client) -> dict[str, int]:
    """The pool's lines of the server's metrics report."""
    return {
        name: int(value)
        for name, value in re.findall(r"pool_(\w+): (\d+)", client.get_metrics())
    }


def _send_and_free(client, table: pa.Table) -> None:
    root = client.arrow_to_java(table)
    assert root.getRowCount() == table.num_rows
    root.detach()
    client.free_object(root.object_id)


def _eventually(check, timeout=5.0):
    deadline = time.monotonic() + timeout
    while True:
        try:
            return check()
        except AssertionError:
            if time.monotonic() > deadline:
                raise
            time.sleep(0.05)


class TestReuse:
    def test_freed_block_is_kept_and_used_again(self):
        client = connect(memory="64MB")
        try:
            table = _table(20)
            before = _pool(client)
            assert before["kept_bytes"] == 0

            root = client.arrow_to_java(table)
            held = _pool(client)
            assert held["blocks_allocated"] == before["blocks_allocated"] + 1
            assert held["kept_bytes"] == 0

            root.detach()
            client.free_object(root.object_id)
            freed = _pool(client)
            assert freed["kept_blocks"] == 1
            assert freed["kept_bytes"] >= table.nbytes

            for _ in range(3):
                root = client.arrow_to_java(table)
                assert client.arrow_from_java(root).equals(table)
                root.detach()
                client.free_object(root.object_id)

            after = _pool(client)
            assert after["blocks_allocated"] == held["blocks_allocated"]
            assert after["blocks_reused"] == before["blocks_reused"] + 3
            assert after["kept_blocks"] == 1
        finally:
            client.close()

    def test_a_table_of_nearly_the_same_size_uses_the_block(self):
        """A new block is a little larger than was asked for (sizes are an
        eighth of a power of two apart), and a request takes a kept block up
        to a quarter larger than it needs."""
        client = connect(memory="64MB")
        try:
            _send_and_free(client, _table(21))
            allocated = _pool(client)["blocks_allocated"]

            _send_and_free(client, _table(21.6))
            _send_and_free(client, _table(19.2))

            assert _pool(client)["blocks_allocated"] == allocated
            assert _pool(client)["blocks_reused"] == 2
        finally:
            client.close()

    def test_a_much_smaller_table_leaves_a_large_block_alone(self):
        client = connect(memory="64MB")
        try:
            _send_and_free(client, _table(24))
            allocated = _pool(client)["blocks_allocated"]

            small = client.arrow_to_java(_table(6))

            during = _pool(client)
            assert during["blocks_allocated"] == allocated + 1
            assert during["kept_blocks"] == 1
            small.detach()
            client.free_object(small.object_id)
            assert _pool(client)["kept_blocks"] == 2
        finally:
            client.close()

    def test_data_is_whole_in_a_block_that_held_other_data(self):
        client = connect(memory="64MB")
        try:
            first = pa.table({"x": pa.array([7] * (20 * MIB // 8), type=pa.int64())})
            _send_and_free(client, first)
            second = pa.table(
                {"y": pa.array([None, 1.5] * (9 * MIB // 8), type=pa.float64())}
            )

            root = client.arrow_to_java(second)

            assert _pool(client)["blocks_reused"] == 1
            assert client.arrow_from_java(root).equals(second)
        finally:
            client.close()

    def test_small_buffers_are_not_the_pool_s(self):
        client = connect(memory="64MB")
        try:
            _send_and_free(client, _table(1))

            pool = _pool(client)
            assert pool["blocks_allocated"] == 0
            assert pool["kept_blocks"] == 0
        finally:
            client.close()

    def test_buffers_java_code_allocates_use_it_too(self):
        client = connect(memory="64MB")
        try:
            root = client.arrow_to_java(pa.table({"x": [1]}))
            allocator = root.getVector("x").getAllocator()

            buffer = allocator.buffer(20 * MIB)
            assert _pool(client)["blocks_allocated"] == 1
            buffer.close()
            assert _pool(client)["kept_blocks"] == 1

            buffer = allocator.buffer(20 * MIB)
            assert _pool(client)["blocks_reused"] == 1
            buffer.close()
        finally:
            client.close()


class TestLimits:
    def test_default_limit_is_the_size_of_the_shared_memory(self):
        client = connect(memory="32MB")
        try:
            assert _pool(client)["max_bytes"] == 32 * MIB
        finally:
            client.close()

    def test_the_largest_table_that_fits_the_shared_memory_is_kept(self):
        """Its block is as large as the limit, no larger: rounding the size up
        as for other blocks would take it over the limit and out of the pool."""
        client = connect(memory="50MB")
        try:
            _send_and_free(client, _table(46))

            pool = _pool(client)
            assert pool["kept_blocks"] == 1
            assert pool["kept_bytes"] <= pool["max_bytes"]
        finally:
            client.close()

    def test_table_under_16_mib_counts_as_its_own_size(self):
        """Arrow rounds requests under 16 MiB up to a power of two unless told
        otherwise. A 10 MiB table that counted as 16 would never fit a pool of
        12."""
        client = connect(memory="12MB")
        try:
            table = _table(10)
            _send_and_free(client, table)
            _send_and_free(client, table)

            pool = _pool(client)
            assert pool["kept_blocks"] == 1
            assert pool["blocks_reused"] == 1
            assert pool["kept_bytes"] <= 12 * MIB
        finally:
            client.close()

    @pytest.mark.parametrize(
        "flag",
        [
            "-Dgatun.arrow.pool.max_bytes=64MB",
            "-Dgatun.arrow.pool.max_bytes=",
            "-Dgatun.arrow.pool.idle_ms=1.5",
        ],
    )
    def test_setting_that_is_not_a_number_stops_the_server(self, flag):
        """Not ignored: the pool would run with its defaults, for a user who
        meant to turn it off."""
        with pytest.raises(RuntimeError, match="takes a whole number"):
            connect(memory="64MB", extra_jvm_flags=[flag])

    def test_blocks_returned_longest_ago_go_first(self):
        client = connect(
            memory="64MB",
            extra_jvm_flags=[f"-Dgatun.arrow.pool.max_bytes={24 * MIB}"],
        )
        try:
            first = client.arrow_to_java(_table(12))
            second = client.arrow_to_java(_table(8))
            third = client.arrow_to_java(_table(10))
            for root in (first, second, third):
                root.detach()
                client.free_object(root.object_id)

            pool = _pool(client)
            # 12 + 8 + 10 MiB do not fit in 24: the first one back was let go
            assert pool["kept_blocks"] == 2
            assert 18 * MIB <= pool["kept_bytes"] <= 24 * MIB

            # ... so a 12 MiB table needs new memory, and an 8 MiB one does not
            allocated = pool["blocks_allocated"]
            _send_and_free(client, _table(8))
            assert _pool(client)["blocks_allocated"] == allocated
        finally:
            client.close()

    def test_a_table_over_the_limit_is_not_pooled(self):
        client = connect(
            memory="64MB",
            extra_jvm_flags=[f"-Dgatun.arrow.pool.max_bytes={16 * MIB}"],
        )
        try:
            _send_and_free(client, _table(30))

            pool = _pool(client)
            assert pool["blocks_allocated"] == 0
            assert pool["kept_blocks"] == 0
        finally:
            client.close()

    def test_zero_turns_the_pool_off(self):
        client = connect(
            memory="64MB", extra_jvm_flags=["-Dgatun.arrow.pool.max_bytes=0"]
        )
        try:
            table = _table(20)
            for _ in range(2):
                root = client.arrow_to_java(table)
                assert client.arrow_from_java(root).equals(table)
                root.detach()
                client.free_object(root.object_id)

            pool = _pool(client)
            assert pool["blocks_allocated"] == 0
            assert pool["blocks_reused"] == 0
            assert pool["kept_bytes"] == 0
        finally:
            client.close()


class TestIdleRelease:
    """These tests wait for the pool to change and assert what follows from
    it; none depends on a request arriving within the idle time."""

    def test_block_nobody_asked_for_is_given_back(self):
        client = connect(
            memory="64MB", extra_jvm_flags=["-Dgatun.arrow.pool.idle_ms=300"]
        )
        try:
            table = _table(20)

            def released():
                pool = _pool(client)
                assert pool["kept_blocks"] == 0
                assert pool["kept_bytes"] == 0

            _send_and_free(client, table)
            _eventually(released)

            # The pool goes on working: with nothing kept, the next table
            # needs new memory, which is given back in its turn
            _send_and_free(client, table)
            pool = _pool(client)
            assert pool["blocks_allocated"] == 2
            assert pool["blocks_reused"] == 0
            _eventually(released)
        finally:
            client.close()

    def test_block_in_use_stays(self):
        client = connect(
            memory="64MB", extra_jvm_flags=["-Dgatun.arrow.pool.idle_ms=300"]
        )
        try:
            held, other = _table(20), _table(8)
            root = client.arrow_to_java(held)

            # The timer runs for the block that came back, and only for it
            _send_and_free(client, other)

            def released():
                assert _pool(client)["kept_blocks"] == 0

            _eventually(released)
            assert client.arrow_from_java(root).equals(held)

            # Its time starts when it comes back
            root.detach()
            client.free_object(root.object_id)
            _eventually(released)
            assert _pool(client)["blocks_allocated"] == 2
        finally:
            client.close()

    def test_blocks_come_back_at_different_times(self):
        client = connect(
            memory="64MB", extra_jvm_flags=["-Dgatun.arrow.pool.idle_ms=2000"]
        )
        try:
            first, second = _table(20), _table(8)
            _send_and_free(client, first)
            time.sleep(1.0)
            _send_and_free(client, second)

            def one_left():
                assert _pool(client)["kept_blocks"] == 1

            _eventually(one_left, timeout=5.0)
            # The one that came back later is the one that is left
            assert _pool(client)["kept_bytes"] in (0, 8 * MIB)

            def none_left():
                assert _pool(client)["kept_blocks"] == 0

            _eventually(none_left, timeout=5.0)
        finally:
            client.close()

    def test_zero_keeps_blocks_until_the_server_exits(self):
        client = connect(
            memory="64MB", extra_jvm_flags=["-Dgatun.arrow.pool.idle_ms=0"]
        )
        try:
            _send_and_free(client, _table(20))

            time.sleep(0.5)

            assert _pool(client)["kept_blocks"] == 1
        finally:
            client.close()


class TestMemoryLimit:
    """A kept block counts against the JVM's limit on direct memory like one
    in use. The pool must not make an allocation fail that would have
    succeeded without it."""

    def test_kept_blocks_make_room_for_a_new_block(self):
        client = connect(
            memory="64MB",
            extra_jvm_flags=["-XX:MaxDirectMemorySize=48m"],
        )
        try:
            _send_and_free(client, _table(20))
            assert _pool(client)["kept_blocks"] == 1
            larger = _table(30)

            # 20 + 30 MiB are more than 48, and the kept block is too small
            root = client.arrow_to_java(larger)

            pool = _pool(client)
            assert pool["kept_blocks"] == 0
            assert pool["blocks_reused"] == 0
            assert client.arrow_from_java(root).equals(larger)
        finally:
            client.close()

    def test_kept_blocks_make_room_for_a_buffer_that_is_not_the_pool_s(self):
        client = connect(
            memory="64MB",
            extra_jvm_flags=[
                "-XX:MaxDirectMemorySize=48m",
                f"-Dgatun.arrow.pool.max_bytes={24 * MIB}",
            ],
        )
        try:
            _send_and_free(client, _table(20))
            assert _pool(client)["kept_blocks"] == 1
            larger = _table(30)  # over the pool's limit: Netty's to allocate

            root = client.arrow_to_java(larger)

            pool = _pool(client)
            assert pool["kept_blocks"] == 0
            assert pool["blocks_allocated"] == 1
            assert client.arrow_from_java(root).equals(larger)
        finally:
            client.close()

    def test_a_block_is_no_larger_than_asked_for_when_more_does_not_fit(self):
        """A new block for 129 MiB is 144 MiB, which this JVM does not have."""
        client = connect(
            memory="160MB",
            extra_jvm_flags=["-XX:MaxDirectMemorySize=140m"],
        )
        try:
            rows = 129 * MIB // 8
            table = pa.table({"x": pa.repeat(pa.scalar(7, pa.int64()), rows)})

            root = client.arrow_to_java(table)

            assert root.getRowCount() == rows
            assert root.getVector("x").get(rows - 1) == 7
            root.detach()
            client.free_object(root.object_id)
            pool = _pool(client)
            assert pool["kept_blocks"] == 1
            assert table.nbytes <= pool["kept_bytes"] < 140 * MIB
            # And the block is as good as another for the next table
            _send_and_free(client, table)
            assert _pool(client)["blocks_reused"] == 1
        finally:
            client.close()

    def test_no_memory_is_the_error_arrow_reports(self):
        """Code written against Arrow catches its exception, not an Error of
        the JVM's."""
        client = connect(
            memory="64MB",
            extra_jvm_flags=["-XX:MaxDirectMemorySize=48m"],
        )
        try:
            root = client.arrow_to_java(pa.table({"x": [1]}))
            allocator = root.getVector("x").getAllocator()
            before = allocator.getAllocatedMemory()

            with pytest.raises(JavaException) as failure:
                allocator.buffer(60 * MIB)

            assert (
                failure.value.java_class
                == "org.apache.arrow.memory.OutOfMemoryException"
            )
            assert allocator.getAllocatedMemory() == before
            # The server goes on, and so does the pool
            buffer = allocator.buffer(20 * MIB)
            buffer.close()
            assert _pool(client)["kept_blocks"] == 1
        finally:
            client.close()
