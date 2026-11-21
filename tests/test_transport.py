"""The client's side of the channel: threads, interruptions, timeouts, closing."""

import threading
import time

import pytest

import gatun.client as client_module
from gatun import JavaException, PayloadTooLargeError, connect
from gatun.client import DeadConnectionError, GatunClient, SocketTimeoutError


class TestThreads:
    def test_threads_share_one_client(self, client):
        """One exchange at a time: threads used to interleave on the channel."""
        errors: list[BaseException] = []

        def work(seed: int):
            try:
                items = client.jvm.java.util.ArrayList()
                for i in range(150):
                    items.add(seed * 1000 + i)
                assert items.size() == 150
                assert items.get(149) == seed * 1000 + 149
            except BaseException as e:  # noqa: BLE001 - reported below
                errors.append(e)

        threads = [threading.Thread(target=work, args=(n,)) for n in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        assert errors == []
        assert client.ping() is True

    def test_threads_build_their_commands_apart(self, client):
        """A command is built before the channel is taken; with one shared
        buffer, threads built into each other's commands."""
        import sys

        errors: list[BaseException] = []
        String = client.jvm.java.lang.String

        def work(seed: int):
            try:
                for i in range(300):
                    text = f"thread-{seed}-call-{i}-" + "x" * (seed * 7)
                    assert String.valueOf(text) == text
            except BaseException as e:  # noqa: BLE001 - reported below
                errors.append(e)

        interval = sys.getswitchinterval()
        sys.setswitchinterval(1e-6)  # switch threads as often as possible
        try:
            threads = [threading.Thread(target=work, args=(n,)) for n in range(8)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()
        finally:
            sys.setswitchinterval(interval)

        assert errors == []

    def test_threads_sending_arrow_tables(self, client):
        """Writing the payload zone and sending the request are one step."""
        import sys

        import pyarrow as pa

        errors: list[BaseException] = []

        def work(rows: int):
            try:
                table = pa.table({"n": list(range(rows))})
                for _ in range(40):
                    assert client.send_arrow_table(table) == f"Received {rows} rows"
            except BaseException as e:  # noqa: BLE001 - reported below
                errors.append(e)

        interval = sys.getswitchinterval()
        sys.setswitchinterval(1e-6)
        try:
            threads = [
                threading.Thread(target=work, args=(rows,))
                for rows in (10, 2000, 30, 4000)
            ]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()
        finally:
            sys.setswitchinterval(interval)

        assert errors == []

    def test_another_thread_waits_out_a_callback(self, client):
        """A callback on one thread does not make other threads reentrant."""
        results: list = []

        def slow(a, b):
            time.sleep(0.05)
            return a - b

        comparator = client.register_callback(slow, "java.util.Comparator")
        items = client.jvm.java.util.ArrayList()
        for value in (5, 4, 3, 2, 1):
            items.add(value)

        def other():
            time.sleep(0.02)
            try:
                results.append(client.jvm.java.lang.Math.max(1, 2))
            except BaseException as e:  # noqa: BLE001 - reported below
                results.append(e)

        thread = threading.Thread(target=other)
        thread.start()
        client.jvm.java.util.Collections.sort(items, comparator)
        thread.join()

        assert results == [2]
        assert list(items) == [1, 2, 3, 4, 5]


class TestCallbackThatWaitsForAnotherThread:
    def test_helper_thread_runs_once_the_callback_times_out(self, java_gateway):
        """A callback that waits for a thread calling Java waits for itself: the
        helper needs the channel, which the callback's own request holds. The
        callback timeout is what ends it."""
        client = GatunClient(str(java_gateway.socket_path), callback_timeout=1.0)
        assert client.connect()
        try:
            outcome: list = []

            def helper():
                try:
                    outcome.append(client.ping())
                except BaseException as e:  # noqa: BLE001 - recorded below
                    outcome.append(e)

            def waits(a, b):
                thread = threading.Thread(target=helper)
                thread.start()
                thread.join(timeout=20)
                return 0

            comparator = client.register_callback(waits, "java.util.Comparator")
            items = client.jvm.java.util.ArrayList()
            items.add(2)
            items.add(1)

            with pytest.raises(JavaException, match="CallbackTimeoutError"):
                client.jvm.java.util.Collections.sort(items, comparator)

            deadline = time.monotonic() + 10
            while not outcome and time.monotonic() < deadline:
                time.sleep(0.05)
            assert outcome == [True]
        finally:
            client.close()


class TestInterruptedExchange:
    def test_interrupt_while_waiting_marks_the_client_dead(
        self, make_client, monkeypatch
    ):
        """Ctrl-C between sending a request and reading its answer.

        The answer is still on its way; the next request would read it as its own.
        """
        client = make_client()
        real = client_module._recv_exactly
        calls = []

        def interrupted(*args, **kwargs):
            calls.append(1)
            raise KeyboardInterrupt

        monkeypatch.setattr(client_module, "_recv_exactly", interrupted)
        with pytest.raises(KeyboardInterrupt):
            client.ping()
        monkeypatch.setattr(client_module, "_recv_exactly", real)

        assert calls
        with pytest.raises(DeadConnectionError):
            client.ping()

    def test_request_that_was_never_sent_leaves_the_client_usable(self, client):
        items = client.jvm.java.util.ArrayList()
        with pytest.raises(PayloadTooLargeError) as exc_info:
            items.add("x" * (client.max_message_size + 1))

        assert exc_info.value.max_size == client.max_message_size
        assert "connect with more" in str(exc_info.value)
        assert client.ping() is True
        assert items.size() == 0


class TestTimeouts:
    @pytest.mark.parametrize("timeout", [0, -1, float("inf"), None])
    def test_values_that_mean_no_timeout(self, java_gateway, timeout):
        """0 used to make every read fail at once; inf raised inside the socket."""
        client = GatunClient(
            str(java_gateway.socket_path),
            socket_timeout=timeout,
            callback_timeout=timeout,
        )
        try:
            assert client.connect()
            assert client.ping() is True
            doubled = client.register_callback(
                lambda value: value * 2, "java.util.function.UnaryOperator"
            )
            items = client.jvm.java.util.ArrayList()
            items.add(4)
            items.replaceAll(doubled)
            assert list(items) == [8]
        finally:
            client.close()

    def test_timeout_set_through_connect(self):
        client = connect(
            memory="16MB", allowed_classes=["java.lang.Thread"], socket_timeout=0.5
        )
        try:
            with pytest.raises(SocketTimeoutError):
                client.jvm.java.lang.Thread.sleep(5000)
            # The late answer would be read by the next request
            with pytest.raises(DeadConnectionError):
                client.ping()
        finally:
            client.close()


class TestClosing:
    def test_use_after_close(self, make_client):
        client = make_client()
        items = client.jvm.java.util.ArrayList()
        client.close()

        with pytest.raises(DeadConnectionError, match="closed"):
            client.ping()
        with pytest.raises(DeadConnectionError, match="closed"):
            items.size()

    def test_close_twice(self, make_client):
        client = make_client()
        client.close()
        client.close()


class TestFailedConnect:
    def test_reason_is_kept(self, tmp_path):
        client = GatunClient(str(tmp_path / "nothing.sock"))
        assert client.connect() is False
        assert isinstance(client.connect_error, OSError)

    def test_context_manager_raises_with_the_reason(self, tmp_path):
        unconnected = GatunClient(str(tmp_path / "nothing.sock"))
        with pytest.raises(RuntimeError) as exc_info:
            unconnected.__enter__()
        assert isinstance(exc_info.value.__cause__, OSError)


class TestBatchFailures:
    def test_batch_too_large_to_send_has_not_run(self, client):
        items = client.jvm.java.util.ArrayList()
        batch = client.batch()
        third = client.max_message_size // 3
        results = [batch.call(items, "add", "x" * third) for _ in range(4)]

        with pytest.raises(PayloadTooLargeError):
            batch.execute()

        with pytest.raises(RuntimeError, match="Must call batch.execute"):
            results[0].get()
        assert items.size() == 0

    def test_results_of_a_batch_that_failed_as_a_whole(self, make_client, monkeypatch):
        """Sent but never answered: no result exists, and each one says why."""
        client = make_client()
        items = client.jvm.java.util.ArrayList()
        batch = client.batch()
        added = batch.call(items, "add", "x")

        def interrupted(*args, **kwargs):
            raise KeyboardInterrupt

        monkeypatch.setattr(client_module, "_recv_exactly", interrupted)
        with pytest.raises(KeyboardInterrupt):
            batch.execute()

        assert added.is_error is True
        with pytest.raises(RuntimeError, match="failed as a whole"):
            added.get()

    def test_batch_inside_a_callback_can_run_afterwards(self, client):
        """A batch refused for reentrancy has not run, and can still be run."""
        items = client.jvm.java.util.ArrayList()
        batch = client.batch()
        added = batch.call(items, "add", "later")
        seen = []

        def callback(a, b):
            try:
                batch.execute()
            except Exception as e:  # noqa: BLE001 - recorded for the assertion
                seen.append(type(e).__name__)
            return 0

        comparator = client.register_callback(callback, "java.util.Comparator")
        pair = client.jvm.java.util.ArrayList()
        pair.add(2)
        pair.add(1)
        client.jvm.java.util.Collections.sort(pair, comparator)

        assert seen and set(seen) == {"ReentrancyError"}
        batch.execute()
        assert added.get() is True
        assert list(items) == ["later"]
