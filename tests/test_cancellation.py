"""Tests for cancelling a request that is running."""

import threading
import time

import pytest

from gatun import connect
from gatun.client import CancelledException


@pytest.fixture
def sleeper():
    """A client allowed to call Thread.sleep, the simplest long request."""
    client = connect(memory="16MB", allowed_classes=["java.lang.Thread"])
    yield client
    client.close()


def _cancel_soon(client, delay: float = 0.4) -> tuple[threading.Thread, list]:
    outcome: list = []

    def run():
        time.sleep(delay)
        outcome.append(client.cancel())

    thread = threading.Thread(target=run)
    thread.start()
    return thread, outcome


class TestCancellation:
    def test_running_request_is_interrupted(self, sleeper):
        thread, outcome = _cancel_soon(sleeper)
        started = time.monotonic()

        with pytest.raises(CancelledException):
            sleeper.jvm.java.lang.Thread.sleep(20_000)

        thread.join()
        assert time.monotonic() - started < 10
        assert outcome == [True]

    def test_session_is_usable_after_a_cancel(self, sleeper):
        thread, _ = _cancel_soon(sleeper)
        with pytest.raises(CancelledException):
            sleeper.jvm.java.lang.Thread.sleep(20_000)
        thread.join()

        # The interrupt belonged to that request; the next one is not affected
        sleeper.jvm.java.lang.Thread.sleep(50)
        items = sleeper.jvm.java.util.ArrayList()
        items.add("after")
        assert items.size() == 1

    def test_cancel_ends_a_batch(self, sleeper):
        """Not only the command that was running: the ones after it never run,
        and the batch fails as a whole instead of reporting them done."""
        numbers = sleeper.jvm.java.util.ArrayList()
        thread, outcome = _cancel_soon(sleeper)

        with pytest.raises(CancelledException), sleeper.batch() as b:
            b.call_static("java.lang.Thread", "sleep", 20_000)
            added = b.call(numbers, "add", 1)

        thread.join()
        assert outcome == [True]
        assert numbers.size() == 0
        with pytest.raises(RuntimeError, match="The request was cancelled"):
            added.get()

    def test_cancel_with_nothing_running(self, client):
        assert client.cancel() is False
        assert client.ping() is True

    def test_cancel_does_not_carry_over_to_the_next_request(self, sleeper):
        assert sleeper.cancel() is False
        sleeper.jvm.java.lang.Thread.sleep(50)

    def test_cancel_reaches_only_its_own_session(self, sleeper):
        from gatun.client import GatunClient

        other = GatunClient(sleeper.socket_path)
        assert other.connect()
        try:
            thread, outcome = _cancel_soon(other)
            started = time.monotonic()
            sleeper.jvm.java.lang.Thread.sleep(1500)
            thread.join()

            assert time.monotonic() - started >= 1.4
            assert outcome == [False]
        finally:
            other.close()

    def test_cancel_during_a_callback(self, sleeper):
        """Cancelled while Python is running a callback: the request still ends."""
        thread_and_outcome = []

        def slow(a, b):
            if not thread_and_outcome:
                thread_and_outcome.append(_cancel_soon(sleeper, delay=0.1))
            time.sleep(0.5)
            return a - b

        comparator = sleeper.register_callback(slow, "java.util.Comparator")
        items = sleeper.jvm.java.util.ArrayList()
        for value in (3, 1, 2):
            items.add(value)

        with pytest.raises(CancelledException):
            sleeper.jvm.java.util.Collections.sort(items, comparator)
        thread_and_outcome[0][0].join()
        assert thread_and_outcome[0][1] == [True]
        assert sleeper.ping() is True

    def test_cancel_on_a_closed_client(self, make_client):
        client = make_client()
        client.close()
        assert client.cancel() is False


class TestCancelledException:
    def test_request_id_is_kept_when_the_server_names_one(self):
        exc = CancelledException(42)
        assert exc.request_id == 42
        assert "42" in str(exc)
        assert "cancelled" in str(exc).lower()

    def test_message_without_a_request_id(self):
        exc = CancelledException()
        assert exc.request_id is None
        assert str(exc) == "The request was cancelled"

    def test_is_not_a_runtime_error(self):
        assert issubclass(CancelledException, Exception)
        assert not issubclass(CancelledException, RuntimeError)
