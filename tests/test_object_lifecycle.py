"""Tests for how JavaObject wrappers are released, and for datetime arguments.

Both used to break the single request/response channel: a finalizer could send
FreeObject in the middle of another exchange, and a datetime argument made a
nested Java call that overwrote the half-built outer command.
"""

import datetime
import gc
import threading

import pytest

from gatun import JavaException, PayloadTooLargeError, connect


class TestWrapperRelease:
    """JavaObject finalizers must never disturb a request in flight."""

    def test_callback_receiving_java_objects(self, client):
        """A callback can be handed Java objects without breaking the call.

        Regression: the wrappers for one callback's arguments were finalized as
        soon as it returned, and the finalizer sent FreeObject while Java was
        already waiting for the next CallbackResponse ("Expected
        CallbackResponse but got action: 4").
        """
        seen = []

        def compare(a, b):
            seen.append((type(a).__name__, type(b).__name__))
            return 0

        comparator = client.register_callback(compare, "java.util.Comparator")
        items = client.jvm.java.util.ArrayList()
        for text in ("b", "a", "c", "d"):
            items.add(client.create_object("java.lang.StringBuilder", text))

        client.jvm.java.util.Collections.sort(items, comparator)

        assert len(seen) >= 2
        assert set(seen) == {("JavaObject", "JavaObject")}
        assert items.size() == 4
        assert client.ping() is True

    def test_wrappers_collected_inside_callback(self, client):
        """Wrappers that die while a callback is running are freed afterwards.

        Regression: the finalizer raised ReentrancyError (swallowed by weakref),
        so the Java objects were never released.
        """
        garbage = [client.create_object("java.lang.StringBuilder") for _ in range(5)]
        garbage_ids = {obj.object_id for obj in garbage}

        def compare(a, b):
            garbage.clear()
            gc.collect()
            return 0

        comparator = client.register_callback(compare, "java.util.Comparator")
        items = client.jvm.java.util.ArrayList()
        for text in ("b", "a", "c"):
            items.add(text)

        client.jvm.java.util.Collections.sort(items, comparator)

        assert garbage_ids <= set(client._pending_frees)
        assert items.size() == 3  # the next request delivers the frees
        assert not client._pending_frees

    def test_many_dropped_objects_keep_connection_alive(self, client):
        """Dropping a large number of wrappers at once is fine.

        Regression: more than 100 unread FreeObject responses made the client
        declare the connection dead.
        """
        objects = [client.create_object("java.util.ArrayList") for _ in range(500)]
        del objects
        gc.collect()

        assert client.ping() is True
        arr = client.create_object("java.util.ArrayList")
        arr.add("still works")
        assert arr.size() == 1

    def test_collected_wrapper_is_freed_by_next_request(self, client):
        """A finalizer only queues the ID; the next request delivers the free."""
        obj = client.create_object("java.util.ArrayList")
        object_id = obj.object_id

        del obj
        gc.collect()
        assert object_id in client._pending_frees

        assert client.ping() is True
        assert not client._pending_frees

    def test_explicit_free_is_immediate(self, client):
        obj = client.create_object("java.util.ArrayList")
        object_id = obj.object_id
        obj.detach()

        client.free_object(object_id)

        assert not client._pending_frees
        with pytest.raises(JavaException, match="not found"):
            client.invoke_method(object_id, "size")

    def test_batch_whose_response_is_too_large_keeps_nothing(self):
        """The client never learns the IDs of what the batch made."""
        # The count is the whole server's, so on a server of its own: on the
        # shared one, sessions of earlier tests are still being cleaned up
        client = connect(memory="16MB")

        def live_objects():
            report = client.get_metrics()
            return int(report.split("current_objects:")[1].split()[0])

        try:
            text = client.create_object("java.lang.String", "ab")
            before = live_objects()

            with pytest.raises(PayloadTooLargeError), client.batch() as b:
                for _ in range(5):
                    b.create("java.util.ArrayList")
                b.call(text, "repeat", 40_000)

            assert live_objects() == before
        finally:
            client.close()

    def test_batch_keeps_its_targets_alive(self, client):
        """A batch target with no other reference survives until the batch runs."""
        with client.batch() as b:
            size = b.call(client.create_object("java.util.ArrayList"), "size")
            gc.collect()

        assert size.get() == 0

    def test_objects_nested_in_a_returned_list_are_freed(self, client):
        """Regression: objects inside a returned collection were registered
        outside the session, so nothing could ever free them."""
        sb = client.create_object("java.lang.StringBuilder", "x")
        nested = client.jvm.java.util.Arrays.asList(sb, sb)
        nested_ids = [obj.object_id for obj in nested]
        assert client.invoke_method(nested_ids[0], "toString") == "x"

        del nested
        gc.collect()

        for object_id in nested_ids:
            with pytest.raises(JavaException, match="not found"):
                client.invoke_method(object_id, "toString")
        assert sb.toString() == "x"

    def test_objects_in_a_response_that_is_too_large_are_released(self):
        """The client never sees the IDs in a response that did not fit, so the
        server must not keep the objects, or count them against the session."""
        client = connect(extra_jvm_flags=["-Dgatun.limits.max_objects=50"])
        try:
            sb = client.create_object("java.lang.StringBuilder", "x")
            with pytest.raises(PayloadTooLargeError):
                client.jvm.java.util.Collections.nCopies(5000, sb)

            held = [client.create_object("java.util.ArrayList") for _ in range(40)]
            assert len(held) == 40
        finally:
            client.close()

    def test_wrappers_dropped_on_another_thread(self, client):
        """Wrappers released by another thread do not corrupt requests in flight."""
        arr = client.create_object("java.util.ArrayList")
        doomed = [client.create_object("java.util.ArrayList") for _ in range(300)]
        started = threading.Event()

        def drop():
            started.wait()
            while doomed:
                doomed.pop()

        worker = threading.Thread(target=drop)
        worker.start()
        started.set()
        for i in range(300):
            arr.add(i)
        worker.join()

        assert arr.size() == 300
        assert client.ping() is True

    def test_dropped_objects_are_freed_on_the_server(self):
        """Queued frees reach the server: the object limit is never hit."""
        client = connect(extra_jvm_flags=["-Dgatun.limits.max_objects=20"])
        try:
            for _ in range(200):
                # Each wrapper is dropped as soon as the next one replaces it
                obj = client.create_object("java.util.ArrayList")
                obj.add(1)
        finally:
            client.close()

    def test_callback_arguments_count_against_object_limit(self):
        """Freeing callback-argument wrappers must not create room that isn't there.

        Regression: the server registered callback arguments without counting
        them, then decremented the session's object count when they were freed,
        so repeated callbacks drove the count down and defeated max_objects.
        """
        client = connect(extra_jvm_flags=["-Dgatun.limits.max_objects=8"])
        try:

            def compare(a, b):
                return 0

            comparator = client.register_callback(compare, "java.util.Comparator")
            items = client.jvm.java.util.ArrayList()
            builders = [
                client.create_object("java.lang.StringBuilder", text)
                for text in ("b", "a", "c", "d")
            ]
            for builder in builders:
                items.add(builder)
            for _ in range(5):
                client.jvm.java.util.Collections.sort(items, comparator)
            gc.collect()
            client.ping()  # delivers the frees for the callback arguments

            held = []
            with pytest.raises(JavaException, match="Object limit exceeded"):
                for _ in range(8):
                    held.append(client.create_object("java.util.ArrayList"))
            assert len(held) < 8
        finally:
            client.close()


class TestDatetimeArguments:
    """datetime values are converted through temporary java.sql objects."""

    def test_naive_datetime(self, client):
        arr = client.jvm.java.util.ArrayList()
        arr.add(datetime.datetime(2026, 1, 2, 3, 4, 5, 123456))

        assert str(arr.get(0)) == "2026-01-02 03:04:05.123456"
        assert client.ping() is True

    def test_aware_datetime(self, client):
        value = datetime.datetime(2026, 1, 2, 3, 4, 5, tzinfo=datetime.UTC)
        arr = client.jvm.java.util.ArrayList()
        arr.add(value)

        assert arr.get(0).getTime() == int(value.timestamp() * 1000)

    def test_date(self, client):
        arr = client.jvm.java.util.ArrayList()
        arr.add(datetime.date(2024, 2, 29))

        assert str(arr.get(0)) == "2024-02-29"

    def test_time(self, client):
        arr = client.jvm.java.util.ArrayList()
        arr.add(datetime.time(23, 59, 58))

        assert str(arr.get(0)) == "23:59:58"

    def test_several_in_one_call(self, client):
        """Each conversion is its own Java call; none may disturb the others."""
        sb = client.jvm.java.lang.StringBuilder()
        formatted = client.jvm.java.lang.String.format(
            "%s|%s|%s|%s",
            datetime.date(2026, 1, 2),
            "plain",
            datetime.time(1, 2, 3),
            datetime.date(2027, 3, 4),
        )

        assert formatted == "2026-01-02|plain|01:02:03|2027-03-04"
        assert sb.length() == 0

    def test_inside_collections(self, client):
        holder = client.jvm.java.util.ArrayList()
        holder.add([datetime.date(2026, 1, 2), "x"])
        holder.add({"when": datetime.time(1, 2, 3)})

        assert holder.toString() == "[[2026-01-02, x], {when=01:02:03}]"

    def test_in_batch(self, client):
        arr = client.jvm.java.util.ArrayList()
        with client.batch() as b:
            b.call(arr, "add", datetime.date(2026, 1, 2))
            b.call(arr, "add", datetime.datetime(2026, 1, 2, 3, 4, 5))
            size = b.call(arr, "size")

        assert size.get() == 2
        assert arr.toString() == "[2026-01-02, 2026-01-02 03:04:05.0]"

    def test_in_invoke_methods(self, client):
        arr = client.jvm.java.util.ArrayList()
        results = client.invoke_methods(
            arr,
            [
                ("add", (datetime.date(2026, 1, 2),)),
                ("add", (datetime.time(1, 2, 3),)),
                ("toString", ()),
            ],
        )

        assert results == [True, True, "[2026-01-02, 01:02:03]"]

    def test_no_extra_java_objects(self, client):
        """The value travels in the command itself; nothing is created to carry it."""
        arr = client.jvm.java.util.ArrayList()
        gc.collect()
        client.ping()
        before = client.get_metrics()

        arr.add(datetime.date(2026, 1, 2))

        assert not client._pending_frees
        assert _action_count(client.get_metrics(), "CreateObject") == _action_count(
            before, "CreateObject"
        )

    def test_before_epoch(self, client):
        value = datetime.datetime(1969, 12, 31, 23, 59, 59, 250000, tzinfo=datetime.UTC)
        arr = client.jvm.java.util.ArrayList()
        arr.add(value)
        arr.add(datetime.date(1900, 3, 1))

        assert arr.get(0).getTime() == -750
        assert arr.get(0).getNanos() == 250_000_000
        assert str(arr.get(1)) == "1900-03-01"

    def test_with_type_hint(self, client):
        """A TypeHint still steers overload resolution for a converted value."""
        from gatun.client import TypeHint

        text = client.jvm.java.lang.String.valueOf(
            TypeHint(datetime.date(2026, 1, 2), "java.lang.Object")
        )

        assert text == "2026-01-02"

    def test_returned_from_callback(self, client):
        """Regression: needed a Java call, which is not allowed inside a callback."""

        def today(_):
            return datetime.date(2026, 1, 2)

        function = client.register_callback(today, "java.util.function.Function")
        holder = client.jvm.java.util.HashMap()
        holder.computeIfAbsent("when", function)

        assert holder.toString() == "{when=2026-01-02}"

    @pytest.mark.parametrize(
        "value",
        [
            datetime.datetime(2026, 1, 2, 3, 4, 5),
            datetime.date(2026, 1, 2),
            datetime.time(3, 4, 5),
        ],
        ids=["datetime", "date", "time"],
    )
    def test_connection_survives(self, client, value):
        """Regression: any datetime argument used to kill the connection."""
        client.jvm.java.util.ArrayList().add(value)

        assert client.jvm.java.lang.Integer.parseInt("42") == 42


def _action_count(metrics: str, action: str) -> int:
    """Read one action's request count out of a metrics report."""
    for line in metrics.splitlines():
        if line.strip().startswith(f"{action}:"):
            return int(line.split("count=")[1].split()[0])
    return 0
