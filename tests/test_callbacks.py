"""Tests for Python callback support (Java calling back into Python)."""

import pytest

from gatun import JavaException


class TestCallbackBasics:
    """Basic callback registration and invocation tests."""

    def test_register_callback_returns_java_object(self, client):
        """Test that registering a callback returns a JavaObject."""
        from gatun import JavaObject

        def my_callback(*args):
            return sum(args) if args else 0

        callback_obj = client.register_callback(
            my_callback, "java.util.function.IntSupplier"
        )
        assert callback_obj is not None
        assert isinstance(callback_obj, JavaObject)

    def test_comparator_for_sorting(self, client):
        """Test using a callback as a Comparator for sorting."""

        def reverse_compare(a, b):
            # Reverse order comparison
            if a < b:
                return 1
            elif a > b:
                return -1
            return 0

        comparator = client.register_callback(reverse_compare, "java.util.Comparator")

        # Create an ArrayList with some values
        arr = client.create_object("java.util.ArrayList")
        arr.add("banana")
        arr.add("apple")
        arr.add("cherry")

        # Sort using Collections.sort with our comparator
        client.invoke_static_method("java.util.Collections", "sort", arr, comparator)

        # Should be sorted in reverse order
        assert arr.get(0) == "cherry"
        assert arr.get(1) == "banana"
        assert arr.get(2) == "apple"

    def test_comparator_for_integer_sorting(self, client):
        """Test using a callback as a Comparator for sorting integers."""

        def compare_ints(a, b):
            # Sort in descending order
            return b - a

        comparator = client.register_callback(compare_ints, "java.util.Comparator")

        # Create an ArrayList with Integer objects
        arr = client.create_object("java.util.ArrayList")
        arr.add(client.invoke_static_method("java.lang.Integer", "valueOf", 5))
        arr.add(client.invoke_static_method("java.lang.Integer", "valueOf", 2))
        arr.add(client.invoke_static_method("java.lang.Integer", "valueOf", 8))
        arr.add(client.invoke_static_method("java.lang.Integer", "valueOf", 1))

        # Sort using Collections.sort with our comparator
        client.invoke_static_method("java.util.Collections", "sort", arr, comparator)

        # Should be sorted in descending order
        assert arr.get(0) == 8
        assert arr.get(1) == 5
        assert arr.get(2) == 2
        assert arr.get(3) == 1

    def test_comparator_natural_order(self, client):
        """Test using a callback as a Comparator for natural ordering."""

        def natural_compare(a, b):
            if a < b:
                return -1
            elif a > b:
                return 1
            return 0

        comparator = client.register_callback(natural_compare, "java.util.Comparator")

        # Create a TreeSet with our comparator to test ordering
        # Note: TreeSet isn't in allowlist, using ArrayList + sort
        arr = client.create_object("java.util.ArrayList")
        arr.add("zebra")
        arr.add("alpha")
        arr.add("beta")

        client.invoke_static_method("java.util.Collections", "sort", arr, comparator)

        # Should be sorted alphabetically
        assert arr.get(0) == "alpha"
        assert arr.get(1) == "beta"
        assert arr.get(2) == "zebra"


class TestCallbackWithCollections:
    """Test callbacks used with Java collections."""

    def test_foreach_with_consumer(self, client):
        """Test using a callback with forEach."""
        collected = []

        def collect(item):
            collected.append(item)

        consumer = client.register_callback(collect, "java.util.function.Consumer")

        # Create a list and iterate with forEach
        arr = client.create_object("java.util.ArrayList")
        arr.add("one")
        arr.add("two")
        arr.add("three")

        arr.forEach(consumer)

        assert collected == ["one", "two", "three"]

    def test_replaceall_with_unaryoperator(self, client):
        """Test using a callback with replaceAll on ArrayList."""

        def uppercase(s):
            return s.upper() if isinstance(s, str) else str(s).upper()

        operator = client.register_callback(
            uppercase, "java.util.function.UnaryOperator"
        )

        arr = client.create_object("java.util.ArrayList")
        arr.add("hello")
        arr.add("world")

        arr.replaceAll(operator)

        assert arr.get(0) == "HELLO"
        assert arr.get(1) == "WORLD"

    def test_removeif_with_predicate(self, client):
        """Test using a callback with removeIf on ArrayList."""

        def is_short(s):
            return len(s) <= 3

        predicate = client.register_callback(is_short, "java.util.function.Predicate")

        arr = client.create_object("java.util.ArrayList")
        arr.add("a")
        arr.add("hello")
        arr.add("hi")
        arr.add("world")
        arr.add("bye")

        arr.removeIf(predicate)

        # Should have removed "a", "hi", "bye" (length <= 3)
        assert arr.size() == 2
        assert arr.get(0) == "hello"
        assert arr.get(1) == "world"

    def test_compute_with_bifunction(self, client):
        """Test using a callback with compute on HashMap."""

        def combine(key, old_value):
            if old_value is None:
                return "new_" + key
            return old_value + "_updated"

        bifunction = client.register_callback(combine, "java.util.function.BiFunction")

        hm = client.create_object("java.util.HashMap")
        hm.put("key1", "value1")

        # compute should update existing value
        hm.compute("key1", bifunction)
        assert hm.get("key1") == "value1_updated"

        # compute should create new value
        hm.compute("key2", bifunction)
        assert hm.get("key2") == "new_key2"


class TestCallbackErrorHandling:
    """Test error handling in callbacks."""

    def test_callback_exception_propagates(self, client):
        """Test that exceptions in callbacks are propagated."""

        def failing_callback(a, b):
            raise ValueError("Intentional error from Python")

        comparator = client.register_callback(failing_callback, "java.util.Comparator")

        arr = client.create_object("java.util.ArrayList")
        arr.add("a")
        arr.add("b")

        # Sorting should raise an exception
        with pytest.raises(JavaException) as excinfo:
            client.invoke_static_method(
                "java.util.Collections", "sort", arr, comparator
            )

        assert "Intentional error from Python" in str(excinfo.value)


class TestCallbackMultipleInvocations:
    """Test callbacks that are invoked multiple times."""

    def test_callback_invoked_multiple_times(self, client):
        """Test that a callback can be invoked multiple times."""
        call_count = [0]

        def counting_compare(a, b):
            call_count[0] += 1
            if a < b:
                return -1
            elif a > b:
                return 1
            return 0

        comparator = client.register_callback(counting_compare, "java.util.Comparator")

        arr = client.create_object("java.util.ArrayList")
        arr.add("c")
        arr.add("a")
        arr.add("b")
        arr.add("d")

        client.invoke_static_method("java.util.Collections", "sort", arr, comparator)

        # The comparator should have been called multiple times during sorting
        assert call_count[0] > 0
        # Verify sorting worked
        assert arr.get(0) == "a"
        assert arr.get(1) == "b"
        assert arr.get(2) == "c"
        assert arr.get(3) == "d"

    def test_callback_state_preserved(self, client):
        """Test that callback state is preserved across invocations."""
        seen_values = []

        def tracking_consumer(value):
            seen_values.append(value)

        consumer = client.register_callback(
            tracking_consumer, "java.util.function.Consumer"
        )

        arr = client.create_object("java.util.ArrayList")
        for i in range(5):
            arr.add(f"item_{i}")

        arr.forEach(consumer)

        assert seen_values == ["item_0", "item_1", "item_2", "item_3", "item_4"]


class TestCallbackConcurrency:
    """Test callback thread safety."""

    def test_multiple_callbacks_same_session(self, client):
        """Test that multiple callbacks can be registered and used in same session."""
        compare_calls = [0]
        consumer_calls = [0]

        def compare(a, b):
            compare_calls[0] += 1
            if a < b:
                return -1
            elif a > b:
                return 1
            return 0

        def consume(item):
            consumer_calls[0] += 1

        comparator = client.register_callback(compare, "java.util.Comparator")
        consumer = client.register_callback(consume, "java.util.function.Consumer")

        # Use both callbacks
        arr = client.create_object("java.util.ArrayList")
        arr.add("c")
        arr.add("a")
        arr.add("b")

        # Sort (uses comparator)
        client.invoke_static_method("java.util.Collections", "sort", arr, comparator)
        assert arr.get(0) == "a"

        # forEach (uses consumer)
        arr.forEach(consumer)

        assert compare_calls[0] > 0
        assert consumer_calls[0] == 3

    def test_callback_with_heavy_computation(self, client):
        """Test callbacks that do significant work don't cause issues."""
        results = []

        def heavy_compare(a, b):
            # Simulate some work
            _ = sum(range(1000))
            results.append((a, b))
            if a < b:
                return -1
            elif a > b:
                return 1
            return 0

        comparator = client.register_callback(heavy_compare, "java.util.Comparator")

        arr = client.create_object("java.util.ArrayList")
        for i in range(10):
            arr.add(str(9 - i))  # Add in reverse order

        client.invoke_static_method("java.util.Collections", "sort", arr, comparator)

        # Verify sorted correctly
        assert arr.get(0) == "0"
        assert arr.get(9) == "9"
        # Verify callback was invoked multiple times
        assert len(results) > 0


class TestCallbackCleanup:
    """Test callback cleanup and unregistration."""

    def test_unregister_callback(self, client):
        """Test that unregistered callbacks are cleaned up."""

        def my_callback(a, b):
            return 0

        callback = client.register_callback(my_callback, "java.util.Comparator")
        callback_id = callback.object_id

        # Verify callback is registered
        assert callback_id in client._callbacks

        # Unregister
        client.unregister_callback(callback_id)

        # Callback should be removed from client's registry
        assert callback_id not in client._callbacks


class TestReentrancyDetection:
    """Test that nested Java calls from callbacks are detected and prevented."""

    def test_nested_call_raises_reentrancy_error(self, client):
        """Test that calling Java from within a callback raises ReentrancyError."""
        from gatun import ReentrancyError

        nested_call_attempted = [False]
        error_caught = [None]

        def reentrant_callback(a, b):
            """Callback that tries to make a nested Java call."""
            nested_call_attempted[0] = True
            try:
                # This should raise ReentrancyError
                client.create_object("java.util.ArrayList")
            except ReentrancyError as e:
                error_caught[0] = e
                raise  # Re-raise so Java sees the error
            return 0

        comparator = client.register_callback(
            reentrant_callback, "java.util.Comparator"
        )

        arr = client.create_object("java.util.ArrayList")
        arr.add("b")
        arr.add("a")

        # The sort will trigger the callback, which will try to make a nested call
        with pytest.raises(JavaException) as excinfo:
            client.invoke_static_method(
                "java.util.Collections", "sort", arr, comparator
            )

        # Verify the callback was called and the error was caught
        assert nested_call_attempted[0], "Callback was not invoked"
        assert error_caught[0] is not None, "ReentrancyError was not caught in callback"
        assert "ReentrancyError" in str(excinfo.value)

    def test_nested_invoke_method_raises_reentrancy_error(self, client):
        """Test that invoke_method from callback also raises ReentrancyError."""
        from gatun import ReentrancyError

        # Create an object before the callback
        other_arr = client.create_object("java.util.ArrayList")
        error_caught = [None]

        def reentrant_invoke_callback(a, b):
            try:
                # Try to call a method on an existing object - should fail
                client.invoke_method(other_arr.object_id, "size")
            except ReentrancyError as e:
                error_caught[0] = e
                raise
            return 0

        comparator = client.register_callback(
            reentrant_invoke_callback, "java.util.Comparator"
        )

        arr = client.create_object("java.util.ArrayList")
        arr.add("x")
        arr.add("y")

        with pytest.raises(JavaException):
            client.invoke_static_method(
                "java.util.Collections", "sort", arr, comparator
            )

        assert error_caught[0] is not None

    def test_callback_without_nesting_works(self, client):
        """Test that normal callbacks without nested calls still work."""
        call_count = [0]

        def simple_callback(a, b):
            # Just do local Python work, no Java calls
            call_count[0] += 1
            if a < b:
                return -1
            elif a > b:
                return 1
            return 0

        comparator = client.register_callback(simple_callback, "java.util.Comparator")

        arr = client.create_object("java.util.ArrayList")
        arr.add("c")
        arr.add("a")
        arr.add("b")

        # This should work fine
        client.invoke_static_method("java.util.Collections", "sort", arr, comparator)

        assert call_count[0] > 0
        assert arr.get(0) == "a"
        assert arr.get(1) == "b"
        assert arr.get(2) == "c"

    def test_in_callback_flag_reset_after_error(self, client):
        """Test that _in_callback flag is reset even if callback raises."""

        def failing_callback(a, b):
            raise ValueError("Intentional failure")

        comparator = client.register_callback(failing_callback, "java.util.Comparator")

        arr = client.create_object("java.util.ArrayList")
        arr.add("b")
        arr.add("a")

        # This will fail due to the callback error
        with pytest.raises(JavaException):
            client.invoke_static_method(
                "java.util.Collections", "sort", arr, comparator
            )

        # But the client should still be usable (flag was reset)
        assert not client._in_callback
        new_arr = client.create_object("java.util.ArrayList")
        assert new_arr.size() == 0


class TestCallbackInterfaces:
    def test_default_methods_run_as_java_wrote_them(self, client):
        """`Comparator.reversed()` is Java code; only `compare` goes to Python."""
        comparator = client.register_callback(
            lambda a, b: a - b, "java.util.Comparator"
        )
        items = client.jvm.java.util.ArrayList()
        for value in (3, 1, 2):
            items.add(value)

        client.jvm.java.util.Collections.sort(items, comparator.reversed())
        assert list(items) == [3, 2, 1]

    def test_chained_default_methods(self, client):
        is_even = client.register_callback(
            lambda value: value % 2 == 0, "java.util.function.Predicate"
        )
        items = client.jvm.java.util.ArrayList()
        for value in range(6):
            items.add(value)

        items.removeIf(is_even.negate())
        assert list(items) == [0, 2, 4]


class TestCallbackResults:
    """What Python returns has to be what the Java method declares."""

    def _sort_with(self, client, callback):
        items = client.jvm.java.util.ArrayList()
        items.add(2)
        items.add(1)
        comparator = client.register_callback(callback, "java.util.Comparator")
        client.jvm.java.util.Collections.sort(items, comparator)
        return list(items)

    def test_none_for_a_primitive_is_reported(self, client):
        """It used to surface as a NullPointerException with no mention of why."""
        with pytest.raises(JavaException, match="returned None for compare"):
            self._sort_with(client, lambda a, b: None)
        assert client.ping() is True

    def test_wrong_type_is_reported(self, client):
        with pytest.raises(
            JavaException, match="String for compare, which returns int"
        ):
            self._sort_with(client, lambda a, b: "bigger")
        assert client.ping() is True

    def test_bool_is_not_an_int(self, client):
        with pytest.raises(JavaException, match="for compare, which returns int"):
            self._sort_with(client, lambda a, b: a > b)

    def test_int_is_widened_for_a_long_or_double(self, client):
        to_long = client.register_callback(
            lambda value: 7, "java.util.function.ToLongFunction"
        )
        to_double = client.register_callback(
            lambda value: 2, "java.util.function.ToDoubleFunction"
        )
        items = client.jvm.java.util.ArrayList()
        items.add("a")
        items.add("b")

        assert items.stream().mapToLong(to_long).sum() == 14
        assert items.stream().mapToDouble(to_double).sum() == 4.0

    def test_result_too_large_for_the_channel(self, client):
        grow = client.register_callback(
            lambda value: "z" * 70_000, "java.util.function.Function"
        )
        items = client.jvm.java.util.ArrayList()
        items.add("a")

        with pytest.raises(JavaException, match="too large to return"):
            items.stream().map(grow).toList()
        assert client.ping() is True

    def test_arguments_too_large_for_the_channel(self, client):
        seen = []
        consumer = client.register_callback(seen.append, "java.util.function.Consumer")
        inner = client.jvm.java.util.ArrayList()
        for _ in range(3):
            inner.add("y" * 30_000)
        outer = client.jvm.java.util.ArrayList()
        outer.add(inner)

        with pytest.raises(JavaException, match="more than the 65536"):
            outer.forEach(consumer)
        assert seen == []
        assert client.ping() is True


class TestCallbacksDuringMultiCommandRequests:
    """The callback's answer travels in the zone the request itself arrived in."""

    def test_callback_inside_a_batch(self, client):
        comparator = client.register_callback(
            lambda a, b: a - b, "java.util.Comparator"
        )
        items = client.jvm.java.util.ArrayList()
        for value in (3, 1, 2):
            items.add(value)

        with client.batch() as b:
            b.call_static("java.util.Collections", "sort", items, comparator)
            size = b.call(items, "size")
            first = b.call(items, "get", 0)
            b.call(items, "add", 9)

        assert size.get() == 3
        assert first.get() == 1
        assert list(items) == [1, 2, 3, 9]

    def test_callback_inside_invoke_methods(self, client):
        is_odd = client.register_callback(
            lambda value: value % 2 == 1, "java.util.function.Predicate"
        )
        items = client.jvm.java.util.ArrayList()
        for value in range(5):
            items.add(value)

        results = client.invoke_methods(
            items, [("removeIf", (is_odd,)), ("size", ()), ("get", (1,))]
        )
        assert results == [True, 3, 2]


OTHER_THREAD_JAVA = """
import java.util.function.Supplier;

public class OtherThread {
  private static volatile String lastError;

  /** Run the supplier on a new thread and wait for it. */
  public static Object call(Supplier<Object> supplier) throws Exception {
    Object[] out = new Object[1];
    Throwable[] failure = new Throwable[1];
    Thread thread = new Thread(() -> {
      try {
        out[0] = supplier.get();
      } catch (Throwable t) {
        failure[0] = t;
      }
    });
    thread.start();
    thread.join();
    if (failure[0] != null) {
      throw new RuntimeException(failure[0]);
    }
    return out[0];
  }

  /** Run the task on a new thread after a delay, without waiting for it. */
  public static void later(Runnable task, long delayMs) {
    lastError = null;
    Thread thread = new Thread(() -> {
      try {
        Thread.sleep(delayMs);
        task.run();
        lastError = "";
      } catch (Throwable t) {
        lastError = String.valueOf(t.getMessage());
      }
    });
    thread.setDaemon(true);
    thread.start();
  }

  public static String lastError() {
    return lastError;
  }
}
"""


class TestCallbacksFromOtherThreads:
    """Only the thread running the request has the channel and its memory."""

    @pytest.fixture
    def other_thread(self, compile_java):
        from gatun import connect

        classes = compile_java("OtherThread", OTHER_THREAD_JAVA)
        client = connect(
            memory="16MB", classpath=[classes], allowed_classes=["OtherThread"]
        )
        yield client
        client.close()

    def test_callback_on_another_thread_is_refused(self, other_thread):
        """Even during the request: the refusal names the thread and the rule."""
        client = other_thread
        calls = []
        supplier = client.register_callback(
            lambda: calls.append(1), "java.util.function.Supplier"
        )

        with pytest.raises(JavaException, match="called from thread") as exc_info:
            client.jvm.OtherThread.call(supplier)

        assert "thread that is running its request" in str(exc_info.value)
        assert calls == []
        assert client.jvm.java.lang.Math.max(1, 2) == 2

    def test_callback_with_no_request_running_is_refused(self, other_thread):
        """Nobody is reading the channel then; the call must not be written into
        whatever request comes next."""
        import time

        client = other_thread
        calls = []
        task = client.register_callback(lambda: calls.append(1), "java.lang.Runnable")

        client.jvm.OtherThread.later(task, 300)
        time.sleep(1.0)  # the task fires while this client is idle

        assert "called from thread" in client.jvm.OtherThread.lastError()
        assert calls == []
        assert client.jvm.java.lang.Math.max(1, 2) == 2

    def test_callback_on_the_request_thread_still_works(self, other_thread):
        client = other_thread
        doubled = client.register_callback(
            lambda value: value * 2, "java.util.function.Function"
        )
        items = client.jvm.java.util.ArrayList(list(range(50)))

        assert items.stream().map(doubled).toList() == [2 * i for i in range(50)]


class TestLongExceptionChains:
    def test_root_cause_survives_a_long_trace(self, compile_java):
        """The cause is at the end of the trace, which is what used to be cut."""
        from gatun import connect

        classes = compile_java(
            "Deep",
            "public class Deep {\n"
            "  public static void fail(int depth) {\n"
            "    if (depth > 0) { fail(depth - 1); return; }\n"
            "    try { inner(400); } catch (RuntimeException e) {\n"
            '      throw new IllegalStateException("wrapper", e);\n'
            "    }\n"
            "  }\n"
            "  private static void inner(int depth) {\n"
            "    if (depth > 0) { inner(depth - 1); return; }\n"
            '    throw new UnsupportedOperationException("the root cause");\n'
            "  }\n"
            "}\n",
        )
        client = connect(memory="16MB", classpath=[classes], allowed_classes=["Deep"])
        try:
            with pytest.raises(JavaException) as exc_info:
                client.jvm.Deep.fail(400)
            text = str(exc_info.value)
            assert "java.lang.IllegalStateException: wrapper" in text
            assert (
                "Caused by: java.lang.UnsupportedOperationException: the root cause"
                in text
            )
            assert "Deep.inner" in text
            assert len(text) < 16_000
            assert client.ping() is True
        finally:
            client.close()
