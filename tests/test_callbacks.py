"""Tests for Python callback support (Java calling back into Python)."""

import asyncio
import gc
import glob
import threading
import time
import weakref

import pytest

import gatun.client
from gatun import JavaException, aconnect, connect
from gatun.client import GatunClient


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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.function.Function;
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

  /** Apply the function to the argument on a new thread and wait for it. */
  public static Object apply(Function<Object, Object> function, Object argument)
      throws Exception {
    return call(() -> function.apply(argument));
  }

  /** Apply the function to 0..count-1, each on its own thread, all released together. */
  public static List<Object> applyAll(Function<Object, Object> function, int count)
      throws Exception {
    Object[] out = new Object[count];
    Throwable[] failure = new Throwable[1];
    CountDownLatch go = new CountDownLatch(1);
    Thread[] threads = new Thread[count];
    for (int i = 0; i < count; i++) {
      int index = i;
      threads[i] = new Thread(() -> {
        try {
          go.await();
          out[index] = function.apply(index);
        } catch (Throwable t) {
          failure[0] = t;
        }
      });
      threads[i].start();
    }
    go.countDown();
    for (Thread thread : threads) {
      thread.join();
    }
    if (failure[0] != null) {
      throw new RuntimeException(failure[0]);
    }
    return new ArrayList<>(List.of(out));
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

  /** Apply the function to the argument `count` times on a new thread, without waiting. */
  public static void repeat(Function<Object, Object> function, Object argument, int count) {
    later(() -> {
      for (int i = 0; i < count; i++) {
        function.apply(argument);
      }
    }, 0);
  }

  /** Null while the task of later() or repeat() has not finished; empty if it succeeded. */
  public static String lastError() {
    return lastError;
  }
}
"""


def _eventually(read, timeout=10.0):
    """The first value `read` returns that is not None."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = read()
        if value is not None:
            return value
        time.sleep(0.05)
    raise AssertionError("nothing was reported in time")


@pytest.fixture
def other_thread_classes(compile_java):
    return compile_java("OtherThread", OTHER_THREAD_JAVA)


class TestCallbacksFromOtherThreads:
    """A thread that is not running the client's request calls Python on the
    client's callback connection."""

    @pytest.fixture
    def other_thread(self, other_thread_classes):
        client = connect(
            memory="16MB",
            classpath=[other_thread_classes],
            allowed_classes=["OtherThread"],
        )
        yield client
        client.close()

    def test_callback_on_another_thread_during_a_request(self, other_thread):
        client = other_thread
        supplier = client.register_callback(lambda: 42, "java.util.function.Supplier")

        assert client.jvm.OtherThread.call(supplier) == 42
        assert client.jvm.java.lang.Math.max(1, 2) == 2

    def test_callback_with_no_request_running(self, other_thread):
        client = other_thread
        fired = threading.Event()
        task = client.register_callback(fired.set, "java.lang.Runnable")

        client.jvm.OtherThread.later(task, 200)

        # Nothing is asked of the client while the task fires
        assert fired.wait(10)
        assert _eventually(client.jvm.OtherThread.lastError) == ""

    def test_callbacks_from_several_threads_run_at_the_same_time(self, other_thread):
        client = other_thread
        together = threading.Barrier(4, timeout=10)

        def double(value):
            together.wait()  # passes only once all four are in Python
            return value * 2

        function = client.register_callback(double, "java.util.function.Function")

        assert client.jvm.OtherThread.applyAll(function, 4) == [0, 2, 4, 6]

    def test_callback_fails(self, other_thread):
        client = other_thread

        def fail():
            raise ValueError("no such luck")

        supplier = client.register_callback(fail, "java.util.function.Supplier")

        with pytest.raises(JavaException, match="no such luck"):
            client.jvm.OtherThread.call(supplier)
        assert client.jvm.java.lang.Math.max(1, 2) == 2

    def test_callback_times_out(self, other_thread_classes):
        client = connect(
            memory="16MB",
            classpath=[other_thread_classes],
            allowed_classes=["OtherThread"],
            callback_timeout=0.3,
        )
        release = threading.Event()
        try:
            supplier = client.register_callback(
                lambda: release.wait(30), "java.util.function.Supplier"
            )

            with pytest.raises(JavaException, match="CallbackTimeoutError"):
                client.jvm.OtherThread.call(supplier)
            assert client.jvm.java.lang.Math.max(1, 2) == 2
        finally:
            release.set()
            client.close()

    def test_callback_may_call_java(self, other_thread):
        """Only the thread that called it waits for it; the client is free."""
        client = other_thread
        seen = []
        task = client.register_callback(
            lambda: seen.append(client.jvm.java.lang.Math.max(3, 4)),
            "java.lang.Runnable",
        )

        client.jvm.OtherThread.later(task, 100)

        assert _eventually(client.jvm.OtherThread.lastError) == ""
        assert seen == [4]

    def test_java_object_passed_from_another_thread(self, other_thread):
        """It is registered for the session by a thread that has none of its own."""
        client = other_thread
        received = []
        function = client.register_callback(
            lambda obj: received.append(obj), "java.util.function.Function"
        )
        text = client.jvm.java.lang.StringBuilder("abc")

        client.jvm.OtherThread.apply(function, text)

        assert received[0].toString() == "abc"
        received[0].append("d")
        assert text.toString() == "abcd"

    def test_objects_passed_from_other_threads_are_counted_and_released(
        self, other_thread_classes
    ):
        client = connect(
            memory="16MB",
            classpath=[other_thread_classes],
            allowed_classes=["OtherThread"],
            extra_jvm_flags=["-Dgatun.limits.max_objects=4"],
        )
        try:
            received = []
            function = client.register_callback(
                lambda obj: received.append(obj), "java.util.function.Function"
            )
            text = client.jvm.java.lang.StringBuilder("abc")  # two objects so far

            # Each call hands Python one more reference, released before the next
            for _ in range(10):
                client.jvm.OtherThread.apply(function, text)
                received.clear()

            # Held, they take the session's remaining slots
            client.jvm.OtherThread.apply(function, text)
            client.jvm.OtherThread.apply(function, text)
            with pytest.raises(JavaException, match="limit"):
                client.jvm.java.util.ArrayList()

            received.clear()
            assert client.jvm.java.util.ArrayList().size() == 0
        finally:
            client.close()

    def test_object_count_is_exact_when_threads_register_at_the_same_time(
        self, other_thread_classes
    ):
        """The request's thread hands out objects in its results while another
        thread hands them out as callback arguments."""
        limit = 40
        client = connect(
            memory="16MB",
            classpath=[other_thread_classes],
            allowed_classes=["OtherThread"],
            extra_jvm_flags=[f"-Dgatun.limits.max_objects={limit}"],
        )
        try:
            function = client.register_callback(
                lambda obj: None, "java.util.function.Function"
            )
            text = client.jvm.java.lang.StringBuilder("abc")
            held = 2  # the callback's proxy and the StringBuilder

            client.jvm.OtherThread.repeat(function, text, 300)
            singleton = client.jvm.java.util.Collections.singletonList
            while client.jvm.OtherThread.lastError() is None:
                # A list holding the StringBuilder: packing it registers an object
                assert len(singleton(text)) == 1
            assert client.jvm.OtherThread.lastError() == ""

            # Every reference handed out above has been dropped again
            gc.collect()
            created = []
            with pytest.raises(JavaException, match="limit"):
                for _ in range(limit):
                    created.append(client.jvm.java.util.ArrayList())
            assert len(created) == limit - held
        finally:
            client.close()

    def test_callback_on_the_request_thread_still_works(self, other_thread):
        client = other_thread
        doubled = client.register_callback(
            lambda value: value * 2, "java.util.function.Function"
        )
        items = client.jvm.java.util.ArrayList(list(range(50)))

        assert items.stream().map(doubled).toList() == [2 * i for i in range(50)]

    def test_callback_on_the_request_thread_still_cannot_call_java(self, other_thread):
        """The request is waiting for it on the channel the call would need."""
        client = other_thread
        function = client.register_callback(
            lambda value: client.jvm.java.lang.Math.abs(value),
            "java.util.function.Function",
        )
        items = client.jvm.java.util.ArrayList([1])

        with pytest.raises(JavaException, match="ReentrancyError"):
            items.stream().map(function).toList()

    def test_connection_takes_no_session_slot(self, other_thread_classes):
        client = connect(
            memory="16MB",
            classpath=[other_thread_classes],
            allowed_classes=["OtherThread"],
            extra_jvm_flags=["-Dgatun.limits.max_sessions=1"],
        )
        try:
            supplier = client.register_callback(
                lambda: "served", "java.util.function.Supplier"
            )
            assert client.jvm.OtherThread.call(supplier) == "served"
        finally:
            client.close()

    def test_socket_it_was_made_to_is_gone(self, other_thread):
        client = other_thread
        client.register_callback(lambda: None, "java.lang.Runnable")
        client.register_callback(lambda: None, "java.lang.Runnable")

        assert client._callback_channel is not None
        assert glob.glob(f"{glob.escape(client.socket_path)}.*.cb") == []

    def test_no_connection_until_a_callback_is_registered(self, other_thread):
        client = other_thread
        assert client.jvm.java.lang.Math.max(1, 2) == 2
        assert client._callback_channel is None

    def test_unclosed_client_is_still_collected(self, other_thread):
        """The thread reading the connection must not be what keeps it alive."""
        owner = other_thread
        dropped = GatunClient(owner.socket_path)
        assert dropped.connect()
        dropped.register_callback(lambda: None, "java.lang.Runnable")
        gone = weakref.ref(dropped)

        del dropped
        gc.collect()

        assert gone() is None

    def test_client_closes_while_java_waits(self, other_thread):
        """The waiting thread gets an exception; nothing else is disturbed."""
        owner = other_thread
        victim = GatunClient(owner.socket_path)
        assert victim.connect()
        entered = threading.Event()
        release = threading.Event()

        def block():
            entered.set()
            release.wait(30)

        try:
            task = victim.register_callback(block, "java.lang.Runnable")
            victim.jvm.OtherThread.later(task, 0)
            assert entered.wait(10)
        finally:
            victim.close()

        try:
            error = _eventually(owner.jvm.OtherThread.lastError)
            assert "connection closed while Python was being called" in error
            assert owner.jvm.java.lang.Math.max(1, 2) == 2
        finally:
            release.set()

    def test_callback_of_a_client_that_is_gone(self, other_thread):
        """Java code can hold on to a proxy for longer than its client lives."""
        owner = other_thread
        gone = GatunClient(owner.socket_path)
        assert gone.connect()
        task = gone.register_callback(lambda: None, "java.lang.Runnable")
        gone.jvm.OtherThread.later(task, 1000)
        gone.close()

        error = _eventually(owner.jvm.OtherThread.lastError)
        assert "after its client disconnected" in error

    def test_connection_that_cannot_be_opened(self, other_thread, monkeypatch, caplog):
        """Callbacks then work as before it existed, and the reason is given."""
        client = other_thread

        def refuse(*args, **kwargs):
            raise OSError("no connection for you")

        monkeypatch.setattr(gatun.client, "CallbackChannel", refuse)
        with caplog.at_level("WARNING", logger="gatun.client"):
            supplier = client.register_callback(
                lambda: 42, "java.util.function.Supplier"
            )
        assert "no connection for you" in caplog.text

        with pytest.raises(JavaException, match="callback connection is not open"):
            client.jvm.OtherThread.call(supplier)
        items = client.jvm.java.util.ArrayList([1, 2])
        doubled = client.register_callback(
            lambda value: value * 2, "java.util.function.Function"
        )
        assert items.stream().map(doubled).toList() == [2, 4]


class TestAsyncCallbacksFromOtherThreads:
    @pytest.fixture
    async def client(self, other_thread_classes):
        client = await aconnect(
            memory="16MB",
            classpath=[other_thread_classes],
            allowed_classes=["OtherThread"],
        )
        yield client
        await client.close()

    async def test_sync_callback_on_another_thread_during_a_request(self, client):
        supplier = await client.register_callback(
            lambda: 42, "java.util.function.Supplier"
        )

        assert await client.jvm.OtherThread.call(supplier) == 42

    async def test_async_callback_with_no_request_running(self, client):
        fired = asyncio.Event()

        async def fire():
            fired.set()

        task = await client.register_callback(fire, "java.lang.Runnable")
        await client.jvm.OtherThread.later(task, 200)

        await asyncio.wait_for(fired.wait(), 10)

    async def test_callbacks_from_several_threads_run_at_the_same_time(self, client):
        together = asyncio.Barrier(4)

        async def double(value):
            await asyncio.wait_for(together.wait(), 10)
            return value * 2

        function = await client.register_callback(double, "java.util.function.Function")

        assert await client.jvm.OtherThread.applyAll(function, 4) == [0, 2, 4, 6]

    async def test_callback_may_call_java(self, client):
        seen = []
        done = asyncio.Event()

        async def ask():
            seen.append(await client.jvm.java.lang.Math.max(3, 4))
            done.set()

        task = await client.register_callback(ask, "java.lang.Runnable")
        await client.jvm.OtherThread.later(task, 100)

        await asyncio.wait_for(done.wait(), 10)
        assert seen == [4]

    async def test_callback_fails(self, client):
        async def fail():
            raise ValueError("no such luck")

        supplier = await client.register_callback(fail, "java.util.function.Supplier")

        with pytest.raises(JavaException, match="no such luck"):
            await client.jvm.OtherThread.call(supplier)
        assert await client.jvm.java.lang.Math.max(1, 2) == 2

    async def test_callbacks_registered_at_once_open_one_connection(self, client):
        first, second = await asyncio.gather(
            client.register_callback(lambda: 1, "java.util.function.Supplier"),
            client.register_callback(lambda: 2, "java.util.function.Supplier"),
        )

        assert await client.jvm.OtherThread.call(first) == 1
        assert await client.jvm.OtherThread.call(second) == 2

    async def test_close_ends_the_connection(self, client):
        await client.register_callback(lambda: None, "java.lang.Runnable")
        channel = client._callback_channel
        assert channel is not None

        await client.close()
        await asyncio.sleep(0)

        assert channel._task.done() or channel._task.cancelling()


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
