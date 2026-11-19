"""Tests for Python callback support (Java calling back into Python)."""

import asyncio
import gc
import glob
import os
import threading
import time
import weakref

import pyarrow as pa
import pytest

import gatun.client
from gatun import (
    DeadConnectionError,
    JavaException,
    PayloadTooLargeError,
    StaleArenaError,
    aconnect,
    connect,
)
from gatun.async_client import AsyncGatunClient
from gatun.callback_channel import AsyncChannelCall
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

    def test_unregister_through_another_handle(self, client):
        """Read back out of Java, the proxy has another object ID."""
        comparator = client.register_callback(lambda a, b: 0, "java.util.Comparator")
        holder = client.jvm.java.util.ArrayList()
        holder.add(comparator)
        again = holder.get(0)
        assert again.object_id != comparator.object_id

        client.unregister_callback(again.object_id)

        assert comparator.object_id not in client._callbacks
        words = client.jvm.java.util.ArrayList(["b", "a"])
        with pytest.raises(JavaException, match="after it was unregistered"):
            client.jvm.java.util.Collections.sort(words, comparator)

    def test_unregister_through_a_handle_that_lives_for_the_call_only(self, client):
        """A handle that dies is queued for release, and the queue is sent
        ahead of the next command."""
        comparator = client.register_callback(lambda a, b: 0, "java.util.Comparator")
        holder = client.jvm.java.util.ArrayList()
        holder.add(comparator)

        client.unregister_callback(holder.get(0))

        assert comparator.object_id not in client._callbacks
        words = client.jvm.java.util.ArrayList(["b", "a"])
        with pytest.raises(JavaException, match="after it was unregistered"):
            client.jvm.java.util.Collections.sort(words, comparator)

    def test_unregister_something_that_is_no_callback(self, client):
        comparator = client.register_callback(lambda a, b: 0, "java.util.Comparator")
        words = client.jvm.java.util.ArrayList(["b", "a"])

        client.unregister_callback(words.object_id)

        assert comparator.object_id in client._callbacks
        client.jvm.java.util.Collections.sort(words, comparator)


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

    def test_arrow_transfer_is_refused_without_waiting_for_the_channel(self, client):
        """The request that holds the channel is the one waiting for the
        callback, so waiting for it could only end with the callback's timeout."""
        waited = []

        def compare(a, b):
            started = time.monotonic()
            try:
                client.arrow_to_java(pa.table({"x": [1]}))
            finally:
                waited.append(time.monotonic() - started)
            return 0

        comparator = client.register_callback(compare, "java.util.Comparator")
        arr = client.create_object("java.util.ArrayList")
        arr.add("b")
        arr.add("a")

        with pytest.raises(JavaException, match="ReentrancyError"):
            client.invoke_static_method(
                "java.util.Collections", "sort", arr, comparator
            )
        assert waited[0] < 5

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

  /** Keep the calling thread, and so its client's channel, busy. */
  public static void pause(long ms) throws InterruptedException {
    Thread.sleep(ms);
  }

  /** Sleep, then make an object: a call that outlasts whatever the test does meanwhile. */
  public static Object makeSlowly(long ms) throws InterruptedException {
    Thread.sleep(ms);
    return new StringBuilder("made slowly");
  }

  private static volatile CountDownLatch ready = new CountDownLatch(1);

  /** Tell interrupt() that its task has got as far as it should before the interrupt. */
  public static void ready() {
    ready.countDown();
  }

  /** As pause(), telling interrupt() first: the interrupt then finds this thread in here. */
  public static void readyThenPause(long ms) throws InterruptedException {
    ready.countDown();
    Thread.sleep(ms);
  }

  /** Run the task on a new thread, interrupt that thread once ready() was called, and wait. */
  public static void interrupt(Runnable task) throws InterruptedException {
    lastError = null;
    CountDownLatch reached = new CountDownLatch(1);
    ready = reached;
    Thread thread = new Thread(() -> {
      try {
        task.run();
        lastError = "";
      } catch (Throwable t) {
        lastError = String.valueOf(t.getMessage());
      }
    });
    thread.start();
    reached.await(10, java.util.concurrent.TimeUnit.SECONDS);
    thread.interrupt();
    thread.join();
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

  /** Like repeat(), for a function that must hand back the very object it was passed. */
  public static void echo(Function<Object, Object> function, Object argument, int count) {
    later(() -> {
      for (int i = 0; i < count; i++) {
        if (function.apply(argument) != argument) {
          throw new IllegalStateException("not the object that was passed");
        }
      }
    }, 0);
  }

  /** Null while the task given last has not finished; empty if it succeeded. */
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

    def test_arguments_are_released_while_the_client_makes_no_requests(
        self, other_thread_classes
    ):
        """A client that only listens sends nothing a release could ride on."""
        limit = 20
        client = connect(
            memory="16MB",
            classpath=[other_thread_classes],
            allowed_classes=["OtherThread"],
            extra_jvm_flags=[f"-Dgatun.limits.max_objects={limit}"],
        )
        try:
            calls = []
            function = client.register_callback(
                lambda obj: calls.append(None), "java.util.function.Function"
            )
            supplier = client.register_callback(
                lambda: 1, "java.util.function.Supplier"
            )
            text = client.jvm.java.lang.StringBuilder("abc")
            held = 3  # the two proxies and the StringBuilder

            client.jvm.OtherThread.repeat(function, text, 200)

            # Nothing is asked of the client while the calls arrive
            def all_released():
                serving = any(t.name == "gatun-callback" for t in threading.enumerate())
                done = len(calls) == 200 and not serving
                return True if done and not client._pending_frees else None

            _eventually(all_released)

            # The server reads the connection in order: once it has this call's
            # answer, it has every release sent before it
            assert client.jvm.OtherThread.call(supplier) == 1
            created = []
            with pytest.raises(JavaException, match="limit"):
                for _ in range(limit):
                    created.append(client.jvm.java.util.ArrayList())
            assert len(created) == limit - held
        finally:
            client.close()

    def test_unregistered_callback_is_refused_before_its_arguments_are_sent(
        self, other_thread_classes
    ):
        """Nobody would release the objects among them."""
        limit = 10
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
            items = client.jvm.java.util.ArrayList([1])
            held = 3  # the proxy, the StringBuilder and the list
            client.unregister_callback(function.object_id)

            for _ in range(3 * limit):
                with pytest.raises(JavaException, match="after it was unregistered"):
                    client.jvm.OtherThread.apply(function, text)
            # On the request's own thread too
            with pytest.raises(JavaException, match="after it was unregistered"):
                items.stream().map(function).toList()

            created = []
            with pytest.raises(JavaException, match="limit"):
                for _ in range(limit):
                    created.append(client.jvm.java.util.HashMap())
            assert len(created) == limit - held
        finally:
            client.close()

    def test_arguments_python_cannot_read_fail_the_call(self, other_thread):
        """The Java thread is waiting for an answer whatever goes wrong."""
        client = other_thread
        function = client.register_callback(
            lambda obj: None, "java.util.function.Function"
        )
        keyed_by_a_map = client.jvm.java.util.HashMap()
        keyed_by_a_map.put(client.jvm.java.util.HashMap(), 1)

        with pytest.raises(JavaException, match="unhashable"):
            client.jvm.OtherThread.apply(function, keyed_by_a_map)
        assert client.jvm.java.lang.Math.max(1, 2) == 2

    def test_registering_a_callback_inside_a_callback_fails_at_once(self, other_thread):
        """It must not wait for the channel: the request waiting for the
        callback holds it."""
        client = other_thread

        def register(value):
            client.register_callback(lambda: None, "java.lang.Runnable")

        function = client.register_callback(register, "java.util.function.Function")
        items = client.jvm.java.util.ArrayList([1])

        started = time.monotonic()
        with pytest.raises(JavaException, match="ReentrancyError"):
            items.stream().map(function).toList()
        assert time.monotonic() - started < 5

    def test_session_is_not_idle_while_python_is_being_called(
        self, other_thread_classes
    ):
        client = connect(
            memory="16MB",
            classpath=[other_thread_classes],
            allowed_classes=["OtherThread"],
            extra_jvm_flags=["-Dgatun.limits.session_idle_timeout_ms=1000"],
        )
        try:
            entered = threading.Event()

            def work():
                entered.set()
                time.sleep(2.5)  # the client asks for nothing meanwhile

            task = client.register_callback(work, "java.lang.Runnable")
            client.jvm.OtherThread.later(task, 0)
            assert entered.wait(10)
            time.sleep(2.7)

            assert client.jvm.OtherThread.lastError() == ""
        finally:
            client.close()

    def test_object_a_callback_returns_is_not_released_under_its_answer(
        self, other_thread
    ):
        """The wrapper dies as soon as the answer is sent, and its release
        follows on the same connection."""
        client = other_thread
        calls = []

        def same(obj):
            calls.append(None)
            return obj

        function = client.register_callback(same, "java.util.function.Function")
        text = client.jvm.java.lang.StringBuilder("abc")

        client.jvm.OtherThread.echo(function, text, 300)

        # Nothing is asked of the client meanwhile
        _eventually(lambda: True if len(calls) == 300 else None)
        assert _eventually(client.jvm.OtherThread.lastError) == ""

    def test_object_a_callback_returns_is_not_released_by_the_next_request(
        self, other_thread
    ):
        """Its release can also go out ahead of a request, on the client's own
        channel, while the answer is still on its way on the other."""
        client = other_thread
        function = client.register_callback(
            lambda obj: obj, "java.util.function.Function"
        )
        text = client.jvm.java.lang.StringBuilder("abc")

        client.jvm.OtherThread.echo(function, text, 300)

        error = None
        while error is None:
            client.ping()
            error = client.jvm.OtherThread.lastError()
        assert error == ""

    def test_object_an_in_band_callback_returns_is_not_released_under_its_answer(
        self, other_thread
    ):
        """Its release can go out on the callback connection, when a callback
        there finishes, while the answer is on the client's own channel."""
        client = other_thread
        same = client.register_callback(lambda obj: obj, "java.util.function.Function")
        ignore = client.register_callback(
            lambda obj: None, "java.util.function.Function"
        )
        items = client.jvm.java.util.ArrayList()
        for _ in range(200):
            items.add(client.jvm.java.lang.StringBuilder("x"))

        client.jvm.OtherThread.repeat(ignore, items, 400)
        returned = items.stream().map(same).toList()

        assert len(returned) == 200
        assert _eventually(client.jvm.OtherThread.lastError) == ""

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

    async def test_arguments_python_cannot_read_fail_the_call(self, client):
        """The Java thread is waiting for an answer whatever goes wrong."""
        function = await client.register_callback(
            lambda obj: None, "java.util.function.Function"
        )
        keyed_by_a_map = await client.jvm.java.util.HashMap()
        await keyed_by_a_map.put(await client.jvm.java.util.HashMap(), 1)

        with pytest.raises(JavaException, match="unhashable"):
            await asyncio.wait_for(
                client.jvm.OtherThread.apply(function, keyed_by_a_map), 20
            )

    async def test_arguments_are_released_while_the_client_makes_no_requests(
        self, client
    ):
        calls = []
        function = await client.register_callback(
            lambda obj: calls.append(None), "java.util.function.Function"
        )
        text = await client.jvm.java.lang.StringBuilder("abc")

        await client.jvm.OtherThread.repeat(function, text, 100)

        async with asyncio.timeout(10):
            channel = client._callback_channel
            while len(calls) < 100 or channel._calls or client._pending_frees:
                await asyncio.sleep(0.05)

    async def test_unregistered_callback_is_refused(self, client):
        function = await client.register_callback(
            lambda obj: None, "java.util.function.Function"
        )
        await client.unregister_callback(function.object_id)

        with pytest.raises(JavaException, match="after it was unregistered"):
            await client.jvm.OtherThread.apply(function, 1)

    async def test_object_a_callback_returns_is_not_released_under_its_answer(
        self, client
    ):
        function = await client.register_callback(
            lambda obj: obj, "java.util.function.Function"
        )
        text = await client.jvm.java.lang.StringBuilder("abc")

        await client.jvm.OtherThread.echo(function, text, 300)

        error = None
        async with asyncio.timeout(20):
            while error is None:
                await asyncio.sleep(0.01)
                error = await client.jvm.OtherThread.lastError()
        assert error == ""

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


class TestJavaCallsInsideChannelCallbacks:
    """A callback that came on the callback connection calls Java on it: the
    Java thread that is waiting for the callback runs the call."""

    @pytest.fixture
    def client(self, other_thread_classes):
        client = connect(
            memory="16MB",
            classpath=[other_thread_classes],
            allowed_classes=["OtherThread"],
            socket_timeout=10,
        )
        yield client
        client.close()

    def test_while_the_request_that_started_it_waits(self, client):
        """The client's channel is busy with that request until the callback returns."""
        supplier = client.register_callback(
            lambda: client.jvm.java.lang.Math.max(3, 4), "java.util.function.Supplier"
        )

        assert client.jvm.OtherThread.call(supplier) == 4

    def test_while_the_client_is_in_another_request(self, client):
        seen = []

        def ask():
            seen.append((client.jvm.java.lang.Math.max(3, 4), time.monotonic()))

        task = client.register_callback(ask, "java.lang.Runnable")
        client.jvm.OtherThread.later(task, 100)

        client.jvm.OtherThread.pause(1500)
        ended = time.monotonic()

        assert [value for value, _ in seen] == [4]
        assert seen[0][1] < ended - 0.5, "the call waited for the request to end"

    def test_callback_uses_the_object_it_was_passed(self, client):
        function = client.register_callback(
            lambda text: text.append("d").toString(), "java.util.function.Function"
        )
        text = client.jvm.java.lang.StringBuilder("abc")

        assert client.jvm.OtherThread.apply(function, text) == "abcd"

    def test_callback_returns_an_object_it_made(self, client):
        def make():
            items = client.jvm.java.util.ArrayList()
            items.add("made in a callback")
            return items

        supplier = client.register_callback(make, "java.util.function.Supplier")

        assert client.jvm.OtherThread.call(supplier) == ["made in a callback"]

    def test_class_first_named_inside_a_callback(self, client):
        """Finding out what a dotted name is takes a request of its own."""
        supplier = client.register_callback(
            lambda: client.jvm.java.util.LinkedHashSet().size(),
            "java.util.function.Supplier",
        )

        assert client.jvm.OtherThread.call(supplier) == 0

    def test_java_exception_inside_a_callback(self, client):
        def parse():
            try:
                client.jvm.java.lang.Integer.parseInt("not a number")
            except JavaException as e:
                return type(e).__name__
            return "no exception"

        supplier = client.register_callback(parse, "java.util.function.Supplier")

        assert client.jvm.OtherThread.call(supplier) == "JavaNumberFormatException"

    def test_call_that_calls_another_callback_on_the_same_thread(self, client):
        """The thread that waits for one callback runs its Java call, which
        calls a second callback."""
        doubled = client.register_callback(
            lambda value: value * 2, "java.util.function.Function"
        )
        items = client.jvm.java.util.ArrayList([1, 2, 3])
        supplier = client.register_callback(
            lambda: items.stream().map(doubled).toList(), "java.util.function.Supplier"
        )

        assert client.jvm.OtherThread.call(supplier) == [2, 4, 6]

    def test_call_that_calls_another_callback_on_a_third_thread(self, client):
        inner = client.register_callback(lambda: 20, "java.util.function.Supplier")
        outer = client.register_callback(
            lambda: client.jvm.OtherThread.call(inner) + 1,
            "java.util.function.Supplier",
        )

        assert client.jvm.OtherThread.call(outer) == 21

    def test_callbacks_on_several_threads_call_java_at_the_same_time(self, client):
        together = threading.Barrier(4, timeout=10)

        def negate(value):
            together.wait()
            return client.jvm.java.lang.Math.negateExact(value)

        function = client.register_callback(negate, "java.util.function.Function")

        assert client.jvm.OtherThread.applyAll(function, 4) == [0, -1, -2, -3]

    def test_objects_dropped_during_a_long_callback_are_released(
        self, other_thread_classes
    ):
        """Not only once it returns: it may run for as long as a stream does."""
        limit = 10
        client = connect(
            memory="16MB",
            classpath=[other_thread_classes],
            allowed_classes=["OtherThread"],
            extra_jvm_flags=[f"-Dgatun.limits.max_objects={limit}"],
        )
        try:

            def churn():
                for _ in range(5 * limit):
                    client.jvm.java.util.ArrayList()
                return "done"

            supplier = client.register_callback(churn, "java.util.function.Supplier")

            assert client.jvm.OtherThread.call(supplier) == "done"
        finally:
            client.close()

    def test_transfer_into_the_payload_zone_is_refused_with_the_reason(self, client):
        """The zone belongs to the client's own channel. Only tables as Java
        objects have another way."""
        table = pa.table({"x": [1, 2, 3]})
        supplier = client.register_callback(
            lambda: client.send_arrow_table(table), "java.util.function.Supplier"
        )

        with pytest.raises(
            JavaException,
            match=r"ReentrancyError: send_arrow_table\(\) cannot be used inside a callback",
        ):
            client.jvm.OtherThread.call(supplier)
        # Outside the callback it works as ever
        assert client.send_arrow_table(table) == "Received 3 rows"

    def test_batch_is_refused_with_the_reason(self, client):
        def batch():
            with client.batch() as b:
                b.call_static("java.lang.Math", "max", 1, 2)

        supplier = client.register_callback(batch, "java.util.function.Supplier")

        with pytest.raises(
            JavaException,
            match="ReentrancyError: Batch cannot be used inside a callback",
        ):
            client.jvm.OtherThread.call(supplier)
        assert client.jvm.java.lang.Math.max(1, 2) == 2

    def test_registering_a_callback_is_refused_with_the_reason(self, client):
        supplier = client.register_callback(
            lambda: client.register_callback(lambda: None, "java.lang.Runnable"),
            "java.util.function.Supplier",
        )

        started = time.monotonic()
        with pytest.raises(JavaException, match="RegisterCallback cannot be used"):
            client.jvm.OtherThread.call(supplier)
        assert time.monotonic() - started < 5

    def test_command_and_response_too_large_for_the_connection(self, client):
        def too_large():
            caught = []
            for attempt in (
                lambda: client.jvm.java.lang.String.valueOf("x" * 70_000),
                lambda: client.jvm.java.lang.String("ab").repeat(40_000),
            ):
                try:
                    attempt()
                except Exception as e:  # noqa: BLE001 - its type is the result
                    caught.append(type(e).__name__)
            # The connection is none the worse for either
            return f"{caught} {client.jvm.java.lang.Math.max(3, 4)}"

        supplier = client.register_callback(too_large, "java.util.function.Supplier")

        assert (
            client.jvm.OtherThread.call(supplier)
            == "['PayloadTooLargeError', 'PayloadTooLargeError'] 4"
        )

    def test_callback_that_timed_out_can_still_call_java(self, other_thread_classes):
        """Java has stopped waiting for it, so its calls take the client's
        own channel."""
        client = connect(
            memory="16MB",
            classpath=[other_thread_classes],
            allowed_classes=["OtherThread"],
            callback_timeout=0.3,
        )
        release = threading.Event()
        try:
            seen = []

            def slow():
                release.wait(30)
                seen.append(client.jvm.java.lang.Math.max(3, 4))

            supplier = client.register_callback(slow, "java.util.function.Supplier")
            with pytest.raises(JavaException, match="CallbackTimeoutError"):
                client.jvm.OtherThread.call(supplier)

            release.set()
            assert _eventually(lambda: seen[0] if seen else None) == 4
        finally:
            release.set()
            client.close()

    def test_java_call_that_gets_no_answer_in_time(self, client):
        caught = []
        over = threading.Event()

        def wait_too_long():
            # Only for the calls made in here: the requests around them are
            # not what is being timed
            client.socket_timeout = 0.5
            try:
                for _ in range(2):
                    try:
                        client.jvm.OtherThread.pause(1500)
                    except Exception as e:  # noqa: BLE001 - its type is the result
                        caught.append((type(e).__name__, time.monotonic()))
            finally:
                client.socket_timeout = 10
                over.set()

        task = client.register_callback(wait_too_long, "java.lang.Runnable")
        client.jvm.OtherThread.pause(1)  # so that the callback need not look it up
        client.jvm.OtherThread.later(task, 0)

        assert over.wait(10)
        assert [name for name, _ in caught] == ["SocketTimeoutError"] * 2
        # The second call is refused at once: the answer to the first is still
        # on its way and would be taken for its own
        assert caught[1][1] - caught[0][1] < 0.4

        # Java finishes the first call and takes the callback's answer
        assert _eventually(client.jvm.OtherThread.lastError) == ""
        assert client.jvm.java.lang.Math.max(1, 2) == 2

    @pytest.mark.parametrize("callback_still_running", [False, True])
    def test_objects_in_a_response_nobody_waited_for_are_released(
        self, client, callback_still_running
    ):
        def live_objects():
            report = client.get_metrics()
            return int(report.split("current_objects:")[1].split()[0])

        caught = []
        over = threading.Event()

        def give_up_on_it():
            client.socket_timeout = 0.4
            try:
                client.jvm.OtherThread.makeSlowly(1200)
            except Exception as e:  # noqa: BLE001 - its type is the result
                caught.append(type(e).__name__)
            finally:
                client.socket_timeout = 10
            if callback_still_running:
                time.sleep(1.5)  # the response comes while the call is still open
            over.set()

        task = client.register_callback(give_up_on_it, "java.lang.Runnable")
        held = client.jvm.OtherThread.makeSlowly(0)  # looked up before the timeout
        before = live_objects()

        client.jvm.OtherThread.later(task, 0)
        assert over.wait(10)
        assert caught == ["SocketTimeoutError"]
        assert _eventually(client.jvm.OtherThread.lastError) == ""

        # Java made the object after the call had given up on it
        assert (
            _eventually(lambda: live_objects() if live_objects() <= before else None)
            == before
        )
        assert held.toString() == "made slowly"

    def test_unregistering_on_a_closed_client_drops_the_function(self, client):
        comparator = client.register_callback(lambda a, b: 0, "java.util.Comparator")
        callback_id = comparator.object_id
        client.close()

        with pytest.raises(DeadConnectionError):
            client.unregister_callback(callback_id)
        assert callback_id not in client._callbacks

    def test_client_closes_while_a_callback_waits_for_java(self, client):
        owner = client
        victim = GatunClient(owner.socket_path)
        assert victim.connect()
        caught = []

        def wait_for_java():
            try:
                victim.jvm.OtherThread.pause(1500)
            except Exception as e:  # noqa: BLE001 - its type is the result
                caught.append(type(e).__name__)

        try:
            task = victim.register_callback(wait_for_java, "java.lang.Runnable")
            victim.jvm.OtherThread.later(task, 0)
            time.sleep(0.5)
        finally:
            victim.close()

        assert _eventually(lambda: caught or None) == ["DeadConnectionError"]
        assert owner.jvm.java.lang.Math.max(1, 2) == 2

    def test_call_still_running_when_its_callback_is_answered(
        self, other_thread_classes
    ):
        """Its response comes after the answer, and must still find the caller."""
        client = connect(
            memory="16MB",
            classpath=[other_thread_classes],
            allowed_classes=["OtherThread"],
            callback_timeout=0.4,
            socket_timeout=10,
        )
        try:
            outcome = []

            def outlast_the_timeout():
                started = time.monotonic()
                try:
                    client.jvm.OtherThread.pause(1200)
                    outcome.append(("returned", time.monotonic() - started))
                except Exception as e:  # noqa: BLE001 - its type is the result
                    outcome.append((type(e).__name__, time.monotonic() - started))

            supplier = client.register_callback(
                outlast_the_timeout, "java.util.function.Supplier"
            )
            with pytest.raises(JavaException, match="CallbackTimeoutError"):
                client.jvm.OtherThread.call(supplier)

            result, took = _eventually(lambda: outcome[0] if outcome else None)
            assert result == "returned"
            assert took < 5
        finally:
            client.close()

    def test_java_thread_is_interrupted_while_it_waits_for_the_callback(self, client):
        """Nothing runs the callback's Java calls for it any more, so they take
        the client's own channel."""
        entered = threading.Event()
        proceed = threading.Event()
        seen = []

        def carry_on():
            entered.set()
            client.jvm.OtherThread.ready()
            proceed.wait(10)
            started = time.monotonic()
            seen.append((client.jvm.java.lang.Math.max(3, 4), started))

        task = client.register_callback(carry_on, "java.lang.Runnable")
        try:
            client.jvm.OtherThread.interrupt(task)
            assert entered.is_set()
            assert "Interrupted" in client.jvm.OtherThread.lastError()

            proceed.set()
            value, started = _eventually(lambda: seen[0] if seen else None)
            assert value == 4
            assert time.monotonic() - started < 5
        finally:
            proceed.set()

    def test_java_thread_is_interrupted_while_it_runs_the_callbacks_call(self, client):
        """The interrupt is for the thread: the call fails, and the thread stops
        waiting for the callback instead of carrying on as if nothing happened."""
        caught = []
        seen = []

        def wait_in_java():
            try:
                client.jvm.OtherThread.readyThenPause(20_000)
            except Exception as e:  # noqa: BLE001 - its type is the result
                caught.append(type(e).__name__)
            seen.append(client.jvm.java.lang.Math.max(3, 4))

        task = client.register_callback(wait_in_java, "java.lang.Runnable")

        started = time.monotonic()
        client.jvm.OtherThread.interrupt(task)

        assert time.monotonic() - started < 5
        assert "Interrupted" in client.jvm.OtherThread.lastError()
        assert _eventually(lambda: seen or None) == [4]
        assert caught == ["CancelledException"]

    def test_callback_unregisters_itself(self, client):
        registered = []

        def once():
            client.unregister_callback(registered[0])
            return "bye"

        supplier = client.register_callback(once, "java.util.function.Supplier")
        registered.append(supplier.object_id)

        assert client.jvm.OtherThread.call(supplier) == "bye"
        assert supplier.object_id not in client._callbacks
        with pytest.raises(JavaException, match="after it was unregistered"):
            client.jvm.OtherThread.call(supplier)

    def test_objects_made_by_a_call_that_outlives_its_session_are_dropped(self, client):
        owner = client

        def live_objects():
            report = owner.get_metrics()
            return int(report.split("current_objects:")[1].split()[0])

        held = owner.jvm.java.util.ArrayList()
        baseline = live_objects()
        assert baseline >= 1, "the server does not count objects"

        victim = GatunClient(owner.socket_path)
        assert victim.connect()
        entered = threading.Event()

        def make():
            entered.set()
            try:
                victim.jvm.OtherThread.makeSlowly(1200)
            except Exception:  # noqa: BLE001, S110 - the client closes under it
                pass

        try:
            task = victim.register_callback(make, "java.lang.Runnable")
            victim.jvm.OtherThread.later(task, 0)
            assert entered.wait(10)
            time.sleep(0.3)
        finally:
            victim.close()

        # The call finishes after its session is gone
        time.sleep(1.5)
        assert _eventually(
            lambda: live_objects() if live_objects() <= baseline else None
        )
        assert held.size() == 0

    def test_calls_inside_callbacks_are_counted(self, client):
        def requests(action):
            for line in client.get_metrics().splitlines():
                if line.strip().startswith(f"{action}:"):
                    return int(line.split("count=")[1].split()[0])
            return 0

        supplier = client.register_callback(
            lambda: client.jvm.java.lang.Math.max(3, 4), "java.util.function.Supplier"
        )
        client.jvm.OtherThread.call(supplier)  # the class and method are known now
        before = requests("InvokeStaticMethod")

        client.jvm.OtherThread.call(supplier)

        # The request itself and the call the callback made
        assert requests("InvokeStaticMethod") == before + 2


class TestAsyncJavaCallsInsideChannelCallbacks:
    @pytest.fixture
    async def client(self, other_thread_classes):
        client = await aconnect(
            memory="16MB",
            classpath=[other_thread_classes],
            allowed_classes=["OtherThread"],
            socket_timeout=10,
        )
        yield client
        await client.close()

    async def test_while_the_request_that_started_it_waits(self, client):
        async def ask():
            return await client.jvm.java.lang.Math.max(3, 4)

        supplier = await client.register_callback(ask, "java.util.function.Supplier")

        assert await client.jvm.OtherThread.call(supplier) == 4

    async def test_callback_uses_the_object_it_was_passed(self, client):
        async def extend(text):
            await text.append("d")
            return await text.toString()

        function = await client.register_callback(extend, "java.util.function.Function")
        text = await client.jvm.java.lang.StringBuilder("abc")

        assert await client.jvm.OtherThread.apply(function, text) == "abcd"

    async def test_calls_made_together_inside_one_callback(self, client):
        """One Java thread runs them, one after another."""

        async def both():
            Math = client.jvm.java.lang.Math
            return list(await asyncio.gather(Math.max(1, 2), Math.min(1, 2)))

        supplier = await client.register_callback(both, "java.util.function.Supplier")

        assert await client.jvm.OtherThread.call(supplier) == [2, 1]

    async def test_call_that_calls_another_callback_on_the_same_thread(self, client):
        doubled = await client.register_callback(
            lambda value: value * 2, "java.util.function.Function"
        )
        items = await client.jvm.java.util.ArrayList([1, 2, 3])

        async def double_all():
            stream = await items.stream()
            return await (await stream.map(doubled)).toList()

        supplier = await client.register_callback(
            double_all, "java.util.function.Supplier"
        )

        assert await client.jvm.OtherThread.call(supplier) == [2, 4, 6]

    async def test_transfer_into_the_payload_zone_is_refused_with_the_reason(
        self, client
    ):
        table = pa.table({"x": [1, 2, 3]})

        async def send():
            await client.send_arrow_table(table)

        supplier = await client.register_callback(send, "java.util.function.Supplier")

        with pytest.raises(
            JavaException,
            match="ReentrancyError: SendArrowBatch cannot be used inside a callback",
        ):
            await client.jvm.OtherThread.call(supplier)
        assert await client.send_arrow_table(table) == "Received 3 rows"

    async def test_task_that_outlives_its_callback_can_still_call_java(self, client):
        """Java has stopped waiting, so the call takes the client's own channel."""
        release = asyncio.Event()
        started = []

        async def later():
            await release.wait()
            return await client.jvm.java.lang.Math.max(3, 4)

        async def start():
            started.append(asyncio.create_task(later()))

        task = await client.register_callback(start, "java.lang.Runnable")
        await client.jvm.OtherThread.later(task, 0)
        async with asyncio.timeout(10):
            while not started:
                await asyncio.sleep(0.05)
        await asyncio.sleep(0.2)  # the callback has been answered by now

        release.set()
        assert await asyncio.wait_for(started[0], 10) == 4

    async def test_callback_cancelled_by_its_timeout_while_java_runs(
        self, other_thread_classes
    ):
        client = await aconnect(
            memory="16MB",
            classpath=[other_thread_classes],
            allowed_classes=["OtherThread"],
            callback_timeout=0.4,
        )
        try:

            async def wait_for_java():
                await client.jvm.OtherThread.pause(1200)

            supplier = await client.register_callback(
                wait_for_java, "java.util.function.Supplier"
            )

            with pytest.raises(JavaException, match="timed out"):
                await client.jvm.OtherThread.call(supplier)
            assert await client.jvm.java.lang.Math.max(1, 2) == 2
        finally:
            await client.close()

    async def test_call_still_running_when_its_callback_is_answered(self, client):
        started = []

        async def record():
            return await client.jvm.OtherThread.makeSlowly(500)

        async def start():
            started.append(asyncio.create_task(record()))
            await asyncio.sleep(0)  # the task has sent its command by now

        task = await client.register_callback(start, "java.lang.Runnable")
        await client.jvm.OtherThread.later(task, 0)
        async with asyncio.timeout(10):
            while not started:
                await asyncio.sleep(0.05)

        made = await asyncio.wait_for(started[0], 5)
        assert await made.toString() == "made slowly"

    async def test_callback_unregisters_itself(self, client):
        registered = []

        async def once():
            await client.unregister_callback(registered[0])
            return "bye"

        supplier = await client.register_callback(once, "java.util.function.Supplier")
        registered.append(supplier.object_id)

        assert await client.jvm.OtherThread.call(supplier) == "bye"
        with pytest.raises(JavaException, match="after it was unregistered"):
            await client.jvm.OtherThread.call(supplier)

    async def test_request_cancelled_while_it_is_being_sent(self):
        """The command may be on its way, so its response would be taken for
        the next one's."""

        class Interrupted:
            def __init__(self):
                self.sent = []

            async def send(self, frame):
                self.sent.append(frame)
                raise asyncio.CancelledError

            def _forget(self, call):
                pass

        channel = Interrupted()
        call = AsyncChannelCall(channel, 1)

        with pytest.raises(asyncio.CancelledError):
            await call.request(b"first", 1)
        with pytest.raises(TimeoutError):
            await call.request(b"second", 1)
        assert channel.sent == [b"first"]

    async def test_request_on_a_closed_connection(self):
        """Nothing was sent, so no response can be taken for the next one's:
        every request fails the same way."""

        class Closed:
            async def send(self, frame):
                raise OSError("The callback connection is closed")

            def _forget(self, call):
                pass

        call = AsyncChannelCall(Closed(), 1)

        for _ in range(2):
            with pytest.raises(OSError, match="closed"):
                await call.request(b"command", 1)

    async def test_objects_in_a_response_nobody_waited_for_are_released(self, client):
        async def live_objects():
            report = await client.get_metrics()
            return int(report.split("current_objects:")[1].split()[0])

        caught = []
        over = asyncio.Event()

        async def give_up_on_it():
            client.socket_timeout = 0.4
            try:
                await client.jvm.OtherThread.makeSlowly(1200)
            except Exception as e:  # noqa: BLE001 - its type is the result
                caught.append(type(e).__name__)
            finally:
                client.socket_timeout = 10
            over.set()

        task = await client.register_callback(give_up_on_it, "java.lang.Runnable")
        held = await client.jvm.OtherThread.makeSlowly(0)
        before = await live_objects()

        await client.jvm.OtherThread.later(task, 0)
        await asyncio.wait_for(over.wait(), 10)
        assert caught == ["SocketTimeoutError"]

        await asyncio.sleep(1.5)  # Java has made the object by now, and answered
        async with asyncio.timeout(10):
            while await live_objects() > before:
                await asyncio.sleep(0.1)
        assert await live_objects() == before
        assert await held.toString() == "made slowly"

    async def test_unregistering_on_a_closed_client_drops_the_function(self, client):
        comparator = await client.register_callback(
            lambda a, b: 0, "java.util.Comparator"
        )
        callback_id = comparator.object_id
        await client.close()

        with pytest.raises(DeadConnectionError):
            await client.unregister_callback(callback_id)
        assert callback_id not in client._callbacks


class TestArrowInsideChannelCallbacks:
    """A callback that came on the callback connection moves Arrow tables
    through shared memory of its own call: the client's belongs to its
    channel, which is busy with the request that is waiting for the callback."""

    @pytest.fixture
    def client(self, other_thread_classes):
        client = connect(
            memory="16MB",
            classpath=[other_thread_classes],
            allowed_classes=["OtherThread"],
            socket_timeout=10,
        )
        yield client
        client.close()

    @staticmethod
    def _call_memory_files(client):
        return glob.glob(f"{client.socket_path}.*.call*.shm")

    def test_table_to_java_and_back(self, client):
        table = pa.table({"x": list(range(1000)), "s": [str(i) for i in range(1000)]})
        seen = []

        def roundtrip():
            root = client.arrow_to_java(table)
            seen.append(client.arrow_from_java(root))
            return root.getRowCount()

        supplier = client.register_callback(roundtrip, "java.util.function.Supplier")

        assert client.jvm.OtherThread.call(supplier) == 1000
        assert seen[0].equals(table)

    def test_while_the_client_is_in_another_request(self, client):
        table = pa.table({"x": [1, 2, 3]})
        seen = []

        def send():
            seen.append((client.arrow_to_java(table).getRowCount(), time.monotonic()))

        task = client.register_callback(send, "java.lang.Runnable")
        client.jvm.OtherThread.later(task, 100)

        client.jvm.OtherThread.pause(1500)
        ended = time.monotonic()

        assert [rows for rows, _ in seen] == [3]
        assert seen[0][1] < ended - 0.5, "the transfer waited for the request to end"

    def test_memory_file_gone_before_the_client_opens_it(self, client, monkeypatch):
        """Java stopped waiting for the callback right after it said where the
        memory is, and removed it. The table takes the client's channel, as
        it does whenever the call is over."""
        send_nested = client._send_nested

        def lose_the_file(call, data):
            routed, result = send_nested(call, data)
            if isinstance(result, str) and result.endswith(".shm"):
                os.remove(result)
            return routed, result

        monkeypatch.setattr(client, "_send_nested", lose_the_file)
        seen = []

        def send():
            seen.append(client.arrow_to_java(pa.table({"x": [1, 2, 3]})))

        task = client.register_callback(send, "java.lang.Runnable")
        client.jvm.OtherThread.later(task, 0)

        root = _eventually(lambda: seen[0] if seen else None)
        assert root.getRowCount() == 3

    def test_memory_file_gone_between_two_transfers(self, client):
        """Java stopped waiting for the callback after its first transfer, and
        removed the memory. Nothing more is written there: the server keeps no
        free space for it any longer."""
        seen = []

        def send():
            first = client.arrow_to_java(pa.table({"x": [1, 2, 3]}))
            memory = gatun.client._channel_call_of(client).memory
            before = bytes(memory.shm[:512])
            os.remove(memory.path)
            second = client.arrow_to_java(pa.table({"x": list(range(1000))}))
            seen.append((first, second, bytes(memory.shm[:512]) == before))

        task = client.register_callback(send, "java.lang.Runnable")
        client.jvm.OtherThread.later(task, 0)

        first, second, untouched = _eventually(lambda: seen[0] if seen else None)
        assert (first.getRowCount(), second.getRowCount()) == (3, 1000)
        assert untouched

    def test_call_lets_go_of_its_memory_when_the_callback_returns(self, client):
        """A thread or task the callback started keeps the call reachable."""
        calls = []

        def send():
            client.arrow_to_java(pa.table({"x": [1]}))
            call = gatun.client._channel_call_of(client)
            calls.append((call, call.memory is not None))

        task = client.register_callback(send, "java.lang.Runnable")
        client.jvm.OtherThread.later(task, 0)

        _eventually(lambda: True if calls and calls[0][0].memory is None else None)
        assert calls[0][1]

    def test_callbacks_at_once_each_have_their_own_memory(self, client):
        everyone_holds_a_view = threading.Barrier(4, timeout=10)

        def read_in_place(index):
            root = client.arrow_to_java(pa.table({"x": [index] * 10_000}))
            view = client.arrow_from_java(root, copy=False)
            everyone_holds_a_view.wait()
            return view.to_pydict()["x"] == [index] * 10_000

        function = client.register_callback(
            read_in_place, "java.util.function.Function"
        )

        assert client.jvm.OtherThread.applyAll(function, 4) == [True] * 4

    def test_view_ends_with_the_callbacks_next_transfer(self, client):
        def read():
            root = client.arrow_to_java(pa.table({"x": [1, 2, 3]}))
            view = client.arrow_from_java(root, copy=False)
            first = view.to_pydict()["x"]
            client.arrow_to_java(pa.table({"x": [4]}))
            try:
                view.to_pydict()
            except StaleArenaError:
                return first
            return "still readable"

        supplier = client.register_callback(read, "java.util.function.Supplier")

        assert client.jvm.OtherThread.call(supplier) == [1, 2, 3]

    def test_view_outlives_the_callback(self, client):
        """Nothing writes to a call's memory once the call is over."""
        table = pa.table({"x": list(range(5000))})
        views = []

        def read():
            views.append(
                client.arrow_from_java(client.arrow_to_java(table), copy=False)
            )

        supplier = client.register_callback(read, "java.util.function.Supplier")
        client.jvm.OtherThread.call(supplier)

        # A transfer on the client's own channel uses other memory
        assert client.arrow_to_java(pa.table({"x": [0] * 5000})).getRowCount() == 5000
        assert views[0].table.equals(table)

    def test_memory_file_lasts_as_long_as_the_call(self, client):
        during = []

        def send():
            during.append(len(self._call_memory_files(client)))
            client.arrow_to_java(pa.table({"x": [1]}))
            during.append(len(self._call_memory_files(client)))
            client.arrow_to_java(pa.table({"x": [2]}))
            during.append(len(self._call_memory_files(client)))

        supplier = client.register_callback(send, "java.util.function.Supplier")
        client.jvm.OtherThread.call(supplier)

        # Made by the first transfer, and used again by the second
        assert during == [0, 1, 1]
        assert self._call_memory_files(client) == []

    def test_memory_file_is_removed_when_the_client_goes_away(self, client):
        owner = client
        victim = GatunClient(owner.socket_path)
        assert victim.connect()
        sent = threading.Event()
        hold = threading.Event()

        def send():
            victim.arrow_to_java(pa.table({"x": [1]}))
            sent.set()
            hold.wait(10)

        try:
            task = victim.register_callback(send, "java.lang.Runnable")
            victim.jvm.OtherThread.later(task, 0)
            assert sent.wait(10)
            assert len(self._call_memory_files(owner)) == 1
        finally:
            victim.close()
            hold.set()

        assert _eventually(lambda: None if self._call_memory_files(owner) else True)

    def test_table_larger_than_the_memory(self, other_thread_classes):
        client = connect(
            memory="192KB",
            classpath=[other_thread_classes],
            allowed_classes=["OtherThread"],
            socket_timeout=10,
        )
        try:

            def send(rows):
                table = pa.table({"x": pa.array(range(rows), pa.int64())})
                try:
                    return client.arrow_to_java(table).getRowCount()
                except PayloadTooLargeError as e:
                    return f"at most {e.max_size}"

            function = client.register_callback(send, "java.util.function.Function")

            # All of a call's memory is payload: more than the client's own zone
            # holds, with its command and response zones taken out
            assert client.jvm.OtherThread.apply(function, 12_000) == 12_000
            assert send(12_000) == "at most 65536"
            assert client.jvm.OtherThread.apply(function, 50_000) == "at most 196608"
        finally:
            client.close()

    def test_table_too_large_to_fetch(self, other_thread_classes):
        client = connect(
            memory="192KB",
            classpath=[other_thread_classes],
            allowed_classes=[
                "OtherThread",
                "org.apache.arrow.vector.util.VectorSchemaRootAppender",
            ],
            socket_timeout=10,
        )
        try:
            caught = []

            def fetch(root):
                try:
                    client.arrow_from_java(root)
                except PayloadTooLargeError:
                    caught.append("too large")
                # The call and its memory are still good
                return client.arrow_from_java(root.slice(0, 10)).num_rows

            function = client.register_callback(fetch, "java.util.function.Function")
            piece = pa.table({"x": pa.array(range(4000), pa.int64())})
            root = client.arrow_to_java(piece)
            Appender = client.jvm.org.apache.arrow.vector.util.VectorSchemaRootAppender
            for _ in range(8):
                Appender.append(root, client.arrow_to_java(piece))

            assert root.getRowCount() == 36_000
            assert client.jvm.OtherThread.apply(function, root) == 10
            assert caught == ["too large"]
        finally:
            client.close()

    def test_after_the_callback_has_timed_out(self, other_thread_classes):
        """Java has stopped waiting, so the table takes the client's own channel."""
        client = connect(
            memory="16MB",
            classpath=[other_thread_classes],
            allowed_classes=["OtherThread"],
            callback_timeout=0.4,
            socket_timeout=10,
        )
        try:
            table = pa.table({"x": [1, 2, 3]})
            done = []

            def slow():
                time.sleep(1.0)
                done.append(client.arrow_from_java(client.arrow_to_java(table)))

            supplier = client.register_callback(slow, "java.util.function.Supplier")
            with pytest.raises(JavaException, match="CallbackTimeoutError"):
                client.jvm.OtherThread.call(supplier)

            assert _eventually(lambda: done or None)[0].equals(table)
            assert self._call_memory_files(client) == []
        finally:
            client.close()

    def test_tables_made_in_callbacks_are_counted_and_released(self, client):
        def live_objects():
            report = client.get_metrics()
            return int(report.split("current_objects:")[1].split()[0])

        made = []

        def send():
            made.append(client.arrow_to_java(pa.table({"x": [1, 2, 3]})))

        supplier = client.register_callback(send, "java.util.function.Supplier")
        before = live_objects()
        client.jvm.OtherThread.call(supplier)
        assert live_objects() == before + 1

        made.clear()
        gc.collect()
        assert (
            _eventually(lambda: live_objects() if live_objects() <= before else None)
            == before
        )


class TestAsyncArrowInsideChannelCallbacks:
    @pytest.fixture
    async def client(self, other_thread_classes):
        client = await aconnect(
            memory="16MB",
            classpath=[other_thread_classes],
            allowed_classes=["OtherThread"],
            socket_timeout=10,
        )
        yield client
        await client.close()

    async def test_table_to_java_and_back(self, client):
        table = pa.table({"x": list(range(1000)), "s": [str(i) for i in range(1000)]})
        seen = []

        async def roundtrip():
            root = await client.arrow_to_java(table)
            seen.append(await client.arrow_from_java(root))
            return await root.getRowCount()

        supplier = await client.register_callback(
            roundtrip, "java.util.function.Supplier"
        )

        assert await client.jvm.OtherThread.call(supplier) == 1000
        assert seen[0].equals(table)
        assert glob.glob(f"{client.socket_path}.*.call*.shm") == []

    async def test_memory_file_gone_before_the_client_opens_it(
        self, client, monkeypatch
    ):
        send_nested = client._send_nested

        async def lose_the_file(call, data):
            routed, result = await send_nested(call, data)
            if isinstance(result, str) and result.endswith(".shm"):
                os.remove(result)
            return routed, result

        monkeypatch.setattr(client, "_send_nested", lose_the_file)
        seen = asyncio.get_running_loop().create_future()

        async def send():
            seen.set_result(await client.arrow_to_java(pa.table({"x": [1, 2, 3]})))

        task = await client.register_callback(send, "java.lang.Runnable")
        await client.jvm.OtherThread.later(task, 0)

        root = await asyncio.wait_for(seen, 10)
        assert await root.getRowCount() == 3

    async def test_memory_file_gone_between_two_transfers(self, client):
        seen = asyncio.get_running_loop().create_future()

        async def send():
            first = await client.arrow_to_java(pa.table({"x": [1, 2, 3]}))
            memory = gatun.client._channel_call_of(client).memory
            before = bytes(memory.shm[:512])
            os.remove(memory.path)
            second = await client.arrow_to_java(pa.table({"x": list(range(1000))}))
            seen.set_result((first, second, bytes(memory.shm[:512]) == before))

        task = await client.register_callback(send, "java.lang.Runnable")
        await client.jvm.OtherThread.later(task, 0)

        first, second, untouched = await asyncio.wait_for(seen, 10)
        assert (await first.getRowCount(), await second.getRowCount()) == (3, 1000)
        assert untouched

    async def test_transfers_made_together_inside_one_callback(self, client):
        """They share the call's memory, one after the other."""

        async def roundtrip(n):
            table = pa.table({"x": [n] * 5000})
            root = await client.arrow_to_java(table)
            return (await client.arrow_from_java(root)).equals(table)

        async def together():
            return list(await asyncio.gather(*(roundtrip(n) for n in range(6))))

        supplier = await client.register_callback(
            together, "java.util.function.Supplier"
        )

        assert await client.jvm.OtherThread.call(supplier) == [True] * 6

    async def test_callbacks_at_once_each_have_their_own_memory(self, client):
        async def roundtrip(index):
            table = pa.table({"x": [index] * 10_000})
            root = await client.arrow_to_java(table)
            await asyncio.sleep(0.05)
            return (await client.arrow_from_java(root)).equals(table)

        function = await client.register_callback(
            roundtrip, "java.util.function.Function"
        )

        assert await client.jvm.OtherThread.applyAll(function, 4) == [True] * 4

    async def test_task_that_outlives_its_callback(self, client):
        """Java has stopped waiting, so the table takes the client's own channel."""
        table = pa.table({"x": [1, 2, 3]})
        release = asyncio.Event()
        started = []

        async def later():
            await release.wait()
            return await client.arrow_from_java(await client.arrow_to_java(table))

        async def start():
            started.append(asyncio.create_task(later()))

        supplier = await client.register_callback(start, "java.util.function.Supplier")
        await client.jvm.OtherThread.call(supplier)
        release.set()

        assert (await asyncio.wait_for(started[0], 5)).equals(table)


class TestCallbackObjects:
    """An object that is not callable implements an interface with several
    methods: each call goes to its method of the same name."""

    class Countdown:
        def __init__(self, start, client=None):
            self.left = start
            self.client = client
            self.came_on_the_callback_connection = []

        def hasMoreElements(self):
            if self.client is not None:
                call = gatun.client._channel_call_of(self.client)
                self.came_on_the_callback_connection.append(call is not None)
            return self.left > 0

        def nextElement(self):
            self.left -= 1
            return self.left

    def test_each_call_goes_to_the_method_of_its_name(self, client):
        enumeration = client.register_callback(
            self.Countdown(3), "java.util.Enumeration"
        )

        assert client.jvm.java.util.Collections.list(enumeration) == [2, 1, 0]

    def test_from_another_thread(self, other_thread_classes):
        client = connect(
            memory="16MB",
            classpath=[other_thread_classes],
            allowed_classes=["OtherThread"],
            socket_timeout=10,
        )
        try:
            countdown = self.Countdown(2, client)
            enumeration = client.register_callback(countdown, "java.util.Enumeration")
            drain = client.register_callback(
                lambda: client.jvm.java.util.Collections.list(enumeration),
                "java.util.function.Supplier",
            )

            # The Java thread that runs the supplier's call is the one that
            # calls the enumeration
            assert client.jvm.OtherThread.call(drain) == [1, 0]
            assert countdown.came_on_the_callback_connection == [True] * 3
        finally:
            client.close()

    def test_method_the_object_lacks(self, client):
        class Half:
            def hasMoreElements(self):
                return True

        enumeration = client.register_callback(Half(), "java.util.Enumeration")

        with pytest.raises(JavaException, match=r"Half has no method nextElement\(\)"):
            client.jvm.java.util.Collections.list(enumeration)

    def test_object_overrides_a_default_method(self, client):
        """Iterator.remove() has a default body, which throws."""

        class Numbers:
            def __init__(self):
                self.left = [1, 2, 3]
                self.removed = 0

            def hasNext(self):
                return bool(self.left)

            def next(self):
                return self.left.pop(0)

            def remove(self):
                self.removed += 1

        numbers = Numbers()
        iterator = client.register_callback(numbers, "java.util.Iterator")

        assert iterator.next() == 1
        iterator.remove()

        assert numbers.removed == 1

    def test_default_method_the_object_lacks_runs_in_java(self, client):
        class ByLength:
            def compare(self, a, b):
                return len(a) - len(b)

        by_length = client.register_callback(ByLength(), "java.util.Comparator")
        words = client.jvm.java.util.ArrayList(["bb", "a", "ccc"])

        client.jvm.java.util.Collections.sort(words, by_length.reversed())

        assert list(words) == ["ccc", "bb", "a"]

    def test_object_with_nothing_to_call_is_refused(self, client):
        class Empty:
            size = 3

            def _private(self):
                return None

        with pytest.raises(TypeError, match="Empty cannot be a callback"):
            client.register_callback(Empty(), "java.lang.Runnable")
        assert client._callbacks == {}

    def test_sync_client_refuses_a_coroutine_function(self, client):
        """It would call it and send the coroutine back unawaited."""

        async def compare(a, b):
            return 0

        class Listener:
            async def run(self):
                return None

        with pytest.raises(TypeError, match="compare is a coroutine function"):
            client.register_callback(compare, "java.util.Comparator")
        with pytest.raises(TypeError, match="Listener.run is a coroutine function"):
            client.register_callback(Listener(), "java.lang.Runnable")
        assert client._callbacks == {}

    def test_callable_answers_every_method(self, client):
        answers = iter([True, "a", True, "b", False])
        enumeration = client.register_callback(
            lambda: next(answers), "java.util.Enumeration"
        )

        assert client.jvm.java.util.Collections.list(enumeration) == ["a", "b"]

    async def test_async_client_sync_and_async_methods(self, java_gateway):
        class Countdown:
            def __init__(self):
                self.left = 3

            def hasMoreElements(self):
                return self.left > 0

            async def nextElement(self):
                await asyncio.sleep(0)
                self.left -= 1
                return self.left

        async with AsyncGatunClient(java_gateway.socket_path) as client:
            enumeration = await client.register_callback(
                Countdown(), "java.util.Enumeration"
            )

            assert await client.jvm.java.util.Collections.list(enumeration) == [
                2,
                1,
                0,
            ]

    async def test_async_client_object_overrides_a_default_method(self, java_gateway):
        class Numbers:
            removed = 0

            def hasNext(self):
                return True

            def next(self):
                return 1

            async def remove(self):
                await asyncio.sleep(0)
                self.removed += 1

        numbers = Numbers()
        async with AsyncGatunClient(java_gateway.socket_path) as client:
            iterator = await client.register_callback(numbers, "java.util.Iterator")
            await iterator.remove()

            assert numbers.removed == 1
            # Through a handle that lives for this one call only
            holder = await client.jvm.java.util.ArrayList()
            await holder.add(iterator)
            await client.unregister_callback(await holder.get(0))
            assert client._callbacks == {}


class TestCallbackOwnership:
    def test_another_session_cannot_unregister_a_callback(self, client, make_client):
        comparator = client.register_callback(
            lambda a, b: (a > b) - (a < b), "java.util.Comparator"
        )
        other = make_client()

        other.unregister_callback(comparator.object_id)

        items = client.jvm.java.util.ArrayList([2, 1])
        client.jvm.java.util.Collections.sort(items, comparator)
        assert list(items) == [1, 2]

    def test_unregistering_inside_an_in_band_callback_leaves_it_registered(
        self, client
    ):
        """The request is refused, so the callback must stay callable."""
        registered = []

        def compare(a, b):
            client.unregister_callback(registered[0])
            return 0

        comparator = client.register_callback(compare, "java.util.Comparator")
        registered.append(comparator.object_id)
        items = client.jvm.java.util.ArrayList([2, 1])

        with pytest.raises(JavaException, match="ReentrancyError"):
            client.jvm.java.util.Collections.sort(items, comparator)
        assert comparator.object_id in client._callbacks
