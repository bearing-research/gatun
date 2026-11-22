"""Tests for async client functionality."""

import pytest

from gatun import JavaException
from gatun.async_client import AsyncGatunClient, AsyncJavaObject, run_sync
from gatun.client import DeadConnectionError, SocketTimeoutError


@pytest.fixture
async def async_client(java_gateway):
    """Provides an async client connected to the test gateway."""
    client = AsyncGatunClient(java_gateway.socket_path)
    connected = await client.connect()
    assert connected, "Failed to connect async client"

    yield client

    await client.close()


class TestAsyncClientBasics:
    """Test basic async client operations."""

    @pytest.mark.asyncio
    async def test_ping(self, async_client):
        """Test ping health check works asynchronously."""
        assert await async_client.ping() is True

    @pytest.mark.asyncio
    async def test_create_object(self, async_client):
        """Test creating a Java object asynchronously."""
        arr = await async_client.create_object("java.util.ArrayList")
        assert isinstance(arr, AsyncJavaObject)

    @pytest.mark.asyncio
    async def test_invoke_method(self, async_client):
        """Test invoking a method asynchronously."""
        arr = await async_client.create_object("java.util.ArrayList")
        await arr.add("hello")
        await arr.add("world")
        size = await arr.size()
        assert size == 2

    @pytest.mark.asyncio
    async def test_invoke_static_method(self, async_client):
        """Test invoking a static method asynchronously."""
        result = await async_client.invoke_static_method(
            "java.lang.Integer", "parseInt", "42"
        )
        assert result == 42

    @pytest.mark.asyncio
    async def test_get_method_result(self, async_client):
        """Test getting results from method calls."""
        arr = await async_client.create_object("java.util.ArrayList")
        await arr.add("first")
        await arr.add("second")

        first = await arr.get(0)
        assert first == "first"

        second = await arr.get(1)
        assert second == "second"


class TestAsyncJVMView:
    """Test async JVM view for package navigation."""

    @pytest.mark.asyncio
    async def test_jvm_view_create_object(self, async_client):
        """Test creating objects via JVM view."""
        ArrayList = async_client.jvm.java.util.ArrayList
        arr = await ArrayList()
        assert isinstance(arr, AsyncJavaObject)

    @pytest.mark.asyncio
    async def test_jvm_view_static_method(self, async_client):
        """Test calling static methods via JVM view."""
        Integer = async_client.jvm.java.lang.Integer
        result = await Integer.parseInt("123")
        assert result == 123

    @pytest.mark.asyncio
    async def test_jvm_view_math_operations(self, async_client):
        """Test Math static methods via JVM view."""
        Math = async_client.jvm.java.lang.Math
        max_val = await Math.max(10, 20)
        assert max_val == 20

        abs_val = await Math.abs(-42)
        assert abs_val == 42


class TestAsyncContextManager:
    """Test async context manager support."""

    @pytest.mark.asyncio
    async def test_client_context_manager(self, java_gateway):
        """Test AsyncGatunClient as async context manager."""
        async with AsyncGatunClient(java_gateway.socket_path) as client:
            arr = await client.create_object("java.util.ArrayList")
            await arr.add("test")
            size = await arr.size()
            assert size == 1


class TestAsyncCallbacks:
    """Test async callback functionality."""

    @pytest.mark.asyncio
    async def test_sync_callback(self, async_client):
        """Test registering a sync callback."""

        def compare(a, b):
            if a < b:
                return -1
            elif a > b:
                return 1
            return 0

        comparator = await async_client.register_callback(
            compare, "java.util.Comparator"
        )
        assert isinstance(comparator, AsyncJavaObject)

        # Use it to sort
        arr = await async_client.create_object("java.util.ArrayList")
        await arr.add("c")
        await arr.add("a")
        await arr.add("b")

        await async_client.invoke_static_method(
            "java.util.Collections", "sort", arr, comparator
        )

        assert await arr.get(0) == "a"
        assert await arr.get(1) == "b"
        assert await arr.get(2) == "c"

    @pytest.mark.asyncio
    async def test_async_callback(self, async_client):
        """Test registering an async callback."""

        async def async_compare(a, b):
            # Simulate async work
            await asyncio.sleep(0)
            if a < b:
                return -1
            elif a > b:
                return 1
            return 0

        import asyncio

        comparator = await async_client.register_callback(
            async_compare, "java.util.Comparator"
        )

        arr = await async_client.create_object("java.util.ArrayList")
        await arr.add("z")
        await arr.add("m")
        await arr.add("a")

        await async_client.invoke_static_method(
            "java.util.Collections", "sort", arr, comparator
        )

        assert await arr.get(0) == "a"
        assert await arr.get(1) == "m"
        assert await arr.get(2) == "z"


class TestRunSync:
    """Test run_sync utility for running sync client in async context."""

    @pytest.mark.asyncio
    async def test_run_sync_create_object(self, client):
        """Test run_sync with create_object."""
        arr = await run_sync(client.create_object, "java.util.ArrayList")
        assert arr is not None

    @pytest.mark.asyncio
    async def test_run_sync_method_call(self, client):
        """Test run_sync with method calls."""
        arr = await run_sync(client.create_object, "java.util.ArrayList")
        await run_sync(arr.add, "hello")
        size = await run_sync(arr.size)
        assert size == 1

    @pytest.mark.asyncio
    async def test_run_sync_static_method(self, client):
        """Test run_sync with static method calls."""
        result = await run_sync(
            client.invoke_static_method,
            "java.lang.Integer",
            "parseInt",
            "99",
        )
        assert result == 99


class TestAsyncHashMap:
    """Test async operations with HashMap."""

    @pytest.mark.asyncio
    async def test_hashmap_operations(self, async_client):
        """Test HashMap put/get operations."""
        hm = await async_client.create_object("java.util.HashMap")

        await hm.put("key1", "value1")
        await hm.put("key2", "value2")

        v1 = await hm.get("key1")
        v2 = await hm.get("key2")

        assert v1 == "value1"
        assert v2 == "value2"

        size = await hm.size()
        assert size == 2


class TestAsyncStringBuilder:
    """Test async operations with StringBuilder."""

    @pytest.mark.asyncio
    async def test_stringbuilder_chaining(self, async_client):
        """Test StringBuilder append operations."""
        sb = await async_client.create_object("java.lang.StringBuilder")

        await sb.append("Hello")
        await sb.append(" ")
        await sb.append("World")

        result = await sb.toString()
        assert result == "Hello World"


class TestAsyncIsInstanceOf:
    """Test async is_instance_of functionality."""

    @pytest.mark.asyncio
    async def test_is_instance_of_same_class(self, async_client):
        """Test is_instance_of with the exact class."""
        arr = await async_client.create_object("java.util.ArrayList")
        assert await async_client.is_instance_of(arr, "java.util.ArrayList") is True

    @pytest.mark.asyncio
    async def test_is_instance_of_interface(self, async_client):
        """Test is_instance_of with an interface."""
        arr = await async_client.create_object("java.util.ArrayList")
        assert await async_client.is_instance_of(arr, "java.util.List") is True

    @pytest.mark.asyncio
    async def test_is_instance_of_unrelated(self, async_client):
        """Test is_instance_of with unrelated class."""
        arr = await async_client.create_object("java.util.ArrayList")
        assert await async_client.is_instance_of(arr, "java.util.Map") is False


class TestAsyncVectorizedAPIs:
    """Test async vectorized APIs (get_fields, invoke_methods, create_objects)."""

    @pytest.mark.asyncio
    async def test_get_fields_basic(self, async_client):
        """Test get_fields reads multiple fields in one round-trip."""
        sb = await async_client.create_object("java.lang.StringBuilder", "hello")

        result = await async_client.get_fields(sb, ["count"])

        assert result == [5]  # "hello" has 5 characters

    @pytest.mark.asyncio
    async def test_get_fields_with_object_id(self, async_client):
        """Test get_fields accepts object_id directly."""
        sb = await async_client.create_object("java.lang.StringBuilder", "abc")

        result = await async_client.get_fields(sb.object_id, ["count"])

        assert result == [3]

    @pytest.mark.asyncio
    async def test_invoke_methods_basic(self, async_client):
        """Test invoke_methods calls multiple methods in one round-trip."""
        arr = await async_client.create_object("java.util.ArrayList")

        results = await async_client.invoke_methods(
            arr,
            [
                ("add", ("a",)),
                ("add", ("b",)),
                ("add", ("c",)),
                ("size", ()),
            ],
        )

        assert results == [True, True, True, 3]

    @pytest.mark.asyncio
    async def test_invoke_methods_with_args(self, async_client):
        """Test invoke_methods with methods that take arguments."""
        sb = await async_client.create_object("java.lang.StringBuilder", "hello")

        results = await async_client.invoke_methods(
            sb,
            [
                ("append", (" ",)),
                ("append", ("world",)),
                ("toString", ()),
            ],
        )

        assert results[2] == "hello world"

    @pytest.mark.asyncio
    async def test_invoke_methods_return_object_refs(self, async_client):
        """Test invoke_methods with return_object_ref flag."""
        arr = await async_client.create_object("java.util.ArrayList")
        await arr.add("x")
        await arr.add("y")

        results = await async_client.invoke_methods(
            arr,
            [
                ("size", ()),
                ("subList", (0, 1)),
            ],
            return_object_refs=[False, True],
        )

        assert results[0] == 2
        # Second result should be an AsyncJavaObject
        assert isinstance(results[1], AsyncJavaObject)

    @pytest.mark.asyncio
    async def test_create_objects_basic(self, async_client):
        """Test create_objects creates multiple objects in one round-trip."""
        objects = await async_client.create_objects(
            [
                ("java.util.ArrayList", ()),
                ("java.util.HashMap", ()),
                ("java.util.HashSet", ()),
            ]
        )

        assert len(objects) == 3

        for obj in objects:
            assert isinstance(obj, AsyncJavaObject)

        # Verify types
        assert await async_client.is_instance_of(objects[0], "java.util.ArrayList")
        assert await async_client.is_instance_of(objects[1], "java.util.HashMap")
        assert await async_client.is_instance_of(objects[2], "java.util.HashSet")

    @pytest.mark.asyncio
    async def test_create_objects_with_args(self, async_client):
        """Test create_objects with constructor arguments."""
        objects = await async_client.create_objects(
            [
                ("java.util.ArrayList", (100,)),  # with initial capacity
                ("java.lang.StringBuilder", ("hello",)),
            ]
        )

        assert len(objects) == 2

        # StringBuilder should have the initial content
        result = await objects[1].toString()
        assert result == "hello"

    @pytest.mark.asyncio
    async def test_create_objects_single(self, async_client):
        """Test create_objects with single object."""
        objects = await async_client.create_objects([("java.util.ArrayList", ())])

        assert len(objects) == 1
        assert await async_client.is_instance_of(objects[0], "java.util.ArrayList")


class TestAsyncFieldOperations:
    """Test async field get/set operations."""

    @pytest.mark.asyncio
    async def test_get_field(self, async_client):
        """Test getting a field value asynchronously."""
        sb = await async_client.create_object("java.lang.StringBuilder", "hello")
        count = await async_client.get_field(sb, "count")
        assert count == 5

    @pytest.mark.asyncio
    async def test_get_field_with_object_id(self, async_client):
        """Test get_field accepts object_id directly."""
        sb = await async_client.create_object("java.lang.StringBuilder", "test")
        count = await async_client.get_field(sb.object_id, "count")
        assert count == 4

    @pytest.mark.asyncio
    async def test_set_field(self, async_client):
        """Test setting a field value asynchronously."""
        sb = await async_client.create_object("java.lang.StringBuilder", "hello world")
        # Set count field to truncate the string
        await async_client.set_field(sb, "count", 5)
        result = await sb.toString()
        assert result == "hello"

    @pytest.mark.asyncio
    async def test_set_field_with_object_id(self, async_client):
        """Test set_field accepts object_id directly."""
        sb = await async_client.create_object("java.lang.StringBuilder", "testing")
        await async_client.set_field(sb.object_id, "count", 4)
        result = await sb.toString()
        assert result == "test"


class TestAsyncDatetimeArguments:
    """datetime values are encoded on the wire, as in the sync client."""

    async def test_datetime_arguments(self, async_client):
        import datetime

        arr = await async_client.jvm.java.util.ArrayList()
        await arr.add(datetime.date(2026, 1, 2))
        await arr.add(datetime.time(1, 2, 3))
        await arr.add(datetime.datetime(2026, 1, 2, 3, 4, 5, 123456))

        assert (
            await arr.toString() == "[2026-01-02, 01:02:03, 2026-01-02 03:04:05.123456]"
        )


class TestAsyncReturnValues:
    """The async client decodes return values the same way the sync client does."""

    async def test_map(self, async_client):
        """Regression: any returned Map raised AttributeError."""
        hm = await async_client.jvm.java.util.HashMap()
        await hm.put("a", 1)
        await hm.put("b", [1, 2])
        copy = await async_client.jvm.java.util.Collections.unmodifiableMap(hm)

        assert copy == {"a": 1, "b": [1, 2]}

    async def test_primitive_array(self, async_client):
        """Regression: decoding needed numpy, which is not a dependency."""
        from gatun import JavaArray

        jvm = async_client.jvm
        int_array_class = await jvm.java.lang.Class.forName("[I")
        int_class = await int_array_class.getComponentType()
        ints = await jvm.java.lang.reflect.Array.newInstance(int_class, 3)

        assert isinstance(ints, JavaArray)
        assert ints.element_type == "Int"
        assert list(ints) == [0, 0, 0]

    async def test_char(self, async_client):
        sb = await async_client.jvm.java.lang.StringBuilder("hi")

        assert await sb.charAt(1) == "i"

    async def test_returned_values_can_be_passed_back(self, async_client):
        """Whatever the shared decoder returns, the client can send again."""
        jvm = async_client.jvm
        hm = await jvm.java.util.HashMap()
        await hm.put("a", 1)
        as_dict = await jvm.java.util.Collections.unmodifiableMap(hm)
        other = await jvm.java.util.TreeMap()
        await other.putAll(as_dict)
        assert await other.toString() == "{a=1}"

        int_array_class = await jvm.java.lang.Class.forName("[I")
        ints = await jvm.java.lang.reflect.Array.newInstance(
            await int_array_class.getComponentType(), 2
        )
        assert await jvm.java.util.Arrays.toString(ints) == "[0, 0]"

        data = await jvm.java.util.Arrays.copyOf(b"hi", 3)
        assert data == b"hi\x00"
        assert await jvm.java.util.Arrays.toString(data) == "[104, 105, 0]"


class TestAsyncObjectLifecycle:
    """Test async object lifecycle methods."""

    @pytest.mark.asyncio
    async def test_free_object(self, async_client):
        """Test freeing an object asynchronously."""
        arr = await async_client.create_object("java.util.ArrayList")
        obj_id = arr.object_id
        # Free the object explicitly
        await async_client.free_object(obj_id)
        # Double free should not raise (the server ignores unknown IDs)
        await async_client.free_object(obj_id)

    @pytest.mark.asyncio
    async def test_unregister_callback(self, async_client):
        """Test unregistering a callback asynchronously."""

        def compare(a, b):
            return 0

        comparator = await async_client.register_callback(
            compare, "java.util.Comparator"
        )
        callback_id = comparator.object_id

        # Unregister should work
        await async_client.unregister_callback(callback_id)

        # Unregistering again should not raise
        await async_client.unregister_callback(callback_id)


class TestAsyncArrowTransfer:
    """Test async Arrow data transfer."""

    @pytest.mark.asyncio
    async def test_send_arrow_table(self, async_client):
        """Test sending an Arrow table asynchronously."""
        import pyarrow as pa

        table = pa.table({"x": [1, 2, 3], "y": ["a", "b", "c"]})
        result = await async_client.send_arrow_table(table)
        assert "3 rows" in result

    async def test_a_closed_table_cannot_be_read_back(self, async_client):
        import pyarrow as pa

        root = await async_client.arrow_to_java(pa.table({"x": [1, 2, 3]}))
        produced = await root.slice(1, 2)
        await produced.close()

        with pytest.raises(JavaException, match="has been closed"):
            await async_client.arrow_from_java(produced)
        assert (await async_client.arrow_from_java(root)).num_rows == 3

    @pytest.mark.asyncio
    async def test_send_arrow_table_large(self, async_client):
        """Test sending a larger Arrow table."""
        import pyarrow as pa

        # Create a table with more data
        table = pa.table(
            {
                "id": list(range(1000)),
                "value": [f"item_{i}" for i in range(1000)],
            }
        )
        result = await async_client.send_arrow_table(table)
        assert "1000 rows" in result


class TestAsyncHandlesAreFreed:
    """A collected handle releases its Java object, as the sync client's does."""

    async def test_collected_object_is_freed_with_the_next_request(self, async_client):
        import gc

        items = await async_client.create_object("java.util.ArrayList")
        object_id = items.object_id
        del items
        gc.collect()

        assert list(async_client._pending_frees) == [object_id]
        await async_client.ping()
        assert not async_client._pending_frees
        with pytest.raises(JavaException, match="not found"):
            await async_client.invoke_method(object_id, "size")

    async def test_temporary_wrapper_lives_through_its_own_call(self, async_client):
        """`await (await make()).size()`: nothing but the pending call refers to
        the wrapper, and it must not be freed ahead of that call."""
        import gc

        size = await (await async_client.create_object("java.util.ArrayList")).size()
        assert size == 0

        add = (await async_client.create_object("java.util.ArrayList")).add
        gc.collect()
        assert await add("x") is True

        items = await async_client.create_object("java.util.ArrayList")
        await items.add("a")
        assert await (await items.stream()).count() == 1

    async def test_temporary_passed_to_the_client(self, async_client):
        """The client's methods keep a temporary alive through the call too."""

        async def builder():
            return await async_client.jvm.java.lang.StringBuilder("abc")

        assert await async_client.invoke_method(await builder(), "length") == 3
        assert await async_client.get_field(await builder(), "count") == 3
        assert await async_client.set_field(await builder(), "count", 2) is None
        assert (
            await async_client.is_instance_of(await builder(), "java.lang.CharSequence")
            is True
        )
        assert await async_client.get_fields(await builder(), ["count"]) == [3]
        assert await async_client.invoke_methods(await builder(), [("length", ())]) == [
            3
        ]
        assert await async_client.ping() is True

    async def test_detached_object_is_kept(self, async_client):
        import gc

        items = await async_client.create_object("java.util.ArrayList")
        object_id = items.object_id
        items.detach()
        del items
        gc.collect()

        await async_client.ping()
        assert await async_client.invoke_method(object_id, "size") == 0
        await async_client.free_object(object_id)

    async def test_object_freed_by_hand_is_not_freed_again(self, async_client):
        import gc

        items = await async_client.create_object("java.util.ArrayList")
        async with items:
            assert await items.size() == 0
        del items
        gc.collect()

        assert not async_client._pending_frees


class TestAsyncConnectionState:
    async def test_timeout_leaves_the_client_dead(self):
        """The late answer is still coming; the next request must not read it."""
        from gatun import aconnect

        client = await aconnect(
            memory="16MB", allowed_classes=["java.lang.Thread"], socket_timeout=0.5
        )
        try:
            with pytest.raises(SocketTimeoutError):
                await client.jvm.java.lang.Thread.sleep(5000)
            with pytest.raises(DeadConnectionError):
                await client.ping()
        finally:
            await client.close()

    async def test_cancelled_task_leaves_the_client_dead(self):
        import asyncio

        from gatun import aconnect

        client = await aconnect(memory="16MB", allowed_classes=["java.lang.Thread"])
        try:
            task = asyncio.ensure_future(client.jvm.java.lang.Thread.sleep(5000))
            await asyncio.sleep(0.3)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            with pytest.raises(DeadConnectionError):
                await client.ping()
        finally:
            await client.close()

    async def test_java_exception_leaves_the_client_usable(self, async_client):
        with pytest.raises(JavaException):
            await async_client.invoke_static_method(
                "java.lang.Integer", "parseInt", "not a number"
            )
        assert await async_client.ping() is True

    async def test_use_after_close(self, java_gateway):
        client = AsyncGatunClient(java_gateway.socket_path)
        assert await client.connect()
        await client.close()
        with pytest.raises(DeadConnectionError, match="closed"):
            await client.ping()

    async def test_failed_connect_keeps_the_reason(self, tmp_path):
        client = AsyncGatunClient(str(tmp_path / "nothing.sock"))
        assert await client.connect() is False
        assert isinstance(client.connect_error, OSError)
        with pytest.raises(RuntimeError) as exc_info:
            await AsyncGatunClient(str(tmp_path / "nothing.sock")).__aenter__()
        assert isinstance(exc_info.value.__cause__, OSError)

    @pytest.mark.parametrize("timeout", [0, float("inf"), None])
    async def test_values_that_mean_no_timeout(self, java_gateway, timeout):
        client = AsyncGatunClient(
            java_gateway.socket_path, socket_timeout=timeout, callback_timeout=timeout
        )
        try:
            assert await client.connect()
            assert await client.ping() is True
            doubled = await client.register_callback(
                lambda value: value * 2, "java.util.function.UnaryOperator"
            )
            items = await client.create_object("java.util.ArrayList")
            await items.add(4)
            await items.replaceAll(doubled)
            assert await items.get(0) == 8
        finally:
            await client.close()


class TestAsyncReentrancy:
    async def test_other_tasks_are_not_blocked_by_a_callback(self, async_client):
        """Only the task running the callback is reentrant, not every task."""
        import asyncio

        outcome = []

        async def slow(a, b):
            await asyncio.sleep(0.05)
            return a - b

        async def other():
            await asyncio.sleep(0.02)
            outcome.append(await async_client.ping())

        comparator = await async_client.register_callback(slow, "java.util.Comparator")
        items = await async_client.create_object("java.util.ArrayList")
        for value in (3, 2, 1):
            await items.add(value)

        task = asyncio.ensure_future(other())
        await async_client.invoke_static_method(
            "java.util.Collections", "sort", items, comparator
        )
        await task
        assert outcome == [True]

    async def test_java_call_from_a_task_the_callback_waits_for(self, async_client):
        """A helper task the callback awaits is as reentrant as the callback."""
        import asyncio

        seen = []

        async def helper():
            try:
                await async_client.ping()
            except Exception as e:  # noqa: BLE001 - recorded for the assertion
                seen.append(type(e).__name__)

        async def delegating(a, b):
            await asyncio.wait_for(asyncio.gather(helper(), helper()), timeout=5)
            return 0

        comparator = await async_client.register_callback(
            delegating, "java.util.Comparator"
        )
        items = await async_client.create_object("java.util.ArrayList")
        await items.add(2)
        await items.add(1)
        await async_client.invoke_static_method(
            "java.util.Collections", "sort", items, comparator
        )
        assert seen and set(seen) == {"ReentrancyError"}

    async def test_task_started_by_a_callback_may_call_java_afterwards(
        self, async_client
    ):
        """Queueing work for later is the way to call Java because of a callback."""
        import asyncio

        outcome = []
        tasks = []
        release = asyncio.Event()

        async def later():
            await release.wait()
            outcome.append(await async_client.ping())

        async def queueing(a, b):
            if not tasks:
                tasks.append(asyncio.ensure_future(later()))
            return 0

        comparator = await async_client.register_callback(
            queueing, "java.util.Comparator"
        )
        items = await async_client.create_object("java.util.ArrayList")
        await items.add(2)
        await items.add(1)
        await async_client.invoke_static_method(
            "java.util.Collections", "sort", items, comparator
        )
        release.set()
        await tasks[0]
        assert outcome == [True]

    async def test_java_call_inside_a_callback_is_refused(self, async_client):
        seen = []

        async def nested(a, b):
            try:
                await async_client.ping()
            except Exception as e:  # noqa: BLE001 - recorded for the assertion
                seen.append(type(e).__name__)
            return 0

        comparator = await async_client.register_callback(
            nested, "java.util.Comparator"
        )
        items = await async_client.create_object("java.util.ArrayList")
        await items.add(2)
        await items.add(1)
        await async_client.invoke_static_method(
            "java.util.Collections", "sort", items, comparator
        )
        assert seen and set(seen) == {"ReentrancyError"}


class TestAsyncJvmView:
    async def test_the_server_decides_what_a_path_is(self, async_client):
        assert await async_client.reflect("java.util.ArrayList") == "class"
        assert await async_client.reflect("java.lang.Math.max") == "method"
        assert await async_client.reflect("java.lang.Integer.MAX_VALUE") == "field"

    async def test_static_field(self, async_client):
        value = await async_client.get_static_field("java.lang.Integer", "MAX_VALUE")
        assert value == 2147483647

    async def test_calling_a_field_says_so(self, async_client):
        with pytest.raises(TypeError, match="static field"):
            await async_client.jvm.java.lang.Integer.MAX_VALUE()
