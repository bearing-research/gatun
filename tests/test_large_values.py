"""Values that do not fit the 64 KB zones of the shared memory.

A command or response larger than its zone follows its length on the socket.
What bounds it is the size of the shared memory the client connected with and
the server's resource limits.
"""

import os
import subprocess

import pyarrow as pa
import pytest

from gatun import (
    JavaException,
    PayloadTooLargeError,
    aconnect,
    connect,
)
from gatun.client import COMMAND_ZONE_SIZE, RESPONSE_ZONE_SIZE

LARGE_JAVA = """
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class Large {
    public static Object echo(Object x) { return x; }

    public static String text(int n) {
        char[] c = new char[n];
        Arrays.fill(c, 'j');
        return new String(c);
    }
    public static byte[] bytes(int n) {
        byte[] b = new byte[n];
        for (int i = 0; i < n; i++) b[i] = (byte) (i % 251);
        return b;
    }
    public static List<Long> longs(int n) {
        List<Long> out = new ArrayList<>(n);
        for (long i = 0; i < n; i++) out.add((1L << 40) + i);
        return out;
    }
    public static Map<String, String> map(int n) {
        Map<String, String> out = new LinkedHashMap<>();
        for (int i = 0; i < n; i++) out.put("key" + i, "value" + i);
        return out;
    }

    public static int length(String s) { return s.length(); }
    public static int count(byte[] b) { return b.length; }
    public static int sizeOf(List<Object> x) { return x.size(); }
    public static int entries(Map<Object, Object> x) { return x.size(); }

    public static Object apply(Function<Object, Object> f, Object x) { return f.apply(x); }

    /** Call the function with a string made here, which no command had to carry. */
    public static Object applyToText(Function<Object, Object> f, int n) {
        return f.apply(text(n));
    }

    /** Apply the function on a new thread and wait for it. */
    public static Object onOtherThread(Function<Object, Object> f, Object x)
            throws Exception {
        Object[] out = new Object[1];
        Thread thread = new Thread(() -> out[0] = f.apply(x));
        thread.start();
        thread.join();
        return out[0];
    }

    /** An exception whose trace is far longer than 64 KB: three causes deep. */
    public static void fail(int depth, int causes) {
        if (depth > 0) {
            fail(depth - 1, causes);
            return;
        }
        RuntimeException e = new RuntimeException("the root cause");
        for (int i = 0; i < causes; i++) {
            e = wrap(600, e);
        }
        throw e;
    }
    private static RuntimeException wrap(int depth, RuntimeException cause) {
        if (depth > 0) return wrap(depth - 1, cause);
        return new IllegalStateException("a wrapper with quite a long message to carry", cause);
    }
}
"""

KB = 1024
MB = 1024 * KB
SIZES = [64 * KB - 1, 64 * KB, MB, 16 * MB]
SIZE_IDS = ["64KB-1", "64KB", "1MB", "16MB"]


def text_of(size):
    return "s" * size


def bytes_of(size):
    return bytes(range(256)) * (size // 256) + bytes(size % 256)


def ints_of(size):
    """A list of ints that takes `size` bytes on the wire: eight each."""
    return list(range(2**40, 2**40 + size // 8))


def dict_of(size):
    """A dict of about `size` bytes: its values are 100 characters each."""
    return {f"key{i}": "v" * 100 for i in range(max(1, size // 110))}


KINDS = {"str": text_of, "bytes": bytes_of, "ints": ints_of, "dict": dict_of}


def same(a, b):
    """Equality, without pytest writing megabytes of difference on failure."""
    return isinstance(a, type(b)) and a == b


@pytest.fixture(scope="module")
def large_classes(tmp_path_factory):
    out = tmp_path_factory.mktemp("large")
    java_home = os.environ.get("JAVA_HOME")
    javac = os.path.join(java_home, "bin", "javac") if java_home else "javac"
    source = out / "Large.java"
    source.write_text(LARGE_JAVA)
    subprocess.run([javac, "--release", "22", "-d", str(out), str(source)], check=True)
    return str(out)


# 16 MB of string, and of ints at eight bytes each, are past two default limits
ROOMY = [
    "-Dgatun.limits.max_string_length=33554432",
    "-Dgatun.limits.max_list_entries=4000000",
]


@pytest.fixture(scope="module")
def client(large_classes):
    client = connect(
        memory="64MB",
        classpath=[large_classes],
        allowed_classes=["Large"],
        extra_jvm_flags=ROOMY,
    )
    yield client
    client.close()


@pytest.fixture
def small(large_classes):
    """The smallest memory there is: 192 KB, three times a zone."""
    client = connect(
        memory="192KB", classpath=[large_classes], allowed_classes=["Large"]
    )
    yield client
    client.close()


@pytest.mark.parametrize("size", SIZES, ids=SIZE_IDS)
class TestBothDirections:
    @pytest.mark.parametrize("kind", KINDS)
    def test_round_trip(self, client, kind, size):
        value = KINDS[kind](size)
        assert same(client.jvm.Large.echo(value), value)

    def test_str_argument(self, client, size):
        assert client.jvm.Large.length(text_of(size)) == size

    def test_str_returned(self, client, size):
        assert same(client.jvm.Large.text(size), "j" * size)

    def test_bytes_argument(self, client, size):
        assert client.jvm.Large.count(bytes_of(size)) == size

    def test_bytes_returned(self, client, size):
        result = client.jvm.Large.bytes(size)
        assert len(result) == size
        assert bytes(result[:502]) == bytes(i % 251 for i in range(502))

    def test_ints_argument(self, client, size):
        assert client.jvm.Large.sizeOf(ints_of(size)) == size // 8

    def test_ints_returned(self, client, size):
        assert same(client.jvm.Large.longs(size // 8), ints_of(size))

    def test_dict_argument(self, client, size):
        value = dict_of(size)
        assert client.jvm.Large.entries(value) == len(value)

    def test_dict_returned(self, client, size):
        count = size // 16
        result = client.jvm.Large.map(count)
        assert len(result) == count
        assert result[f"key{count - 1}"] == f"value{count - 1}"

    @pytest.mark.parametrize("kind", KINDS)
    def test_in_a_batch(self, client, kind, size):
        value = KINDS[kind](size)
        with client.batch() as b:
            first = b.call_static("Large", "echo", value)
            small_one = b.call_static("Large", "length", "abc")
            second = b.call_static("Large", "echo", value)

        assert same(first.get(), value)
        assert small_one.get() == 3
        assert same(second.get(), value)

    @pytest.mark.parametrize("kind", KINDS)
    def test_callback_on_the_callback_connection(self, client, kind, size):
        """Java's argument, the Java call the callback makes with it and that
        call's result, and the callback's own result: four large frames."""
        value = KINDS[kind](size)
        seen = []

        def inside(argument):
            seen.append(same(argument, value))
            # Travels on the callback connection too
            return client.jvm.Large.echo(argument)

        function = client.register_callback(inside, "java.util.function.Function")
        try:
            result = client.jvm.Large.onOtherThread(function, value)
        finally:
            client.unregister_callback(function)

        assert seen == [True]
        assert same(result, value)

    @pytest.mark.parametrize("kind", ["str", "dict"])
    def test_callback_in_band(self, client, kind, size):
        value = KINDS[kind](size)
        seen = []

        def inside(argument):
            seen.append(same(argument, value))
            return argument

        function = client.register_callback(inside, "java.util.function.Function")
        try:
            result = client.jvm.Large.apply(function, value)
        finally:
            client.unregister_callback(function)

        assert seen == [True]
        assert same(result, value)

    def test_a_batch_inside_a_callback(self, client, size):
        value = text_of(size)

        def inside(argument):
            with client.batch() as b:
                echoed = b.call_static("Large", "echo", argument)
                made = b.call_static("Large", "text", 10)
            return [len(echoed.get()), made.get()]

        function = client.register_callback(inside, "java.util.function.Function")
        try:
            assert client.jvm.Large.onOtherThread(function, value) == [size, "j" * 10]
        finally:
            client.unregister_callback(function)


class TestTheBorder:
    def test_every_size_around_the_zones(self, client):
        """A string's command and its response cross 64 KB at different
        lengths, and each crossing has a last size in shared memory and a first
        one on the socket."""
        echo = client.jvm.Large.echo
        for size in range(COMMAND_ZONE_SIZE - 300, COMMAND_ZONE_SIZE + 40):
            value = "b" * size
            assert same(echo(value), value), size

    def test_instance_methods_and_constructors(self, client):
        text = "x" * 100_000
        builder = client.jvm.java.lang.StringBuilder(text)
        assert builder.length() == 100_000
        builder.append(text)
        assert builder.length() == 200_000
        assert same(builder.toString(), text * 2)

        items = client.jvm.java.util.ArrayList()
        items.add(text)
        assert same(items.get(0), text)

    def test_fields_and_vectorized_calls(self, client):
        text = "v" * 200_000
        builders = client.create_objects(
            [("java.lang.StringBuilder", (text,)), ("java.lang.StringBuilder", ())]
        )
        assert client.invoke_methods(
            builders[0], [("length", ()), ("toString", ())]
        ) == [200_000, text]

    def test_many_small_results_that_are_large_together(self, client):
        with client.batch() as b:
            results = [b.call_static("Large", "text", 1000) for _ in range(500)]

        assert all(r.get() == "j" * 1000 for r in results)

    def test_small_calls_still_use_shared_memory(self, client):
        """Nothing but the length goes on the socket for a call that fits."""
        sent = []
        real = client.sock

        class Recording:
            def sendall(self, data):
                sent.append(len(data))
                return real.sendall(data)

            def __getattr__(self, name):
                return getattr(real, name)

        client.sock = Recording()
        try:
            assert client.jvm.Large.length("abc") == 3
            assert client.jvm.Large.length("x" * 60_000) == 60_000
            assert sent == [4, 4]
            assert client.jvm.Large.length("x" * 70_000) == 70_000
            assert sent[2] == 4 and sent[3] > 70_000
        finally:
            client.sock = real

    def test_a_large_builder_is_not_kept(self, client):
        client.jvm.Large.length("x" * (4 * MB))
        assert len(client._get_builder(large=True).Bytes) <= MB


class TestLimits:
    def test_the_memory_size_is_the_limit(self, client, small):
        assert client.max_message_size == 64 * MB
        assert small.max_message_size == 192 * KB

    def test_between_a_zone_and_the_memory(self, small):
        value = "m" * 150_000
        assert same(small.jvm.Large.echo(value), value)

    def test_a_command_beyond_the_memory_is_not_sent(self, small):
        with pytest.raises(PayloadTooLargeError) as raised:
            small.jvm.Large.length("x" * 200_000)

        assert raised.value.zone == "Command"
        assert raised.value.max_size == 192 * KB
        assert raised.value.payload_size > 200_000
        assert "connect with more" in str(raised.value)
        assert small.jvm.Large.length("abc") == 3

    def test_a_response_beyond_the_memory(self, small):
        with pytest.raises(PayloadTooLargeError) as raised:
            small.jvm.Large.text(200_000)

        assert raised.value.zone == "Response"
        assert raised.value.max_size == 192 * KB
        assert small.jvm.Large.text(100_000) == "j" * 100_000

    def test_a_batch_beyond_the_memory(self, small):
        with small.batch() as b:
            results = [b.call_static("Large", "text", 50_000) for _ in range(5)]
        # The results taken together are what does not fit
        with pytest.raises(RuntimeError, match="batch failed as a whole"):
            results[0].get()

        with pytest.raises(PayloadTooLargeError), small.batch() as b:
            for _ in range(5):
                b.call_static("Large", "length", "x" * 50_000)
        assert small.ping() is True

    def test_a_callback_result_beyond_the_memory(self, small):
        function = small.register_callback(
            lambda value: "z" * 200_000, "java.util.function.Function"
        )
        with pytest.raises(JavaException, match="too large to return"):
            small.jvm.Large.apply(function, "x")
        assert small.ping() is True

    def test_callback_arguments_beyond_the_memory(self, small):
        function = small.register_callback(len, "java.util.function.Function")
        assert small.jvm.Large.applyToText(function, 150_000) == 150_000
        with pytest.raises(JavaException, match="more than the 196608"):
            small.jvm.Large.applyToText(function, 300_000)
        assert small.ping() is True

    def test_the_string_limit_still_applies(self, large_classes):
        client = connect(
            memory="64MB", classpath=[large_classes], allowed_classes=["Large"]
        )
        try:
            assert client.jvm.Large.length("x" * (9 * MB)) == 9 * MB
            with pytest.raises(JavaException, match="max_string_length"):
                client.jvm.Large.length("x" * (11 * MB))
            assert client.ping() is True
        finally:
            client.close()

    def test_the_list_limit_still_applies(self, large_classes):
        client = connect(
            memory="64MB", classpath=[large_classes], allowed_classes=["Large"]
        )
        try:
            assert client.jvm.Large.sizeOf(list(range(100_000))) == 100_000
            with pytest.raises(JavaException, match="max_list_entries"):
                client.jvm.Large.sizeOf(list(range(100_001)))
        finally:
            client.close()


class TestStackTraces:
    def test_a_long_trace_arrives_whole(self, client):
        with pytest.raises(JavaException) as raised:
            client.jvm.Large.fail(900, 3)

        e = raised.value
        assert len(e.stack_trace.encode()) > RESPONSE_ZONE_SIZE
        assert "trace shortened" not in e.stack_trace
        assert e.stack_trace.rstrip().count("Caused by: ") == 3
        assert "Caused by: java.lang.RuntimeException: the root cause" in e.stack_trace
        # Every frame of the outermost exception is there
        frames = len(e.java_exception.getStackTrace())
        first = e.stack_trace.split("\nCaused by: ")[0]
        assert first.count("\n\tat ") == frames

    def test_it_is_shortened_where_it_cannot_fit(self, small):
        """A quarter of the memory is the most an error's text may take."""
        with pytest.raises(JavaException) as raised:
            small.jvm.Large.fail(900, 3)

        e = raised.value
        assert "trace shortened" in e.stack_trace
        assert "Caused by: java.lang.RuntimeException: the root cause" in e.stack_trace
        assert small.ping() is True


class TestArrowIsLeftAlone:
    TABLE = pa.table({"n": list(range(10_000)), "s": [str(i) for i in range(10_000)]})

    def test_an_open_loan(self, client):
        with client.arrow_to_java(self.TABLE, copy=False) as root:
            value = "x" * (2 * MB)
            assert same(client.jvm.Large.echo(value), value)
            assert root.getRowCount() == 10_000
            assert client.arrow_from_java(root).equals(self.TABLE)

    def test_a_table_read_in_place(self, client):
        root = client.arrow_to_java(self.TABLE)
        try:
            view = client.arrow_from_java(root, copy=False)
            value = "x" * (2 * MB)
            assert same(client.jvm.Large.echo(value), value)
            # The view is still valid: nothing was written to the payload zone
            assert view.table.equals(self.TABLE)
        finally:
            root.close()

    def test_a_batch_loaded_in_place(self, client):
        with client.arrow_context() as ctx:
            ctx.send(self.TABLE)
            value = "x" * (2 * MB)
            assert same(client.jvm.Large.echo(value), value)
            assert ctx.receive().equals(self.TABLE)


class TestAsyncClient:
    @pytest.fixture
    async def aclient(self, large_classes):
        client = await aconnect(
            memory="64MB",
            classpath=[large_classes],
            allowed_classes=["Large"],
            extra_jvm_flags=ROOMY,
        )
        yield client
        await client.close()

    @pytest.mark.parametrize("size", SIZES, ids=SIZE_IDS)
    @pytest.mark.parametrize("kind", KINDS)
    async def test_round_trip(self, aclient, kind, size):
        value = KINDS[kind](size)
        result = await aclient.invoke_static_method("Large", "echo", value)
        assert same(result, value)

    async def test_the_border(self, aclient):
        for size in range(COMMAND_ZONE_SIZE - 300, COMMAND_ZONE_SIZE + 40, 7):
            value = "b" * size
            result = await aclient.invoke_static_method("Large", "echo", value)
            assert same(result, value), size

    @pytest.mark.parametrize("size", [64 * KB, MB], ids=["64KB", "1MB"])
    async def test_callback_on_the_callback_connection(self, aclient, size):
        value = text_of(size)

        async def inside(argument):
            return await aclient.invoke_static_method("Large", "echo", argument)

        function = await aclient.register_callback(
            inside, "java.util.function.Function"
        )
        result = await aclient.invoke_static_method(
            "Large", "onOtherThread", function, value
        )
        assert same(result, value)

    async def test_callback_in_band(self, aclient):
        value = text_of(MB)
        function = await aclient.register_callback(
            lambda argument: argument, "java.util.function.Function"
        )
        result = await aclient.invoke_static_method("Large", "apply", function, value)
        assert same(result, value)

    async def test_beyond_the_memory(self, large_classes):
        client = await aconnect(
            memory="192KB", classpath=[large_classes], allowed_classes=["Large"]
        )
        try:
            assert client.max_message_size == 192 * KB
            with pytest.raises(PayloadTooLargeError):
                await client.invoke_static_method("Large", "length", "x" * 200_000)
            with pytest.raises(PayloadTooLargeError):
                await client.invoke_static_method("Large", "text", 200_000)
            assert await client.invoke_static_method("Large", "length", "abc") == 3
        finally:
            await client.close()
