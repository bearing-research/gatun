"""A raised JavaException carries the Java exception that was thrown."""

import gc
import os
import subprocess

import flatbuffers
import pytest

from gatun import (
    JavaException,
    JavaIllegalArgumentException,
    JavaNoSuchFieldException,
    JavaNoSuchMethodException,
    JavaNumberFormatException,
    JavaObject,
    JavaSecurityException,
    aconnect,
    connect,
)
from gatun.async_client import AsyncJavaObject
from gatun.client import _release_unread_response
from gatun.generated.org.gatun.protocol import BatchResponse, Response

THROWER_JAVA = """
import java.util.function.Function;

public class Thrower {
    public static class CodedException extends Exception {
        private final int code;
        public CodedException(String message, int code, Throwable cause) {
            super(message, cause);
            this.code = code;
        }
        public int getCode() { return code; }
        public String describe() { return "coded " + code; }
    }

    public int value = 1;

    public Thrower() {}
    public Thrower(String message) { throw new IllegalArgumentException(message); }

    public static void fail(String message, int code) throws CodedException {
        throw new CodedException(message, code, new IllegalStateException("the cause"));
    }
    public String failHere(String message) {
        throw new UnsupportedOperationException(message);
    }
    public String ok() { return "ok"; }

    public static String messageOf(Throwable t) { return t.getMessage(); }
    public static void rethrow(Throwable t) throws Throwable { throw t; }
    public static boolean same(Object a, Object b) { return a == b; }

    /** Apply the function on a new thread and wait for it. */
    public static Object onOtherThread(Function<Object, Object> f, Object x)
            throws Exception {
        Object[] out = new Object[1];
        Thread thread = new Thread(() -> out[0] = f.apply(x));
        thread.start();
        thread.join();
        return out[0];
    }
    public static Object here(Function<Object, Object> f, Object x) { return f.apply(x); }
}
"""

ALLOWED = ["Thrower", "Thrower.CodedException"]


@pytest.fixture(scope="module")
def thrower_classes(tmp_path_factory):
    out = tmp_path_factory.mktemp("thrower")
    java_home = os.environ.get("JAVA_HOME")
    javac = os.path.join(java_home, "bin", "javac") if java_home else "javac"
    source = out / "Thrower.java"
    source.write_text(THROWER_JAVA)
    subprocess.run([javac, "--release", "22", "-d", str(out), str(source)], check=True)
    return str(out)


@pytest.fixture(scope="module")
def client(thrower_classes):
    """A server of its own, so that its object count is this module's."""
    client = connect(
        memory="16MB", classpath=[thrower_classes], allowed_classes=ALLOWED
    )
    yield client
    client.close()


def live_objects(client):
    report = client.get_metrics()
    return int(report.split("current_objects:")[1].split()[0])


def thrown_by(call, *args):
    """The exception `call` raises. Returned, not kept in a frame's locals."""
    try:
        call(*args)
    except JavaException as e:
        return e
    raise AssertionError("nothing was raised")


class TestTheObject:
    def test_methods_of_throwable(self, client):
        e = thrown_by(client.jvm.Thrower.fail, "boom", 7)

        thrown = e.java_exception
        assert isinstance(thrown, JavaObject)
        assert thrown.getMessage() == "boom"
        assert thrown.toString() == "Thrower$CodedException: boom"

        cause = thrown.getCause()
        assert isinstance(cause, JavaObject)
        assert cause.getMessage() == "the cause"
        assert client.is_instance_of(cause, "java.lang.IllegalStateException")
        assert cause.getCause() is None

    def test_stack_trace_elements(self, client):
        e = thrown_by(client.jvm.Thrower.fail, "boom", 7)

        trace = e.java_exception.getStackTrace()
        assert len(trace) > 1
        assert trace[0].getClassName() == "Thrower"
        assert trace[0].getMethodName() == "fail"
        assert trace[0].getLineNumber() > 0

    def test_methods_of_its_own_class(self, client):
        e = thrown_by(client.jvm.Thrower.fail, "boom", 7)

        assert e.java_exception.getCode() == 7
        assert e.java_exception.describe() == "coded 7"

    def test_is_instance_of(self, client):
        e = thrown_by(client.jvm.Thrower.fail, "boom", 7)

        thrown = e.java_exception
        assert client.is_instance_of(thrown, "Thrower.CodedException") is True
        assert client.is_instance_of(thrown, "java.lang.Exception") is True
        assert client.is_instance_of(thrown, "java.lang.Throwable") is True
        assert client.is_instance_of(thrown, "java.lang.RuntimeException") is False

    def test_what_was_there_before_is_unchanged(self, client):
        e = thrown_by(client.jvm.Thrower.fail, "boom", 7)

        assert sorted(vars(e)) == [
            "java_class",
            "java_exception",
            "message",
            "stack_trace",
        ]
        assert e.java_class == "Thrower$CodedException"
        assert e.message == "boom"
        assert e.stack_trace.startswith("Thrower$CodedException: boom\n")
        assert "Caused by: java.lang.IllegalStateException: the cause" in e.stack_trace
        assert str(e) == e.stack_trace

    def test_it_is_an_argument_like_any_object(self, client):
        e = thrown_by(client.jvm.Thrower.fail, "boom", 7)

        assert client.jvm.Thrower.messageOf(e.java_exception) == "boom"
        again = thrown_by(client.jvm.Thrower.rethrow, e.java_exception)
        assert again.java_class == "Thrower$CodedException"
        # The same Java object, under an ID of its own
        assert again.java_exception.object_id != e.java_exception.object_id
        assert client.jvm.Thrower.same(again.java_exception, e.java_exception)

    def test_an_exception_made_in_python_has_none(self):
        e = JavaException("java.lang.RuntimeException", "made here")
        assert e.java_exception is None
        assert str(e) == "java.lang.RuntimeException: made here"


class TestEveryKindOfCall:
    def test_constructor(self, client):
        e = thrown_by(client.create_object, "Thrower", "bad start")
        assert isinstance(e, JavaIllegalArgumentException)
        assert e.java_exception.getMessage() == "bad start"

    def test_instance_method(self, client):
        thrower = client.create_object("Thrower")
        e = thrown_by(thrower.failHere, "not here")
        assert e.java_class == "java.lang.UnsupportedOperationException"
        assert e.java_exception.getMessage() == "not here"
        assert e.java_exception.getStackTrace()[0].getMethodName() == "failHere"

    def test_a_jdk_method(self, client):
        e = thrown_by(client.jvm.java.lang.Integer.parseInt, "x")
        assert isinstance(e, JavaNumberFormatException)
        assert e.java_exception.getMessage() == 'For input string: "x"'

    def test_errors_the_server_raises_itself(self, client):
        thrower = client.create_object("Thrower")

        e = thrown_by(client.get_field, thrower, "missing")
        assert isinstance(e, JavaNoSuchFieldException)
        assert client.is_instance_of(e.java_exception, "java.lang.NoSuchFieldException")

        e = thrown_by(thrower.noSuchMethod)
        assert isinstance(e, JavaNoSuchMethodException)
        assert "noSuchMethod" in e.java_exception.getMessage()

        e = thrown_by(client.create_object, "java.lang.ProcessBuilder", ["ls"])
        assert isinstance(e, JavaSecurityException)
        assert "not allowed" in e.java_exception.getMessage()

    def test_set_field_and_static_field(self, client):
        thrower = client.create_object("Thrower")
        e = thrown_by(client.set_field, thrower, "value", "text")
        assert e.java_exception is not None
        e = thrown_by(client.get_static_field, "Thrower", "missing")
        assert isinstance(e, JavaNoSuchFieldException)
        assert e.java_exception is not None

    def test_vectorized_calls(self, client):
        thrower = client.create_object("Thrower")

        e = thrown_by(
            client.invoke_methods, thrower, [("ok", ()), ("failHere", ("second",))]
        )
        assert e.java_exception.getMessage() == "second"

        e = thrown_by(
            client.create_objects, [("Thrower", ()), ("Thrower", ("bad start",))]
        )
        assert e.java_exception.getMessage() == "bad start"

        e = thrown_by(client.get_fields, thrower, ["value", "missing"])
        assert client.is_instance_of(e.java_exception, "java.lang.NoSuchFieldException")

    def test_batch(self, client):
        thrower = client.create_object("Thrower")
        with client.batch() as b:
            first = b.call_static("Thrower", "fail", "first", 1)
            fine = b.call(thrower, "ok")
            second = b.call(thrower, "failHere", "second")
            third = b.create("Thrower", "third")

        assert fine.get() == "ok"
        assert first.is_error and second.is_error and third.is_error
        e = thrown_by(first.get)
        assert e.java_exception.getCode() == 1
        assert e.java_exception.getCause().getMessage() == "the cause"
        assert thrown_by(second.get).java_exception.getMessage() == "second"
        assert thrown_by(third.get).java_exception.getMessage() == "third"

    def test_batch_gives_the_same_exception_as_a_single_call(self, client):
        alone = thrown_by(client.jvm.Thrower.fail, "boom", 7)
        with client.batch() as b:
            failed = b.call_static("Thrower", "fail", "boom", 7)
        in_batch = thrown_by(failed.get)

        assert type(in_batch) is type(alone)
        assert in_batch.java_class == alone.java_class
        assert in_batch.message == alone.message == "boom"
        assert in_batch.stack_trace.splitlines()[0] == alone.stack_trace.splitlines()[0]

    def test_batch_that_stops(self, client):
        with client.batch(stop_on_error=True) as b:
            b.call_static("java.lang.Integer", "parseInt", "1")
            failed = b.call_static("Thrower", "fail", "stop", 2)
            b.call_static("java.lang.Integer", "parseInt", "3")

        assert thrown_by(failed.get).java_exception.getCode() == 2

    def test_batch_built_with_the_builder(self, client):
        """A list among the arguments takes the batch off the hand-packed path."""
        with client.batch() as b:
            b.call_static("java.util.Objects", "hashCode", [1, 2])
            failed = b.call_static("Thrower", "fail", "built", 5)

        assert thrown_by(failed.get).java_exception.getCode() == 5

    def test_a_python_callback_that_raises(self, client):
        def refuse(value):
            raise ValueError(f"no {value}")

        function = client.register_callback(refuse, "java.util.function.Function")
        try:
            e = thrown_by(client.jvm.Thrower.here, function, "thanks")
        finally:
            client.unregister_callback(function)
        assert "no thanks" in e.java_exception.getMessage()


class TestRelease:
    def test_released_with_the_python_exception(self, client):
        gc.collect()
        before = live_objects(client)

        e = thrown_by(client.jvm.Thrower.fail, "boom", 7)
        # The exception, and the cause that came with the question
        cause = e.java_exception.getCause()
        assert live_objects(client) == before + 2

        del cause
        assert live_objects(client) == before + 1
        del e
        assert live_objects(client) == before

    def test_the_object_outlives_the_exception_when_it_is_kept(self, client):
        e = thrown_by(client.jvm.Thrower.fail, "kept", 9)
        thrown = e.java_exception
        del e

        assert thrown.getCode() == 9

    def test_an_exception_nobody_looks_at(self, client):
        gc.collect()
        before = live_objects(client)

        for _ in range(50):
            with pytest.raises(JavaNumberFormatException):
                client.jvm.java.lang.Integer.parseInt("x")

        assert live_objects(client) == before

    def test_batch_errors_are_released_with_the_batch(self, client):
        gc.collect()
        before = live_objects(client)

        with client.batch() as b:
            failed = [b.call_static("Thrower", "fail", "x", n) for n in range(5)]
        assert all(r.is_error for r in failed)
        assert live_objects(client) == before + 5

        del b, failed
        gc.collect()
        assert live_objects(client) == before

    def test_an_error_raised_by_get_needs_no_collector(self, client):
        """As a single call's exception goes with the exception, a batch's goes
        with the batch: raising the stored one gave it a traceback that refers
        back to the batch, a cycle only the garbage collector broke."""
        gc.collect()
        before = live_objects(client)
        gc.disable()
        try:
            with client.batch() as b:
                failed = b.call_static("Thrower", "fail", "x", 1)
            for _ in range(3):
                try:
                    failed.get()
                except JavaException:
                    pass
            del b, failed
            assert live_objects(client) == before
        finally:
            gc.enable()

    def test_a_cancelled_request_keeps_nothing(self, thrower_classes):
        import threading

        from gatun import CancelledException

        client = connect(
            memory="16MB",
            classpath=[thrower_classes],
            allowed_classes=[*ALLOWED, "java.lang.Thread"],
        )
        try:
            before = live_objects(client)
            threading.Timer(0.3, client.cancel).start()
            with pytest.raises(CancelledException):
                client.jvm.java.lang.Thread.sleep(30_000)
            assert live_objects(client) == before
        finally:
            client.close()


class TestObjectLimit:
    @pytest.fixture
    def small(self, thrower_classes):
        client = connect(
            memory="16MB",
            classpath=[thrower_classes],
            allowed_classes=ALLOWED,
            extra_jvm_flags=["-Dgatun.limits.max_objects=50"],
        )
        yield client
        client.close()

    def test_ten_thousand_dropped_exceptions(self, small):
        """Each one is released when it is dropped, with the next call. No
        garbage collection is needed for that, so none is allowed here."""
        parse = small.jvm.java.lang.Integer.parseInt
        fail = small.jvm.Thrower.fail
        gc.collect()
        gc.disable()
        try:
            for n in range(10_000):
                try:
                    if n % 2:
                        parse("x")
                    else:
                        fail("boom", n)
                except JavaException as e:
                    # At the limit an exception comes without its object
                    assert e.java_exception is not None, n
        finally:
            gc.enable()

        assert live_objects(small) == 0
        # The session can still hold as many objects as before
        held = [small.create_object("java.util.ArrayList") for _ in range(50)]
        assert len(held) == 50

    def test_at_the_limit_the_error_is_still_the_error(self, small):
        held = [small.create_object("java.util.ArrayList") for _ in range(50)]

        e = thrown_by(small.jvm.Thrower.fail, "boom", 7)
        assert e.java_class == "Thrower$CodedException"
        assert e.message == "boom"
        assert e.java_exception is None

        del held[0]
        e = thrown_by(small.jvm.Thrower.fail, "boom", 8)
        assert e.java_exception.getCode() == 8

    def test_a_batch_at_the_limit(self, small):
        held = [small.create_object("java.util.ArrayList") for _ in range(48)]
        with small.batch() as b:
            failed = [b.call_static("Thrower", "fail", "x", n) for n in range(4)]

        errors = [thrown_by(r.get) for r in failed]
        assert [e.java_class for e in errors] == ["Thrower$CodedException"] * 4
        assert [e.java_exception is not None for e in errors] == [
            True,
            True,
            False,
            False,
        ]
        assert len(held) == 48


class TestCallbackConnection:
    def test_a_call_that_throws_inside_a_callback_from_another_thread(self, client):
        seen = []

        def inside(value):
            # This call travels on the callback connection
            try:
                client.jvm.Thrower.fail(value, 11)
            except JavaException as e:
                seen.append(e.java_class)
                thrown = e.java_exception
                return [thrown.getCode(), thrown.getCause().getMessage()]
            return None

        function = client.register_callback(inside, "java.util.function.Function")
        try:
            result = client.jvm.Thrower.onOtherThread(function, "inner")
        finally:
            client.unregister_callback(function)

        assert result == [11, "the cause"]
        assert seen == ["Thrower$CodedException"]

    def test_a_batch_inside_such_a_callback(self, client):
        def inside(value):
            with client.batch() as b:
                failed = b.call_static("Thrower", "fail", value, 12)
            return thrown_by(failed.get).java_exception.getCode()

        function = client.register_callback(inside, "java.util.function.Function")
        try:
            assert client.jvm.Thrower.onOtherThread(function, "inner") == 12
        finally:
            client.unregister_callback(function)

    def test_those_objects_are_released_too(self, client):
        def inside(value):
            for _ in range(20):
                with pytest.raises(JavaException):
                    client.jvm.Thrower.fail(value, 13)
            return "done"

        gc.collect()
        before = live_objects(client)
        function = client.register_callback(inside, "java.util.function.Function")
        try:
            assert client.jvm.Thrower.onOtherThread(function, "inner") == "done"
        finally:
            client.unregister_callback(function)
        del function
        gc.collect()
        assert live_objects(client) == before


def error_response(builder, error_object):
    message = builder.CreateString("java.lang.RuntimeException: x")
    kind = builder.CreateString("java.lang.RuntimeException")
    Response.ResponseStart(builder)
    Response.ResponseAddIsError(builder, True)
    Response.ResponseAddErrorMsg(builder, message)
    Response.ResponseAddErrorType(builder, kind)
    if error_object:
        Response.ResponseAddErrorObject(builder, error_object)
    return Response.ResponseEnd(builder)


class _Queue:
    def __init__(self):
        self.freed = []

    def _queue_free(self, object_id):
        self.freed.append(object_id)


class TestResponsesNobodyReads:
    """A nested call that timed out: its late response names objects that only
    the client can release."""

    def test_an_error(self):
        builder = flatbuffers.Builder(128)
        builder.Finish(error_response(builder, 42))
        client = _Queue()

        _release_unread_response(client, bytes(builder.Output()))

        assert client.freed == [42]

    def test_an_error_without_an_object(self):
        builder = flatbuffers.Builder(128)
        builder.Finish(error_response(builder, 0))
        client = _Queue()

        _release_unread_response(client, bytes(builder.Output()))

        assert client.freed == []

    def test_errors_in_a_batch(self):
        builder = flatbuffers.Builder(256)
        errors = [error_response(builder, n) for n in (7, 0, 9)]
        BatchResponse.BatchResponseStartResponsesVector(builder, len(errors))
        for offset in reversed(errors):
            builder.PrependUOffsetTRelative(offset)
        responses = builder.EndVector()
        BatchResponse.BatchResponseStart(builder)
        BatchResponse.BatchResponseAddResponses(builder, responses)
        batch = BatchResponse.BatchResponseEnd(builder)
        Response.ResponseStart(builder)
        Response.ResponseAddBatch(builder, batch)
        builder.Finish(Response.ResponseEnd(builder))
        client = _Queue()

        _release_unread_response(client, bytes(builder.Output()))

        assert client.freed == [7, 9]


class TestAsyncClient:
    @pytest.fixture
    async def aclient(self, thrower_classes):
        client = await aconnect(
            memory="16MB",
            classpath=[thrower_classes],
            allowed_classes=ALLOWED,
            extra_jvm_flags=["-Dgatun.limits.max_objects=50"],
        )
        yield client
        await client.close()

    async def test_the_object(self, aclient):
        with pytest.raises(JavaException) as raised:
            await aclient.invoke_static_method("Thrower", "fail", "boom", 7)

        thrown = raised.value.java_exception
        assert isinstance(thrown, AsyncJavaObject)
        assert await thrown.getMessage() == "boom"
        assert await thrown.getCode() == 7
        cause = await thrown.getCause()
        assert await cause.getMessage() == "the cause"
        trace = await thrown.getStackTrace()
        assert await aclient.is_instance_of(thrown, "Thrower.CodedException") is True
        assert isinstance(trace, AsyncJavaObject)
        assert str(raised.value) == raised.value.stack_trace

    async def test_every_kind_of_call(self, aclient):
        with pytest.raises(JavaIllegalArgumentException) as raised:
            await aclient.create_object("Thrower", "bad start")
        assert await raised.value.java_exception.getMessage() == "bad start"

        thrower = await aclient.create_object("Thrower")
        with pytest.raises(JavaException) as raised:
            await thrower.failHere("not here")
        assert await raised.value.java_exception.getMessage() == "not here"

        with pytest.raises(JavaNoSuchFieldException) as raised:
            await aclient.get_field(thrower, "missing")
        assert raised.value.java_exception is not None

        with pytest.raises(JavaException) as raised:
            await aclient.invoke_methods(thrower, [("failHere", ("vector",))])
        assert await raised.value.java_exception.getMessage() == "vector"

    async def test_ten_thousand_dropped_exceptions(self, aclient):
        gc.collect()
        gc.disable()
        try:
            for n in range(10_000):
                try:
                    await aclient.invoke_static_method("Thrower", "fail", "boom", n)
                except JavaException as e:
                    assert e.java_exception is not None, n
        finally:
            gc.enable()

        held = [await aclient.create_object("java.util.ArrayList") for _ in range(50)]
        assert len(held) == 50

    async def test_inside_a_callback_from_another_thread(self, aclient):
        async def inside(value):
            try:
                await aclient.invoke_static_method("Thrower", "fail", value, 21)
            except JavaException as e:
                return await e.java_exception.getCode()
            return None

        function = await aclient.register_callback(
            inside, "java.util.function.Function"
        )
        result = await aclient.invoke_static_method(
            "Thrower", "onOtherThread", function, "inner"
        )
        assert result == 21
