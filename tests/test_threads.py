"""Several threads of one client.

Each thread that calls Java has a channel of its own in the client's session:
a socket, a shared memory file and a JVM thread. So a call does not wait for
another thread's call, every call of a Python thread runs on one JVM thread,
and the objects, the limits and the callbacks are the client's.
"""

import asyncio
import gc
import glob
import os
import socket
import subprocess
import threading
import time

import pyarrow as pa
import pytest

import gatun.client as client_module
from gatun import (
    CancelledException,
    DeadConnectionError,
    JavaException,
    ReentrancyError,
    SocketTimeoutError,
    StaleArenaError,
    aconnect,
    connect,
)

CHANNELS_JAVA = """
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

public class Channels {
    private static final AtomicInteger STARTED = new AtomicInteger();

    private static final ThreadLocal<String> LOCAL = new ThreadLocal<>();

    public static void set(String value) { LOCAL.set(value); }
    public static String get() { return LOCAL.get(); }

    public static long threadId() { return Thread.currentThread().threadId(); }
    public static String threadName() { return Thread.currentThread().getName(); }

    public static String sleep(long millis) throws InterruptedException {
        STARTED.incrementAndGet();
        Thread.sleep(millis);
        return "slept";
    }

    /** How many calls of sleep have begun; it only grows, whatever ends. */
    public static int sleepsStarted() { return STARTED.get(); }

    /** Live threads of the server whose name starts with a prefix. */
    public static int threads(String prefix) {
        int count = 0;
        for (Thread thread : Thread.getAllStackTraces().keySet()) {
            if (thread.getName().startsWith(prefix)) {
                count++;
            }
        }
        return count;
    }

    public static Object apply(Function<Object, Object> function, Object value) {
        return function.apply(value);
    }

    public static Object applyOnNewThread(Function<Object, Object> function, Object value)
            throws Exception {
        Object[] result = new Object[1];
        Throwable[] failure = new Throwable[1];
        Thread thread = new Thread(() -> {
            try {
                result[0] = function.apply(value);
            } catch (Throwable e) {
                failure[0] = e;
            }
        }, "started-by-java");
        thread.start();
        thread.join();
        if (failure[0] != null) {
            throw new RuntimeException(failure[0]);
        }
        return result[0];
    }

    public static Object sleepThenMake(long millis) throws InterruptedException {
        Thread.sleep(millis);
        return new java.util.ArrayList<String>();
    }

    public static void fail(String message) {
        throw new IllegalStateException(message);
    }
}
"""


@pytest.fixture(scope="module")
def channels_classes(tmp_path_factory):
    out = tmp_path_factory.mktemp("channels")
    java_home = os.environ.get("JAVA_HOME")
    javac = os.path.join(java_home, "bin", "javac") if java_home else "javac"
    source = out / "Channels.java"
    source.write_text(CHANNELS_JAVA)
    subprocess.run([javac, "--release", "22", "-d", str(out), str(source)], check=True)
    return str(out)


def start(channels_classes, **options):
    flags = options.pop("extra_jvm_flags", [])
    return connect(
        memory="16MB",
        classpath=[channels_classes],
        allowed_classes=["Channels"],
        extra_jvm_flags=flags,
        **options,
    )


@pytest.fixture(scope="module")
def shared(channels_classes):
    client = start(channels_classes)
    yield client
    client.close()


@pytest.fixture
def client(channels_classes):
    client = start(channels_classes)
    yield client
    client.close()


def on_thread(function, *args):
    """Run `function` on a new thread and return what it returned, or raise
    what it raised."""
    outcome = {}

    def run():
        try:
            outcome["value"] = function(*args)
        except BaseException as e:  # noqa: BLE001 - handed to the caller
            outcome["error"] = e

    thread = threading.Thread(target=run)
    thread.start()
    thread.join()
    if "error" in outcome:
        raise outcome["error"]
    return outcome["value"]


def shm_files(client):
    return sorted(glob.glob(f"{client.socket_path}.*.shm"))


def wait_until(condition, timeout=10.0):
    deadline = time.monotonic() + timeout
    while not condition():
        if time.monotonic() > deadline:
            return False
        time.sleep(0.02)
    return True


class TestCallsAtTheSameTime:
    def test_a_short_call_does_not_wait_for_a_long_one(self, shared):
        items = shared.jvm.java.util.ArrayList()
        items.add("x")
        Channels = shared.jvm.Channels
        result = []
        sleeper = threading.Thread(target=lambda: result.append(Channels.sleep(1500)))
        sleeper.start()
        time.sleep(0.3)

        # The fastest of a few: the claim is about waiting for the other
        # thread, which would cost each of them a second
        took = []
        for _ in range(5):
            started = time.perf_counter()
            assert items.size() == 1
            took.append(time.perf_counter() - started)

        assert sleeper.is_alive()
        assert min(took) < 0.05
        sleeper.join()
        assert result == ["slept"]

    def test_long_calls_run_side_by_side(self, shared):
        Channels = shared.jvm.Channels
        threads = [
            threading.Thread(target=lambda: Channels.sleep(600)) for _ in range(8)
        ]
        started = time.perf_counter()
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        # One after the other they take 4.8 s
        assert time.perf_counter() - started < 2.4

    def test_many_threads_calling_the_same_object(self, shared):
        """One Java object, called by twelve threads with nothing in Python
        between them: each thread's own text comes out whole, and so does
        the count of what all of them appended to the one buffer."""
        everyones = shared.jvm.java.lang.StringBuffer()  # synchronized in Java
        seen = []
        ready = threading.Barrier(12)

        def work(n):
            mine = shared.jvm.java.lang.StringBuilder()
            ready.wait()
            for i in range(200):
                mine.append(f"{n}:{i};")
                everyones.append("x")
            seen.append((n, mine.toString()))

        threads = [threading.Thread(target=work, args=(n,)) for n in range(12)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        assert everyones.length() == 12 * 200
        assert len(seen) == 12
        for n, text in seen:
            assert text == "".join(f"{n}:{i};" for i in range(200))


class TestOneSession:
    def test_an_object_made_on_one_thread_is_used_on_another(self, shared):
        made = on_thread(lambda: shared.jvm.java.util.ArrayList())
        made.add("from the main thread")

        assert on_thread(made.size) == 1
        assert on_thread(lambda: made.get(0)) == "from the main thread"
        assert on_thread(shared.is_instance_of, made, "java.util.List") is True

    def test_an_object_is_released_by_whichever_thread_calls_next(self, shared):
        made = on_thread(lambda: shared.create_object("java.util.ArrayList"))
        object_id = made.object_id
        del made

        # The release rides with this thread's next command
        shared.jvm.java.lang.Math.abs(-1)

        with pytest.raises(JavaException, match="not found"):
            shared.invoke_method(object_id, "size")

    def test_the_object_limit_counts_every_thread(self, channels_classes):
        client = start(
            channels_classes, extra_jvm_flags=["-Dgatun.limits.max_objects=10"]
        )
        try:
            held = [client.create_object("java.util.ArrayList") for _ in range(5)]
            held += on_thread(
                lambda: [client.create_object("java.util.ArrayList") for _ in range(5)]
            )

            with pytest.raises(JavaException, match="max_objects|limit"):
                client.create_object("java.util.ArrayList")
            with pytest.raises(JavaException, match="max_objects|limit"):
                on_thread(client.create_object, "java.util.ArrayList")

            del held[:5]
            assert on_thread(client.create_object, "java.util.ArrayList") is not None
        finally:
            client.close()

    def test_the_object_limit_holds_for_threads_that_create_at_once(
        self, channels_classes
    ):
        client = start(
            channels_classes, extra_jvm_flags=["-Dgatun.limits.max_objects=30"]
        )
        try:
            held = []
            refused = []
            ready = threading.Barrier(8)

            def create():
                ready.wait()
                for _ in range(10):
                    try:
                        held.append(client.create_object("java.util.ArrayList"))
                    except JavaException:
                        refused.append(1)

            threads = [threading.Thread(target=create) for _ in range(8)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()

            assert len(held) == 30
            assert len(refused) == 50
        finally:
            client.close()

    def test_an_exception_raised_on_one_thread_is_examined_on_another(self, shared):
        def fail():
            try:
                shared.jvm.Channels.fail("on a thread")
            except JavaException as e:
                return e

        error = on_thread(fail)

        assert error.java_exception.getMessage() == "on a thread"
        assert shared.is_instance_of(
            error.java_exception, "java.lang.IllegalStateException"
        )

    def test_a_batch_on_another_thread(self, shared):
        items = shared.jvm.java.util.ArrayList()

        def fill():
            with shared.batch() as b:
                for i in range(50):
                    b.call(items, "add", i)
                size = b.call(items, "size")
            return size.get()

        assert on_thread(fill) == 50
        assert items.size() == 50

    def test_channels_take_no_session_slot(self, channels_classes):
        client = start(
            channels_classes, extra_jvm_flags=["-Dgatun.limits.max_sessions=1"]
        )
        try:
            names = {on_thread(client.jvm.Channels.threadName) for _ in range(3)}
            assert len(names) == 3
        finally:
            client.close()


class TestOneJvmThreadForEachPythonThread:
    def test_every_call_of_a_thread_runs_on_one_jvm_thread(self, shared):
        Channels = shared.jvm.Channels

        def ids():
            return [Channels.threadId() for _ in range(20)], Channels.threadName()

        mine, my_name = ids()
        theirs, their_name = on_thread(ids)
        others, other_name = on_thread(ids)

        assert len(set(mine)) == len(set(theirs)) == len(set(others)) == 1
        assert len({mine[0], theirs[0], others[0]}) == 3
        assert my_name.startswith("gatun-session-")
        assert their_name.startswith("gatun-channel-")
        assert other_name.startswith("gatun-channel-")
        assert their_name != other_name

    def test_a_java_thread_local_stays_with_its_python_thread(self, shared):
        Channels = shared.jvm.Channels
        Channels.set("main")
        ready = threading.Barrier(2)
        seen = {}

        def work(name):
            before = Channels.get()
            Channels.set(name)
            ready.wait()
            # Other threads have set theirs by now
            seen[name] = (before, [Channels.get() for _ in range(10)])

        threads = [threading.Thread(target=work, args=(n,)) for n in ("a", "b")]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        assert seen["a"] == (None, ["a"] * 10)
        assert seen["b"] == (None, ["b"] * 10)
        assert Channels.get() == "main"

    def test_a_thread_pool_keeps_its_jvm_threads(self, shared):
        from concurrent.futures import ThreadPoolExecutor

        Channels = shared.jvm.Channels

        def who(_):
            return threading.get_ident(), Channels.threadId()

        with ThreadPoolExecutor(max_workers=4) as pool:
            pairs = list(pool.map(who, range(200)))

        by_python_thread = {}
        for python_thread, jvm_thread in pairs:
            by_python_thread.setdefault(python_thread, set()).add(jvm_thread)
        assert all(len(jvm) == 1 for jvm in by_python_thread.values())
        jvm_threads = [next(iter(jvm)) for jvm in by_python_thread.values()]
        assert len(set(jvm_threads)) == len(jvm_threads)


class TestWhenAThreadEnds:
    def test_a_thousand_short_lived_threads_leave_nothing(self, client):
        Channels = client.jvm.Channels
        results = []

        def work(n):
            results.append(Channels.threadId() if n % 2 else Channels.threadName())

        for start_at in range(0, 1000, 50):
            threads = [
                threading.Thread(target=work, args=(n,))
                for n in range(start_at, start_at + 50)
            ]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()

        assert len(results) == 1000
        # Each had a JVM thread of its own
        assert len(set(results)) == 1000
        # The server notices each closed connection a moment later
        assert wait_until(lambda: Channels.threads("gatun-channel-") == 0)
        assert wait_until(lambda: shm_files(client) == [client.memory_path])
        assert not client._other_channels()

    def test_its_objects_stay(self, client):
        made = on_thread(lambda: client.jvm.java.lang.StringBuilder("kept"))
        assert wait_until(lambda: len(shm_files(client)) == 1)

        assert made.toString() == "kept"

    def test_the_first_channel_outlives_the_thread_that_connected(
        self, channels_classes
    ):
        client = on_thread(start, channels_classes)
        try:
            # This thread joins the session the other one made
            assert client.jvm.Channels.threadName().startswith("gatun-channel-")
            assert len(shm_files(client)) == 2
            assert on_thread(client.jvm.java.lang.Integer.parseInt, "7") == 7
        finally:
            client.close()
        assert wait_until(lambda: shm_files(client) == [])


class TestAChannelThatIsLost:
    def test_a_timeout_ends_the_threads_channel_and_not_the_client(
        self, channels_classes
    ):
        client = start(channels_classes, socket_timeout=1.5)
        try:
            Channels = client.jvm.Channels
            items = client.jvm.java.util.ArrayList()

            def work():
                before = Channels.threadId()
                with pytest.raises(SocketTimeoutError):
                    Channels.sleep(5000)
                # The thread goes on, with a new channel and a new JVM thread
                items.add("after")
                return before, Channels.threadId()

            before, after = on_thread(work)

            assert before != after
            assert items.size() == 1
            assert client.ping() is True
        finally:
            client.close()

    def test_what_the_lost_request_returns_is_released(self, channels_classes):
        client = start(
            channels_classes,
            socket_timeout=1.0,
            extra_jvm_flags=["-Dgatun.limits.max_objects=5"],
        )
        try:

            def work():
                for _ in range(6):
                    # Java returns a new object a moment later, to nobody
                    with pytest.raises(SocketTimeoutError):
                        client.jvm.Channels.sleepThenMake(1500)

            on_thread(work)
            assert wait_until(
                lambda: client.jvm.Channels.threads("gatun-channel-") == 0
            )

            # Six objects nobody can release would be past the limit of five
            kept = [client.create_object("java.util.ArrayList") for _ in range(5)]
            assert len(kept) == 5
        finally:
            client.close()

    def test_close_reaches_a_thread_inside_a_call(self, channels_classes):
        client = start(channels_classes)
        errors = []

        def work():
            try:
                client.jvm.Channels.sleep(10_000)
            except BaseException as e:  # noqa: BLE001
                errors.append(e)

        thread = threading.Thread(target=work)
        thread.start()
        time.sleep(0.4)
        client.close()
        thread.join(timeout=5)

        assert not thread.is_alive()
        assert len(errors) == 1
        assert isinstance(errors[0], DeadConnectionError)

    def test_after_close_no_thread_can_call(self, channels_classes):
        client = start(channels_classes)
        items = client.jvm.java.util.ArrayList()
        on_thread(items.size)
        client.close()

        with pytest.raises(DeadConnectionError):
            items.size()
        with pytest.raises(DeadConnectionError):
            on_thread(items.size)

    def test_a_hundred_threads_make_their_first_call_at_once(self, client):
        """Each is a connection to the join socket at the same moment."""
        Channels = client.jvm.Channels
        Channels.threadId()
        ready = threading.Barrier(100)
        ids = []

        def first_call():
            ready.wait()
            ids.append(Channels.threadId())

        threads = [threading.Thread(target=first_call) for _ in range(100)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        assert len(set(ids)) == 100
        assert client._shared_channel is False

    def test_a_refused_join_is_tried_again(self, channels_classes, monkeypatch, caplog):
        """macOS refuses a connection when the listening socket's queue is
        full. That is no server without a join socket: one refusal must not
        put the client's threads on one channel for good."""
        client = start(channels_classes)
        refusals = [1, 1, 1]

        class Refusing(socket.socket):
            def connect(self, address):
                if str(address).endswith(".join") and refusals:
                    refusals.pop()
                    raise ConnectionRefusedError(61, "Connection refused")
                return super().connect(address)

        monkeypatch.setattr(client_module.socket, "socket", Refusing)
        try:
            Channels = client.jvm.Channels
            with caplog.at_level("WARNING", logger="gatun.client"):
                name = on_thread(Channels.threadName)

            assert refusals == []
            assert name.startswith("gatun-channel-")
            assert name != Channels.threadName()
            assert client._shared_channel is False
            assert caplog.text == ""
        finally:
            client.close()

    def test_a_join_that_is_always_refused_fails_the_call(
        self, channels_classes, monkeypatch
    ):
        client = start(channels_classes)

        class Refusing(socket.socket):
            def connect(self, address):
                if str(address).endswith(".join"):
                    raise ConnectionRefusedError(61, "Connection refused")
                return super().connect(address)

        monkeypatch.setattr(client_module.socket, "socket", Refusing)
        monkeypatch.setattr(client_module, "_JOIN_RETRY_SECONDS", 0.3)
        try:
            with pytest.raises(DeadConnectionError, match="Could not open a channel"):
                on_thread(client.ping)

            assert client._shared_channel is False
            assert client.ping() is True
        finally:
            client.close()

    def test_close_between_a_command_and_its_answer(self, channels_classes):
        """The thread finds its channel's socket gone when it turns to read."""
        for where in (lambda call: call(), on_thread):
            client = start(channels_classes)
            real = client._write_command

            def write_then_close(data, channel=None, real=real, client=client):
                real(data, channel)
                on_thread(client.close)

            client._write_command = write_then_close
            with pytest.raises(DeadConnectionError, match="closed while"):
                where(client.ping)

    def test_borrowing_a_channel_that_has_ended(self, client):
        """What a thread does on another thread's channel (ending a loan) must
        not cost it its own when that channel is gone, or goes meanwhile."""
        Channels = client.jvm.Channels
        release = threading.Event()
        theirs = []

        def other():
            Channels.threadId()
            theirs.append(client._channel())
            release.wait(30)

        thread = threading.Thread(target=other)
        thread.start()
        assert wait_until(lambda: theirs)

        def borrower():
            jvm_thread = Channels.threadId()
            mine = client._channel()
            with client._borrowing(theirs[0], wait=False) as held:
                assert held is True
                theirs[0].close()
                # The calling thread's channel is the borrowed one in here,
                # ended or not: no new one is opened in its place
                assert client._channel() is theirs[0]
                assert len(client._loan_ends) == 0
            assert client._channel() is mine
            assert not mine.dead
            with client._borrowing(theirs[0]) as held:
                assert held is False
                assert client._channel() is mine
            return jvm_thread == Channels.threadId()

        try:
            assert on_thread(borrower) is True
        finally:
            release.set()
            thread.join()

    def test_without_a_join_socket_threads_take_turns(self, channels_classes, caplog):
        client = start(channels_classes)
        try:
            os.unlink(f"{client.socket_path}.join")
            Channels = client.jvm.Channels

            with caplog.at_level("WARNING", logger="gatun.client"):
                names = {on_thread(Channels.threadName) for _ in range(3)}

            assert names == {Channels.threadName()}
            assert caplog.text.count("no join socket") == 1
        finally:
            client.close()


TABLE = pa.table({"n": list(range(20_000)), "s": [str(i) for i in range(20_000)]})


class TestArrow:
    def test_two_threads_move_tables_at_the_same_time(self, shared):
        barrier = threading.Barrier(2)

        def work(seed):
            table = pa.table({"n": [seed * 1_000_000 + i for i in range(50_000)]})
            barrier.wait()
            for _ in range(20):
                root = shared.arrow_to_java(table)
                assert root.getRowCount() == 50_000
                assert shared.arrow_from_java(root).equals(table)
                with shared.arrow_to_java(table, copy=False) as lent:
                    assert shared.arrow_from_java(lent).equals(table)
                view = shared.arrow_from_java(root, copy=False)
                assert view.table.equals(table)
                shared.free_object(root.object_id)
                root.detach()
            return True

        threads_done = []
        threads = [
            threading.Thread(target=lambda s=s: threads_done.append(work(s)))
            for s in (1, 2)
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        assert threads_done == [True, True]

    def test_a_view_is_read_from_another_thread(self, shared):
        root = shared.arrow_to_java(TABLE)
        view = shared.arrow_from_java(root, copy=False)

        assert on_thread(view.to_pydict) == TABLE.to_pydict()
        # A transfer on the other thread is in that thread's memory
        on_thread(lambda: shared.arrow_from_java(shared.arrow_to_java(TABLE)))
        assert view.table.equals(TABLE)

        # One on this thread ends it, as always
        shared.arrow_from_java(root)
        with pytest.raises(StaleArenaError):
            on_thread(view.to_pydict)

    def test_a_view_ends_with_its_thread(self, shared):
        def make():
            root = shared.arrow_to_java(TABLE)
            return shared.arrow_from_java(root, copy=False)

        view = on_thread(make)

        with pytest.raises(StaleArenaError):
            view.to_pydict()

    def test_a_loan_is_ended_from_another_thread(self, shared):
        loan = shared.arrow_to_java(TABLE, copy=False)
        root = loan.root
        assert on_thread(root.getRowCount) == 20_000
        assert on_thread(lambda: shared.arrow_from_java(root).equals(TABLE))

        on_thread(loan.close)

        assert loan.closed
        # The channel was free, so the server has been told: the room is back
        assert shared._loans == []
        assert shared.arrow_from_java(shared.arrow_to_java(TABLE)).equals(TABLE)

    def test_a_loan_ends_with_its_thread(self, shared):
        def lend():
            loan = shared.arrow_to_java(TABLE, copy=False)
            assert loan.root.getVector("n").getValueCount() == 20_000
            return loan

        loan = on_thread(lend)

        # The memory it lay in went with the thread's channel, so the server
        # ended the loan: the table is closed, as after reset_payload_arena()
        assert wait_until(lambda: loan.root.getVector("n").getValueCount() == 0)
        with pytest.raises(JavaException, match="loan has ended"):
            shared.arrow_from_java(loan.root)
        loan.close()
        assert loan.closed

    def test_a_loan_of_a_thread_that_ended_gives_its_object_back(
        self, channels_classes
    ):
        """The server ends such a loan with the thread's channel and keeps
        the table's object. Nothing but the client can release it."""
        client = start(
            channels_classes, extra_jvm_flags=["-Dgatun.limits.max_objects=12"]
        )
        small = pa.table({"n": [1, 2, 3]})
        try:

            def lend():
                return client.arrow_to_java(small, copy=False)

            def lend_and_drop():
                client.arrow_to_java(small, copy=False)
                # Its end is queued on this thread's channel, which ends first
                gc.collect()

            for round_ in range(40):
                if round_ % 3 == 0:
                    on_thread(lend).close()
                elif round_ % 3 == 1:
                    on_thread(lend)  # dropped here, after its thread
                    gc.collect()
                else:
                    on_thread(lend_and_drop)
                assert client.ping() is True

            held = [client.create_object("java.util.ArrayList") for _ in range(10)]
            assert len(held) == 10
        finally:
            client.close()

    def test_a_loan_of_a_thread_whose_call_timed_out(self, channels_classes):
        client = start(
            channels_classes, extra_jvm_flags=["-Dgatun.limits.max_objects=6"]
        )
        small = pa.table({"n": [1, 2, 3]})
        try:

            def lend_and_time_out():
                loan = client.arrow_to_java(small, copy=False)
                client.socket_timeout = 0.3
                try:
                    client.jvm.Channels.sleep(1500)
                except SocketTimeoutError:
                    pass
                finally:
                    client.socket_timeout = 30.0
                loan.close()

            for _ in range(10):
                on_thread(lend_and_time_out)
                assert client.ping() is True
        finally:
            client.close()

    def test_an_arena_of_another_thread_is_refused(self, shared):
        arena = on_thread(shared.get_payload_arena)

        with pytest.raises(ValueError, match="on this thread"):
            shared.send_arrow_buffers(TABLE, arena, {})

        mine = shared.get_payload_arena()
        shared.send_arrow_buffers(TABLE, mine, {})
        assert shared.get_arrow_data().table.equals(TABLE)
        shared.reset_payload_arena()

    def test_buffers_sent_and_read_back_on_two_threads(self, shared):
        def work(rows):
            table = pa.table({"n": list(range(rows))})
            with shared.arrow_context() as ctx:
                for _ in range(5):
                    ctx.send(table)
                    assert ctx.receive().equals(table)
            return rows

        assert on_thread(work, 1000) == 1000
        assert work(3000) == 3000


class TestCallbacks:
    def test_registered_on_one_thread_called_in_band_by_another(self, shared):
        calls = []

        def function(value):
            calls.append(threading.current_thread())
            try:
                shared.jvm.java.lang.Math.abs(-1)
            except ReentrancyError:
                # Only a callback made by the request's own thread is refused
                return f"in band: {value}"
            return f"on the callback connection: {value}"

        proxy = shared.register_callback(function, "java.util.function.Function")

        assert shared.jvm.Channels.apply(proxy, "main") == "in band: main"
        assert on_thread(shared.jvm.Channels.apply, proxy, "other") == "in band: other"

    def test_registered_on_another_thread_called_in_band_by_this_one(self, shared):
        proxy = on_thread(
            shared.register_callback,
            lambda value: value.upper(),
            "java.util.function.Function",
        )

        assert shared.jvm.Channels.apply(proxy, "abc") == "ABC"

    def test_called_from_a_thread_java_started(self, shared):
        items = shared.jvm.java.util.ArrayList()

        def function(value):
            # Not the request's thread, so it may call Java
            items.add(value)
            return shared.jvm.Channels.threadName()

        proxy = shared.register_callback(function, "java.util.function.Function")

        assert shared.jvm.Channels.applyOnNewThread(proxy, "x") == "started-by-java"
        assert (
            on_thread(shared.jvm.Channels.applyOnNewThread, proxy, "y")
            == "started-by-java"
        )
        assert list(items) == ["x", "y"]

    def test_callbacks_in_band_on_two_threads_at_once(self, shared):
        barrier = threading.Barrier(2, timeout=10)

        def function(value):
            # Both are inside their callback at the same moment
            barrier.wait()
            return value * 2

        proxy = shared.register_callback(function, "java.util.function.Function")
        results = []
        threads = [
            threading.Thread(
                target=lambda n=n: results.append(shared.jvm.Channels.apply(proxy, n))
            )
            for n in (1, 2)
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        assert sorted(results) == [2, 4]

    def test_releasing_inside_a_callback_opens_no_channel(self, client, monkeypatch):
        """A callback's thread releases through the callback connection, or
        with the next request. It has no use for a channel of its own."""
        opened = []
        real = client._open_channel

        def recording():
            opened.append(threading.current_thread().name)
            return real()

        monkeypatch.setattr(client, "_open_channel", recording)
        spare = [client.create_object("java.util.ArrayList") for _ in range(2)]
        ids = [handle.object_id for handle in spare]
        for handle in spare:
            handle.detach()
        table = pa.table({"n": [1, 2, 3]})
        rows = []

        def in_band(value):
            client.free_object(ids[0])
            return value

        def on_the_connection(value):
            client.free_object(ids[1])
            # A copy that ends with the block, which close() frees
            with client.arrow_to_java(table, copy=False) as root:
                rows.append(root.getRowCount())
            return value

        Channels = client.jvm.Channels
        first = client.register_callback(in_band, "java.util.function.Function")
        second = client.register_callback(
            on_the_connection, "java.util.function.Function"
        )

        assert Channels.apply(first, "a") == "a"
        assert Channels.applyOnNewThread(second, "b") == "b"

        assert rows == [3]
        assert opened == []
        for object_id in ids:
            with pytest.raises(JavaException, match="not found"):
                client.invoke_method(object_id, "size")

    def test_an_arena_used_inside_a_callback_does_not_wait(self, channels_classes):
        """The request's thread holds its channel while it waits for the
        callback: an allocation that waited for the channel would wait for
        itself, until the callback timeout."""
        client = start(channels_classes, callback_timeout=20.0)
        try:
            arena = client.get_payload_arena()
            view_before = client._payload_writes
            allocated = []

            def allocate(value):
                allocated.append(arena.allocate(64).length)
                return value

            proxy = client.register_callback(allocate, "java.util.function.Function")
            Channels = client.jvm.Channels

            started = time.monotonic()
            assert Channels.apply(proxy, "in band") == "in band"
            assert Channels.applyOnNewThread(proxy, "other") == "other"
            assert time.monotonic() - started < 10

            assert allocated == [64, 64]
            # Counted on the channel the arena belongs to, whoever allocated
            assert client._payload_writes == view_before + 2
        finally:
            client.close()


class TestWhatIsSharedIsOpenedOnce:
    def test_the_first_callback_is_registered_on_a_thread_that_joined(self, client):
        proxy = on_thread(
            client.register_callback,
            lambda value: f"got {value}",
            "java.util.function.Function",
        )

        # The callback connection outlives the thread that opened it
        assert client.jvm.Channels.applyOnNewThread(proxy, "a") == "got a"
        assert client.jvm.Channels.apply(proxy, "b") == "got b"

    def test_threads_registering_at_once_open_one_callback_connection(self, client):
        barrier = threading.Barrier(6)
        proxies = []

        def register(n):
            barrier.wait()
            proxies.append(
                (
                    n,
                    client.register_callback(
                        lambda value, n=n: value + n, "java.util.function.Function"
                    ),
                )
            )

        threads = [threading.Thread(target=register, args=(n,)) for n in range(6)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        assert len(proxies) == 6
        for n, proxy in proxies:
            assert client.jvm.Channels.applyOnNewThread(proxy, 100) == 100 + n
        assert glob.glob(f"{client.socket_path}.*.cb") == []


class TestArrowLimit:
    def test_a_reset_gives_back_this_threads_bytes_only(self, channels_classes):
        table = pa.table({"n": list(range(100_000))})  # 800 KB
        client = start(
            channels_classes,
            extra_jvm_flags=["-Dgatun.limits.max_arrow_bytes=2000000"],
        )
        try:
            arena = client.get_payload_arena()
            client.send_arrow_buffers(table, arena, {})

            def other():
                mine = client.get_payload_arena()
                client.send_arrow_buffers(table, mine, {})
                # This thread's 800 KB come off the count; the first thread's stay
                client.reset_payload_arena()
                mine = client.get_payload_arena()
                client.send_arrow_buffers(table, mine, {})
                with pytest.raises(JavaException, match="(?i)arrow"):
                    # 800 KB here and 800 KB there are counted: 800 more is too much
                    client.arrow_to_java(table)

            on_thread(other)
        finally:
            client.close()

    def test_a_thread_that_ends_gives_its_bytes_back(self, channels_classes):
        table = pa.table({"n": list(range(100_000))})  # 800 KB
        client = start(
            channels_classes,
            extra_jvm_flags=["-Dgatun.limits.max_arrow_bytes=2000000"],
        )
        try:

            def send():
                arena = client.get_payload_arena()
                client.send_arrow_buffers(table, arena, {})

            # Ten threads' arenas would be 8 MB if they stayed counted
            for _ in range(10):
                on_thread(send)
                assert wait_until(lambda: len(shm_files(client)) == 1)

            root = client.arrow_to_java(table)
            assert root.getRowCount() == 100_000
        finally:
            client.close()


class TestCancel:
    def sleepers(self, client, *millis):
        """One thread per duration, each in a sleep of that many milliseconds
        in Java once this returns. A sleep that a test cancels must be long:
        the others may end while the last thread is still getting there."""
        outcomes = {}

        def work(n):
            try:
                outcomes[n] = client.jvm.Channels.sleep(millis[n])
            except BaseException as e:  # noqa: BLE001
                outcomes[n] = e

        before = client.jvm.Channels.sleepsStarted()
        threads = [threading.Thread(target=work, args=(n,)) for n in range(len(millis))]
        for thread in threads:
            thread.start()
        # Each thread's first call opens a channel of its own, which can take
        # longer than any fixed pause on a loaded machine
        wait_for_sleeps(client, before + len(millis), outcomes)
        return threads, outcomes

    def test_cancel_interrupts_every_request_in_flight(self, client):
        threads, outcomes = self.sleepers(client, 30_000, 30_000)

        assert client.cancel() is True
        for thread in threads:
            thread.join(timeout=10)

        assert [type(outcomes[n]) for n in range(2)] == [CancelledException] * 2
        assert client.ping() is True
        assert client.cancel() is False

    def test_cancel_one_thread(self, client):
        threads, outcomes = self.sleepers(client, 30_000, 2500)

        assert client.cancel(threads[0]) is True
        for thread in threads:
            thread.join(timeout=10)

        assert isinstance(outcomes[0], CancelledException)
        assert outcomes[1] == "slept"

    def test_cancel_a_thread_that_is_not_calling(self, client):
        idle = threading.Thread(target=lambda: None)
        idle.start()
        idle.join()
        threads, outcomes = self.sleepers(client, 30_000)

        assert client.cancel(idle) is False
        assert client.cancel(threading.current_thread()) is False
        assert client.cancel(threads[0]) is True
        threads[0].join(timeout=10)
        assert isinstance(outcomes[0], CancelledException)

    def test_a_cancelled_thread_keeps_its_jvm_thread(self, client):
        Channels = client.jvm.Channels

        def work():
            before = Channels.threadId()
            started = Channels.sleepsStarted()

            def cancel_once_asleep():
                wait_for_sleeps(client, started + 1, {})
                client.cancel()

            canceller = threading.Thread(target=cancel_once_asleep)
            canceller.start()
            with pytest.raises(CancelledException):
                Channels.sleep(30_000)
            canceller.join()
            return before, Channels.threadId()

        before, after = on_thread(work)
        assert before == after


def wait_for_sleeps(client, count, outcomes):
    """Wait until `count` calls of Channels.sleep have begun. Well inside the
    30 s that a sleep to be cancelled lasts, so that it is still running; a
    thread that failed on the way ends the wait with its error."""
    deadline = time.monotonic() + 20
    while client.jvm.Channels.sleepsStarted() < count:
        failed = [o for o in outcomes.values() if isinstance(o, BaseException)]
        assert not failed, failed
        assert time.monotonic() < deadline, outcomes
        time.sleep(0.01)


class TestIdleTimeout:
    def test_a_request_on_any_thread_keeps_the_session(self, channels_classes):
        client = start(
            channels_classes,
            extra_jvm_flags=["-Dgatun.limits.session_idle_timeout_ms=1000"],
        )
        try:
            # The first channel is idle for 3 s while another thread works
            assert on_thread(client.jvm.Channels.sleep, 3000) == "slept"
            assert client.ping() is True
        finally:
            client.close()

    def test_a_call_while_another_thread_is_inside_a_long_request(
        self, channels_classes
    ):
        """A running request records no activity until it ends. The calls
        other threads make meanwhile are not calls to an idle session."""
        client = start(
            channels_classes,
            extra_jvm_flags=["-Dgatun.limits.session_idle_timeout_ms=1000"],
        )
        try:
            Channels = client.jvm.Channels
            Channels.threadId()
            outcome = []
            worker = threading.Thread(
                target=lambda: outcome.append(Channels.sleep(4000))
            )
            worker.start()

            time.sleep(2.5)
            assert client.ping() is True
            assert on_thread(Channels.threadName).startswith("gatun-channel-")

            worker.join()
            assert outcome == ["slept"]
            assert client.ping() is True
        finally:
            client.close()

    def test_an_idle_session_ends_with_all_its_channels(self, channels_classes):
        client = start(
            channels_classes,
            extra_jvm_flags=["-Dgatun.limits.session_idle_timeout_ms=1000"],
        )
        try:
            ready, checked = threading.Event(), threading.Event()
            outcome = []

            def work():
                client.jvm.Channels.threadName()
                ready.set()
                checked.wait(timeout=10)
                try:
                    outcome.append(client.jvm.Channels.threadName())
                except BaseException as e:  # noqa: BLE001
                    outcome.append(e)

            thread = threading.Thread(target=work)
            thread.start()
            ready.wait(timeout=10)
            assert len(shm_files(client)) == 2

            # Nobody calls; the reaper closes the session and both channels
            assert wait_until(lambda: shm_files(client) == [], timeout=10)
            checked.set()
            thread.join()

            assert isinstance(outcome[0], Exception)
            with pytest.raises(Exception):  # noqa: B017 - any refusal will do
                client.ping()
        finally:
            client.close()


class TestAsyncClientIsUnchanged:
    async def test_two_calls_awaited_together_run_one_after_the_other(
        self, channels_classes
    ):
        client = await aconnect(
            memory="16MB", classpath=[channels_classes], allowed_classes=["Channels"]
        )
        try:
            started = time.perf_counter()
            names = await asyncio.gather(
                client.invoke_static_method("Channels", "sleep", 400),
                client.invoke_static_method("Channels", "sleep", 400),
                client.invoke_static_method("Channels", "threadName"),
                client.invoke_static_method("Channels", "threadName"),
            )
            took = time.perf_counter() - started

            # One channel: the two sleeps add up, and one JVM thread ran it all
            assert took >= 0.8
            assert names[2] == names[3]
            assert names[2].startswith("gatun-session-")
        finally:
            await client.close()
