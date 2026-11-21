"""Tests for the launcher: starting, supervising and stopping the server process.

Most of these run the launcher against a stand-in server (a short Python script
installed as $JAVA_HOME/bin/java) so that process handling can be tested
without a JVM, and with servers that misbehave in ways the real one does not.
"""

import os
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import pytest

from gatun import GatunClient, connect, launcher
from gatun.config import get_config, reset_config
from gatun.launcher import (
    MIN_MEMORY_BYTES,
    OUTPUT_TAIL_LINES,
    _drain_pipe,
    _parse_memory_size,
    launch_gateway,
)

KB = 1024
MB = 1024 * KB
GB = 1024 * MB

# The stand-in server. Like the real one it takes the socket path as its last
# argument, replaces whatever is at that path and accepts connections. It ends
# itself after two minutes, so that a test which fails to stop it (these tests
# exist because that can happen) does not leave it running for good.
STAND_IN_SERVER = """\
import os, signal, socket, sys, time

signal.alarm(120)
socket_path = sys.argv[-1]
{startup}
server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
if os.path.exists(socket_path):
    os.unlink(socket_path)
server.bind(socket_path)
server.listen()
while True:
    server.accept()[0].close()
"""

IGNORE_SIGTERM = "signal.signal(signal.SIGTERM, signal.SIG_IGN)"
NEVER_LISTEN = "time.sleep(600)"


@pytest.fixture
def socket_dir():
    """A short directory for socket paths.

    A Unix socket path is limited to about 104 bytes, which pytest's tmp_path
    exceeds on macOS.
    """
    path = tempfile.mkdtemp(prefix="gatun_launcher_", dir="/tmp")
    yield Path(path)
    shutil.rmtree(path, ignore_errors=True)


@pytest.fixture
def config():
    """The launcher's configuration, reloaded so a test may change it."""
    reset_config()
    yield get_config()
    reset_config()


@pytest.fixture
def stand_in_server(tmp_path, monkeypatch):
    """Make the launcher start a Python stand-in instead of a JVM.

    Call it with Python statements for the stand-in to run before it listens.
    """

    def install(startup: str = "") -> None:
        script = tmp_path / "server.py"
        script.write_text(STAND_IN_SERVER.format(startup=startup))
        java = tmp_path / "bin" / "java"
        java.parent.mkdir(exist_ok=True)
        java.write_text(f'#!/bin/sh\nexec "{sys.executable}" "{script}" "$@"\n')
        java.chmod(0o755)
        monkeypatch.setenv("JAVA_HOME", str(tmp_path))

    return install


@pytest.fixture
def launch():
    """launch_gateway(), with every server it started stopped after the test."""
    sessions = []

    def _launch(**kwargs):
        session = launch_gateway(**kwargs)
        sessions.append(session)
        return session

    yield _launch
    for session in sessions:
        session.stop()


@pytest.fixture
def started_processes(monkeypatch):
    """Every process the launcher starts, including those of failed launches."""
    processes = []

    class RecordingPopen(subprocess.Popen):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            processes.append(self)

    monkeypatch.setattr(launcher.subprocess, "Popen", RecordingPopen)
    yield processes
    for process in processes:
        if process.poll() is None:
            process.kill()
            process.wait()


def accepts_connections(path) -> bool:
    probe = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        probe.connect(str(path))
    except OSError:
        return False
    finally:
        probe.close()
    return True


class TestMemorySize:
    @pytest.mark.parametrize(
        ("memory", "expected"),
        [
            ("64MB", 64 * MB),
            ("512KB", 512 * KB),
            ("1GB", GB),
            ("64M", 64 * MB),
            ("512K", 512 * KB),
            ("1G", GB),
            ("64mb", 64 * MB),
            ("512kb", 512 * KB),
            ("1g", GB),
            ("1.5GB", GB + GB // 2),
            (" 16 MB ", 16 * MB),
            ("1048576", MB),
            (1048576, MB),
        ],
    )
    def test_accepted_forms(self, memory, expected):
        assert _parse_memory_size(memory) == expected

    @pytest.mark.parametrize(
        "memory",
        [
            "-1MB",
            "-1048576",
            "",
            "MB",
            "lots",
            "64TB",
            "64MBB",
            "1e3MB",
            "0x100000",
            "1048576.5",
            -1,
            0,
            True,
            1048576.0,
            None,
            b"64MB",
        ],
    )
    def test_anything_else_is_one_clear_error(self, memory):
        """Regression: these raised a bare "could not convert string to float",
        an AttributeError, or were passed on to the server."""
        with pytest.raises(ValueError) as exc_info:
            _parse_memory_size(memory)

        message = str(exc_info.value)
        assert f"Invalid memory size: {memory!r}" in message
        assert "'64MB'" in message
        assert "bytes" in message

    @pytest.mark.parametrize("memory", ["0MB", "64KB", "128KB", "0.1MB", 1, "4096"])
    def test_too_small_to_work(self, memory):
        """With less than a command zone and a response zone, the two overlap."""
        with pytest.raises(ValueError, match="too small") as exc_info:
            _parse_memory_size(memory)

        message = str(exc_info.value)
        assert repr(memory) in message
        assert f"{MIN_MEMORY_BYTES:,} bytes" in message

    def test_minimum_is_accepted(self):
        assert _parse_memory_size(MIN_MEMORY_BYTES) == MIN_MEMORY_BYTES
        assert _parse_memory_size(f"{MIN_MEMORY_BYTES // KB}KB") == MIN_MEMORY_BYTES

    @pytest.mark.parametrize("memory", ["-1MB", "0MB", "64 megabytes"])
    def test_bad_size_is_rejected_before_launch(self, memory, started_processes):
        with pytest.raises(ValueError, match="[Mm]emory size"):
            launch_gateway(memory=memory)
        assert started_processes == []

    def test_size_from_configuration_is_checked_too(self, config, monkeypatch):
        monkeypatch.setattr(config, "memory", "1 potato")
        with pytest.raises(ValueError, match="Invalid memory size: '1 potato'"):
            launch_gateway()

    def test_server_runs_at_the_minimum_size(self):
        """The minimum must be a size the real server works with, given as an
        int to cover that form end to end."""
        with connect(memory=MIN_MEMORY_BYTES) as client:
            assert client.memory_size == MIN_MEMORY_BYTES
            items = client.jvm.java.util.ArrayList()
            items.add("x" * 1000)
            assert items.get(0) == "x" * 1000


ARROW_AND_FRIENDS = [name.decode() for name in launcher._HOST_ARROW_CLASSES]


def make_jar(path, names, **zip_options):
    """A JAR with empty entries of these names."""
    import zipfile

    with zipfile.ZipFile(path, "w", **zip_options) as jar:
        jar.writestr("META-INF/MANIFEST.MF", "Manifest-Version: 1.0\n")
        for name in names:
            jar.writestr(name, b"")
    return str(path)


class TestServerJar:
    """Which JAR the server starts from: the fat one, or on a classpath that
    brings Arrow, the thin one that uses that Arrow."""

    def test_no_classpath(self):
        assert launcher._server_jar(None) == launcher.JAR_PATH
        assert launcher._server_jar([]) == launcher.JAR_PATH

    def test_both_jars_are_packaged(self):
        assert launcher.JAR_PATH.exists()
        assert launcher.THIN_JAR_PATH.exists()

    def test_the_thin_jar_is_the_servers_classes_alone(self):
        import zipfile

        with zipfile.ZipFile(launcher.THIN_JAR_PATH) as jar:
            names = [n for n in jar.namelist() if n.endswith(".class")]
        assert "org/gatun/server/GatunServer.class" in names
        assert all(name.startswith("org/gatun/") for name in names)

    def test_the_fat_jar_has_no_jackson_or_netty_under_their_own_names(self):
        import zipfile

        with zipfile.ZipFile(launcher.JAR_PATH) as jar:
            names = jar.namelist()
        assert "org/apache/arrow/vector/VectorSchemaRoot.class" in names
        assert not [n for n in names if n.startswith(("com/fasterxml/", "io/netty/"))]
        assert not [
            n
            for n in names
            if n.startswith("META-INF/versions/")
            and n.endswith(".class")
            and "/gatun/shaded/" not in n
        ]

    def test_classpath_with_arrow_in_several_jars(self, tmp_path):
        jars = [
            make_jar(tmp_path / f"lib{n}.jar", [name, f"other/Thing{n}.class"])
            for n, name in enumerate(ARROW_AND_FRIENDS)
        ]
        other = make_jar(tmp_path / "app.jar", ["com/example/App.class"])

        assert launcher._server_jar([other, *jars]) == launcher.THIN_JAR_PATH

    def test_classpath_with_arrow_in_one_jar_of_any_name(self, tmp_path):
        uber = make_jar(tmp_path / "application.jar", ARROW_AND_FRIENDS)

        assert launcher._server_jar([uber]) == launcher.THIN_JAR_PATH

    def test_classpath_without_arrow(self, tmp_path):
        jar = make_jar(tmp_path / "app.jar", ["com/example/App.class"])

        assert launcher._server_jar([jar]) == launcher.JAR_PATH

    @pytest.mark.parametrize("left_out", ARROW_AND_FRIENDS[1:])
    def test_arrow_without_something_it_needs(self, tmp_path, caplog, left_out):
        """The thin JAR could not run there, so the fat one does, and says so."""
        names = [name for name in ARROW_AND_FRIENDS if name != left_out]
        jar = make_jar(tmp_path / "arrow-vector.jar", names)

        with caplog.at_level("WARNING", logger="gatun.launcher"):
            assert launcher._server_jar([jar]) == launcher.JAR_PATH
        assert left_out in caplog.text

    def test_no_warning_without_arrow(self, tmp_path, caplog):
        """Netty and FlatBuffers alone are nobody's Arrow."""
        jar = make_jar(tmp_path / "netty-buffer.jar", ARROW_AND_FRIENDS[3:])

        with caplog.at_level("WARNING", logger="gatun.launcher"):
            assert launcher._server_jar([jar]) == launcher.JAR_PATH
        assert caplog.text == ""

    def test_a_relocated_arrow_is_not_arrow(self, tmp_path):
        relocated = make_jar(
            tmp_path / "shaded.jar",
            [f"shaded/{name}" for name in ARROW_AND_FRIENDS]
            + [f"META-INF/versions/17/{name}" for name in ARROW_AND_FRIENDS],
        )

        assert launcher._server_jar([relocated]) == launcher.JAR_PATH

    def test_wildcard_entry(self, tmp_path):
        """`dir/*` is every JAR in the directory, as it is for the JVM."""
        make_jar(tmp_path / "all.jar", ARROW_AND_FRIENDS)
        (tmp_path / "notes.txt").write_text("not a jar")

        assert launcher._server_jar([f"{tmp_path}/*"]) == launcher.THIN_JAR_PATH
        assert launcher._server_jar([str(tmp_path)]) == launcher.JAR_PATH

    def test_directory_of_classes(self, tmp_path):
        for name in ARROW_AND_FRIENDS:
            target = tmp_path / name
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(b"")

        assert launcher._server_jar([str(tmp_path)]) == launcher.THIN_JAR_PATH

    def test_entries_joined_with_the_path_separator(self, tmp_path):
        first = make_jar(tmp_path / "a.jar", ARROW_AND_FRIENDS[:2])
        second = make_jar(tmp_path / "b.jar", ARROW_AND_FRIENDS[2:])

        assert (
            launcher._server_jar([first + os.pathsep + second])
            == launcher.THIN_JAR_PATH
        )

    def test_entries_that_are_missing_or_no_jar(self, tmp_path):
        text = tmp_path / "readme.jar"
        text.write_text("PK is not enough to make a zip file " * 10)
        empty = tmp_path / "empty.jar"
        empty.write_bytes(b"")
        real = make_jar(tmp_path / "real.jar", ARROW_AND_FRIENDS)

        classpath = [str(tmp_path / "missing.jar"), str(text), str(empty)]
        assert launcher._server_jar(classpath) == launcher.JAR_PATH
        assert launcher._server_jar([*classpath, real]) == launcher.THIN_JAR_PATH

    def test_jar_with_a_comment_and_many_entries(self, tmp_path):
        import zipfile

        path = tmp_path / "big.jar"
        with zipfile.ZipFile(path, "w") as jar:
            for n in range(3000):
                jar.writestr(f"com/example/pkg{n % 50}/Class{n}.class", b"")
            for name in ARROW_AND_FRIENDS:
                jar.writestr(name, b"")
            jar.comment = b"c" * 5000

        assert launcher._server_jar([str(path)]) == launcher.THIN_JAR_PATH

    def test_zip64_jar(self, tmp_path):
        """More than 65,535 entries: the directory's place is in a second record."""
        import zipfile

        path = tmp_path / "huge.jar"
        with zipfile.ZipFile(path, "w") as jar:
            for n in range(66_000):
                jar.writestr(f"p/C{n}.class", b"")
            for name in ARROW_AND_FRIENDS:
                jar.writestr(name, b"")

        directory = launcher._central_directory(str(path))
        assert launcher._jar_has(directory, b"p/C65999.class")
        assert not launcher._jar_has(directory, b"p/C66000.class")
        assert launcher._server_jar([str(path)]) == launcher.THIN_JAR_PATH

    def test_the_directory_search_agrees_with_zipfile(self):
        """On a real JAR, with thousands of entries: every name is found, and
        no part of a name is taken for one."""
        import zipfile

        directory = launcher._central_directory(str(launcher.JAR_PATH))
        with zipfile.ZipFile(launcher.JAR_PATH) as jar:
            names = jar.namelist()

        assert len(names) > 1000
        assert all(launcher._jar_has(directory, name.encode()) for name in names)
        assert not launcher._jar_has(
            directory, b"apache/arrow/vector/VectorSchemaRoot.class"
        )
        assert not launcher._jar_has(
            directory, b"org/apache/arrow/vector/VectorSchemaRoot"
        )

    def test_the_thin_jar_is_what_starts(self, tmp_path, started_processes):
        uber = make_jar(tmp_path / "application.jar", ARROW_AND_FRIENDS)

        # Empty classes: the server cannot start on this classpath
        with pytest.raises(RuntimeError, match="failed to start"):
            launch_gateway(classpath=[uber])

        (process,) = started_processes
        command = process.args
        classpath = command[command.index("-cp") + 1].split(os.pathsep)
        assert classpath == [str(launcher.THIN_JAR_PATH), uber]


class TestSignalHandlers:
    @pytest.fixture(autouse=True)
    def _restore_handlers(self, monkeypatch):
        # Handlers are installed once per process; let each test install afresh
        monkeypatch.setattr(launcher, "_signal_handlers_installed", False)
        saved = {sig: signal.getsignal(sig) for sig in (signal.SIGTERM, signal.SIGINT)}
        yield
        for sig, handler in saved.items():
            signal.signal(sig, handler)

    def test_ignored_signal_stays_ignored(self):
        """Regression: a process that ignored SIGINT or SIGTERM (nohup, a shell
        background job) got a handler that made the signal kill it."""
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        signal.signal(signal.SIGINT, signal.SIG_IGN)

        launcher._install_signal_handlers()

        assert signal.getsignal(signal.SIGTERM) is signal.SIG_IGN
        assert signal.getsignal(signal.SIGINT) is signal.SIG_IGN

    def test_default_action_is_replaced_by_cleanup(self):
        signal.signal(signal.SIGTERM, signal.SIG_DFL)

        launcher._install_signal_handlers()

        assert signal.getsignal(signal.SIGTERM) is launcher._signal_handler

    def test_application_handler_is_left_alone(self):
        def handler(signum, frame):
            pass

        signal.signal(signal.SIGTERM, handler)

        launcher._install_signal_handlers()

        assert signal.getsignal(signal.SIGTERM) is handler
        # Python's own SIGINT handler (KeyboardInterrupt) counts as one too
        assert signal.getsignal(signal.SIGINT) is signal.default_int_handler


class TestServerProcess:
    def test_server_runs_in_its_own_process_group(self, stand_in_server, launch):
        """Ctrl-C in a terminal signals the foreground process group; the
        server must not be in it, or it dies under a program that handles
        KeyboardInterrupt and carries on."""
        stand_in_server()
        pid = launch().process.pid

        assert os.getpgid(pid) != os.getpgid(0)
        assert os.getsid(pid) != os.getsid(0)

    def test_real_server_runs_in_its_own_process_group(self, launch):
        session = launch()

        assert os.getpgid(session.process.pid) != os.getpgid(0)
        assert accepts_connections(session.socket_path)

    def test_server_is_told_its_parent(self, stand_in_server, launch):
        """The server exits when the process named by gatun.parent.pid is gone,
        which is what ends it when this process is killed outright."""
        stand_in_server()
        args = launch().process.args

        assert args.count(f"-Dgatun.parent.pid={os.getpid()}") == 1
        # Before the main class, or the JVM would take it as a program argument
        assert args.index(f"-Dgatun.parent.pid={os.getpid()}") < args.index("-jar")

    def test_parent_set_by_the_caller_is_not_duplicated(self, stand_in_server, launch):
        """The JVM keeps the last of several -D flags for one property, so
        passing both would silently drop one of them."""
        stand_in_server()
        args = launch(extra_jvm_flags=["-Dgatun.parent.pid=1"]).process.args

        assert [arg for arg in args if arg.startswith("-Dgatun.parent.pid=")] == [
            "-Dgatun.parent.pid=1"
        ]


class TestStop:
    def test_stop_reaps_the_server_and_removes_its_socket(self, stand_in_server):
        stand_in_server()
        session = launch_gateway()
        process, socket_path = session.process, session.socket_path

        session.stop()

        assert process.returncode is not None
        assert process.stdout.closed
        assert process.stderr.closed
        assert not os.path.exists(socket_path)
        assert session not in launcher._active_sessions

    def test_killed_server_is_reaped(self, stand_in_server, monkeypatch):
        """Regression: after kill() nothing waited for the process, which left
        a zombie and returncode unset."""
        monkeypatch.setattr(launcher, "STOP_GRACE_SECONDS", 0.2)
        stand_in_server(IGNORE_SIGTERM)
        session = launch_gateway()
        process, socket_path = session.process, session.socket_path

        session.stop()

        assert process.returncode == -signal.SIGKILL
        assert process.stdout.closed
        # A killed server does not remove its own socket
        assert not os.path.exists(socket_path)

    def test_stop_twice(self, stand_in_server):
        stand_in_server()
        session = launch_gateway()

        session.stop()
        session.stop()

    def test_second_stop_leaves_a_newer_server_alone(
        self, stand_in_server, launch, socket_dir
    ):
        """With a fixed socket path, the files of a stopped session are the
        next server's."""
        stand_in_server()
        socket_path = str(socket_dir / "s.sock")
        old = launch_gateway(socket_path=socket_path)
        old.stop()
        launch(socket_path=socket_path)

        old.stop()

        assert accepts_connections(socket_path)

    def test_stop_does_not_delete_a_file_that_is_not_a_socket(
        self, stand_in_server, socket_dir
    ):
        """A server that exits before binding leaves the path as it found it,
        and so must the cleanup."""
        stand_in_server("sys.exit(3)")
        not_a_socket = socket_dir / "notes.txt"
        not_a_socket.write_text("keep me")

        with pytest.raises(RuntimeError, match="failed to start"):
            launch_gateway(socket_path=str(not_a_socket))

        assert not_a_socket.read_text() == "keep me"


class TestFork:
    def test_stop_is_ignored_outside_the_launching_process(
        self, stand_in_server, launch
    ):
        stand_in_server()
        session = launch()
        process = session.process

        # What a forked child sees: the session, in a process with another pid
        session._owner_pid = os.getpid() + 1
        try:
            session.stop()
        finally:
            session._owner_pid = os.getpid()

        assert process.poll() is None
        assert accepts_connections(session.socket_path)

    def test_forked_children_do_not_stop_the_parents_server(
        self, stand_in_server, socket_dir, tmp_path
    ):
        """Regression: a child inherits the atexit hook and the signal handler,
        so one that exited or was terminated (multiprocessing.Pool.terminate())
        stopped the parent's server and unlinked its socket."""
        stand_in_server()
        script = tmp_path / "forking.py"
        script.write_text(
            """\
import os, signal, socket, sys, time

from gatun.launcher import launch_gateway

session = launch_gateway(socket_path=sys.argv[1])
process = session.process


def in_child(body):
    pid = os.fork()
    if pid == 0:
        body()
        os._exit(1)
    return pid


# Exits through the interpreter, so the inherited atexit hooks run
os.waitpid(in_child(lambda: sys.exit(0)), 0)

# Terminated: the inherited signal handler runs, then the signal's default.
# The child says when it is running: Python drops signals that reach a child
# before it has finished setting up after the fork.
ready, running = os.pipe()


def wait_to_be_terminated():
    os.write(running, b"x")
    time.sleep(30)


pid = in_child(wait_to_be_terminated)
os.read(ready, 1)
os.kill(pid, signal.SIGTERM)
_, status = os.waitpid(pid, 0)
assert os.WIFSIGNALED(status) and os.WTERMSIG(status) == signal.SIGTERM, status


# Stops the session it inherited, as closing an inherited client does
def stop_explicitly():
    session.stop()
    os._exit(0)


os.waitpid(in_child(stop_explicitly), 0)

time.sleep(0.5)  # a signalled server needs a moment to be seen as gone
assert process.poll() is None, "the server was stopped by a child"
probe = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
probe.connect(sys.argv[1])
probe.close()

session.stop()
assert process.returncode is not None
assert not os.path.exists(sys.argv[1])
print("server outlived the children")
"""
        )

        result = subprocess.run(
            [sys.executable, str(script), str(socket_dir / "s.sock")],
            capture_output=True,
            text=True,
            timeout=60,
            check=False,
        )

        assert result.returncode == 0, result.stderr
        assert "server outlived the children" in result.stdout


class TestSocketInUse:
    def test_refuses_a_path_something_is_listening_on(
        self, socket_dir, started_processes
    ):
        """Regression: the readiness check connected to whatever was listening
        and reported success, while the new server deleted the socket."""
        socket_path = str(socket_dir / "s.sock")
        listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        listener.bind(socket_path)
        listener.listen()
        try:
            with pytest.raises(RuntimeError) as exc_info:
                launch_gateway(socket_path=socket_path)

            message = str(exc_info.value)
            assert f"already listening on {socket_path}" in message
            assert "different socket_path" in message
            # Refused before anything was started or touched
            assert started_processes == []
            assert accepts_connections(socket_path)
        finally:
            listener.close()

    def test_refuses_a_path_from_configuration(
        self, socket_dir, started_processes, config, monkeypatch
    ):
        socket_path = str(socket_dir / "s.sock")
        monkeypatch.setattr(config, "socket_path", socket_path)
        listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        listener.bind(socket_path)
        listener.listen()
        try:
            with pytest.raises(RuntimeError, match="already listening"):
                launch_gateway()
            assert started_processes == []
        finally:
            listener.close()

    def test_stale_socket_file_is_replaced(self, stand_in_server, launch, socket_dir):
        socket_path = str(socket_dir / "s.sock")
        dead = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        dead.bind(socket_path)
        dead.close()
        assert os.path.exists(socket_path)
        stand_in_server()

        session = launch(socket_path=socket_path)

        assert session.process.poll() is None
        assert accepts_connections(socket_path)

    def test_real_server_is_not_taken_over(self, launch, socket_dir):
        """A second launch on a live server's path must leave that server and
        its clients working."""
        socket_path = str(socket_dir / "s.sock")
        # A leftover file from an earlier run must not get in the way
        dead = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        dead.bind(socket_path)
        dead.close()

        first = launch(socket_path=socket_path, memory="16MB")
        client = GatunClient(socket_path)
        assert client.connect()
        try:
            items = client.jvm.java.util.ArrayList()
            items.add("before")

            with pytest.raises(RuntimeError, match="already listening"):
                launch_gateway(socket_path=socket_path, memory="32MB")

            assert first.process.poll() is None
            items.add("after")
            assert items.size() == 2
            assert client.memory_size == 16 * MB
        finally:
            client.close()


class TestServerOutput:
    def test_only_a_tail_is_kept(self):
        """Regression: every line was kept for the life of the server."""
        lines = [f"line {i}\n" for i in range(10_000)]
        storage: list[str] = []

        _drain_pipe(iter(lines), storage, limit=50)

        assert 50 <= len(storage) <= 100
        assert storage == lines[-len(storage) :]

    def test_short_output_is_kept_whole(self):
        lines = [f"line {i}\n" for i in range(20)]
        storage: list[str] = []

        _drain_pipe(iter(lines), storage, limit=50)

        assert storage == lines

    def test_output_during_startup_does_not_block_the_server(
        self, stand_in_server, launch
    ):
        """Regression: nothing read the pipes until the server was ready, so a
        server that wrote more than a pipe buffer while starting blocked on the
        write and the launch timed out."""
        stand_in_server(
            """
for i in range(5000):
    print(f"out {i} " + "x" * 100)
    print(f"err {i} " + "x" * 100, file=sys.stderr)
print("startup finished", file=sys.stderr)
sys.stdout.flush()
sys.stderr.flush()
"""
        )

        session = launch()

        # The reader may be a moment behind the server
        deadline = time.monotonic() + 5
        while (
            "startup finished\n" not in session._stderr_lines[-1:]
            and time.monotonic() < deadline
        ):
            time.sleep(0.01)
        assert session._stderr_lines[-1] == "startup finished\n"
        assert len(session._stderr_lines) <= 2 * OUTPUT_TAIL_LINES
        assert len(session._stdout_lines) <= 2 * OUTPUT_TAIL_LINES
        assert session._stdout_lines[-1].startswith("out 4999 ")


class TestFailedStartup:
    def test_server_that_exits_reports_its_output(
        self, stand_in_server, started_processes
    ):
        stand_in_server(
            """
for i in range(3000):
    print(f"warning {i} " + "x" * 100, file=sys.stderr)
print("fatal: cannot start", file=sys.stderr)
sys.exit(3)
"""
        )

        with pytest.raises(RuntimeError) as exc_info:
            launch_gateway()

        message = str(exc_info.value)
        assert "failed to start" in message
        assert "exit code 3" in message
        assert "fatal: cannot start" in message
        assert "warning 2999 " in message
        (process,) = started_processes
        assert process.stdout.closed
        assert process.stderr.closed

    def test_timeout_stops_the_server(
        self, stand_in_server, started_processes, config, monkeypatch, socket_dir
    ):
        """Regression: on a timeout the server was sent SIGTERM and forgotten:
        not waited for, not killed if it ignored that, its pipes left open."""
        monkeypatch.setattr(config, "startup_timeout", 0.5)
        monkeypatch.setattr(launcher, "STOP_GRACE_SECONDS", 0.2)
        stand_in_server(
            f"{IGNORE_SIGTERM}\n"
            "print('still starting', file=sys.stderr, flush=True)\n"
            f"{NEVER_LISTEN}"
        )
        socket_path = str(socket_dir / "s.sock")

        with pytest.raises(RuntimeError) as exc_info:
            launch_gateway(socket_path=socket_path)

        message = str(exc_info.value)
        assert "Timed out after 0.5s" in message
        assert socket_path in message
        assert "still starting" in message
        (process,) = started_processes
        assert process.returncode == -signal.SIGKILL
        assert process.stdout.closed
        assert process.stderr.closed

    def test_interrupt_during_startup_stops_the_server(
        self, stand_in_server, started_processes, monkeypatch
    ):
        """Regression: KeyboardInterrupt while waiting for the server left it
        running with nothing referring to it."""
        stand_in_server(NEVER_LISTEN)
        real_sleep = time.sleep
        interrupted = []

        def interrupt_once(seconds):
            # Only the first sleep, which is the launcher waiting between
            # readiness checks; waiting for the process to exit sleeps too
            if not interrupted:
                interrupted.append(True)
                raise KeyboardInterrupt
            real_sleep(seconds)

        monkeypatch.setattr(launcher.time, "sleep", interrupt_once)

        with pytest.raises(KeyboardInterrupt):
            launch_gateway()

        (process,) = started_processes
        assert process.returncode is not None
        assert process.stdout.closed

    def test_missing_java_is_reported_as_such(self, tmp_path, monkeypatch):
        monkeypatch.setenv("JAVA_HOME", str(tmp_path / "no-such-jdk"))

        with pytest.raises(FileNotFoundError):
            launch_gateway()
