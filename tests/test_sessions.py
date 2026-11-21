"""Sessions: what one client can reach, and how the server treats its connections."""

import os
import stat
import subprocess
import sys
import textwrap
import time

import flatbuffers
import pytest

from gatun import (
    JavaException,
    JavaIllegalArgumentException,
    connect,
)
from gatun.client import GatunClient
from gatun.generated.org.gatun.protocol import Command as Cmd


def _second_client(client) -> GatunClient:
    other = GatunClient(client.socket_path)
    assert other.connect()
    return other


class TestObjectsBelongToTheirSession:
    """Object IDs come from one counter, so a client can guess another's IDs."""

    def test_another_sessions_object_cannot_be_called(self, client):
        mine = client.create_object("java.util.ArrayList")
        mine.add("secret")
        other = _second_client(client)
        try:
            with pytest.raises(JavaException, match="not found"):
                other.invoke_method(mine.object_id, "get", 0)
        finally:
            other.close()

    def test_another_sessions_object_cannot_be_read_field_by_field(self, client):
        mine = client.create_object("java.lang.StringBuilder", "secret")
        other = _second_client(client)
        try:
            with pytest.raises(JavaException, match="not found"):
                other.get_fields(mine.object_id, ["count"])
            with pytest.raises(JavaException, match="not found"):
                other.is_instance_of(mine.object_id, "java.lang.Object")
        finally:
            other.close()

    def test_another_sessions_object_cannot_be_freed(self, client):
        mine = client.create_object("java.util.ArrayList")
        mine.add("kept")
        other = _second_client(client)
        try:
            other.free_object(mine.object_id)
            other.ping()
        finally:
            other.close()
        assert mine.get(0) == "kept"

    def test_another_sessions_object_cannot_be_passed_as_an_argument(self, client):
        mine = client.create_object("java.util.ArrayList")
        other = _second_client(client)
        try:
            theirs = other.create_object("java.util.ArrayList")
            with pytest.raises(JavaIllegalArgumentException, match="another session"):
                mine.add(theirs)
        finally:
            other.close()
        assert mine.size() == 0

    def test_freed_object_as_an_argument_is_an_error_not_null(self, client):
        """A stale reference used to arrive in Java as null."""
        holder = client.create_object("java.util.ArrayList")
        gone = client.create_object("java.util.ArrayList")
        gone_id = gone.object_id
        gone.detach()
        client.free_object(gone_id)

        with pytest.raises(JavaIllegalArgumentException, match="was freed"):
            holder.add(gone)
        assert holder.size() == 0


class TestFilesArePrivate:
    def test_socket_is_owner_only(self, client):
        mode = stat.S_IMODE(os.stat(client.socket_path).st_mode)
        assert mode == 0o600

    def test_shared_memory_is_owner_only(self, client):
        mode = stat.S_IMODE(os.stat(client.memory_path).st_mode)
        assert mode == 0o600

    def test_file_left_at_the_shared_memory_path_is_replaced(self):
        """The server makes the file itself; it never maps one that was put there."""
        client = connect(memory="16MB")
        try:
            # The next session's path is predictable: <socket>.<n>.shm
            base = client.memory_path.rsplit(".", 2)[0]
            current = int(client.memory_path.rsplit(".", 2)[1])
            planted = f"{base}.{current + 1}.shm"
            with open(planted, "wb") as f:
                f.write(b"planted")
            os.chmod(planted, 0o666)

            other = _second_client(client)
            try:
                assert other.memory_path == planted
                info = os.stat(planted)
                assert stat.S_IMODE(info.st_mode) == 0o600
                assert info.st_size == other.memory_size
                assert other.jvm.java.lang.Math.max(1, 2) == 2
            finally:
                other.close()
        finally:
            client.close()


class TestRequestsTheServerCannotRun:
    def test_bytes_that_are_not_a_command_get_an_answer(self, client):
        with pytest.raises(JavaIllegalArgumentException, match="Malformed command"):
            client._send_raw(b"\xff" * 64)
        assert client.ping() is True

    def test_unknown_action_is_an_error(self, client):
        """An action this server does not know used to be answered as a success."""
        builder = flatbuffers.Builder(64)
        Cmd.CommandStart(builder)
        Cmd.CommandAddAction(builder, 120)
        builder.Finish(Cmd.CommandEnd(builder))

        with pytest.raises(JavaException, match="Unknown action"):
            client._send_raw(builder.Output())
        assert client.ping() is True

    def test_collection_that_contains_itself(self, client):
        """Converting it for the reply used to overflow the stack."""
        looped = client.create_object("java.util.ArrayList")
        looped.add(looped)

        with pytest.raises(JavaIllegalArgumentException, match="contains itself"):
            client.invoke_method(looped.object_id, "clone")
        assert looped.size() == 1

    def test_long_stack_trace_arrives_whole(self, client):
        """A deep recursion's trace is a thousand frames, more than the 64 KB
        of the response zone, and none of them is left out."""
        looped = client.create_object("java.util.ArrayList")
        looped.add(looped)

        # hashCode() of a list that contains itself recurses until the stack ends
        with pytest.raises(JavaException) as exc_info:
            looped.hashCode()
        trace = str(exc_info.value)
        assert "StackOverflowError" in trace
        # The JVM records 1024 frames of a stack (MaxJavaStackTraceDepth)
        assert trace.count("\n\tat ") == 1024
        assert "more" not in trace.splitlines()[-1]
        assert client.ping() is True


class TestSessionLimit:
    def test_single_session_server_can_be_connected_to(self):
        """The launcher's own check that the server is up briefly holds a slot;
        the client connecting right behind it waits for the slot."""
        client = connect(
            memory="16MB", extra_jvm_flags=["-Dgatun.limits.max_sessions=1"]
        )
        try:
            assert client.ping() is True

            second = GatunClient(client.socket_path, handshake_timeout=10.0)
            assert second.connect() is False
            second.close()
            assert client.ping() is True
        finally:
            client.close()

    def test_slot_is_reused_after_a_disconnect(self):
        client = connect(
            memory="16MB", extra_jvm_flags=["-Dgatun.limits.max_sessions=2"]
        )
        try:
            for _ in range(3):
                other = _second_client(client)
                assert other.ping() is True
                other.close()
        finally:
            client.close()


class TestIdleTimeout:
    def test_idle_session_is_closed_without_waiting_for_a_request(self):
        client = connect(
            memory="16MB",
            extra_jvm_flags=["-Dgatun.limits.session_idle_timeout_ms=300"],
        )
        try:
            shm_path = client.memory_path
            assert os.path.exists(shm_path)
            deadline = time.monotonic() + 10
            while os.path.exists(shm_path) and time.monotonic() < deadline:
                time.sleep(0.1)
            assert not os.path.exists(shm_path), "the idle session was never closed"

            with pytest.raises(Exception):  # noqa: B017 - the transport error varies
                client.ping()
        finally:
            client.close()

    def test_request_longer_than_the_timeout(self):
        """Idle time is counted from the end of a request, not from its start."""
        client = connect(
            memory="16MB",
            allowed_classes=["java.lang.Thread"],
            extra_jvm_flags=["-Dgatun.limits.session_idle_timeout_ms=600"],
        )
        try:
            client.jvm.java.lang.Thread.sleep(1500)
            assert client.ping() is True
            client.jvm.java.lang.Thread.sleep(1500)
            assert client.jvm.java.lang.Math.max(1, 2) == 2
        finally:
            client.close()

    def test_a_large_command_after_the_timeout_is_told_the_reason(self):
        """A command over 64 KB follows its length on the socket. The server
        reads it to its end before it answers, or the client fails writing it
        and never sees why."""
        seen = []
        # Between the timeout and the reaper's next round the session's own
        # thread answers. The reaper may get there first: try again then.
        for attempt in range(6):
            client = connect(
                memory="16MB",
                extra_jvm_flags=["-Dgatun.limits.session_idle_timeout_ms=2000"],
            )
            try:
                text = client.jvm.java.lang.StringBuilder()
                time.sleep(2.02 + 0.15 * attempt)
                try:
                    text.append("x" * 3_000_000)
                except JavaException as e:
                    seen.append(str(e))
                    break
                except Exception as e:  # noqa: BLE001 - closed by the reaper
                    seen.append(type(e).__name__)
            finally:
                client.close()

        assert "session_idle_timeout_ms" in seen[-1], seen

    def test_busy_session_is_not_closed(self):
        client = connect(
            memory="16MB",
            extra_jvm_flags=["-Dgatun.limits.session_idle_timeout_ms=1500"],
        )
        try:
            for _ in range(8):
                assert client.ping() is True
                time.sleep(0.3)
        finally:
            client.close()


class TestServerFollowsItsParent:
    def test_server_exits_when_python_is_killed(self, tmp_path):
        """A killed Python process runs no cleanup; the server must notice by itself."""
        script = textwrap.dedent(
            """
            import os, sys
            from gatun import connect

            client = connect(memory="16MB")
            print(client._server_session.process.pid, client.socket_path, flush=True)
            sys.stdin.read()
            """
        )
        child = subprocess.Popen(
            [sys.executable, "-c", script],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            text=True,
        )
        try:
            assert child.stdout is not None
            server_pid, socket_path = child.stdout.readline().split()
            server_pid = int(server_pid)
            os.kill(server_pid, 0)  # running

            child.kill()
            child.wait()

            deadline = time.monotonic() + 15
            while time.monotonic() < deadline:
                try:
                    os.kill(server_pid, 0)
                except ProcessLookupError:
                    break
                time.sleep(0.1)
            else:
                os.kill(server_pid, 9)
                pytest.fail("the server outlived the process that launched it")
        finally:
            if child.poll() is None:
                child.kill()
                child.wait()
            if os.path.exists(socket_path):
                os.unlink(socket_path)


class TestNoRoomForSharedMemory:
    def test_connect_fails_with_the_reason(self):
        """The file is sparse, so a full disk would otherwise show up as a crash
        on first write, long after connecting."""
        with pytest.raises(RuntimeError, match="Not enough free space") as exc_info:
            connect(memory="4096GB")
        assert "4398046511104 bytes needed" in str(exc_info.value)
