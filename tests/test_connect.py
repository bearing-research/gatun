"""Tests for connect() and aconnect() convenience functions."""

import gc
import glob
import os
import weakref

import pyarrow as pa
import pytest

from gatun import GatunClient, aconnect, connect
from gatun.async_client import AsyncGatunClient


class TestConnect:
    """Tests for the sync connect() function."""

    def test_connect_returns_client(self):
        """Test connect() returns a connected GatunClient."""
        client = connect()
        try:
            assert isinstance(client, GatunClient)
            assert client.shm is not None  # Connected
            # Verify we can use it
            arr = client.jvm.java.util.ArrayList()
            arr.add("test")
            assert arr.size() == 1
        finally:
            client.close()

    def test_connect_as_context_manager(self):
        """Test connect() works as a context manager."""
        with connect() as client:
            assert isinstance(client, GatunClient)
            arr = client.jvm.java.util.ArrayList()
            arr.add("hello")
            assert arr.size() == 1

    def test_connect_with_memory(self):
        """Test connect() accepts memory parameter."""
        with connect(memory="32MB") as client:
            arr = client.jvm.java.util.ArrayList()
            assert arr is not None

    def test_connect_client_has_session(self):
        """Test connected client has server session attached."""
        with connect() as client:
            assert hasattr(client, "_server_session")
            assert client._server_session is not None


class TestAconnect:
    """Tests for the async aconnect() function."""

    @pytest.mark.asyncio
    async def test_aconnect_returns_client(self):
        """Test aconnect() returns a connected AsyncGatunClient."""
        client = await aconnect()
        try:
            assert isinstance(client, AsyncGatunClient)
            assert client.shm is not None  # Connected
            # Verify we can use it
            arr = await client.jvm.java.util.ArrayList()
            await arr.add("test")
            size = await arr.size()
            assert size == 1
        finally:
            await client.close()

    @pytest.mark.asyncio
    async def test_aconnect_as_context_manager(self):
        """Test aconnect() works as an async context manager."""
        async with await aconnect() as client:
            assert isinstance(client, AsyncGatunClient)
            arr = await client.jvm.java.util.ArrayList()
            await arr.add("hello")
            size = await arr.size()
            assert size == 1

    @pytest.mark.asyncio
    async def test_aconnect_with_memory(self):
        """Test aconnect() accepts memory parameter."""
        async with await aconnect(memory="32MB") as client:
            arr = await client.jvm.java.util.ArrayList()
            assert arr is not None

    @pytest.mark.asyncio
    async def test_aconnect_client_has_session(self):
        """Test async connected client has server session attached."""
        async with await aconnect() as client:
            assert hasattr(client, "_server_session")
            assert client._server_session is not None


class TestServerLifetime:
    """A server launched by connect() belongs to the client it returned."""

    def test_close_stops_the_server(self):
        """Regression: the JVM kept running until interpreter exit."""
        client = connect()
        process = client._server_session.process
        socket_path = client.socket_path
        assert process.poll() is None

        client.close()

        assert process.poll() is not None
        assert client._server_session is None
        assert not os.path.exists(socket_path)

    def test_close_twice(self):
        client = connect()
        client.close()
        client.close()

    def test_close_with_a_busy_mapping_still_closes_the_socket(self):
        """Regression: the mapping cannot be released while a buffer export of
        it is alive; that failure aborted close() before it reached the socket."""
        client = connect()
        process = client._server_session.process
        sock = client.sock
        shm_file = client.shm_file
        export = memoryview(client.shm)

        client.close()

        assert sock.fileno() == -1
        assert shm_file.closed
        assert client.sock is None
        assert process.poll() is not None
        export.release()

    def test_close_leaves_no_shared_memory_files(self):
        """The server is stopped right after the socket closes, before the
        session thread gets to delete its own shared memory file."""
        for _ in range(4):
            client = connect()
            socket_path = client.socket_path
            assert client.ping() is True
            assert glob.glob(f"{glob.escape(socket_path)}.*.shm")

            client.close()

            assert glob.glob(f"{glob.escape(socket_path)}*") == []

    def test_stopped_session_is_not_kept_for_exit(self):
        """stop() drops the session's atexit entry, so a process that opens and
        closes many clients does not accumulate stopped sessions."""
        client = connect()
        session = weakref.ref(client._server_session)

        client.close()
        gc.collect()

        assert session() is None

    async def test_async_close_stops_the_server(self):
        client = await aconnect()
        process = client._server_session.process

        await client.close()

        assert process.poll() is not None


class TestServerDefaults:
    """What the launcher's default JVM flags leave the server with."""

    def test_netty_allocates_without_zero_filling(self):
        """The server's Netty is relocated in the JAR, and the name of the
        switch that lets it hand out direct memory unfilled is relocated too.
        Under the plain name the server's Netty never sees it, and every Arrow
        buffer is filled with zeros before the data is copied over them."""
        platform = "gatun.shaded.io.netty.util.internal.PlatformDependent"
        with connect(allowed_classes=[platform]) as client:
            unfilled = client.invoke_static_method(platform, "useDirectBufferNoCleaner")

            assert unfilled is True

    def test_arrow_debug_allocator_is_off(self, client):
        """It records a stack trace for each buffer operation, and a small
        arrow_to_java() took more than twice as long with it."""
        system = client.jvm.java.lang.System

        assert system.getProperty("arrow.memory.debug.allocator") is None

    def test_arrow_debug_allocator_on_request(self):
        """Still the way to find out where a leaked Arrow buffer came from."""
        table = pa.table({"x": [1, 2, 3], "s": ["a", "b", "c"]})
        debug = ["-Darrow.memory.debug.allocator=true"]
        with connect(extra_jvm_flags=debug) as client:
            system = client.jvm.java.lang.System
            assert system.getProperty("arrow.memory.debug.allocator") == "true"

            root = client.arrow_to_java(table)

            assert client.arrow_from_java(root).equals(table)


class TestTimeoutArguments:
    """None disables a timeout, as documented; omitting it uses the default."""

    def test_defaults(self):
        from gatun.client import (
            DEFAULT_CALLBACK_TIMEOUT,
            DEFAULT_SOCKET_TIMEOUT,
            HANDSHAKE_TIMEOUT,
            GatunClient,
        )

        client = GatunClient("/nonexistent.sock")

        assert client.socket_timeout == DEFAULT_SOCKET_TIMEOUT
        assert client.callback_timeout == DEFAULT_CALLBACK_TIMEOUT
        assert client.handshake_timeout == HANDSHAKE_TIMEOUT

    def test_none_is_kept(self):
        """Regression: None was silently replaced by the default."""
        from gatun.async_client import AsyncGatunClient
        from gatun.client import GatunClient

        for cls in (GatunClient, AsyncGatunClient):
            client = cls(
                "/nonexistent.sock",
                socket_timeout=None,
                callback_timeout=None,
                handshake_timeout=None,
            )
            assert client.socket_timeout is None
            assert client.callback_timeout is None
            assert client.handshake_timeout is None

    def test_client_without_timeouts_works(self, java_gateway):
        from gatun.client import GatunClient

        client = GatunClient(
            str(java_gateway.socket_path),
            socket_timeout=None,
            callback_timeout=None,
            handshake_timeout=None,
        )
        assert client.connect()
        try:
            assert client.jvm.java.lang.Integer.parseInt("7") == 7
            comparator = client.register_callback(
                lambda a, b: (a > b) - (a < b), "java.util.Comparator"
            )
            items = client.jvm.java.util.ArrayList([3, 1, 2])
            client.jvm.java.util.Collections.sort(items, comparator)
            assert items.toString() == "[1, 2, 3]"
        finally:
            client.close()
