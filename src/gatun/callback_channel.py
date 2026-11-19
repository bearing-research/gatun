"""The callback connection of a client.

A callback made by the Java thread that is running the client's request comes
back in band, on the request's own channel. Every other Java thread (a Spark
listener, an executor's worker, a timer), at any time, calls Python on this
second connection, which something on the Python side is always reading.

The connection carries no shared memory. Each frame is a four-byte
little-endian length followed by a FlatBuffer: a Response with is_callback set
from Java, a CallbackResponse command from Python. Calls from several threads
are in flight at once and are answered in any order; the call_id in both frames
pairs them.

The classes here only move frames. Running the callback and building its answer
is the client's job (_serve_channel_callback), which is why they hold the client
weakly: a client that is dropped without close() must still be collected, and
its sockets with it.
"""

from __future__ import annotations

import asyncio
import logging
import socket
import struct
import threading
import weakref
from typing import Any

from gatun.generated.org.gatun.protocol import Response

logger = logging.getLogger(__name__)

# The size of the zones the in-band path uses, so both paths carry the same
# calls. Must match CallbackChannel.MAX_FRAME_SIZE on the server.
MAX_FRAME_SIZE = 64 * 1024

# What the server sends once it serves the connection: its protocol version
_READY = struct.Struct("<I")
_LENGTH = struct.Struct("<I")


def _frame(data: bytes | bytearray) -> bytes:
    return _LENGTH.pack(len(data)) + bytes(data)


def _checked_size(header: bytes) -> int:
    (size,) = _LENGTH.unpack(header)
    if not 0 < size <= MAX_FRAME_SIZE:
        raise OSError(f"Invalid callback frame size: {size}")
    return int(size)


class CallbackChannel:
    """The sync client's callback connection: a thread that is always reading."""

    def __init__(self, client: Any, path: str, timeout: float | None):
        """Connect to the socket the server opened for this client's session."""
        self._client = weakref.ref(client)
        self._send_lock = threading.Lock()
        self._closed = False
        self._sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            self._sock.settimeout(timeout)
            self._sock.connect(path)
            if self._read(_READY.size) is None:
                raise OSError("The server closed the callback connection")
            self._sock.settimeout(None)
        except BaseException:
            self._sock.close()
            raise
        self._thread = threading.Thread(
            target=self._serve, name="gatun-callbacks", daemon=True
        )
        self._thread.start()

    def _read(self, n: int) -> bytes | None:
        """Read exactly n bytes; None if the connection ended first."""
        data = bytearray()
        while len(data) < n:
            chunk = self._sock.recv(n - len(data))
            if not chunk:
                return None
            data.extend(chunk)
        return bytes(data)

    def _serve(self) -> None:
        try:
            while True:
                header = self._read(_LENGTH.size)
                if header is None:
                    return
                data = self._read(_checked_size(header))
                if data is None:
                    return
                resp = Response.Response.GetRootAsResponse(data, 0)
                if not resp.IsCallback():
                    logger.debug("Dropped a frame that is not a callback request")
                    continue
                client = self._client()
                if client is None:
                    return
                # A thread per call: calls overlap, one may run for hours, and
                # one may be what another is waiting for
                threading.Thread(
                    target=client._serve_channel_callback,
                    args=(self, resp),
                    name="gatun-callback",
                    daemon=True,
                ).start()
                # Not held while waiting for the next frame
                del client
        except OSError as e:
            if not self._closed:
                logger.warning("The callback connection failed: %s", e)
        finally:
            self.close()

    def send(self, data: bytes | bytearray) -> None:
        """Send one frame. Raises OSError once the connection is closed."""
        frame = _frame(data)
        with self._send_lock:
            self._sock.sendall(frame)

    def close(self) -> None:
        """Close the connection. Java threads waiting for Python on it fail."""
        if self._closed:
            return
        self._closed = True
        try:
            # shutdown() is what wakes the reader thread out of recv()
            self._sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        self._sock.close()


class AsyncCallbackChannel:
    """The async client's callback connection: a task that is always reading."""

    def __init__(
        self,
        client: Any,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ):
        self._client = weakref.ref(client)
        self._reader = reader
        self._writer = writer
        self._send_lock = asyncio.Lock()
        self._closed = False
        self._calls: set[asyncio.Task[None]] = set()
        self._task = asyncio.create_task(self._serve())

    @classmethod
    async def open(
        cls, client: Any, path: str, timeout: float | None
    ) -> AsyncCallbackChannel:
        """Connect to the socket the server opened for this client's session."""

        async def connect() -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
            reader, writer = await asyncio.open_unix_connection(path)
            try:
                await reader.readexactly(_READY.size)
            except BaseException:
                writer.close()
                raise
            return reader, writer

        try:
            reader, writer = await asyncio.wait_for(connect(), timeout)
        except (asyncio.IncompleteReadError, TimeoutError) as e:
            raise OSError(f"The callback connection could not be opened: {e!r}") from e
        return cls(client, reader, writer)

    async def _serve(self) -> None:
        try:
            while True:
                header = await self._reader.readexactly(_LENGTH.size)
                data = await self._reader.readexactly(_checked_size(header))
                resp = Response.Response.GetRootAsResponse(data, 0)
                if not resp.IsCallback():
                    logger.debug("Dropped a frame that is not a callback request")
                    continue
                client = self._client()
                if client is None:
                    return
                # A task per call: calls overlap, and one may be what another
                # is waiting for
                call = asyncio.create_task(client._serve_channel_callback(self, resp))
                self._calls.add(call)
                call.add_done_callback(self._calls.discard)
                # Not held while waiting for the next frame
                del client
        except asyncio.IncompleteReadError:
            pass  # the connection ended
        except OSError as e:
            if not self._closed:
                logger.warning("The callback connection failed: %s", e)
        finally:
            self.close()

    async def send(self, data: bytes | bytearray) -> None:
        """Send one frame. Raises OSError once the connection is closed."""
        if self._closed:
            raise OSError("The callback connection is closed")
        frame = _frame(data)
        async with self._send_lock:
            self._writer.write(frame)
            await self._writer.drain()

    def close(self) -> None:
        """Close the connection and cancel the callbacks still running on it."""
        if self._closed:
            return
        self._closed = True
        try:
            current = asyncio.current_task()
        except RuntimeError:
            current = None  # no running loop: the tasks cannot run again anyway
        for task in (self._task, *self._calls):
            # close() can be called by the reader itself, or from a callback
            if task is not current and not task.done():
                try:
                    task.cancel()
                except RuntimeError:
                    pass  # its event loop is gone
        try:
            self._writer.close()
        except (OSError, RuntimeError):
            pass  # already closed, or its event loop is gone
