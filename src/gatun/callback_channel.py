"""The callback connection of a client.

A callback made by the Java thread that is running the client's request comes
back in band, on the request's own channel. Every other Java thread (a Spark
listener, an executor's worker, a timer), at any time, calls Python on this
second connection, which something on the Python side is always reading.

The connection carries no shared memory. Each frame is a four-byte
little-endian length followed by a FlatBuffer: a Response with is_callback set
from Java, a CallbackResponse command from Python. Calls from several threads
are in flight at once and are answered in any order; the call_id in both frames
pairs them. Java also sends a Response with callback_released and no call_id,
when its garbage collector has taken a callback's proxy and nothing can call
the callback again.

Until a callback answers, the Java thread that called it is waiting, and it is
the one thread that is certain to be free: the client's own channel may be busy
with a request, possibly the request that is waiting for that thread. So a
Java call the callback makes goes out on this connection too, as a
NestedCommand frame with the call's ID, and the waiting thread runs it and
sends back a Response with the same ID. ChannelCall is that route.

An answer can name a Java object whose wrapper dies the moment the answer has
been sent, and the release that follows must not reach the server first. The
server reads this connection in order, so a release sent on it after the
answer is safe. One sent on the client's own channel carries the number of
answers sent so far (answers_sent) and the server waits until it has read
that many.

The classes here only move frames. Running the callback and building its answer
is the client's job (_serve_channel_callback), which is why they hold the client
weakly: a client that is dropped without close() must still be collected, and
its sockets with it.
"""

from __future__ import annotations

import asyncio
import logging
import mmap
import os
import queue
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


class CallMemory:
    """A call's own shared memory, which its callback's Arrow tables go through.

    The client's payload zone belongs to its channel, which may be busy with
    the very request that is waiting for the callback. Java creates this file
    for the call's first transfer and removes it when the call ends. The
    mapping here lives for as long as anything refers to it, a table read in
    place for example, and nothing writes there once the call is over.

    It has the attributes of a client that the Arrow helpers and
    ArrowTableView use, so they treat it like a client's payload zone.
    """

    def __init__(self, path: str, lock: Any = None):
        self.path = path
        with open(path, "r+b") as file:
            self.shm: mmap.mmap | None = mmap.mmap(file.fileno(), 0)
        # All of it is payload
        self.payload_offset = 0
        self.response_offset = len(self.shm)
        self._arena_epoch = 0
        # Every transfer starts at the front, so each one ends the views of
        # the one before
        self._payload_writes = 0
        self._payload_generation = 0
        # Held for a whole transfer, and by a view while it copies
        self._exchange_lock = lock if lock is not None else threading.RLock()

    def removed(self) -> bool:
        """Whether Java has removed the file, which it does when the call ends.

        From then on the server keeps no free space for it, so a page written
        here for the first time may be one the disk does not have.
        """
        return not os.path.exists(self.path)


class ChannelCall:
    """One call Java is waiting on, as the callback's route back to Java."""

    def __init__(self, channel: CallbackChannel, call_id: int):
        self.channel = channel
        self.call_id = call_id
        self._replies: queue.SimpleQueue[bytes | None] = queue.SimpleQueue()
        # One request of this call at a time: the Java thread runs them one
        # after another, and a reply carries nothing but the call's ID
        self._exchange_lock = threading.Lock()
        # Orders every request before the answer: Java stops listening for
        # this call once it has the answer
        self._state_lock = threading.Lock()
        self._answered = False
        self._lost = False
        # Orders a response's arrival against the moment its request gives up
        self._reply_lock = threading.Lock()
        # The connection keeps the route for as long as a reply can come:
        # until the callback is done and no request is waiting for one
        self._in_flight = False
        self._finished = False
        # An Arrow transfer is several steps on the one memory: writing it,
        # the request that refers to it, reading the result out of it
        self.transfer_lock = threading.RLock()
        self.memory: CallMemory | None = None

    @property
    def active(self) -> bool:
        """Whether Java is still waiting for the callback's answer."""
        return not self._answered

    def answer(self, frame: bytes | bytearray) -> None:
        """Send the callback's answer. No request goes out after it."""
        with self._state_lock:
            self._answered = True
            self.channel.send_answer(frame)

    def abandon(self) -> None:
        """Java has stopped waiting for the callback (its thread was interrupted)."""
        with self._state_lock:
            self._answered = True

    def request(self, frame: bytes | bytearray, timeout: float | None) -> bytes | None:
        """Send a command for Java to run and wait for its response.

        Returns None, having sent nothing, if Java is no longer waiting for the
        callback: the caller outlived it (a thread it started, a callback that
        timed out). Raises TimeoutError if no response came in time, and from
        then on, since the late response would be taken for the next one;
        OSError if the connection closed.
        """
        with self._exchange_lock:
            if self._lost:
                raise TimeoutError("an earlier call of this callback was abandoned")
            with self._state_lock:
                if self._answered:
                    return None
                self._in_flight = True
                try:
                    self.channel.send(frame)
                except BaseException:
                    self._in_flight = False
                    raise
            try:
                reply = self._replies.get(timeout=timeout)
            except queue.Empty:
                self._give_up()
                raise TimeoutError("no response from Java") from None
            except BaseException:
                self._give_up()
                raise
            finally:
                with self._state_lock:
                    self._in_flight = False
                    if self._finished:
                        self.channel._forget(self)
            if reply is None:
                raise OSError("The callback connection closed")
            return reply

    def finish(self) -> None:
        """The callback is done. A request of it that is still waiting for its
        response, in a thread that outlived it, keeps the route until it has it."""
        with self._state_lock:
            self._answered = True
            self._finished = True
            if not self._in_flight:
                self.channel._forget(self)
        # A context copied during the callback (a task or thread it started)
        # keeps this call reachable. What reads the memory in place holds it
        # itself.
        self.memory = None

    def _give_up(self) -> None:
        """Stop waiting for the response to a request that was sent. It may
        still come, or be here already, and nobody will read it."""
        with self._reply_lock:
            self._lost = True
            while True:
                try:
                    unread = self._replies.get_nowait()
                except queue.Empty:
                    break
                if unread is not None:
                    self.channel._discard(unread)

    def deliver(self, reply: bytes | None) -> None:
        """Called by the connection's reader; None once the connection is gone."""
        with self._reply_lock:
            if self._lost and reply is not None:
                self.channel._discard(reply)
            else:
                self._replies.put(reply)


class CallbackChannel:
    """The sync client's callback connection: a thread that is always reading."""

    def __init__(self, client: Any, path: str, timeout: float | None):
        """Connect to the socket the server opened for this client's session."""
        self._client = weakref.ref(client)
        self._send_lock = threading.Lock()
        self._closed = False
        self._calls: dict[int, ChannelCall] = {}
        self._calls_lock = threading.Lock()
        # Counted before the answer goes out, and so before its result is dropped
        self.answers_sent = 0
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
                if resp.CallbackReleased():
                    self._callback_released(resp.CallbackId())
                    continue
                if not resp.IsCallback():
                    # The response to a command a callback sent
                    with self._calls_lock:
                        call = self._calls.get(resp.CallId())
                    if call is not None:
                        call.deliver(data)
                    else:
                        self._discard(data)
                    continue
                client = self._client()
                if client is None:
                    return
                # Looked up before the next frame is read: that one may say
                # that Java has let go of this callback (the caller was
                # interrupted and held the only reference to the proxy)
                handler = client._callbacks.get(resp.CallbackId())
                # A thread per call: calls overlap, one may run for hours, and
                # one may be what another is waiting for
                threading.Thread(
                    target=client._serve_channel_callback,
                    args=(self, resp, handler),
                    name="gatun-callback",
                    daemon=True,
                ).start()
                # Not held while waiting for the next frame
                del client, handler
        except OSError as e:
            if not self._closed:
                logger.warning("The callback connection failed: %s", e)
        finally:
            self.close()

    def _callback_released(self, callback_id: int) -> None:
        """Java's garbage collector took a callback's proxy: the client let go
        of its handle and no Java code holds it, so nothing calls it again."""
        client = self._client()
        if client is None:
            return
        handler = client._callbacks.pop(callback_id, None)
        if handler is None:
            return
        logger.debug("Callback %d dropped: Java no longer holds its proxy", callback_id)
        # The function's last reference ends on a thread of its own. A
        # finalizer of it, or of something only it kept alive, may call Java
        # and wait for the client's channel, which a request that is waiting
        # for a callback may hold: this thread has to go on reading.
        held = [handler]
        del handler
        threading.Thread(
            target=self._let_go, args=(held,), name="gatun-callback-drop", daemon=True
        ).start()

    def _let_go(self, held: list[Any]) -> None:
        held.clear()
        # Java objects the function kept alive are queued for release now, and
        # the client may not make a request for a long time
        client = self._client()
        if client is not None:
            client._release_on_channel(self)

    def _discard(self, reply: bytes) -> None:
        """A response whose request stopped waiting for it (it timed out). The
        server registered the objects in it for the client all the same."""
        logger.debug("Dropped a response for a call that is over")
        client = self._client()
        if client is not None:
            client._release_unread_response(reply)

    def send(self, data: bytes | bytearray) -> None:
        """Send one frame. Raises OSError once the connection is closed."""
        frame = _frame(data)
        with self._send_lock:
            self._sock.sendall(frame)

    def send_answer(self, data: bytes | bytearray) -> None:
        """Send a callback's answer, counting it."""
        frame = _frame(data)
        with self._send_lock:
            self.answers_sent += 1
            self._sock.sendall(frame)

    def open_call(self, call_id: int) -> ChannelCall:
        """The route for the callback that is about to run for this call."""
        call = ChannelCall(self, call_id)
        with self._calls_lock:
            self._calls[call_id] = call
            if self._closed:
                call.deliver(None)
        return call

    def _forget(self, call: ChannelCall) -> None:
        with self._calls_lock:
            self._calls.pop(call.call_id, None)

    def close(self) -> None:
        """Close the connection. Java threads waiting for Python on it fail."""
        with self._calls_lock:
            if self._closed:
                return
            self._closed = True
            # Callbacks waiting for Java to run something for them
            for call in self._calls.values():
                call.deliver(None)
        try:
            # shutdown() is what wakes the reader thread out of recv()
            self._sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        self._sock.close()


class AsyncChannelCall:
    """One call Java is waiting on, as the callback's route back to Java."""

    def __init__(self, channel: AsyncCallbackChannel, call_id: int):
        self.channel = channel
        self.call_id = call_id
        self._replies: asyncio.Queue[bytes | None] = asyncio.Queue()
        # One request of this call at a time; see ChannelCall
        self._exchange_lock = asyncio.Lock()
        self._answered = False
        self._lost = False
        self._in_flight = False
        self._finished = False
        # One Arrow transfer of this call at a time; see ChannelCall
        self.transfer_lock = asyncio.Lock()
        self.memory: CallMemory | None = None

    @property
    def active(self) -> bool:
        """Whether Java is still waiting for the callback's answer."""
        return not self._answered

    async def answer(self, frame: bytes | bytearray) -> None:
        """Send the callback's answer. No request goes out after it."""
        # Frames go out in the order their senders got here: the connection's
        # send lock is fair, and nothing awaits between this flag and it
        self._answered = True
        await self.channel.send_answer(frame)

    def abandon(self) -> None:
        """Java has stopped waiting for the callback (its thread was interrupted)."""
        self._answered = True

    async def request(
        self, frame: bytes | bytearray, timeout: float | None
    ) -> bytes | None:
        """Send a command for Java to run and wait for its response.

        Returns None, having sent nothing, if Java is no longer waiting for the
        callback: the caller is a task that outlived it. Raises TimeoutError if
        no response came in time, and from then on, since the late response
        would be taken for the next one; OSError if the connection closed.
        """
        async with self._exchange_lock:
            if self._lost:
                raise TimeoutError("an earlier call of this callback was abandoned")
            if self._answered:
                return None
            self._in_flight = True
            try:
                # From here on the frame may be on its way, whatever interrupts
                # this task: a cancellation can land in the middle of the send
                await self.channel.send(frame)
                reply = await asyncio.wait_for(self._replies.get(), timeout)
            except TimeoutError:
                self._give_up()
                raise TimeoutError("no response from Java") from None
            except OSError:
                # The connection is closed: no response is on its way, and the
                # next request fails the same way
                raise
            except BaseException:
                self._give_up()
                raise
            finally:
                self._in_flight = False
                if self._finished:
                    self.channel._forget(self)
            if reply is None:
                raise OSError("The callback connection closed")
            return reply

    def finish(self) -> None:
        """The callback is done. A request of it that is still waiting for its
        response, in a task that outlived it, keeps the route until it has it."""
        self._answered = True
        self._finished = True
        if not self._in_flight:
            self.channel._forget(self)
        # See ChannelCall.finish
        self.memory = None

    def _give_up(self) -> None:
        """Stop waiting for the response to a request that was sent. It may
        still come, or be here already, and nobody will read it."""
        self._lost = True
        while not self._replies.empty():
            unread = self._replies.get_nowait()
            if unread is not None:
                self.channel._discard(unread)

    def deliver(self, reply: bytes | None) -> None:
        """Called by the connection's reader; None once the connection is gone."""
        if self._lost and reply is not None:
            self.channel._discard(reply)
        else:
            self._replies.put_nowait(reply)


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
        self._routes: dict[int, AsyncChannelCall] = {}
        # Counted before the answer goes out, and so before its result is dropped
        self.answers_sent = 0
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
                if resp.CallbackReleased():
                    self._callback_released(resp.CallbackId())
                    continue
                if not resp.IsCallback():
                    # The response to a command a callback sent
                    route = self._routes.get(resp.CallId())
                    if route is not None:
                        route.deliver(data)
                    else:
                        self._discard(data)
                    continue
                client = self._client()
                if client is None:
                    return
                # Looked up before the next frame is read; see CallbackChannel
                handler = client._callbacks.get(resp.CallbackId())
                # A task per call: calls overlap, and one may be what another
                # is waiting for
                call = asyncio.create_task(
                    client._serve_channel_callback(self, resp, handler)
                )
                self._calls.add(call)
                call.add_done_callback(self._calls.discard)
                # Not held while waiting for the next frame
                del client, handler
        except asyncio.IncompleteReadError:
            pass  # the connection ended
        except OSError as e:
            if not self._closed:
                logger.warning("The callback connection failed: %s", e)
        finally:
            self.close()

    def _callback_released(self, callback_id: int) -> None:
        """Java's garbage collector took a callback's proxy; see CallbackChannel."""
        client = self._client()
        if client is None or client._callbacks.pop(callback_id, None) is None:
            return
        logger.debug("Callback %d dropped: Java no longer holds its proxy", callback_id)
        # Java objects the function kept alive are queued for release now, and
        # the client may not make a request for a long time
        release = asyncio.create_task(client._release_on_channel(self))
        self._calls.add(release)
        release.add_done_callback(self._calls.discard)

    def _discard(self, reply: bytes) -> None:
        """A response whose request stopped waiting for it; see CallbackChannel."""
        logger.debug("Dropped a response for a call that is over")
        client = self._client()
        if client is not None:
            client._release_unread_response(reply)

    async def send(self, data: bytes | bytearray) -> None:
        """Send one frame. Raises OSError once the connection is closed."""
        if self._closed:
            raise OSError("The callback connection is closed")
        frame = _frame(data)
        async with self._send_lock:
            self._writer.write(frame)
            await self._writer.drain()

    async def send_answer(self, data: bytes | bytearray) -> None:
        """Send a callback's answer, counting it."""
        if self._closed:
            raise OSError("The callback connection is closed")
        self.answers_sent += 1
        await self.send(data)

    def open_call(self, call_id: int) -> AsyncChannelCall:
        """The route for the callback that is about to run for this call."""
        route = AsyncChannelCall(self, call_id)
        self._routes[call_id] = route
        if self._closed:
            route.deliver(None)
        return route

    def _forget(self, route: AsyncChannelCall) -> None:
        self._routes.pop(route.call_id, None)

    def close(self) -> None:
        """Close the connection and cancel the callbacks still running on it."""
        if self._closed:
            return
        self._closed = True
        # Callbacks waiting for Java to run something for them
        for route in self._routes.values():
            route.deliver(None)
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
