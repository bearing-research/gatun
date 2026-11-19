"""Async support for Gatun client.

This module provides:
- AsyncGatunClient: Full async/await client using asyncio
- run_sync: Utility to run sync GatunClient methods in a thread pool

Example using AsyncGatunClient:
    async with AsyncGatunClient(socket_path) as client:
        arr = await client.create_object("java.util.ArrayList")
        await arr.add("hello")
        size = await arr.size()

Example using run_sync with existing sync client:
    client = GatunClient(socket_path)
    client.connect()

    # In async context:
    result = await run_sync(client.create_object, "java.util.ArrayList")
"""

import asyncio
import inspect
import logging
import mmap
import os
import struct
import weakref
from collections import deque
from collections.abc import Awaitable, Callable
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from typing import TYPE_CHECKING, Any, cast

import flatbuffers
import pyarrow as pa

if TYPE_CHECKING:
    from gatun.arena import AsyncScopedArenaContext

from gatun.callback_channel import (
    AsyncCallbackChannel,
    AsyncChannelCall,
    CallMemory,
)
from gatun.client import (
    _CALL_ENDED,
    _OPEN_CALL_MEMORY,
    DEFAULT_CALLBACK_TIMEOUT,
    DEFAULT_SOCKET_TIMEOUT,
    HANDSHAKE_TIMEOUT,
    MAX_CALLBACK_ERROR_SIZE,
    PROTOCOL_VERSION,
    RESPONSE_ZONE_SIZE,
    CallbackTimeoutError,
    DeadConnectionError,
    JavaException,
    JavaObject,
    PayloadTooLargeError,
    ProtocolDesyncError,
    ReentrancyError,
    SocketTimeoutError,
    _arrow_stream_size,
    _arrow_table_to_send,
    _build_free_command,
    _callback_target,
    _channel_call_of,
    _close_shared_memory,
    _enter_callback,
    _exit_callback,
    _finish_arrow_from_object,
    _finish_arrow_to_object,
    _finish_register_callback,
    _in_callback_of,
    _LRUCache,
    _nested_command,
    _raise_java_exception_impl,
    _read_arrow_stream,
    _release_unread_response,
    _remember_reflection,
    _require_payload_room,
    _take_free_chunk,
    _unpack_value_impl,
    _usable_timeout,
    _ValueEncoder,
    _write_arrow_table,
)
from gatun.generated.org.gatun.protocol import Action as Act
from gatun.generated.org.gatun.protocol import Command as Cmd
from gatun.generated.org.gatun.protocol import (
    CreateObjectsRequest,
    GetFieldsRequest,
    InvokeMethodsRequest,
    MethodCall,
    ObjectSpec,
    Response,
)

logger = logging.getLogger(__name__)

# Default thread pool for run_sync
_default_executor: ThreadPoolExecutor | None = None


def get_default_executor() -> ThreadPoolExecutor:
    """Get or create the default thread pool executor."""
    global _default_executor
    if _default_executor is None:
        _default_executor = ThreadPoolExecutor(
            max_workers=4, thread_name_prefix="gatun-"
        )
    return _default_executor


async def run_sync[T](
    func: Callable[..., T],
    *args,
    executor: ThreadPoolExecutor | None = None,
    **kwargs,
) -> T:
    """Run a synchronous function in a thread pool executor.

    This is useful for running blocking GatunClient methods in an async context.

    Args:
        func: The synchronous function to run
        *args: Positional arguments to pass to the function
        executor: Optional custom executor. Uses default if not provided.
        **kwargs: Keyword arguments to pass to the function

    Returns:
        The result of the function call

    Example:
        client = GatunClient(socket_path)
        client.connect()

        # Run sync method in thread pool
        result = await run_sync(client.create_object, "java.util.ArrayList")
        await run_sync(result.add, "hello")
        size = await run_sync(result.size)
    """
    loop = asyncio.get_running_loop()
    if executor is None:
        executor = get_default_executor()

    if kwargs:
        func = partial(func, **kwargs)

    return await loop.run_in_executor(executor, func, *args)


class AsyncJavaObject:
    """Async wrapper for Java objects returned from the JVM."""

    def __init__(self, client: "AsyncGatunClient", object_id: int):
        self._client = client
        self.object_id = object_id
        # A finalizer cannot await, so it only queues the ID; the client sends
        # the queue ahead of its next request
        self._finalizer = weakref.finalize(self, client._queue_free, object_id)

    def __getattr__(self, name: str):
        """Return an async method invoker for the given name."""
        if name.startswith("_"):
            raise AttributeError(name)
        return _AsyncMethodInvoker(self, name)

    def __repr__(self):
        return f"<AsyncJavaObject id={self.object_id}>"

    def detach(self) -> None:
        """Keep the Java object alive after this wrapper is garbage collected."""
        self._finalizer.detach()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self._finalizer.detach()
        await self._client.free_object(self.object_id)
        return False


class _AsyncMethodInvoker:
    """Callable that invokes a method asynchronously on a Java object."""

    def __init__(self, target: AsyncJavaObject, method_name: str):
        # The wrapper itself, not just its ID: in `await (await make()).size()`
        # this is the only reference left to the wrapper, and a collected wrapper
        # frees its Java object ahead of the very call being made on it
        self._target = target
        self._method_name = method_name

    async def __call__(self, *args):
        target = self._target
        return await target._client.invoke_method(
            target.object_id, self._method_name, *args
        )


class AsyncJVMView:
    """Async package-style navigation for Java classes."""

    def __init__(self, client: "AsyncGatunClient", path: str = ""):
        self._client = client
        self._path = path

    def __getattr__(self, name: str):
        if name.startswith("_"):
            raise AttributeError(name)
        new_path = f"{self._path}.{name}" if self._path else name
        return AsyncJavaClass(self._client, new_path)


class AsyncJavaClass:
    """Async callable that creates objects or invokes static methods."""

    def __init__(self, client: "AsyncGatunClient", path: str):
        self._client = client
        self._path = path

    def __getattr__(self, name: str):
        if name.startswith("_"):
            raise AttributeError(name)
        return AsyncJavaClass(self._client, f"{self._path}.{name}")

    async def __call__(self, *args):
        # The server says whether the path is a class or a static method. The
        # spelling only decides when it cannot tell (a class that will not load,
        # which create_object then reports properly).
        kind = await self._client._reflect_cached(self._path)
        class_name, _, member = self._path.rpartition(".")
        if kind == "field":
            raise TypeError(f"{self._path} is a static field, not a method")
        if kind == "none":
            kind = "method" if class_name and member[:1].islower() else "class"
        if kind == "method":
            if not class_name:
                raise ValueError(f"Invalid method path: {self._path}")
            return await self._client.invoke_static_method(class_name, member, *args)
        return await self._client.create_object(self._path, *args)

    def __repr__(self):
        return f"<AsyncJVM {self._path}>"


class AsyncGatunClient(_ValueEncoder):
    """Async client for communicating with the Gatun Java server.

    This client uses asyncio for non-blocking I/O operations. All Java
    interactions are performed asynchronously.

    Example:
        async with AsyncGatunClient(socket_path) as client:
            # Create Java objects
            arr = await client.create_object("java.util.ArrayList")
            await arr.add("hello")
            await arr.add("world")
            size = await arr.size()  # Returns 2

            # Use JVM view for package navigation
            Integer = client.jvm.java.lang.Integer
            result = await Integer.parseInt("42")
    """

    _object_ref_types = (JavaObject, AsyncJavaObject)

    def __init__(
        self,
        socket_path: str | None = None,
        callback_timeout: float | None = DEFAULT_CALLBACK_TIMEOUT,
        socket_timeout: float | None = DEFAULT_SOCKET_TIMEOUT,
        handshake_timeout: float | None = HANDSHAKE_TIMEOUT,
    ):
        """Initialize an async Gatun client.

        Args:
            socket_path: Path to the Unix domain socket for communication.
                        Defaults to ~/gatun.sock.
            callback_timeout: Timeout in seconds for callback execution.
                             Prevents hung callbacks from deadlocking the protocol.
                             Defaults to DEFAULT_CALLBACK_TIMEOUT (30s).
                             Set to None to disable timeout (not recommended).
            socket_timeout: Timeout in seconds for socket read operations.
                           Used for response reads to prevent indefinite hangs.
                           Defaults to DEFAULT_SOCKET_TIMEOUT (30s).
                           Set to None to disable timeout (not recommended).
            handshake_timeout: Timeout in seconds for the initial handshake.
                              Shorter than socket_timeout since handshake should be fast.
                              Defaults to HANDSHAKE_TIMEOUT (10s).
                              Set to None to disable timeout (not recommended).
        """
        if socket_path is None:
            socket_path = os.path.expanduser("~/gatun.sock")

        self.socket_path = socket_path
        self.memory_path: str | None = (
            None  # Set during connect() from server handshake
        )
        # None disables the corresponding timeout
        self.callback_timeout = _usable_timeout(callback_timeout)
        self.socket_timeout = _usable_timeout(socket_timeout)
        self.handshake_timeout = _usable_timeout(handshake_timeout)
        # Server process launched for this client by aconnect(); stopped on close()
        self._server_session: Any = None

        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None
        self._lock = asyncio.Lock()

        self.shm_file: Any = None  # BufferedRandom when connected
        self.shm: mmap.mmap | None = None

        self.memory_size = 0
        self.command_offset = 0
        self.payload_offset = 65536  # 64KB - must match GatunServer.PAYLOAD_OFFSET
        self.response_offset = 0

        self._jvm: AsyncJVMView | None = None
        self._callbacks: dict[int, Any] = {}

        # Where Java threads other than the one running a request call Python.
        # Opened with the first register_callback; see _ensure_callback_channel
        self._callback_channel: AsyncCallbackChannel | None = None
        self._callback_channel_tried = False
        self._callback_channel_lock = asyncio.Lock()

        # Tasks that are running a callback. A Java call from one of them would
        # wait for a channel that is waiting for it. Other tasks are unaffected:
        # they queue on the lock like any other request.
        self._callback_tasks: set[asyncio.Task[Any]] = set()

        # Set once an exchange was abandoned midway (timeout, cancellation, bad
        # response). Its reply may still arrive, and the next request would take
        # it for its own, so the connection is not used again.
        self._dead = False
        self._closed = False
        # Why the last connect() failed, for callers that report it
        self.connect_error: BaseException | None = None

        # IDs of collected AsyncJavaObjects, released ahead of the next request
        self._pending_frees: deque[int] = deque()
        self._free_builder = flatbuffers.Builder(64)

        # What the server said each dotted path is (class, method, field)
        self._reflect_cache: _LRUCache[str] = _LRUCache(1024)

    @property
    def jvm(self) -> AsyncJVMView:
        """Access Java classes via package navigation."""
        if self._jvm is None:
            self._jvm = AsyncJVMView(self)
        return self._jvm

    async def _read_with_timeout(self, n: int, timeout: float | None) -> bytes:
        """Read exactly n bytes with optional timeout.

        Args:
            n: Number of bytes to read
            timeout: Timeout in seconds, or None for no timeout

        Returns:
            The bytes read

        Raises:
            SocketTimeoutError: If the read times out
            asyncio.IncompleteReadError: If connection closes before n bytes
        """
        if timeout is None:
            return await self._reader.readexactly(n)
        try:
            return await asyncio.wait_for(self._reader.readexactly(n), timeout=timeout)
        except TimeoutError:
            raise SocketTimeoutError(f"Socket read timed out after {timeout}s")

    async def connect(self) -> bool:
        """Connect to the Gatun server asynchronously."""
        try:
            self._reader, self._writer = await asyncio.open_unix_connection(
                self.socket_path
            )

            # Read handshake header with timeout
            # Format: [4 bytes: version] [4 bytes: arena_epoch] [8 bytes: memory size]
            #         [2 bytes: shm_path_length] [N bytes: shm_path (UTF-8)]
            handshake_header = await self._read_with_timeout(18, self.handshake_timeout)
            server_version, _, self.memory_size, shm_path_len = struct.unpack(
                "<IIQH", handshake_header
            )

            if server_version != PROTOCOL_VERSION:
                raise RuntimeError(
                    f"Protocol version mismatch: client={PROTOCOL_VERSION}, "
                    f"server={server_version}"
                )

            # Read SHM path with timeout
            shm_path_bytes = await self._read_with_timeout(
                shm_path_len, self.handshake_timeout
            )
            self.memory_path = shm_path_bytes.decode("utf-8")

            self.response_offset = self.memory_size - 65536  # 64KB response zone

            # Map shared memory (still sync - mmap doesn't have async API). Opening a
            # local file is effectively instant, and the handle is held for the life
            # of the mapping and released in close().
            self.shm_file = open(self.memory_path, "r+b")  # noqa: ASYNC230, SIM115
            self.shm = mmap.mmap(self.shm_file.fileno(), self.memory_size)

            self.connect_error = None
            return True
        except Exception as e:  # noqa: BLE001 - connect() reports every failure as False
            logger.error("Connection failed: %s", e)
            self.connect_error = e
            return False

    def _release_unread_response(self, reply: bytes) -> None:
        _release_unread_response(self, reply)

    def _queue_free(self, object_id: int) -> None:
        """Queue a Java object for release. Called by AsyncJavaObject finalizers."""
        if not self._dead and not self._closed:
            self._pending_frees.append(object_id)

    def _mark_dead(self) -> None:
        """Stop using the connection after an exchange was abandoned midway."""
        if self._dead:
            return
        self._dead = True
        self._pending_frees.clear()
        channel, self._callback_channel = self._callback_channel, None
        if channel is not None:
            channel.close()
        writer, self._writer = self._writer, None
        self._reader = None
        if writer is not None:
            try:
                writer.close()
            except (OSError, RuntimeError):
                pass  # already closed, or its event loop is gone

    @property
    def _in_callback(self) -> bool:
        """Whether a callback is running, in any task."""
        return bool(self._callback_tasks)

    async def _send_raw(
        self,
        data: bytes,
        wait_for_response: bool = True,
        expects_response: bool = True,
        prepare: Callable[[], None] | None = None,
        finish: Callable[[Any], Any] | None = None,
    ):
        """Write command to SHM and signal Java asynchronously.

        `prepare` runs before the command is written and `finish` is applied to
        the response, both while this request holds the channel. A request that
        carries data in the payload zone must touch the zone only from there:
        another task's request can run before the lock is taken or after it is
        released, and it uses the same zone.
        """
        self._require_usable(expects_response)

        # Inside a callback that came on the callback connection, the call goes
        # back the same way: the Java thread waiting for the callback runs it,
        # and the client's own channel is left alone
        call = _channel_call_of(self) if expects_response else None
        if call is not None:
            routed, result = await self._send_nested(call, data)
            if routed:
                return result

        async with self._lock:
            if self._dead or self._writer is None:
                raise DeadConnectionError("Connection is closed. Create a new client.")
            max_command_size = self.payload_offset
            if len(data) > max_command_size:
                raise PayloadTooLargeError(len(data), max_command_size, "Command")

            # Release objects whose wrappers were garbage collected
            while self._pending_frees:
                ids = _take_free_chunk(self._pending_frees)
                if not ids:
                    break
                channel = self._callback_channel
                answers_sent = channel.answers_sent if channel is not None else 0
                try:
                    await self._exchange(
                        _build_free_command(self._free_builder, ids, answers_sent)
                    )
                except JavaException as e:
                    logger.debug("FreeObject for %d objects failed: %s", len(ids), e)

            if prepare is not None:
                prepare()
            response = await self._exchange(data, wait_for_response)
            if wait_for_response:
                return finish(response) if finish is not None else response

    def _require_usable(self, expects_response: bool = True) -> None:
        if self._closed:
            raise DeadConnectionError("Client is closed. Create a new client.")
        if self._dead:
            raise DeadConnectionError(
                "Connection is dead after a timeout, cancellation or protocol "
                "error. Create a new client."
            )
        # A call from a callback would deadlock: from the task running it, or from
        # a task it started and may be waiting for (gather, create_task)
        if expects_response and (
            asyncio.current_task() in self._callback_tasks or _in_callback_of(self)
        ):
            raise ReentrancyError(
                "Cannot call Java from within a callback. "
                "Nested calls would deadlock because the original request is still waiting. "
                "Queue work for later or restructure code to avoid nested calls."
            )

    async def _call_memory(self, call: AsyncChannelCall) -> CallMemory | None:
        """The call's shared memory, which Java creates when first asked.

        None if Java is no longer waiting for the callback. The caller holds
        the call's transfer lock.
        """
        memory = call.memory
        if memory is None:
            routed, path = await self._send_nested(call, _OPEN_CALL_MEMORY)
            if not routed:
                return None
            try:
                memory = CallMemory(path)
            except FileNotFoundError:
                # Java stopped waiting for the callback after it replied, and
                # removed the file
                call.abandon()
                return None
            if call.active:
                call.memory = memory
        elif memory.removed():
            # Java stopped waiting for the callback (its thread was
            # interrupted) and this callback has not sent a command since
            call.abandon()
            call.memory = None
            return None
        return memory

    async def _send_nested(
        self, call: AsyncChannelCall, data: bytes
    ) -> tuple[bool, Any]:
        """Send a command on the callback connection, as part of `call`.

        Returns (False, None) if Java is no longer waiting for the callback,
        in which case nothing was sent and the command belongs on the client's
        own channel.
        """
        frame = _nested_command(call.call_id, data)
        # A callback that runs for hours would otherwise hold on to every
        # object it dropped until it returns
        if self._pending_frees:
            await self._release_on_channel(call.channel)
        try:
            reply = await call.request(frame, self.socket_timeout)
        except TimeoutError as e:
            raise SocketTimeoutError(
                f"A Java call made inside a callback got no answer: {e}"
            ) from None
        except OSError as e:
            raise DeadConnectionError(
                f"The callback connection closed while a callback was calling Java: {e}"
            ) from None
        if reply is None:
            return False, None
        resp = Response.Response.GetRootAsResponse(reply, 0)
        if resp.IsError() and resp.ErrorType() == _CALL_ENDED:
            # The Java thread gave up on the callback (it was interrupted) and
            # did not run the command
            call.abandon()
            return False, None
        return True, self._parse_response(resp)

    async def _exchange(self, data: bytes, wait_for_response: bool = True):
        """One command and its response. The caller holds the lock."""
        assert self.shm is not None and self._writer is not None
        # Write to shared memory (sync - mmap doesn't have async API)
        self.shm.seek(self.command_offset)
        self.shm.write(data)

        try:
            # Signal Java
            self._writer.write(struct.pack("<I", len(data)))
            await self._writer.drain()
            if not wait_for_response:
                return None
            return await self._read_response()
        except JavaException:
            raise  # a complete response that happens to be an error
        except BaseException as e:
            # Whatever else ends the exchange (a timeout, the task being
            # cancelled, a closed socket) leaves a reply that may still arrive
            if not getattr(e, "_gatun_complete", False):
                self._mark_dead()
            raise

    async def _read_response(self):
        """Read and parse response from the server.

        Raises:
            SocketTimeoutError: If the socket read times out
            asyncio.IncompleteReadError: If socket closes unexpectedly
            ProtocolDesyncError: If the response size is invalid
        """
        # What the last in-band callback answered with; see the sync client's
        # _read_response
        answered_with: list[Any] = []
        while True:
            # Read response length with timeout
            length_data = await self._read_with_timeout(4, self.socket_timeout)
            response_len = struct.unpack("<I", length_data)[0]
            answered_with.clear()

            # Validate response size before reading from SHM
            # This catches protocol desync (garbage data) early
            max_response_size = self.memory_size - self.response_offset
            if response_len == 0:
                raise ProtocolDesyncError(
                    "Invalid response size: 0 bytes (empty response)",
                    response_size=response_len,
                )
            if response_len > RESPONSE_ZONE_SIZE:
                raise ProtocolDesyncError(
                    f"Response size {response_len} exceeds RESPONSE_ZONE_SIZE ({RESPONSE_ZONE_SIZE})",
                    response_size=response_len,
                )
            if response_len > max_response_size:
                raise ProtocolDesyncError(
                    f"Response size {response_len} exceeds available SHM space ({max_response_size})",
                    response_size=response_len,
                )

            # Read response from shared memory
            self.shm.seek(self.response_offset)
            response_data = self.shm.read(response_len)

            # Parse response
            resp = Response.Response.GetRootAsResponse(response_data, 0)

            # Check if this is a callback request from Java
            if resp.IsCallback():
                answered_with.append(await self._handle_callback(resp))
                # After handling callback, continue reading for the actual response
                continue

            return self._parse_response(resp)

    def _parse_response(self, resp) -> Any:
        """What a response means to the caller: its value, or its exception."""
        if resp.IsError():
            error_msg = resp.ErrorMsg().decode("utf-8")
            error_type_bytes = resp.ErrorType()
            error_type = (
                error_type_bytes.decode("utf-8")
                if error_type_bytes
                else "java.lang.RuntimeException"
            )
            try:
                _raise_java_exception_impl(error_type, error_msg)
            except Exception as e:
                # Not every server error maps to a JavaException subclass
                # (PayloadTooLargeError, CancelledException); all of them
                # arrived as a complete response
                e._gatun_complete = True  # type: ignore[attr-defined]
                raise

        # Location of an Arrow IPC stream (arrow_from_java)
        arrow_stream = resp.ArrowStream()
        if arrow_stream is not None:
            return arrow_stream.Offset(), arrow_stream.Length()

        return self._unpack_value(resp.ReturnValType(), resp.ReturnVal())

    async def _handle_callback(
        self,
        resp,
        reply: Callable[[Any, bool, str | None], Awaitable[None]] | None = None,
        channel_call: AsyncChannelCall | None = None,
        handler: Any = None,
    ):
        """Handle a callback invocation request from Java.

        Without `reply` the request came in band: the client's own request is
        waiting on this callback, and the answer goes back on its channel. The
        task is recorded while its callback runs, so that a Java call made
        from it is refused instead of deadlocking.

        With `reply` (result, is_error, error message) the request came on the
        callback connection, from some other Java thread, and `channel_call`
        is the call that thread is waiting on. The callback may call Java:
        its calls, and those of tasks it starts, travel on the callback
        connection as part of that call.
        """
        callback_id = resp.CallbackId()
        in_band = reply is None

        async def answer(result: Any, is_error: bool, error_msg: str | None) -> None:
            if reply is None:
                await self._send_callback_response(
                    callback_id, result, is_error, error_msg
                )
            else:
                await reply(result, is_error, error_msg)

        # Execute the callback with timeout and, in band, the reentrancy guard
        task = asyncio.current_task() if in_band else None
        if task is not None:
            self._callback_tasks.add(task)
        scope_token = _enter_callback(self, channel_call)
        try:
            # Unpacked even when the callback is gone (unregistered while Java
            # was calling it): the server registered the objects among the
            # arguments for this client, and their wrappers are what frees
            # them. Inside the try, because Java is waiting for an answer
            # whatever goes wrong here.
            args = []
            for i in range(resp.CallbackArgsLength()):
                arg = resp.CallbackArgs(i)
                args.append(self._unpack_value(arg.ValType(), arg.Val()))

            if handler is None:
                handler = self._callbacks.get(callback_id)
            if handler is None:
                raise LookupError(f"Callback {callback_id} not found")
            callback_fn = _callback_target(handler, resp.CallbackMethod())
            if inspect.iscoroutinefunction(callback_fn):
                pending = callback_fn(*args)
            else:
                # A sync callback runs in a worker thread so it cannot stall the loop
                pending = asyncio.get_running_loop().run_in_executor(
                    None, callback_fn, *args
                )
            timeout = self.callback_timeout
            if timeout is not None:
                try:
                    result = await asyncio.wait_for(pending, timeout=timeout)
                except TimeoutError:
                    raise CallbackTimeoutError(
                        f"Callback execution timed out after {timeout}s"
                    )
            else:
                result = await pending
            await answer(result, False, None)
            return result
        except ReentrancyError as e:
            # Propagate reentrancy error with clear message
            await answer(None, True, f"ReentrancyError: {e}")
        except Exception as e:  # noqa: BLE001 - any callback failure is forwarded to Java
            await answer(None, True, str(e))
        finally:
            _exit_callback(*scope_token)
            if task is not None:
                self._callback_tasks.discard(task)

    async def _serve_channel_callback(
        self, channel: AsyncCallbackChannel, resp, handler: Any = None
    ) -> None:
        """Run one callback that arrived on the callback connection.

        Runs in a task of its own, started by the connection's reader.
        """
        callback_id = resp.CallbackId()
        call_id = resp.CallId()
        channel_call = channel.open_call(call_id)
        answered = False

        async def reply(result: Any, is_error: bool, error_msg: str | None) -> None:
            nonlocal answered
            answered = True
            await channel_call.answer(
                self._build_callback_response(
                    callback_id, result, is_error, error_msg, call_id
                )
            )

        try:
            await self._handle_callback(resp, reply, channel_call, handler)
        except OSError as e:
            # The connection closed under the callback; Java was told by that
            logger.debug("Callback %d could not be answered: %s", callback_id, e)
        except Exception as e:
            # A Java thread is waiting for this call and nothing else ends its wait
            if not answered:
                try:
                    await reply(None, True, f"{type(e).__name__}: {e}")
                except OSError:
                    pass
            raise
        finally:
            channel_call.finish()
            await self._release_on_channel(channel)

    async def _release_on_channel(self, channel: AsyncCallbackChannel) -> None:
        """Send the queued object releases on the callback connection.

        They normally ride ahead of the client's next request. A client that
        only receives callbacks, or sits in one long request, makes none, and
        the arguments of every call would stay registered on the server.
        """
        frees = self._pending_frees
        while frees:
            ids = _take_free_chunk(frees)
            if not ids:
                return
            try:
                await channel.send(_build_free_command(flatbuffers.Builder(64), ids))
            except OSError:
                # The connection is gone; the next request can still send them
                frees.extend(ids)
                return

    async def _ensure_callback_channel(self) -> None:
        """Open the callback connection, the first time a callback is registered.

        The server opens a socket for this session and says where; whoever
        connects to it is this session's callback connection. Without it
        callbacks still work on the thread that runs the request, so a failure
        to open it is logged and not raised: the server tells any other thread
        that calls a callback why it cannot.
        """
        # Tasks registering callbacks at the same time open it once
        async with self._callback_channel_lock:
            if self._callback_channel_tried:
                return
            builder = flatbuffers.Builder(64)
            Cmd.CommandStart(builder)
            Cmd.CommandAddAction(builder, Act.Action.OpenCallbackChannel)
            builder.Finish(Cmd.CommandEnd(builder))

            # Only after the request went out: one that could not be sent at
            # all (a call from inside a callback) is worth trying again
            path: str | None = None
            try:
                path = cast(str, await self._send_raw(builder.Output()))
                self._callback_channel_tried = True
                self._callback_channel = await AsyncCallbackChannel.open(
                    self, path, self.handshake_timeout
                )
            except (JavaException, OSError) as e:
                self._callback_channel_tried = True
                logger.warning(
                    "Could not open the callback connection%s: %s. Callbacks will "
                    "work only on the Java thread that runs the request.",
                    f" at {path}" if path else "",
                    e,
                )

    async def _send_callback_response(
        self, callback_id: int, result, is_error: bool, error_msg: str | None
    ):
        """Send the result of an in-band callback back to Java, on the channel of
        the request that is waiting for it."""
        data = self._build_callback_response(callback_id, result, is_error, error_msg)

        # Write to command zone
        self.shm.seek(self.command_offset)
        self.shm.write(data)

        # Signal Java
        self._writer.write(struct.pack("<I", len(data)))
        await self._writer.drain()

    def _build_callback_response(
        self,
        callback_id: int,
        result,
        is_error: bool,
        error_msg: str | None,
        call_id: int = 0,
    ) -> bytes:
        """Build the CallbackResponse command that answers a callback.

        call_id is the call's ID on the callback connection; 0 in band.
        """
        builder = flatbuffers.Builder(1024)

        # Build arguments: [result, is_error]
        arg_offsets = []

        # First arg: the result value
        result_offset = self._build_argument(builder, result)
        arg_offsets.append(result_offset)

        # Second arg: is_error flag
        error_offset = self._build_argument(builder, is_error)
        arg_offsets.append(error_offset)

        # Build arguments vector
        Cmd.CommandStartArgsVector(builder, len(arg_offsets))
        for offset in reversed(arg_offsets):
            builder.PrependUOffsetTRelative(offset)
        args_vec = builder.EndVector()

        # Build target_name (error message if error)
        error_name_off = None
        if error_msg:
            # Java is waiting for this response, so it has to fit the command zone
            if len(error_msg) > MAX_CALLBACK_ERROR_SIZE:
                error_msg = error_msg[:MAX_CALLBACK_ERROR_SIZE] + "...<truncated>"
            error_name_off = builder.CreateString(error_msg)

        # Build command
        Cmd.CommandStart(builder)
        Cmd.CommandAddAction(builder, Act.Action.CallbackResponse)
        Cmd.CommandAddArgs(builder, args_vec)
        if error_name_off:
            Cmd.CommandAddTargetName(builder, error_name_off)
        Cmd.CommandAddCallId(builder, call_id)
        cmd = Cmd.CommandEnd(builder)
        builder.Finish(cmd)

        # The command zone in band, a frame on the callback connection: the
        # same size
        data = bytes(builder.Output())
        if len(data) > self.payload_offset and not is_error:
            return self._build_callback_response(
                callback_id,
                None,
                True,
                f"Callback result is too large to return: {len(data):,} bytes "
                f"exceeds the {self.payload_offset:,} bytes an answer can take",
                call_id,
            )
        return data

    def _unpack_value(self, val_type: int, val_table: Any) -> Any:
        """Unpack a FlatBuffer Value union to a Python object."""
        return _unpack_value_impl(val_type, val_table, self, AsyncJavaObject)

    async def create_object(self, class_name: str, *args) -> AsyncJavaObject:
        """Create a new Java object asynchronously."""
        builder = flatbuffers.Builder(1024)
        name_off = builder.CreateString(class_name)

        arg_offsets = []
        for arg in args:
            arg_offsets.append(self._build_argument(builder, arg))

        args_vec = None
        if arg_offsets:
            Cmd.CommandStartArgsVector(builder, len(arg_offsets))
            for offset in reversed(arg_offsets):
                builder.PrependUOffsetTRelative(offset)
            args_vec = builder.EndVector()

        Cmd.CommandStart(builder)
        Cmd.CommandAddAction(builder, Act.Action.CreateObject)
        Cmd.CommandAddTargetName(builder, name_off)
        if args_vec:
            Cmd.CommandAddArgs(builder, args_vec)
        cmd = Cmd.CommandEnd(builder)
        builder.Finish(cmd)

        return cast(AsyncJavaObject, await self._send_raw(builder.Output()))

    async def invoke_method(self, obj_id, method_name: str, *args):
        """Invoke a method on a Java object asynchronously."""
        if isinstance(obj_id, (JavaObject, AsyncJavaObject)):
            obj_id = obj_id.object_id

        builder = flatbuffers.Builder(1024)
        name_off = builder.CreateString(method_name)

        arg_offsets = []
        for arg in args:
            arg_offsets.append(self._build_argument(builder, arg))

        args_vec = None
        if arg_offsets:
            Cmd.CommandStartArgsVector(builder, len(arg_offsets))
            for offset in reversed(arg_offsets):
                builder.PrependUOffsetTRelative(offset)
            args_vec = builder.EndVector()

        Cmd.CommandStart(builder)
        Cmd.CommandAddAction(builder, Act.Action.InvokeMethod)
        Cmd.CommandAddTargetId(builder, obj_id)
        Cmd.CommandAddTargetName(builder, name_off)
        if args_vec:
            Cmd.CommandAddArgs(builder, args_vec)
        cmd = Cmd.CommandEnd(builder)
        builder.Finish(cmd)

        return await self._send_raw(builder.Output())

    async def invoke_static_method(self, class_name: str, method_name: str, *args):
        """Invoke a static method on a class asynchronously."""
        builder = flatbuffers.Builder(1024)
        full_name = f"{class_name}.{method_name}"
        name_off = builder.CreateString(full_name)

        arg_offsets = []
        for arg in args:
            arg_offsets.append(self._build_argument(builder, arg))

        args_vec = None
        if arg_offsets:
            Cmd.CommandStartArgsVector(builder, len(arg_offsets))
            for offset in reversed(arg_offsets):
                builder.PrependUOffsetTRelative(offset)
            args_vec = builder.EndVector()

        Cmd.CommandStart(builder)
        Cmd.CommandAddAction(builder, Act.Action.InvokeStaticMethod)
        Cmd.CommandAddTargetName(builder, name_off)
        if args_vec:
            Cmd.CommandAddArgs(builder, args_vec)
        cmd = Cmd.CommandEnd(builder)
        builder.Finish(cmd)

        return await self._send_raw(builder.Output())

    async def get_field(self, obj_id, field_name: str):
        """Get a field value from a Java object asynchronously."""
        if isinstance(obj_id, (JavaObject, AsyncJavaObject)):
            obj_id = obj_id.object_id

        builder = flatbuffers.Builder(256)
        name_off = builder.CreateString(field_name)

        Cmd.CommandStart(builder)
        Cmd.CommandAddAction(builder, Act.Action.GetField)
        Cmd.CommandAddTargetId(builder, obj_id)
        Cmd.CommandAddTargetName(builder, name_off)
        cmd = Cmd.CommandEnd(builder)
        builder.Finish(cmd)

        return await self._send_raw(builder.Output())

    async def set_field(self, obj_id, field_name: str, value):
        """Set a field value on a Java object asynchronously."""
        if isinstance(obj_id, (JavaObject, AsyncJavaObject)):
            obj_id = obj_id.object_id

        builder = flatbuffers.Builder(256)
        name_off = builder.CreateString(field_name)

        arg_offset = self._build_argument(builder, value)
        Cmd.CommandStartArgsVector(builder, 1)
        builder.PrependUOffsetTRelative(arg_offset)
        args_vec = builder.EndVector()

        Cmd.CommandStart(builder)
        Cmd.CommandAddAction(builder, Act.Action.SetField)
        Cmd.CommandAddTargetId(builder, obj_id)
        Cmd.CommandAddTargetName(builder, name_off)
        Cmd.CommandAddArgs(builder, args_vec)
        cmd = Cmd.CommandEnd(builder)
        builder.Finish(cmd)

        return await self._send_raw(builder.Output())

    async def is_instance_of(self, obj, class_name: str) -> bool:
        """Check if a Java object is an instance of a class asynchronously.

        This is equivalent to Java's `instanceof` operator.

        Args:
            obj: AsyncJavaObject instance or object ID
            class_name: Fully qualified Java class name
                       (e.g., "java.util.List", "java.util.ArrayList")

        Returns:
            True if the object is an instance of the specified class.
        """
        if isinstance(obj, (JavaObject, AsyncJavaObject)):
            obj_id = obj.object_id
        else:
            obj_id = obj

        builder = flatbuffers.Builder(256)
        name_off = builder.CreateString(class_name)

        Cmd.CommandStart(builder)
        Cmd.CommandAddAction(builder, Act.Action.IsInstanceOf)
        Cmd.CommandAddTargetId(builder, obj_id)
        Cmd.CommandAddTargetName(builder, name_off)
        cmd = Cmd.CommandEnd(builder)
        builder.Finish(cmd)

        return cast(bool, await self._send_raw(builder.Output()))

    async def ping(self) -> bool:
        """Check if the Java server is alive and responsive.

        This is a lightweight health check that sends a minimal request
        to the server and waits for a response. Useful for:
        - Verifying connection is still valid
        - Detecting stale connections before making expensive calls
        - Health checks in monitoring systems

        Returns:
            True if the server responded successfully.

        Raises:
            Various exceptions if the connection is dead or server unresponsive.

        Example:
            if await client.ping():
                print("Server is alive")
        """
        builder = flatbuffers.Builder(256)

        Cmd.CommandStart(builder)
        Cmd.CommandAddAction(builder, Act.Action.Ping)
        cmd = Cmd.CommandEnd(builder)
        builder.Finish(cmd)

        return cast(bool, await self._send_raw(builder.Output()))

    async def get_metrics(self) -> str:
        """Get server metrics report asynchronously.

        Returns a human-readable report of server performance metrics including:
        - Total request counts and error rates
        - Requests per second
        - Current and peak object counts
        - Arrow data transfer statistics
        - Callback statistics
        - Per-action latency percentiles (p50, p99)

        Returns:
            String containing the metrics report.

        Example:
            metrics = await client.get_metrics()
            print(metrics)
        """
        builder = flatbuffers.Builder(256)

        Cmd.CommandStart(builder)
        Cmd.CommandAddAction(builder, Act.Action.GetMetrics)
        cmd = Cmd.CommandEnd(builder)
        builder.Finish(cmd)

        return cast(str, await self._send_raw(builder.Output()))

    async def get_fields(self, obj, field_names: list[str]) -> list[Any]:
        """Get multiple field values from a Java object in a single round-trip.

        This is more efficient than calling get_field() multiple times when
        you need to read several fields from the same object.

        Args:
            obj: AsyncJavaObject instance or object ID
            field_names: List of field names to read

        Returns:
            List of field values in the same order as field_names.

        Example:
            sb = await client.create_object("java.lang.StringBuilder", "hello")
            values = await client.get_fields(sb, ["count"])  # [5]
        """
        if isinstance(obj, (JavaObject, AsyncJavaObject)):
            obj_id = obj.object_id
        else:
            obj_id = obj

        builder = flatbuffers.Builder(1024)

        # Build field names vector (strings must be created before the table)
        field_name_offsets = [builder.CreateString(name) for name in field_names]
        GetFieldsRequest.StartFieldNamesVector(builder, len(field_name_offsets))
        for offset in reversed(field_name_offsets):
            builder.PrependUOffsetTRelative(offset)
        field_names_vec = builder.EndVector()

        # Build GetFieldsRequest
        GetFieldsRequest.Start(builder)
        GetFieldsRequest.AddFieldNames(builder, field_names_vec)
        get_fields_off = GetFieldsRequest.End(builder)

        # Build Command
        Cmd.CommandStart(builder)
        Cmd.CommandAddAction(builder, Act.Action.GetFields)
        Cmd.CommandAddTargetId(builder, obj_id)
        Cmd.CommandAddGetFields(builder, get_fields_off)
        cmd = Cmd.CommandEnd(builder)
        builder.Finish(cmd)

        return cast(list[Any], await self._send_raw(builder.Output()))

    async def invoke_methods(
        self,
        obj,
        calls: list[tuple[str, tuple]],
        return_object_refs: bool | list[bool] = False,
    ) -> list[Any]:
        """Invoke multiple methods on the same Java object in a single round-trip.

        This is more efficient than calling invoke_method() multiple times when
        you need to call several methods on the same object.

        Args:
            obj: AsyncJavaObject instance or object ID
            calls: List of (method_name, args_tuple) pairs.
                   Each tuple is (method_name, (arg1, arg2, ...))
            return_object_refs: If True (or list of bools), return results as
                               ObjectRefs instead of auto-converting to Python.

        Returns:
            List of results in the same order as calls.

        Example:
            arr = await client.create_object("java.util.ArrayList")
            results = await client.invoke_methods(arr, [
                ("add", ("a",)),
                ("add", ("b",)),
                ("size", ()),
            ])
            # results = [True, True, 2]
        """
        if isinstance(obj, (JavaObject, AsyncJavaObject)):
            obj_id = obj.object_id
        else:
            obj_id = obj

        # Normalize return_object_refs to a list
        if isinstance(return_object_refs, bool):
            return_object_refs = [return_object_refs] * len(calls)

        builder = flatbuffers.Builder(4096)

        # Build each MethodCall
        method_call_offsets = []
        for i, (method_name, args) in enumerate(calls):
            # Build method name string
            method_name_off = builder.CreateString(method_name)

            # Build arguments
            arg_offsets = []
            for arg in args:
                arg_offset = self._build_argument(builder, arg)
                arg_offsets.append(arg_offset)

            args_vec = None
            if arg_offsets:
                MethodCall.StartArgsVector(builder, len(arg_offsets))
                for offset in reversed(arg_offsets):
                    builder.PrependUOffsetTRelative(offset)
                args_vec = builder.EndVector()

            # Build MethodCall
            MethodCall.Start(builder)
            MethodCall.AddMethodName(builder, method_name_off)
            if args_vec:
                MethodCall.AddArgs(builder, args_vec)
            if return_object_refs[i]:
                MethodCall.AddReturnObjectRef(builder, True)
            method_call_offsets.append(MethodCall.End(builder))

        # Build method calls vector
        InvokeMethodsRequest.StartMethodCallsVector(builder, len(method_call_offsets))
        for offset in reversed(method_call_offsets):
            builder.PrependUOffsetTRelative(offset)
        method_calls_vec = builder.EndVector()

        # Build InvokeMethodsRequest
        InvokeMethodsRequest.Start(builder)
        InvokeMethodsRequest.AddMethodCalls(builder, method_calls_vec)
        invoke_methods_off = InvokeMethodsRequest.End(builder)

        # Build Command
        Cmd.CommandStart(builder)
        Cmd.CommandAddAction(builder, Act.Action.InvokeMethods)
        Cmd.CommandAddTargetId(builder, obj_id)
        Cmd.CommandAddInvokeMethods(builder, invoke_methods_off)
        cmd = Cmd.CommandEnd(builder)
        builder.Finish(cmd)

        return cast(list[Any], await self._send_raw(builder.Output()))

    async def create_objects(
        self, specs: list[tuple[str, tuple]]
    ) -> list["AsyncJavaObject"]:
        """Create multiple Java objects in a single round-trip.

        This is more efficient than calling create_object() multiple times when
        you need to create several objects.

        Args:
            specs: List of (class_name, args_tuple) pairs.
                   Each tuple is (fully_qualified_class_name, (arg1, arg2, ...))

        Returns:
            List of AsyncJavaObject instances in the same order as specs.

        Example:
            lists = await client.create_objects([
                ("java.util.ArrayList", ()),
                ("java.util.ArrayList", (100,)),
                ("java.util.HashMap", ()),
            ])
        """
        builder = flatbuffers.Builder(4096)

        # Build each ObjectSpec
        object_spec_offsets = []
        for class_name, args in specs:
            # Build class name string
            class_name_off = builder.CreateString(class_name)

            # Build arguments
            arg_offsets = []
            for arg in args:
                arg_offset = self._build_argument(builder, arg)
                arg_offsets.append(arg_offset)

            args_vec = None
            if arg_offsets:
                ObjectSpec.StartArgsVector(builder, len(arg_offsets))
                for offset in reversed(arg_offsets):
                    builder.PrependUOffsetTRelative(offset)
                args_vec = builder.EndVector()

            # Build ObjectSpec
            ObjectSpec.Start(builder)
            ObjectSpec.AddClassName(builder, class_name_off)
            if args_vec:
                ObjectSpec.AddArgs(builder, args_vec)
            object_spec_offsets.append(ObjectSpec.End(builder))

        # Build objects vector
        CreateObjectsRequest.StartObjectsVector(builder, len(object_spec_offsets))
        for offset in reversed(object_spec_offsets):
            builder.PrependUOffsetTRelative(offset)
        objects_vec = builder.EndVector()

        # Build CreateObjectsRequest
        CreateObjectsRequest.Start(builder)
        CreateObjectsRequest.AddObjects(builder, objects_vec)
        create_objects_off = CreateObjectsRequest.End(builder)

        # Build Command
        Cmd.CommandStart(builder)
        Cmd.CommandAddAction(builder, Act.Action.CreateObjects)
        Cmd.CommandAddCreateObjects(builder, create_objects_off)
        cmd = Cmd.CommandEnd(builder)
        builder.Finish(cmd)

        return cast(list["AsyncJavaObject"], await self._send_raw(builder.Output()))

    async def register_callback(
        self, callback_fn: Any, interface_name: str
    ) -> AsyncJavaObject:
        """Register a Python callable, or object, as a Java interface implementation.

        The callback can be either a sync function or an async coroutine. For
        an interface with several methods it can be an object that is not
        callable: each call goes to its method of the same name, sync or async.
        """
        command = _finish_register_callback(
            flatbuffers.Builder(256), interface_name, callback_fn
        )
        # Java may call this callback from any thread from here on
        await self._ensure_callback_channel()

        result: AsyncJavaObject = cast(AsyncJavaObject, await self._send_raw(command))

        # Store the callback function
        callback_id = result.object_id
        self._callbacks[callback_id] = callback_fn

        return result

    async def unregister_callback(self, callback: AsyncJavaObject | int) -> None:
        """Unregister a previously registered callback.

        Args:
            callback: What register_callback() returned, or any other handle
                on the same Java object. Or the object ID of one, while the
                handle is alive.
        """
        # `callback` stays referenced until the server has answered; see the
        # sync client
        callback_id = (
            callback.object_id
            if isinstance(callback, (JavaObject, AsyncJavaObject))
            else callback
        )
        builder = flatbuffers.Builder(64)
        Cmd.CommandStart(builder)
        Cmd.CommandAddAction(builder, Act.Action.UnregisterCallback)
        Cmd.CommandAddTargetId(builder, callback_id)
        cmd = Cmd.CommandEnd(builder)
        builder.Finish(cmd)

        try:
            unregistered = await self._send_raw(builder.Output())
        except BaseException:
            # A request that was refused must not leave Java calling a function
            # that is gone. On a client that is closed or dead the session is
            # over, and its callbacks with it.
            if self._closed or self._dead:
                self._callbacks.pop(callback_id, None)
            raise
        self._callbacks.pop(callback_id, None)
        # The callback's own ID, when it was named by another handle
        if isinstance(unregistered, int):
            self._callbacks.pop(unregistered, None)

    async def reflect(self, full_name: str) -> str:
        """Ask the server what a dotted name is.

        Returns "class", "method", "field", or "none" if it cannot be resolved.
        """
        builder = flatbuffers.Builder(256)
        name_off = builder.CreateString(full_name)
        Cmd.CommandStart(builder)
        Cmd.CommandAddAction(builder, Act.Action.Reflect)
        Cmd.CommandAddTargetName(builder, name_off)
        builder.Finish(Cmd.CommandEnd(builder))
        return cast(str, await self._send_raw(builder.Output()))

    async def _reflect_cached(self, full_name: str) -> str:
        cached = self._reflect_cache.get(full_name)
        if cached is None:
            cached = await self.reflect(full_name)
            _remember_reflection(self._reflect_cache, full_name, cached)
        return cached

    async def get_static_field(self, class_name: str, field_name: str):
        """Get a static field value from a Java class."""
        builder = flatbuffers.Builder(256)
        name_off = builder.CreateString(f"{class_name}.{field_name}")
        Cmd.CommandStart(builder)
        Cmd.CommandAddAction(builder, Act.Action.GetStaticField)
        Cmd.CommandAddTargetName(builder, name_off)
        builder.Finish(Cmd.CommandEnd(builder))
        return await self._send_raw(builder.Output())

    async def free_object(self, object_id: int):
        """Free a Java object asynchronously."""
        if self._writer is None or self._dead:
            return

        channel = self._callback_channel
        builder = flatbuffers.Builder(64)
        Cmd.CommandStart(builder)
        Cmd.CommandAddAction(builder, Act.Action.FreeObject)
        Cmd.CommandAddTargetId(builder, object_id)
        if channel is not None:
            # See _build_free_command
            Cmd.CommandAddAnswersSent(builder, channel.answers_sent)
        cmd = Cmd.CommandEnd(builder)
        builder.Finish(cmd)

        try:
            await self._send_raw(builder.Output())
        except Exception:  # noqa: BLE001, S110 - freeing is best-effort; the server reclaims at session end
            pass

    async def send_arrow_table(self, table: pa.Table):
        """Send an Arrow table to Java via shared memory asynchronously.

        The data flow is:
        1. Python serializes the table as an Arrow IPC stream straight into
           shared memory (one pass over the data)
        2. Java reads the stream from shared memory

        Java only counts the rows. To hand the table to Java code use
        arrow_to_java(); to have Java read the buffers in place use
        send_arrow_buffers().
        """
        size = _arrow_stream_size(table)

        builder = flatbuffers.Builder(256)
        Cmd.CommandStart(builder)
        Cmd.CommandAddAction(builder, Act.Action.SendArrowBatch)
        cmd = Cmd.CommandEnd(builder)
        builder.Finish(cmd)

        # Written into the payload zone once this request holds the channel
        return await self._send_raw(
            builder.Output(),
            prepare=lambda: _write_arrow_table(self, table, 0, size, "Arrow batch"),
        )

    async def arrow_to_java(self, table: pa.Table | pa.RecordBatch) -> AsyncJavaObject:
        """Copy an Arrow table into the JVM and return a handle to it.

        The handle refers to an org.apache.arrow.vector.VectorSchemaRoot that owns
        its memory. Pass it to any Java method that takes one, or get a table
        back with arrow_from_java().

        Async handles are not released by garbage collection. The table's memory
        stays in use, and counts against max_arrow_bytes, until you free the
        handle or the connection closes:

            async with await client.arrow_to_java(table) as root:
                ...
            # or: await client.free_object(root.object_id)

        Raises:
            PayloadTooLargeError: If the table does not fit in shared memory.
            UnsupportedArrowTypeError: If a column is dictionary-encoded.
        """
        table = _arrow_table_to_send(table)
        size = _arrow_stream_size(table)
        command = _finish_arrow_to_object(flatbuffers.Builder(256), 0, size)
        # Inside a callback that Java made from another thread the table goes
        # through the call's own memory: the payload zone belongs to the
        # channel, which may be busy with the request waiting for the callback
        call = _channel_call_of(self)
        if call is not None:
            self._require_usable()
            async with call.transfer_lock:
                memory = await self._call_memory(call)
                if memory is not None:
                    _require_payload_room(memory, size, 0)
                    _write_arrow_table(memory, table, 0, size)
                    routed, result = await self._send_nested(call, command)
                    if routed:
                        return cast(AsyncJavaObject, result)
        # Written into the payload zone once this request holds the channel
        return cast(
            AsyncJavaObject,
            await self._send_raw(
                command, prepare=lambda: _write_arrow_table(self, table, 0, size)
            ),
        )

    async def arrow_from_java(self, table: "AsyncJavaObject | int") -> pa.Table:
        """Copy an Arrow table held by a Java object into Python.

        Works for any org.apache.arrow.vector.VectorSchemaRoot the JVM holds. The
        result is an ordinary pyarrow Table that owns its data; the Java table is
        left as it is.

        Raises:
            PayloadTooLargeError: If the table does not fit in shared memory.
            JavaIllegalArgumentException: If the object is not a VectorSchemaRoot.
        """
        if isinstance(table, (JavaObject, AsyncJavaObject)):
            object_id = table.object_id
        else:
            object_id = table

        command = _finish_arrow_from_object(flatbuffers.Builder(256), object_id)
        # Inside a callback that Java made from another thread, through the
        # call's own memory; see arrow_to_java
        call = _channel_call_of(self)
        if call is not None:
            self._require_usable()
            async with call.transfer_lock:
                memory = await self._call_memory(call)
                if memory is not None:
                    routed, location = await self._send_nested(call, command)
                    if routed:
                        return _read_arrow_stream(memory, location)
        # Copied out of the payload zone before another request can reuse it
        return cast(
            pa.Table,
            await self._send_raw(
                command, finish=lambda location: _read_arrow_stream(self, location)
            ),
        )

    def arrow_context(self) -> "AsyncScopedArenaContext":
        """Get an async context manager for scoped Arrow transfers.

        Provides a convenient way to send Arrow data with automatic cleanup.
        Uses send_arrow_table() for transfers (IPC format).

        Returns:
            AsyncScopedArenaContext for use with async with statement

        Example:
            async with client.arrow_context() as ctx:
                await ctx.send(table1)
                await ctx.send(table2)
            # Automatically cleaned up on exit
        """
        from gatun.arena import AsyncScopedArenaContext

        return AsyncScopedArenaContext(self)

    async def close(self):
        """Close the connection."""
        self._closed = True
        try:
            writer, self._writer = self._writer, None
            self._reader = None
            if writer is not None:
                try:
                    writer.close()
                    await writer.wait_closed()
                except OSError:
                    pass
            channel, self._callback_channel = self._callback_channel, None
            if channel is not None:
                channel.close()
            _close_shared_memory(self)
        finally:
            # A server started for this client by aconnect() goes away with it
            session, self._server_session = self._server_session, None
            if session is not None:
                # stop() waits for the JVM to exit; keep the event loop free meanwhile
                await asyncio.to_thread(session.stop)

    async def __aenter__(self):
        # Only connect if not already connected
        if self.shm is None and not await self.connect():
            raise RuntimeError(
                f"Failed to connect to Gatun server at {self.socket_path}: "
                f"{self.connect_error}"
            ) from self.connect_error
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
        return False
