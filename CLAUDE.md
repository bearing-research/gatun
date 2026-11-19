# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**Status: Alpha (experimental)** - APIs may change without notice.

Gatun is a high-performance Python-to-Java bridge using shared memory (mmap) and Unix domain sockets for inter-process communication. It uses FlatBuffers for serialization and Apache Arrow for efficient bulk data transfer.

## Architecture

### Components
- **gatun-core** (Java): Server that manages Java objects and handles RPC requests
- **src/gatun** (Python): Client library with sync/async clients, launcher, and config
- **schemas/commands.fbs**: FlatBuffers schema defining the wire protocol

### Communication Flow
1. Python launcher starts Java server subprocess
2. Client connects via Unix domain socket
3. Server creates per-client shared memory file and sends handshake with SHM path
4. Commands are serialized to FlatBuffers, written to shared memory
5. Length prefix sent over socket signals Java to process command
6. Response written to shared memory, length sent back to Python
7. On disconnect, server cleans up the client's shared memory file

### Memory Layout
- Command zone: offset 0, 64KB
- Payload zone (Arrow data): offset 65536 up to the response zone. Split in half: first half is Python -> Java, second half is Java -> Python
- Response zone: last 64KB of shared memory

These sizes are hardcoded on both sides and must stay in sync: `PAYLOAD_OFFSET` / `RESPONSE_ZONE_SIZE` in `GatunServer.java`, `COMMAND_ZONE_SIZE` / `RESPONSE_ZONE_SIZE` in `client.py`, and the literal `65536` offsets in the `connect()` paths of both `client.py` and `async_client.py`.

### Where Changes Must Be Mirrored
The protocol is implemented several times over, so a wire-level change usually touches all of these:
- `schemas/commands.fbs` — add the `Action` / table / field
- `GatunServer.handleClient` — top-level dispatch (an `if/else if` chain on `cmd.action()`); new actions also need a name in `Metrics.getActionName`
- `GatunServer.executeCommand` — a second, separate dispatch used only for sub-commands inside `Action.Batch`; an action missing here works standalone but fails in `client.batch()`
- `src/gatun/client.py` — sync client, plus `BatchContext` if the action should be batchable
- `src/gatun/callback_channel.py` and `CallbackChannel.java` — the callback connection, for a change to how callbacks travel (see Python Callbacks)
- `GatunServer.executeNested` and `_NESTABLE_ACTIONS` in `client.py` — the actions a callback may send on the callback connection; the two lists must match, and an action belongs there only if it never touches the session's shared memory
- `src/gatun/async_client.py` — shares value encoding and decoding with the sync client (the `_ValueEncoder` mixin and `_unpack_value_impl` in `client.py`) but has its own `_read_response`, and implements only a subset of the sync API (no `batch`, `cancel`, `send_arrow_buffers` / `get_arrow_data` or `arrow_from_java(copy=False)`; it does have `reflect`, `get_static_field` and `arrow_to_java` / `arrow_from_java`)
- `PROTOCOL_VERSION` in both `GatunServer.java` and `client.py` for breaking changes

Java-side responsibilities are split: `GatunServer` (dispatch, object/callback registries, allowlist, value packing), `MethodResolver` (overload scoring, varargs), `ReflectionCache` (class/method/field/handle caches), `ArrowMemoryHandler` (Arrow IPC + buffer-descriptor transfer, epoch checks; one instance per session, since the loaded batch points into that session's shared memory), `ResourceLimits` + `SessionQuota` (per-session limits), `observability/` (logging, metrics, JFR).

On the Python side, `client.jvm.a.b.C` navigation goes through `_JVMNode`, which asks the server (`Action.Reflect`) whether a dotted path is a class, method, or field and caches the answer in a per-client LRU (`_reflect_cached`; "none" is cached only for a name inside a known class, since a name that is not a class yet may become one). Names starting with `_` raise `AttributeError` without asking. An attribute of a capitalized parent that the server says is a field is fetched eagerly as a static field; spelling does not decide (`Encoders.STRING` is a method, `System.out` a field).

### Protocol Handshake
Format: `[4 bytes: version][4 bytes: arena_epoch][8 bytes: memory size][2 bytes: shm_path_len][N bytes: shm_path]`
- Protocol version (currently 4) is verified on connect
- Version mismatch raises RuntimeError with upgrade instructions
- SHM path is the per-client shared memory file path (e.g., `/tmp/gatun.sock.1.shm`)

## Build Commands

### Python (from repository root)
```bash
JAVA_HOME=/opt/homebrew/opt/openjdk uv sync    # Install deps and build JAR
JAVA_HOME=/opt/homebrew/opt/openjdk uv sync --reinstall-package gatun  # Rebuild after schema/Java changes
JAVA_HOME=/opt/homebrew/opt/openjdk uv build   # Build wheel and sdist
uv run pytest                        # Run all tests (with coverage)
uv run pytest tests/test_gatun_core.py::test_name  # Run single test
uv run pytest --no-cov               # Run without coverage (faster)
uv run ruff check .                  # Lint
uv run ruff format .                 # Format
uv run mypy src --config-file=pyproject.toml  # Type check (same as the pre-commit hook)
```

Prerequisites: `flatc` on `PATH` (`brew install flatbuffers`) and a JDK 22+. `JAVA_HOME` matters twice — Gradle uses it at build time, and `launcher.py` runs `$JAVA_HOME/bin/java` (falling back to `java` on `PATH`) at test time.

### Testing notes
- There are no Java unit tests. All Java behavior is exercised through pytest against the packaged JAR in `src/gatun/jars/`, so after editing Java or the schema you must run `uv sync --reinstall-package gatun` before tests will see the change. A plain `uv sync` does not rebuild.
- `tests/conftest.py` starts one server per pytest session on a fixed socket (`<tmpdir>/gatun_test.sock`, 64MB), so two pytest runs cannot overlap on one machine. The `client` fixture gives each test a fresh connection to it; `make_client` is a factory for tests that need several connections in sequence.
- Tests that need non-default server settings (`test_resource_limits.py`, `test_observability.py`, `test_connect.py`) launch their own server via `connect(...)` instead of using the fixtures.
- `tests/test_spark_integration.py` is skipped unless a Spark distribution is extracted at `spark-4.1.1-bin-hadoop3/` in the repo root (gitignored).
- `tests/test_readme_examples.py` mirrors the README examples; keep the two in step.
- `asyncio_mode = "auto"`: async tests need no marker.

Coverage reports are generated automatically:
- Terminal: shows missing lines after test run
- HTML: `htmlcov/index.html` for detailed browsing
- Config in `pyproject.toml` under `[tool.coverage.*]`

### Java (from repository root, if needed separately)
```bash
./gradlew :gatun-core:build          # Build the Java server
./gradlew :gatun-core:shadowJar      # Build fat JAR (gatun-server-all.jar)
./gradlew :gatun-core:run            # Run the server directly
./gradlew spotlessApply              # Format Java (google-java-format); spotlessCheck to verify
```

## Generated Code

FlatBuffers code is generated from `schemas/commands.fbs`:
- Java: `gatun-core/src/main/java/org/gatun/protocol/`
- Python: `src/gatun/generated/org/gatun/protocol/`

Both directories are gitignored, as is the built JAR in `src/gatun/jars/` — never edit or commit them; a fresh clone has none of them until the first `uv sync`.

`gatun_build_backend.py` is a PEP 517 backend wrapping setuptools. On every wheel/sdist/editable build it:
1. Runs `flatc` if the schema is newer than the generated files (mtime comparison), then patches the output: Python imports are rewritten from `org.gatun.protocol.*` to `gatun.generated.org.gatun.protocol.*`, and the Java `ValidateVersion()` call is rewritten to `FLATBUFFERS_RUNTIME_VERSION`.
2. Runs `./gradlew :gatun-core:shadowJar` if any file under `gatun-core/src` or `gatun-core/build.gradle.kts` is newer than the packaged JAR, and copies the result to `src/gatun/jars/gatun-server-all.jar`.

`FLATBUFFERS_RUNTIME_VERSION` in the backend must match the `flatbuffers-java` version in `gatun-core/build.gradle.kts`; bump them together.

The shadow JAR relocates Netty (`gatun.shaded.io.netty`) so it can coexist with Spark's Netty, and pins Jackson to Spark 4.x's version unshaded so the two share classes. Keep that in mind when changing Java dependencies.

## Key Implementation Details

- Java 22+ required. 22 is the first release where the Foreign Function & Memory API (`java.lang.foreign`, used to map the shared memory) is final; on 21 it needed `--enable-preview`. The Gradle toolchain and `--release` are pinned to 22 so the JAR runs on 22 and later, and CI runs on 22 to exercise that floor. Do not raise it casually.
- Python 3.13+ required (3.14 also supported)
- Arrow memory requires JVM flags: `--add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED`
- Both clients use `weakref.finalize` for automatic Java object cleanup (`detach()` opts a handle out). The finalizer only queues the object ID (`GatunClient._queue_free`); the queue is sent as chunked `FreeObject` commands just before the next request, because a finalizer can fire in the middle of another exchange. Never send on the socket from a finalizer. An explicit `client.free_object(id)` is sent immediately.
- `_build_value` must not make Java calls: the outer command is half-built in a pooled builder and the channel carries one request at a time. Values that need a Java-side object are encoded on the wire instead (see `TemporalVal` for `datetime` / `date` / `time`).
- Socket path: Generated randomly in `/tmp/gatun_<pid>_<random>.sock` by default. The launcher refuses a fixed `socket_path` that a live server is listening on
- The launched server runs in its own process group and is passed `-Dgatun.parent.pid`; it exits when that process is gone (`GatunServer.watchParent`), which covers a Python process that was killed
- A table from `get_arrow_data().table` is built on `pa.py_buffer(memoryview(shm)[...])`, so it holds a buffer export of the mapping and keeps it alive past `close()`. Do not go back to `pa.foreign_buffer` on a raw address there: such a table reads unmapped memory after `close()`
- Use `T | None` syntax instead of `Optional[T]`
- No fallback imports (e.g., no `tomli` for older Python)

## Quick Start

```python
from gatun import connect

# Start server and connect
client = connect()

# Create Java objects via JVM view
ArrayList = client.jvm.java.util.ArrayList
my_list = ArrayList()
my_list.add("hello")
my_list.add("world")
print(my_list.size())  # 2

# Call static methods
result = client.jvm.java.lang.Integer.parseInt("42")  # 42

# Clean up
client.close()
```

## Supported Operations

### JVM View API (Recommended)
The `client.jvm` property provides package-style navigation:
```python
# Access classes via package path
ArrayList = client.jvm.java.util.ArrayList
HashMap = client.jvm.java.util.HashMap

# Instantiate classes
my_list = ArrayList()           # No-arg constructor
my_list = ArrayList(100)        # With initial capacity

# Call static methods
result = client.jvm.java.lang.Integer.parseInt("42")  # 42
result = client.jvm.java.lang.Math.max(10, 20)        # 20
result = client.jvm.java.lang.Math.sqrt(16.0)         # 4.0 (note: use floats for double params)

# String operations
result = client.jvm.java.lang.String.valueOf(123)     # "123"
result = client.jvm.java.lang.String.format("Hello %s!", "World")  # "Hello World!"
```

### java_import
Import classes for shorter access paths:
```python
from gatun import java_import

# Wildcard import
java_import(client.jvm, "java.util.*")
arr = client.jvm.ArrayList()  # instead of client.jvm.java.util.ArrayList()

# Single class import
java_import(client.jvm, "java.lang.StringBuilder")
sb = client.jvm.StringBuilder("hello")
```

### Collections Examples
```python
# HashMap
hm = client.jvm.java.util.HashMap()
hm.put("key1", "value1")
hm.put("key2", 42)
hm.get("key1")  # "value1"
hm.size()       # 2

# TreeMap (sorted keys)
tm = client.jvm.java.util.TreeMap()
tm.put("zebra", 1)
tm.put("apple", 2)
tm.firstKey()  # "apple"
tm.lastKey()   # "zebra"

# HashSet (no duplicates)
hs = client.jvm.java.util.HashSet()
hs.add("a")
hs.add("b")
hs.add("a")  # duplicate ignored
hs.size()    # 2
hs.contains("a")  # True

# StringBuilder for efficient string building
sb = client.jvm.java.lang.StringBuilder("Hello")
sb.append(" ")
sb.append("World!")
sb.toString()  # "Hello World!"

# Collections utility methods
java_import(client.jvm, "java.util.*")
arr = client.jvm.ArrayList()
arr.add("banana")
arr.add("apple")
arr.add("cherry")
client.jvm.Collections.sort(arr)     # Sorts in place
client.jvm.Collections.reverse(arr)  # Reverses in place

# Arrays.asList returns a Python list (auto-converted)
result = client.jvm.java.util.Arrays.asList("a", "b", "c")  # ['a', 'b', 'c']
```

### Passing Python Collections
Python lists and dicts are automatically converted to Java collections:
```python
arr = client.jvm.java.util.ArrayList()
arr.add([1, 2, 3])                    # Converted to Java List
arr.add({"name": "Alice", "age": 30}) # Converted to Java Map
```

### Async Client
```python
from gatun import aconnect
import asyncio

async def main():
    client = await aconnect()

    # All operations are async
    arr = await client.jvm.java.util.ArrayList()
    await arr.add("hello")
    size = await arr.size()  # 1

    # Static methods
    result = await client.jvm.java.lang.Integer.parseInt("42")  # 42

    await client.close()

asyncio.run(main())
```

### Python Callbacks
Register Python functions as Java interface implementations:
```python
def compare(a, b):
    return -1 if a < b else (1 if a > b else 0)

comparator = client.register_callback(compare, "java.util.Comparator")

arr = client.jvm.java.util.ArrayList()
arr.add(3)
arr.add(1)
arr.add(2)
client.jvm.java.util.Collections.sort(arr, comparator)
# arr is now [1, 2, 3]
```

Async callbacks are also supported:
```python
async def async_compare(a, b):
    await asyncio.sleep(0.01)
    return -1 if a < b else (1 if a > b else 0)

comparator = await async_client.register_callback(async_compare, "java.util.Comparator")
```

**Important: Reentrancy Policy**

A callback made by the thread that is running the client's request cannot call Java. Attempting to raises `ReentrancyError`:

```python
from gatun import ReentrancyError

def bad_callback(a, b):
    # This will raise ReentrancyError - cannot call Java from callback
    client.create_object("java.util.ArrayList")
    return 0

comparator = client.register_callback(bad_callback, "java.util.Comparator")
arr = client.jvm.java.util.ArrayList()
arr.add("b")
arr.add("a")

# This raises JavaException containing ReentrancyError
client.jvm.java.util.Collections.sort(arr, comparator)
```

Why this restriction exists: Gatun uses a single shared memory channel per client. When Java calls back into Python, the original request is still waiting for a response. A nested Java call would try to use the same channel, causing a deadlock.

Workarounds:
- Queue work for later instead of calling Java immediately in the callback
- Use pure Python logic in callbacks (no Java calls)
- Pre-create any Java objects you need before the callback is invoked

The policy above is for callbacks made by the thread that is running the request, which travel in band. Default methods of the interface (`Comparator.reversed()`) run in Java and never reach Python.

**Callbacks from other threads: the callback connection**

Any other Java thread (a Spark listener, a timer, an executor's worker, a parallel stream), at any time, calls Python on the session's callback connection. `GatunServer.invokeCallback` chooses by `SessionControl.isSessionThread()`: `exchangeCallback` in band, `callOnChannel` otherwise.
- It is a second Unix socket with no shared memory: each session maps its memory with `Arena.ofConfined()`, so no other thread can touch it. Frames are `[4 bytes length][FlatBuffer]`, at most 64KB: a `Response` with `is_callback` from Java, a `CallbackResponse` command from Python, paired by `call_id`. Calls from many threads are in flight at once.
- It is opened lazily, by the first `register_callback` (`_ensure_callback_channel`): `Action.OpenCallbackChannel` on the main channel makes the server listen on `<socket>.<session id>.cb`, a socket that accepts one connection and is then removed. The connection takes no session slot. If it cannot be opened, the client logs a warning and in-band callbacks still work.
- Server: `CallbackChannel` has one reader and one writer thread; callers only use queues. Callers are threads the server does not own, and interrupting a thread that is blocked on a channel closes the channel. `callOnChannel` sets the `sessionContext` thread-local to the proxy's session while it packs and decodes, because `sessionObject` skips its ownership check on a thread with no session.
- Python: `CallbackChannel` (a daemon reader thread, one thread per call) and `AsyncCallbackChannel` (a reader task, one task per call) hand each request to the client's `_serve_channel_callback`. They hold the client weakly, so a client dropped without `close()` is still collected. These callbacks are not recorded as running "inside a request" (`_callback_threads`; their `_CallbackScope` has a `call`), so they may call Java.
- Their Java calls travel on the callback connection too. Until a callback answers, the Java thread that called it is waiting, and it is the one thread certain to be free: the session's own thread may be busy with a request, possibly the one waiting for that thread. `_send_raw` finds the call the current context is running in (`_channel_call_of`) and sends the command wrapped in an `Action.NestedCommand` frame with the call's ID (`_nested_command`, `ChannelCall.request`); `GatunServer.callOnChannel` runs it through `executeNested` between waits and replies with a `Response` carrying the same `call_id`. One request per call at a time, and none after the callback's answer (`ChannelCall`'s two locks): Java stops listening for a call once it has the answer. A caller that finds its callback already answered (a thread it started, a callback that timed out) falls back to the main channel. A request already on its way when the callback answers still gets its response: the connection keeps a call's route until the callback is done and no request is waiting (`ChannelCall.finish`).
- The Java thread can stop waiting first: it was interrupted (`Future.cancel(true)`, `shutdownNow()`). `CallbackChannel.end` then answers every command that arrives for the call, or already sits in its inbox, with an error of type `org.gatun.server.CallbackCallEnded` (`GatunServer.CALL_ENDED`, `_CALL_ENDED` in `client.py`) instead of leaving it without a reply; the client marks the call abandoned and sends the command on the main channel, where it has not run yet. `runNested` restores the interrupt flag when a nested command was interrupted: the interrupt was for the thread.
- `runNested` checks `ctx.closed` before and after the command and drops what it registered for a session that has ended, like `callOnChannel` does for a callback's arguments. It records the command in `Metrics` and the request log itself; the session's `SessionObserver` follows the one request the session's own thread runs and is not thread-safe.
- Only `executeNested`'s actions travel that way. Everything else is refused in Python with `ReentrancyError` before it is sent: the session's shared memory is confined to the session's thread, so every Arrow transfer that uses it is out (see the next point for the two that have another way), and batches, vectorized calls, `RegisterCallback` and `GetMetrics` are simply not served there (`UnregisterCallback` is: a one-shot listener unregisters itself).
- `arrow_to_java` / `arrow_from_java` inside such a callback go through the call's own shared memory (`GatunServer.CallMemory`, `CallMemory` in `callback_channel.py`). The first transfer of a call sends `Action.OpenCallMemory` as a nested command: the Java thread waiting for the callback creates `<socket>.<session id>.call<call id>.shm`, as large as the session's and all of it payload, maps it with an arena confined to itself (it is the thread that runs the call's nested commands) and returns the path. `ArrowToObject` / `ArrowFromObject` as nested commands then mean that memory, through the same `arrowToObject` / `arrowFromObject` the session thread uses. The files are sparse and callbacks at once each have one, so a new one must leave the existing ones their whole size in free space (`callMemoryBytes`) or the transfer is refused: a disk that fills up under a mapping is a fault, not an error. `callOnChannel` unmaps and deletes the file when the call ends (a client that finds it gone treats the call as over, and looks before each transfer, `CallMemory.removed`: no space is kept for a file that is gone, so nothing may be written to its mapping); the client's mapping lives until nothing refers to it, and nothing writes there after the call, so a `copy=False` view ends only with the call's next transfer (`CallMemory._payload_writes`). A transfer is several steps on that one memory, so it holds the call's `transfer_lock` throughout (an `asyncio.Lock` in the async client, where tasks of one callback share the call). The public methods try this route first and fall back to the client's channel when the call is over, like `_send_nested`; `@_holding_channel` is on the `_on_channel` halves. `@_holding_channel` methods refuse as well. A nested call that times out leaves its `ChannelCall` unusable (the late response would be taken for the next one) but not the client. That response still names objects Java registered for the client: the reader hands it to `_release_unread_response`, which queues them for release without wrapping them.
- Because several threads now register objects for one session, `sessionObjectIds` is a concurrent set and the server tracks exactly which IDs each pack registered (`packRecorder`). Do not go back to counting the set before and after, or to ranges of the ID counter: both pick up what another thread registered meanwhile.
- A proxy outlives its session, and its registration, in whatever Java code holds it; called then, it throws `IllegalStateException`. `invokeCallback` checks `SessionContext.closed` and `callbackRegistry` before it packs anything, on both paths: the objects among the arguments are registered for the client while packing, and a client that will not run the callback would never release them. For the same reason `callOnChannel` drops what it registered when the request could not be delivered or the session ended meanwhile. The Python side keeps a callback until `unregister_callback`.
- Object releases also travel on this connection: after each callback the client sends its queued frees as a `FreeObject` frame with no `call_id` (`_release_on_channel`, `GatunServer.handleChannelNotice`). A client that only listens, or sits in one long request, sends nothing on the main channel for them to ride on.
- A release must never overtake a callback's answer that names the object: the wrapper dies the moment the answer is sent, and the release may travel on the other connection. Three things keep the order. The server's reader decodes each answer itself, before it reads the next frame (`decodeChannelAnswer`), so a release that follows on the callback connection is safe. A `FreeObject` on the main channel carries `answers_sent`, the number of answers sent on the callback connection so far, and the server waits until it has decoded that many (`awaitAnswersBefore`); read the count after taking the IDs off the queue. And the result of an in-band answer is kept alive until Java's next message on the main channel (`answered_with` in `_read_response`).
- A session with a call in flight on the connection is not idle (`SessionContext.channelCalls`, checked by the reaper).
- Both clients answer a channel call whatever fails on the way (`_serve_channel_callback`): the Java thread waits with no timeout of its own.

### Request Cancellation
Cancel the request running on a client, from another thread:
```python
from gatun import CancelledException

threading.Timer(0.5, client.cancel).start()
try:
    client.jvm.java.lang.Thread.sleep(60_000)   # needs allowed_classes=["java.lang.Thread"]
except CancelledException:
    ...
# client.cancel() returns True if a request was interrupted, False if none was running
```
- The session's channel is busy with the request being cancelled, so `cancel()` opens a short-lived second connection and sends `Action.Cancel` with `target_name` set to the session's SHM path. The server looks the session up in `GatunServer.sessions` and interrupts its thread (`SessionControl.cancel`).
- The interrupt is delivered only while the thread runs Java code, never during socket I/O (`beginSocketUse` / `endSocketUse`): interrupting a blocked channel read would close the channel. A cancel that arrives during a callback exchange takes effect when the exchange ends.
- `InterruptedException` maps to `CancelledException`. Java code can poll `GatunServer.checkCancelled()`.
- Sync client only. With `gatun.limits.max_sessions` reached, the second connection waits two seconds for a slot (`takeSessionSlot`), is refused, and `cancel()` returns False.

### Threads and Connection State
- The sync client is thread-safe by serialization: `_send_raw` holds `_exchange_lock` (an `RLock`) for the whole exchange, callbacks included. Commands are built before the lock is taken, so the builder pool is per thread (`_get_builder`), and the methods that use the payload zone around their request hold the lock throughout (`@_holding_channel`). Reentrancy is per thread (`_callback_threads`) and per context (`_callback_scopes`, a `ContextVar` that tasks started by a callback inherit; the scope's `active` flag lets a task that outlives its callback call Java). A plain thread started by a sync callback inherits nothing: if the callback waits for it, it waits for the callback timeout.
- Any `BaseException` that interrupts an exchange after the command was written (timeout, `KeyboardInterrupt`, task cancellation) marks the client dead: the answer is still coming and the next request would read it. A mapped Java exception does not. After `close()` or death, calls raise `DeadConnectionError`.
- Timeouts of `None`, `0`, negative or `inf` mean no timeout (`_usable_timeout`). `connect()` / `aconnect()` take `socket_timeout` and `callback_timeout`, and use the configured `connect_timeout` for the handshake.

### is_instance_of
Check if a Java object is an instance of a class (equivalent to `instanceof`):
```python
arr = client.create_object("java.util.ArrayList")
client.is_instance_of(arr, "java.util.List")       # True
client.is_instance_of(arr, "java.util.Collection") # True
client.is_instance_of(arr, "java.util.Map")        # False
```

### Batch API
Execute multiple commands in a single round-trip (6x speedup for bulk operations):
```python
arr = client.create_object("java.util.ArrayList")

# Batch operations - single round-trip for all commands
with client.batch() as b:
    for i in range(100):
        b.call(arr, "add", i)
    size_result = b.call(arr, "size")

print(size_result.get())  # 100

# Available batch methods:
# b.create(class_name, *args)           - Create object
# b.call(obj, method_name, *args)       - Instance method
# b.call_static(class_name, method, *args) - Static method
# b.get_field(obj, field_name)          - Get field
# b.set_field(obj, field_name, value)   - Set field

# Error handling
with client.batch(stop_on_error=True) as b:  # Stop on first error
    r1 = b.call(arr, "add", "valid")
    r2 = b.call_static("java.lang.Integer", "parseInt", "invalid")  # Errors
    r3 = b.call(arr, "size")  # Skipped

# Results via BatchResult.get() - raises exception if command failed
print(r1.get())      # True
print(r2.is_error)   # True
```

### Vectorized APIs
For even faster bulk operations on the same target, use vectorized APIs (2-5x speedup):

```python
# invoke_methods - Multiple method calls on same object in one round-trip
arr = client.create_object("java.util.ArrayList")
results = client.invoke_methods(arr, [
    ("add", ("a",)),
    ("add", ("b",)),
    ("add", ("c",)),
    ("size", ()),
])
# results = [True, True, True, 3]

# With return_object_ref for specific calls
results = client.invoke_methods(arr, [
    ("size", ()),
    ("subList", (0, 2)),
], return_object_refs=[False, True])  # Second result as JavaObject

# create_objects - Create multiple objects in one round-trip
list1, map1, set1 = client.create_objects([
    ("java.util.ArrayList", ()),
    ("java.util.HashMap", ()),
    ("java.util.HashSet", ()),
])

# With constructor arguments
objects = client.create_objects([
    ("java.util.ArrayList", (100,)),      # Initial capacity
    ("java.lang.StringBuilder", ("hi",)), # Initial string
])

# get_fields - Read multiple fields from one object in one round-trip
sb = client.create_object("java.lang.StringBuilder", "hello")
values = client.get_fields(sb, ["count"])  # [5]
```

**When to use which API:**
| API | Best For |
|-----|----------|
| `invoke_methods` | Multiple method calls on **same object** |
| `create_objects` | Creating multiple objects at startup |
| `get_fields` | Reading multiple fields from one object |
| `batch` | Mixed operations on **different objects** |

### Low-Level API
For direct control:
```python
client.create_object("java.util.ArrayList")           # No-arg constructor
client.create_object("java.util.ArrayList", 100)      # With initial capacity
client.invoke_method(object_id, "methodName", arg1)   # Direct method call
client.invoke_static_method("java.lang.Math", "max", 10, 20)
client.get_field(obj, "fieldName")                    # Get field value
client.set_field(obj, "fieldName", value)             # Set field value
client.is_instance_of(obj, "java.util.List")          # Check instance type
client.ping()                                         # Health check (returns True if alive)
client.get_metrics()                                  # Get server metrics report

# Vectorized operations (single round-trip for multiple operations)
client.get_fields(obj, ["field1", "field2"])          # Multiple field reads
client.invoke_methods(obj, [("method1", (arg,)), ("method2", ())])  # Multiple calls
client.create_objects([("class1", ()), ("class2", (arg,))])  # Multiple creations
```

### Arrow Data Transfer
Multiple methods for transferring Arrow data:

#### Tables as Java Objects
The only path that lets Java code touch the data. `send_arrow_table` only counts rows, and `send_arrow_buffers` / `get_arrow_data` hold one batch private to `ArrowMemoryHandler` that can only be read back.
```python
root = client.arrow_to_java(table)       # JavaObject -> org.apache.arrow.vector.VectorSchemaRoot
root.getRowCount()
client.jvm.com.example.Stats.summarize(root)   # any Java method taking a VectorSchemaRoot

produced = root.slice(1, 2)               # a table made on the Java side
result = client.arrow_from_java(produced)  # pyarrow.Table that owns its data
view = client.arrow_from_java(produced, copy=False)  # ArrowTableView, read in place
produced.close()                          # Java-made tables are the caller's to close
```
- Wire: `Action.ArrowToObject` / `ArrowFromObject`; the table travels as an Arrow IPC stream in the payload zone, located by `arrow_stream` (a `BufferDescriptor`) on `Command` / `Response`. Both clients implement them; they are not batchable. They are the one Arrow path that works inside a callback from another thread, where the stream is in the call's own memory instead (see Python Callbacks).
- A table made by `arrow_to_java` owns its memory (it does not point into shared memory). `GatunServer.ownedTables` counts the registry entries that refer to it and closes it with the last one, so freeing the handle releases the memory. It is charged to `max_arrow_bytes` until then, separately from the arena's count, which `reset_payload_arena` clears.
- The stream is written after any batch loaded in place by `send_arrow_buffers` (the client tracks its end in `_arrow_loaded_end`; the server refuses an overlapping stream). Any write that reaches the second half of the payload zone, where `ArrowTableView`s point, invalidates them (`GatunClient._note_payload_write`).
- `arrow_to_java` and `send_arrow_table` serialize straight into the payload zone (`_write_arrow_table`: a `pa.FixedSizeBufferWriter` over `pa.py_buffer(memoryview(shm)[...])`). The stream's length is needed first, for the fit check and the command, and comes from a `pa.MockOutputStream` pass that writes no data (`_arrow_stream_size`). The buffer, sink and writer each hold a buffer export of the mapping, so they are dropped before returning; a leftover one makes `mmap.close()` fail.
- `arrow_from_java(obj, copy=False)` (sync client only) reads the stream where Java wrote it and returns an `ArrowTableView(any_write=True)`. Java writes that stream from the start of the free part of the zone, so unlike a `get_arrow_data()` view it ends on any payload write: it follows `_payload_writes`, where a `get_arrow_data()` view follows `_payload_generation`. Views end before the bytes change, never after a transfer succeeds: Python-side writes go through `_write_table_to_payload` or the client's arena (`PayloadArena._on_allocate`), both of which call `_note_payload_write` first, and a command that makes Java write (`get_arrow_data`, `arrow_from_java`) bumps before it is sent and calls `_end_views` if it fails. The view's copying methods hold `_exchange_lock`, so a transfer on another thread cannot run under them.
- One transfer must fit in the payload zone; move larger tables in slices.
- In the async client anything that touches the payload zone runs inside `_send_raw`'s lock (its `prepare` / `finish` hooks), since tasks share one zone.

#### IPC Format (Simple)
```python
import pyarrow as pa

table = pa.table({"x": [1, 2, 3], "y": ["a", "b", "c"]})
result = client.send_arrow_table(table)  # "Received 3 rows"
```

#### Scoped Context Manager (Recommended)
Handles arena lifecycle automatically with proper cleanup:
```python
import pyarrow as pa

# Send and receive with automatic cleanup
with client.arrow_context() as ctx:
    ctx.send(table1)          # Auto-resets arena between sends
    ctx.send(table2)          # Safe to send multiple tables
    result = ctx.receive()    # Get data back as PyArrow table
# Arena automatically closed on exit

# Async version
async with client.arrow_context() as ctx:
    await ctx.send(table)
```

#### Size Validation
Check size before transfer to avoid errors:
```python
from gatun import estimate_arrow_size, PayloadTooLargeError

# Estimate buffer size needed
estimated = estimate_arrow_size(large_table)
print(f"Estimated: {estimated:,} bytes")

# Size is validated automatically - raises PayloadTooLargeError if too large
try:
    client.send_arrow_buffers(large_table, arena, {})
except PayloadTooLargeError as e:
    print(f"Too large: {e.payload_size:,} > {e.max_size:,} bytes")
```

#### Zero-Copy Buffer Transfer (Manual Control)
For maximum performance with fine-grained control:
```python
import pyarrow as pa

table = pa.table({"name": ["Alice", "Bob"], "age": [25, 30]})

# Get arena backed by client's shared memory
arena = client.get_payload_arena()
schema_cache = {}

# Send table via zero-copy buffer transfer
client.send_arrow_buffers(table, arena, schema_cache)

# For multiple batches, reset arena between sends
for batch in table.to_batches():
    arena.reset()  # Reuse arena for each batch
    client.send_arrow_buffers(batch, arena, schema_cache)

arena.close()
```

Data flow (Python -> Java):
1. Python copies Arrow buffers into shared memory (one memcpy)
2. Python sends buffer descriptors (offsets/lengths) to Java
3. Java wraps buffers directly as ArrowBuf (zero-copy read)

### JavaObject Features
JavaObject wrappers support Python protocols for seamless integration:
```python
# Iteration - iterate over Java collections
arr = client.jvm.java.util.ArrayList()
arr.add("a")
arr.add("b")
for item in arr:
    print(item)  # "a", "b"

# Indexing - access elements by index
print(arr[0])  # "a"
print(arr[1])  # "b"

# Length - get collection size
print(len(arr))  # 2

# Works with any Java Iterable/Collection
hashset = client.jvm.java.util.HashSet()
hashset.add(1)
hashset.add(2)
print(list(hashset))  # [1, 2]
```

### JavaArray for Primitive Arrays
Primitive arrays (`int[]`, `long[]`, `double[]`, etc.) are returned as `JavaArray`:
```python
from gatun import JavaArray
import pyarrow as pa

# Primitive arrays from Java are JavaArray instances
original = pa.array([1, 2, 3], type=pa.int32())
int_array = client.jvm.java.util.Arrays.copyOf(original, 3)
print(isinstance(int_array, JavaArray))  # True
print(int_array.element_type)  # "Int"
print(list(int_array))  # [1, 2, 3]

# Create JavaArray manually for specific element types
int_array = JavaArray([1, 2, 3], element_type="Int")
str_array = JavaArray(["a", "b"], element_type="String")
result = client.jvm.java.util.Arrays.toString(int_array)  # "[1, 2, 3]"
```

### Object Arrays as JavaObject
Object arrays (`Object[]`, `String[]`) are returned as `JavaObject` references to allow `Array.set/get`:
```python
# Object arrays from toArray() are JavaObject (not JavaArray)
arr = client.jvm.java.util.ArrayList()
arr.add("x")
arr.add("y")
java_array = arr.toArray()  # Returns JavaObject

# Use len() and iteration (not .size() or .length)
print(len(java_array))    # 2
print(java_array[0])      # "x"
print(list(java_array))   # ["x", "y"]

# Can still pass back to Java methods
result = client.jvm.java.util.Arrays.toString(java_array)  # "[x, y]"

# Array.set/get work on Object arrays
Array = client.jvm.java.lang.reflect.Array
Array.set(java_array, 0, "modified")
print(Array.get(java_array, 0))  # "modified"
```

### Supported Argument/Return Types
- Primitives: `int`, `long`, `double`, `boolean`
- `String`
- `list` -> Java `List` (ArrayList)
- `dict` -> Java `Map` (HashMap)
- `bytes` -> Java `byte[]`
- `JavaArray` -> Java primitive arrays (`int[]`, `double[]`, etc.); `char[]` is `element_type="Char"` with one-character strings as elements
- `pyarrow.Array` -> Java arrays (int32->int[], int64->long[], float64->double[], bool->boolean[], string->String[])
- `array.array` -> Java arrays (typecode 'i'->int[], 'q'->long[], 'd'->double[])
- `datetime.datetime` / `date` / `time` -> `java.sql.Timestamp` / `Date` / `Time` (arguments only; returned values stay `JavaObject`s). Naive datetimes are local time.
- Object references (returned as `JavaObject` wrappers, including Object arrays)
- `null`/`None`

**Note:** Object arrays (`Object[]`, `String[]`) return as `JavaObject`, not `JavaArray`. Use `len()` instead of `.size()` or `.length`.

## Exception Handling

Java exceptions are mapped to Python exceptions:
- `java.lang.SecurityException` -> `JavaSecurityException`
- `java.lang.IllegalArgumentException` -> `JavaIllegalArgumentException`
- `java.lang.NoSuchMethodException` -> `JavaNoSuchMethodException`
- `java.lang.NoSuchFieldException` -> `JavaNoSuchFieldException`
- `java.lang.ClassNotFoundException` -> `JavaClassNotFoundException`
- `java.lang.NullPointerException` -> `JavaNullPointerException`
- `java.lang.IndexOutOfBoundsException` -> `JavaIndexOutOfBoundsException`
- `java.lang.NumberFormatException` -> `JavaNumberFormatException`
- `java.lang.InterruptedException` -> `CancelledException`
- `java.lang.RuntimeException` -> `JavaRuntimeException`
- Anything else -> the base `JavaException`

Python-side exceptions:
- `ReentrancyError` - Raised when attempting nested Java calls from within a callback
- `PayloadTooLargeError` - Raised when data exceeds shared memory capacity
- `StaleArenaError` - Raised when using an arena after client reconnected
- `CallbackTimeoutError` - Raised when a callback runs longer than the callback timeout
- `SocketTimeoutError` / `ProtocolDesyncError` / `DeadConnectionError` - Transport failures; after a desync the client is marked dead and further calls raise `DeadConnectionError`
- `UnsupportedArrowTypeError` - Raised when a table's schema contains an Arrow type the buffer-transfer path cannot handle (currently dictionary-encoded columns)

## Security

### Class Allowlist
Only allowlisted classes can be instantiated or used for static methods. The built-in list (`ClassAllowlist.java`):
- Collections: `java.util.ArrayList`, `LinkedList`, `HashMap`, `LinkedHashMap`, `TreeMap`, `HashSet`, `LinkedHashSet`, `TreeSet`, `Collections`, `Arrays`
- Strings: `java.lang.String`, `StringBuilder`, `StringBuffer`
- Primitives: `java.lang.Integer`, `Long`, `Double`, `Float`, `Boolean`, `Byte`, `Short`, `Character`, `Math`
- System: `java.lang.System` (for setting system properties such as `spark.master`)
- Reflection: `java.lang.Class`, `java.lang.reflect.Array` (for array operations)
- Date/time: `java.sql.Timestamp`, `Date`, `Time`
- Prefixes: anything under `org.apache.spark.`, `org.apache.log4j.`, or `scala.`

Attempting to use non-allowlisted classes (e.g., `Runtime`, `ProcessBuilder`) raises `SecurityException`.

Other libraries are added per server, without a rebuild:
```python
client = connect(
    classpath=["/path/to/mylib.jar"],
    allowed_classes=["java.util.concurrent.atomic.AtomicLong"],  # exact class names
    allowed_prefixes=["com.example"],  # a package and its subpackages
)
```
The same lists can come from `[tool.gatun] allowed_classes` / `allowed_prefixes` in `pyproject.toml` or from `GATUN_ALLOWED_CLASSES` / `GATUN_ALLOWED_PREFIXES` (comma-separated). All sources are combined and added to the built-in list; nothing removes a built-in entry. The launcher passes them to the server as the `gatun.allowlist.classes` / `gatun.allowlist.prefixes` system properties, read once at JVM start.

In tests, pass `allowed_classes` to `connect()` rather than editing the built-in list. A nested class may be listed, and named, as `a.b.Outer.Inner` or `a.b.Outer$Inner`.

What the allowlist covers, and what it does not (the README's "What the Allowlist Is and Is Not" is the user-facing version; keep the two in step):
- It gates object creation and static method/field access by class name (`requireClassAllowed`).
- Instance members are open on whatever an allowed call returns, except objects of `ClassAllowlist.RESTRICTED_TYPES` (reflection objects, method handles, class loaders, `Runtime`, `Process*`, `Thread`, `Module`), which need their type in `allowed_classes` (`checkInstanceMember`, called at every instance method/field site). `REFUSED_MEMBERS` (`System.exit` / `load` / `loadLibrary`, `Class.newInstance`) are refused whatever is allowed.
- Class lookup never initializes: `ReflectionCache` loads with `Class.forName(name, false, loader)`, and `Class.forName` called from Python is swapped for `ReflectionCache.classForName` in `CachedMethod`.
- It is a guardrail, not a sandbox. Allowed prefixes and classpath JARs can do anything, and `GetField` / `SetField` reach non-public fields (`get_fields(sb, ["count"])` relies on that). The trust boundary is the OS user: see Session Isolation.

### Resource Limits
Per-session limits (object count, string length, list entries, nesting depth, batch size, Arrow bytes, idle timeout) and the server-wide `max_sessions` are defined in `ResourceLimits.java` as `gatun.limits.*` system properties read once at JVM start. An idle session is closed by a reaper thread when its timeout passes; idle time runs from the end of the last request. Override them per server with `connect(extra_jvm_flags=["-Dgatun.limits.max_objects=5"])`; see `tests/test_resource_limits.py`.

### Session Isolation
- The trust boundary is the OS user: the socket and every SHM file are `0600`, the server checks the peer's UID on accept (`SO_PEERCRED`), and it creates each SHM file itself (`CREATE_NEW`) after deleting whatever was at that path
- Each client session gets its own shared memory file (per-client SHM)
- Object IDs come from one server-wide counter, but a session reaches only the IDs it created: every lookup goes through `sessionObject` / `requireSessionObject`, for targets and for `ObjectRef` arguments alike. Use those, never the registry directly, when adding an action
- Objects are automatically cleaned up when session ends
- SHM files are deleted when the client disconnects. They are sparse (not zero-filled); the server checks for free space before creating one and refuses the session if there is not enough
- Double-free attempts are silently ignored
- Each command is copied out of shared memory before it is read (`commandCopy` in `handleClient`): a callback's answer arrives in the command zone while a batch or `InvokeMethods` request is still being worked through

## Observability

Gatun includes comprehensive observability features for debugging and monitoring.

### Enabling from Python
The easiest way to enable observability features:
```python
from gatun import connect

# Enable trace mode for method resolution debugging
client = connect(trace=True)

# Enable verbose logging
client = connect(log_level="FINE")

# Both
client = connect(trace=True, log_level="FINE")
```

Or via environment variables:
```bash
export GATUN_TRACE=true
export GATUN_LOG_LEVEL=FINE
python my_script.py
```

### Structured Logging
Log format: `sessionId=N requestId=N action=X target=Y latency_us=Z status=OK|ERROR`

Log levels:
- `INFO` - Server start/stop only (default)
- `FINE` - Request/response logging with timing
- `FINER` - Object registry add/remove events
- `FINEST` - All internal details

### Trace Mode
When enabled (`trace=True` or `GATUN_TRACE=true`), logs detailed method resolution:
- Class and method name
- Number of overload candidates
- Chosen method signature
- Specificity score
- Actual argument types

Useful for debugging "wrong method called" or overload resolution issues.

### Metrics
Access server metrics programmatically from Python:
```python
# Get server metrics report
metrics = client.get_metrics()
print(metrics)
```

The report includes:
- Request counts and rates per action type
- Latency percentiles (p50, p99) per action
- Object registry counts (current and peak)
- Arrow transfer metrics (rows, bytes)
- Callback invocation counts

Async clients also support `await client.get_metrics()`.

### JFR Events
Gatun emits JFR (Java Flight Recorder) events for profiling:
```bash
java -XX:StartFlightRecording=filename=gatun.jfr,settings=profile -jar gatun-server.jar
```

Events:
- `org.gatun.Request` - Request start/end with timing
- `org.gatun.MethodResolution` - Method overload resolution decisions
- `org.gatun.ObjectRegistry` - Object add/remove events
- `org.gatun.Callback` - Python callback round-trips
- `org.gatun.ArrowTransfer` - Arrow data transfers
- `org.gatun.Session` - Session lifecycle

### Request Ring Buffer
Each session maintains a ring buffer of the last 64 requests. On errors, recent request history is logged to help diagnose failures.

## Configuration

Configure via `pyproject.toml`:
```toml
[tool.gatun]
memory = "64MB"                  # KB/MB/GB, a fraction ("1.5GB") or bytes; minimum 192KB
socket_path = "/tmp/gatun.sock"  # Optional: fixed path instead of random
jvm_flags = ["-Xmx512m"]         # Appended to the launcher's default flags
connect_timeout = 5.0            # seconds
startup_timeout = 5.0            # seconds to wait for the server socket
allowed_prefixes = ["com.example"]  # added to the built-in class allowlist
```

Or environment variables:
```bash
GATUN_MEMORY=64MB
GATUN_SOCKET_PATH=/tmp/gatun.sock
GATUN_CONNECT_TIMEOUT=5
GATUN_STARTUP_TIMEOUT=5
GATUN_DEBUG=true   # Stream Java server stdout/stderr instead of capturing it
```

Precedence: function arguments > environment variables > `pyproject.toml` > defaults (memory defaults to `16MB`). The allow lists are the exception: every source is combined. The config is found by walking up from the current directory and is cached process-wide; call `reset_config()` in tests that change it.

## BridgeAdapter (PySpark Integration)

The `BridgeAdapter` abstract class provides a unified interface for JVM communication:

```python
from gatun.bridge import BridgeAdapter
from gatun.bridge_adapters import GatunAdapter

# Create adapter (launches Gatun server)
bridge = GatunAdapter(memory="256MB")

# Object lifecycle
obj = bridge.new("java.util.ArrayList")  # Create object
bridge.detach(obj)                        # Prevent auto-cleanup
bridge.close()                            # Close connection

# Method calls
bridge.call(obj, "add", "hello")          # Instance method
result = bridge.call_static("java.lang.Math", "max", 10, 20)  # Static method

# Field access
value = bridge.get_field(obj, "fieldName")
bridge.set_field(obj, "fieldName", value)

# Array operations (for Java array manipulation)
arr = bridge.new_array("java.lang.String", 3)  # Create String[3]
bridge.array_set(arr, 0, "hello")              # Set element
value = bridge.array_get(arr, 0)               # Get element
length = bridge.array_length(arr)              # Get length

# Type checking
bridge.is_instance_of(obj, "java.util.List")  # True
```

### PySpark Usage
Nothing in this repo or in stock PySpark switches PySpark over to Gatun; there is no `PYSPARK_USE_GATUN` switch. Running PySpark on Gatun needs a PySpark build ported to the `BridgeAdapter` interface (`docs/bridge_adapter_spec.md`). What this repo tests is driving Spark's JVM classes directly via `connect(classpath=<spark jars>)` (`tests/test_spark_integration.py`), which needs a JDK Spark supports: it passes on 22 and fails on 27. That file's `dataframe_from_arrow` / `arrow_from_dataframe` helpers show a pyarrow table becoming a Spark DataFrame and a query result coming back as Arrow, through `arrow_to_java` / `arrow_from_java` and Spark-internal converters (`ArrowConverters.toDataFrame`, `Dataset.toArrowBatchRdd`).

## Logging

- Java: `java.util.logging` (Logger for `GatunServer`)
- Python: `logging` module (loggers for `gatun.client` and `gatun.launcher`)

## Method Overload Resolution

Gatun uses specificity scoring to resolve overloaded Java methods:
1. Exact type matches get highest priority
2. Compatible types (e.g., `String` matching `CharSequence`) get medium priority
3. `Object` parameters get lowest priority
4. Non-varargs methods are preferred over varargs

Note: For methods with `double` parameters (like `Math.pow`), pass Python floats:
```python
Math.pow(2.0, 10.0)  # Works: 1024.0
Math.pow(2, 10)      # May fail: int args don't match double params
```
