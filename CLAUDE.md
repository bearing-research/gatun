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
6. Response written to shared memory, length sent back to Python. A command or a response larger than its zone follows its length on the socket instead (see Values Larger Than a Zone)
7. On disconnect, server cleans up the client's shared memory file

### Memory Layout
- Command zone: offset 0, 64KB
- Payload zone (Arrow data): offset 65536 up to the response zone. Split in half: first half is Python -> Java, second half is Java -> Python
- Response zone: last 64KB of shared memory

These sizes are hardcoded on both sides and must stay in sync: `PAYLOAD_OFFSET` / `RESPONSE_ZONE_SIZE` in `GatunServer.java`, `COMMAND_ZONE_SIZE` in `wire.py` (which `client.py` takes it from), `RESPONSE_ZONE_SIZE` in `client.py`, and the literal `65536` offsets in the `connect()` paths of both `client.py` and `async_client.py`.

### Values Larger Than a Zone
A command or a response of more than 64 KB follows its length on the socket; the length alone says which, on both sides (`> PAYLOAD_OFFSET` / `> RESPONSE_ZONE_SIZE`). Nothing is written to shared memory for it, least of all to the payload zone, which holds lent tables and views the message knows nothing about.
- The limit is `maxMessageSize` in `GatunServer` and `_max_message` (`max_message_size`) in both clients: the session's memory size, capped by what a FlatBuffer can address (`MAX_FLATBUFFER_SIZE`, the same number on both sides). The resource limits apply to the values inside as before. A command beyond it is refused in Python before anything is sent (`PayloadTooLargeError`); a response beyond it becomes that error.
- A message within its zone takes the path it always took, byte for byte. The hand-packed commands never grow past the zone (`_PACKED_LIMIT`), and the releases that ride on a command leave it inside (`_with_pending_frees`), so everything large is the builder's.
- Server: `readMessage` / `writeMessage` in `GatunServer`. The in-band callback exchange uses them as well (`exchangeCallback`: the request Java sends and the answer it reads), and `writeErrorResponse`. After a large response the session's `responseBuilder` is replaced, or it would keep the buffer it grew to. A large message is read and written a megabyte at a time (`readSliced` / `writeSliced`, `IO_SLICE`; the callback connection's reader and writer use them too): the JDK copies a heap buffer through a direct buffer as large as what remains of it and keeps that buffer for the thread, so one 500 MB message moved whole would hold 500 MB of direct memory until the session ends. Do not hand a channel a whole large heap buffer.
- A length that is none (zero, negative, beyond the limit) is answered with an error and ends the session. What follows such a length is not a length, and reading on would take it for one: four bytes that pass for the size of the command still lying in the command zone would run that command again. A large command that arrives after the idle timeout is read to its end and dropped before the timeout is reported (`skip`), or the client would fail writing it.
- The counts a command carries (arguments, array elements) are checked against the size of the command they were read from (`checkArgumentCount`, `arrayLength`), before anything is allocated for them.
- Client: `_write_command` and `_read_response` (`_recv_message`, `_recv_large`) in `client.py`; `_exchange` and `_read_response` in `async_client.py`. A builder that grew past `_KEPT_BUILDER_SIZE` is dropped from the thread's pool when its large command is sent (`_drop_grown_builders`), not when the thread next asks for a builder: after one large command its calls may all be packed by hand, which takes none. Before `connect()` there is no limit (`_max_message` is 0) and the size check is skipped, so that such a client reports that it is not connected.
- The callback connection has no shared memory, so its frames only got a larger limit: the constructor argument of `CallbackChannel` in Java, `_checked_size(header, limit)` in `callback_channel.py`, `_nested_command(call_id, data, limit)`. A large frame takes the connection's one writer for as long as it is written.
- A message that was announced must be read to its end: a timeout or an interrupt part way leaves the rest on the socket, so the sync client marks itself dead there as it does for the length.
- An error's text is at most `maxErrorText` characters: `MAX_ERROR_TEXT` (1 MiB) or a quarter of the largest message. `formatException` then shortens each exception's frames, not the end, where the root cause is.

### Where Changes Must Be Mirrored
The protocol is implemented several times over, so a wire-level change usually touches all of these:
- `schemas/commands.fbs` — add the `Action` / table / field
- `GatunServer.handleClient` — top-level dispatch (an `if/else if` chain on `cmd.action()`). Logs and metrics take an action's name from the generated `Action.names` (`RequestContext.actionName`)
- `GatunServer.executeCommand` — a second, separate dispatch used only for sub-commands inside `Action.Batch`; an action missing here works standalone but fails in `client.batch()`
- `src/gatun/client.py` — sync client, plus `BatchContext` if the action should be batchable
- `src/gatun/callback_channel.py` and `CallbackChannel.java` — the callback connection, for a change to how callbacks travel (see Python Callbacks)
- `GatunServer.executeNested` and `_NESTABLE_ACTIONS` in `client.py` — the actions a callback may send on the callback connection; the two lists must match (but for `Batch`, which `runNested` runs itself because it packs its own response), and an action belongs there only if it never touches the session's shared memory
- `src/gatun/wire.py` — the commands and responses of single calls and of batches, written and read with `struct` instead of the FlatBuffers library (see Key Implementation Details). Its offsets are written out by hand from the schema: a change to the fields of `Command`, `Argument`, `BatchCommand`, `Response`, `BatchResponse` or the plain value tables must be made there too, and `tests/test_wire.py` reads its output back with the generated classes
- `src/gatun/async_client.py` — shares value encoding and decoding with the sync client (the `_ValueEncoder` mixin and `_unpack_value_impl` in `client.py`) but has its own `_read_response`, and implements only a subset of the sync API (no `batch`, `cancel`, `send_arrow_buffers` / `get_arrow_data`, `arrow_from_java(copy=False)` or `arrow_to_java(copy=False)`; it does have `reflect`, `get_static_field`, `arrow_to_java` / `arrow_from_java` and `arrow_to_java_batches`)
- `PROTOCOL_VERSION` in both `GatunServer.java` and `client.py` for breaking changes

Java-side responsibilities are split: `GatunServer` (dispatch, object/callback registries, allowlist, value packing), `MethodResolver` (overload scoring, varargs), `ReflectionCache` (class/method/field/handle caches), `ArrowMemoryHandler` (Arrow IPC + buffer-descriptor transfer, epoch checks; one instance per session, since the loaded batch points into that session's shared memory), `ArrowLoans` (tables read in place for a client, and the mapping they are read through), `SharedMemoryBuffers` (reference counting for every Arrow buffer in shared memory), `ArrowBlockPool` (large Arrow buffers kept for reuse), `ResourceLimits` + `SessionQuota` (per-session limits), `observability/` (logging, metrics, JFR).

On the Python side, `client.jvm.a.b.C` navigation goes through `_JVMNode`, which asks the server (`Action.Reflect`) whether a dotted path is a class, method, or field and caches the answer in a per-client LRU (`_reflect_cached`; "none" is cached only for a name inside a known class, since a name that is not a class yet may become one). Names starting with `_` raise `AttributeError` without asking. An attribute of a capitalized parent that the server says is a field is fetched eagerly as a static field; spelling does not decide (`Encoders.STRING` is a method, `System.out` a field).

### Protocol Handshake
Format: `[4 bytes: version][4 bytes: arena_epoch][8 bytes: memory size][2 bytes: shm_path_len][N bytes: shm_path]`
- Protocol version (currently 8) is verified on connect
- Version mismatch raises RuntimeError with upgrade instructions
- SHM path is the per-client shared memory file path (e.g., `/tmp/gatun.sock.1.shm`)
- A further thread of a client joins its session on `<socket>.join`: it sends `[4 bytes: version][2 bytes: length][the session's SHM path]` (the path of the first channel, which names the session) and gets the handshake above, with an SHM file of its own. A session that is unknown or over is answered by closing the connection. See Threads and Channels.

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
- `tests/test_spark_integration.py` runs on the Spark it finds and is skipped when there is none (`find_spark_jars`): `$SPARK_HOME`, a distribution extracted in the repo root (`spark-*-bin-*/`, gitignored), or an installed `pyspark`, in that order. CI's `spark` job installs `pyspark` 4.1.1 and 4.3.0.
- `GATUN_TEST_CLASSPATH` (entries joined with `:`; `dir/*` is every JAR in a directory) puts the test session's server on a classpath. With Spark's `jars/*` the server is the thin JAR on Spark's Arrow, and the whole suite runs on it; CI's `spark` job does that.
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
./gradlew :gatun-core:thinJar        # Build thin JAR (gatun-server-thin.jar), for a classpath with its own Arrow
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
2. Runs `./gradlew :gatun-core:shadowJar :gatun-core:thinJar` if any file under `gatun-core/src` or `gatun-core/build.gradle.kts` is newer than the packaged JARs, and copies the results to `src/gatun/jars/` (`gatun-server-all.jar`, `gatun-server-thin.jar`).

`FLATBUFFERS_RUNTIME_VERSION` in the backend must match the `flatbuffers-java` version in `gatun-core/build.gradle.kts`; bump them together.

There are two server JARs, and the launcher picks one per server (`_server_jar` in `launcher.py`):
- **The fat JAR** (`gatun-server-all.jar`) has everything: the server, Arrow, and Arrow's Netty and Jackson, both relocated (`gatun.shaded.io.netty`, `gatun.shaded.com.fasterxml.jackson`), so neither is in front of a Netty or Jackson on the classpath. `slf4j-api`, `commons-codec` and `flatbuffers-java` are in it under their own names. It is used when the classpath has no Arrow the server can run on, which includes no classpath at all.
- **The thin JAR** (`gatun-server-thin.jar`) is the server's own classes and nothing else. It is used when the classpath brings Arrow (Spark's does), and the server then runs on that Arrow, with that Arrow's Netty, Jackson and FlatBuffers. In front of such a classpath the fat JAR's Arrow would shadow the classpath's class by class, and Spark 4.3 (Arrow 19, Jackson 2.21) fails on it.
- Arrow itself cannot be relocated: Java code on the classpath has to receive the `VectorSchemaRoot` that `arrow_to_java()` makes, as its own class. FlatBuffers cannot be relocated in the thin JAR either: `ArrowMemoryHandler` and `ArrowLoans` read Arrow's own messages (`org.apache.arrow.flatbuf`), which extend that library's `Table`.
- "Brings Arrow" means the classpath has one class of each library the server's code needs (`_HOST_ARROW_CLASSES`: arrow-vector, arrow-format, arrow-memory-core, arrow-memory-netty and its buffer patch, netty-buffer, netty-common, FlatBuffers), and that its Arrow is not older than the one the server is compiled against (`MIN_HOST_ARROW`, read from the `pom.properties` in arrow-vector's JAR or from the JAR's name; an Arrow that does not say is taken). The launcher reads each JAR's central directory as bytes and looks for the entry (`_central_directory`, `_jar_has`); `zipfile` would build an object for each of a quarter of a million entries. Only when the classpath as given lacks something does it open the JARs' manifests and follow `Class-Path`, as the JVM does (`_manifest_class_path`). A classpath with Arrow that is older or not all there gets the fat JAR and a warning. Bump `MIN_HOST_ARROW` with the Arrow version in `build.gradle.kts`.
- `server_jar` (`connect()`, `aconnect()`, `launch_gateway()`, `GatunAdapter`; `[tool.gatun] server_jar`; `GATUN_SERVER_JAR`) is `"auto"`, `"thin"` or `"full"`, with the usual precedence; anything else is a `ValueError`. A thin JAR that is missing where the fat one exists (a checkout built before there were two) falls back to the fat one with a warning in `"auto"`, and is an error for `"thin"`.
- The server is compiled against the Arrow in `build.gradle.kts` (18.3.0) and runs on the classpath's, so it must keep to Arrow API that the supported Spark versions share. CI runs the Spark tests and the Arrow tests on Spark 4.1.1 (Arrow 18.3) and 4.3.0 (Arrow 19): `GATUN_TEST_CLASSPATH` puts the test session's server on a classpath.
- The server's generated protocol classes never call `ValidateVersion()`, so on the thin JAR they run on whatever FlatBuffers the classpath's Arrow needs.

Keep all of that in mind when changing Java dependencies. The relocation renames Netty's system properties too: the fat JAR's Netty reads `gatun.shaded.io.netty.*` (`DEFAULT_JVM_FLAGS` in `launcher.py` passes `tryReflectionSetAccessible` under both names, the plain one for a Netty on the classpath, which is also the thin JAR's). With it the server's Arrow memory is counted by Netty (`gatun.shaded.io.netty.maxDirectMemory`, or `io.netty.maxDirectMemory` on the thin JAR), not by the JVM's direct memory limit. On the thin JAR that counter is shared with the classpath's own Netty buffers, and a `-Dgatun.shaded.io.netty.*` flag does nothing: the launcher warns and does not pass it on under the plain name, which would set it for the classpath's code as well.

## Key Implementation Details

- Java 22+ required. 22 is the first release where the Foreign Function & Memory API (`java.lang.foreign`, used to map the shared memory) is final; on 21 it needed `--enable-preview`. The Gradle toolchain and `--release` are pinned to 22 so the JAR runs on 22 and later, and CI runs on 22 to exercise that floor. Do not raise it casually.
- Python 3.11+ required; CI runs 3.11 to 3.14. Syntax and library features newer than 3.11 are out: no `class C[T]` / `def f[T]` (PEP 695), no `type` statements, no f-string that reuses its own quote. `[tool.mypy] python_version` is the floor, so mypy reports most of them
- Arrow memory requires JVM flags: `--add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED`
- Arrow buffers over Netty's chunk size (4 MiB) come from the server's own pool (`ArrowBlockPool`, the root allocator's `AllocationManager.Factory`): Netty hands those out unpooled, as new memory every time, and a 128 MiB `arrow_to_java` spent most of its time faulting pages in. A freed block is kept (`gatun.arrow.pool.max_bytes`, by default the shared memory size; the blocks returned longest ago go first) and given back after `gatun.arrow.pool.idle_ms` (a minute). `getSize()` of a pooled allocation is the size asked for, not the block's: Arrow's accounting releases `getSize()` bytes and reserved the request. Blocks come from Netty's unpooled allocator like before, so a kept block counts against the direct memory limit, and an allocation that runs out of memory empties the pool and tries once more; that goes for the allocations the pool passes on to Netty too, which fail with Arrow's `OutOfMemoryException`, not an `OutOfMemoryError`. A new block is rounded up by at most an eighth, and is the size asked for when the rounded one does not fit. The root allocator has the pool's rounding policy (`ArrowBlockPool.rounding`): Arrow's default takes every request under 16 MiB to a power of two, so a 10 MiB table would count as 16 against the pool's limit. A pool property that is not a whole number stops the server at startup. The pool's numbers are in `get_metrics()`
- Arrow's debug allocator is off by default (it costs about 2 ms per small `arrow_to_java`); `extra_jvm_flags=["-Darrow.memory.debug.allocator=true"]` turns it on to find where a leaked buffer was allocated
- Copies of Arrow data go into memory that gets reused (`_read_arrow_stream` copies into `pa.allocate_buffer`, not a `bytes` object; pyarrow's default mimalloc or jemalloc pool keeps the memory of a dropped table): from 32 MiB up, glibc gives a new mapping for every allocation, and the page faults of touching new memory cost several times the copy
- Both clients use `weakref.finalize` for automatic Java object cleanup (`detach()` opts a handle out). The finalizer only queues the object ID (`GatunClient._queue_free`), because a finalizer can fire in the middle of another exchange. Never send on the socket from a finalizer. The queue goes out with the next request: a packed command or batch (see `wire.py` below) carries up to 512 IDs in `Command.free_ids`, which the server releases before it runs the command (`freeRiding` in `handleClient`), so a dropped handle costs no round trip. Before any other command, when a packed one has no room left, and when the queue holds more than one command carries, the whole queue is sent as `FreeObject` commands of its own (`_flush_pending_frees`): every request leaves it empty or nearly so, or a program that drops more than 512 handles per call would fill the server. A request that ends in `CancelledException` puts its IDs back on the queue: the interrupt may have come before the server released them, and it ignores an ID it has released already. An explicit `client.free_object(id)` is sent immediately.
- `_build_value` must not make Java calls: the outer command is half-built in a pooled builder and the channel carries one request at a time. Values that need a Java-side object are encoded on the wire instead (see `TemporalVal` for `datetime` / `date` / `time`).
- Socket path: Generated randomly in `/tmp/gatun_<pid>_<random>.sock` by default. The launcher refuses a fixed `socket_path` that a live server is listening on
- The launched server runs in its own process group and is passed `-Dgatun.parent.pid`; it exits when that process is gone (`GatunServer.watchParent`), which covers a Python process that was killed
- A table from `get_arrow_data().table` is built on `pa.py_buffer(memoryview(shm)[...])`, so it holds a buffer export of the mapping and keeps it alive past `close()`. Do not go back to `pa.foreign_buffer` on a raw address there: such a table reads unmapped memory after `close()`
- The server's threads that wait on a socket are platform threads: one per channel (`gatun-session-N` for a session's first, `gatun-channel-N` for each thread that joined; a new thread each time so that its thread-locals end with it), the one that accepts joins (`gatun-join`) and the callback connection's reader and writer. A virtual thread blocked on a socket is woken through the JDK's poller thread and then a carrier, which cost about 30 μs per request and more per callback from another thread; a platform thread is made runnable by the kernel. They are daemon threads. Do not move them back to virtual threads without measuring single calls on Linux.
- The FlatBuffers library for Python is pure Python and costs several microseconds per table, which was most of a call's time. `wire.py` writes the same bytes with `struct` for the commands of single calls: `create_object`, `invoke_method`, `invoke_static_method`, `get_field`, `set_field`, `get_static_field` and `is_instance_of`, in both clients, when every argument is exactly an `int`, `float`, `bool`, `str`, `None` or a Java object (`pack_command`). It returns None for anything else (a subclass, a `TypeHint`, a collection, an integer past 64 bits) and the method goes on to the builder, which is why each of them still has its builder code. A `batch()` is packed the same way when every one of its commands can be (`pack_batch`; `BatchContext._build` is the builder's version). A packed command is some tens of bytes longer than the builder's, since every field of its tables is present, so one within 512 bytes of the command zone's size is also left to the builder, which may fit it into the zone (`_PACKED_LIMIT`). `wire.read_plain` reads a response that is one such value and nothing else but the `call_id` of a reply on the callback connection, in `_read_response` and `_send_nested`, before the generated classes are tried, and `wire.batch_values` does that for each response of a batch; an error, a callback request and every other value go the old way. The server cannot tell the difference and has no code for it.
- A list whose elements are all `int`, all `float`, all `bool` or all `str` (the exact type) travels as one vector of `ListVal` (`longs`, `doubles`, `bools`, or `string_lengths` + `string_bytes`), in both directions; any other list is one `Argument` per element (`items`). A map is two such lists, keys and values. The Python builder costs several microseconds per table, so the client packs these with `struct` (`_build_list`) and reads them with `struct.unpack_from` at the vectors' vtable slots, which are written out in `client.py` (`_LIST_LONGS` and the others): change the order of `ListVal`'s fields in the schema and they must follow. Java keeps the rule that a list of integers is all `Integer` if every value fits and all `Long` otherwise (`convertListVal`), also for integers that came one by one; the integers of a map are typed one at a time, as a single argument is
- Numbers that fit no primitive travel as two values of their own, in both directions: `BigIntVal` (`java.math.BigInteger`; two's complement bytes, since Python refuses to print or parse an int of more than 4300 digits) and `DecimalVal` (`java.math.BigDecimal`; the text of `BigDecimal.toString()` / `str(Decimal)`, which keeps the scale). Python sends a `BigIntVal` only for an `int` past 64 bits (`_build_value`; the hand-packed path gives such a command to the builder); Java sends one for every object whose class is exactly `BigInteger`, and a `DecimalVal` for exactly `BigDecimal` (`packValue`). A subclass, and every other `Number` (`AtomicLong`), is an object reference as before. Because the Java type of a Python int depends on its size, `MethodResolver` lets an `int` or `long` argument match a `BigInteger` or `BigDecimal` parameter, below every other match (`isPythonBigNumber`, converted in `coerce`). A float never matches `BigDecimal`. Values that are `numbers.Integral` or `numbers.Real` without being `int` or `float` (numpy's scalars) are sent as `int(value)` / `float(value)`, and numpy's bool, which registers with neither, as `bool(value)`: a type that has `dtype` and no `__array_finalize__` (a scalar, not an array) with `dtype.kind == "b"` (`_is_bool_scalar`; looked up on the type, since a class handle's `__getattr__` would ask Java); numpy is not imported, and is a dev dependency for the tests
- `return_object_ref` (`Command.return_object_ref`) makes the server register the result and return a reference instead of a converted value: for `InvokeMethod`, `InvokeStaticMethod`, `GetField` and `GetStaticField`, in `handleClient` and `executeCommand` alike. It is the way to get a `BigDecimal`, a `String` or a `List` as a Java object; constructors always return one. A batch's commands carry it as the fifth element of their tuple (`BatchContext._command_data`, `wire.pack_batch`)
- `register_converter(python_type, fn)` (`_ValueEncoder`, both clients): converters run when a call's arguments cannot be packed by hand and before the builder is taken, since they may call Java (`_convert_arguments`; the async client awaits them). A call whose arguments are all `int`, `float`, `bool`, `str`, `None` or Java objects never asks, which is why those types take no converter. A batch converts when it is executed, if `wire.pack_batch` gave it to the builder. `_converting` (a `ContextVar`) holds the types being converted in this context, so that a converter which passes its own argument on to Java is not called for it again
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

A callable answers every abstract method of its interface. An object that is not callable answers each call with its method of the same name (`_callback_target` in `client.py`, from `Response.callback_method`), which is how an interface with several methods is implemented. Registration sends the object's method names (`Command.callback_methods`, `_callback_methods`), and the proxy sends a default method to Python only if its name is among them; the sync client refuses coroutine functions at registration:
```python
class Listener:
    def onQueryStarted(self, event): ...
    def onQueryProgress(self, event): ...

listener = client.register_callback(Listener(), "com.example.QueryListener")
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

The policy above is for callbacks made by the thread that is running the request, which travel in band. Default methods of the interface (`Comparator.reversed()`) run in Java and never reach Python, unless the handler is an object with a method of that name.

**Callbacks from other threads: the callback connection**

Any other Java thread (a Spark listener, a timer, an executor's worker, a parallel stream), at any time, calls Python on the session's callback connection. `GatunServer.invokeCallback` chooses by `SessionControl.isSessionThread()`: `exchangeCallback` in band, `callOnChannel` otherwise.
- It is a second Unix socket with no shared memory: each session maps its memory with `Arena.ofConfined()`, so no other thread can touch it. Frames are `[4 bytes length][FlatBuffer]`, as large as a command or response may be on the main channel (see Values Larger Than a Zone): a `Response` with `is_callback` from Java, a `CallbackResponse` command from Python, paired by `call_id`. Calls from many threads are in flight at once.
- It is opened lazily, by the first `register_callback` (`_ensure_callback_channel`): `Action.OpenCallbackChannel` on the main channel makes the server listen on `<socket>.<session id>.cb`, a socket that accepts one connection and is then removed. The connection takes no session slot. If it cannot be opened, the client logs a warning and in-band callbacks still work.
- Server: `CallbackChannel` has one reader and one writer thread; callers only use queues. Callers are threads the server does not own, and interrupting a thread that is blocked on a channel closes the channel. `callOnChannel` sets the `sessionContext` thread-local to the proxy's session while it packs and decodes, because `sessionObject` skips its ownership check on a thread with no session.
- Python: `CallbackChannel` (a daemon reader thread, one thread per call) and `AsyncCallbackChannel` (a reader task, one task per call) hand each request to the client's `_serve_channel_callback`. They hold the client weakly, so a client dropped without `close()` is still collected. These callbacks are not recorded as running "inside a request" (`_callback_threads`; their `_CallbackScope` has a `call`), so they may call Java.
- Their Java calls travel on the callback connection too. Until a callback answers, the Java thread that called it is waiting, and it is the one thread certain to be free: the session's own thread may be busy with a request, possibly the one waiting for that thread. `_send_raw` finds the call the current context is running in (`_channel_call_of`) and sends the command wrapped in an `Action.NestedCommand` frame with the call's ID (`_nested_command`, `ChannelCall.request`); `GatunServer.callOnChannel` runs it through `executeNested` between waits and replies with a `Response` carrying the same `call_id`. One request per call at a time, and none after the callback's answer (`ChannelCall`'s two locks): Java stops listening for a call once it has the answer. A caller that finds its callback already answered (a thread it started, a callback that timed out) falls back to the channel of the thread it runs on, which it opens if it has none (see Threads and Channels). A request already on its way when the callback answers still gets its response: the connection keeps a call's route until the callback is done and no request is waiting (`ChannelCall.finish`).
- The Java thread can stop waiting first: it was interrupted (`Future.cancel(true)`, `shutdownNow()`). `CallbackChannel.end` then answers every command that arrives for the call, or already sits in its inbox, with an error of type `org.gatun.server.CallbackCallEnded` (`GatunServer.CALL_ENDED`, `_CALL_ENDED` in `client.py`) instead of leaving it without a reply; the client marks the call abandoned and sends the command on the main channel, where it has not run yet. `runNested` restores the interrupt flag when a nested command was interrupted: the interrupt was for the thread.
- `runNested` checks `ctx.closed` before and after the command and drops what it registered for a session that has ended, like `callOnChannel` does for a callback's arguments. It records the command in `Metrics` and the request log itself; the session's `SessionObserver` follows the one request the session's own thread runs and is not thread-safe.
- Only `executeNested`'s actions travel that way. Everything else is refused in Python with `ReentrancyError` before it is sent: the session's shared memory is confined to the session's thread, so every Arrow transfer that uses it is out (see the next point for the two that have another way). The vectorized actions, `Batch` and `RegisterCallback` run through the methods the session's thread uses for them (`getFields`, `invokeMethods`, `createObjects`, `runBatch`, `registerCallback`); a vectorized command that fails part way releases what it had made (`runNested`'s `vector`), and a response that cannot be delivered takes a newly registered callback with it (`discardNested`).
- `arrow_to_java` / `arrow_from_java` inside such a callback go through the call's own shared memory (`GatunServer.CallMemory`, `CallMemory` in `callback_channel.py`). The first transfer of a call sends `Action.OpenCallMemory` as a nested command: the Java thread waiting for the callback creates `<socket>.<session id>.call<call id>.shm`, as large as the session's and all of it payload, maps it with an arena confined to itself (it is the thread that runs the call's nested commands) and returns the path. `ArrowToObject` / `ArrowFromObject` as nested commands then mean that memory, through the same `arrowToObject` / `arrowFromObject` the session thread uses. The files are sparse and callbacks at once each have one, so a new one must leave the existing ones their whole size in free space (`callMemoryBytes`) or the transfer is refused: a disk that fills up under a mapping is a fault, not an error. `callOnChannel` unmaps and deletes the file when the call ends (a client that finds it gone treats the call as over, and looks before each transfer, `CallMemory.removed`: no space is kept for a file that is gone, so nothing may be written to its mapping); the client's mapping lives until nothing refers to it, and nothing writes there after the call, so a `copy=False` view ends only with the call's next transfer (`CallMemory._payload_writes`). A transfer is several steps on that one memory, so it holds the call's `transfer_lock` throughout (an `asyncio.Lock` in the async client, where tasks of one callback share the call). The public methods try this route first and fall back to the client's channel when the call is over, like `_send_nested`; `@_holding_channel` is on the `_on_channel` halves. `@_holding_channel` methods refuse as well. A nested call that times out leaves its `ChannelCall` unusable (the late response would be taken for the next one) but not the client. That response still names objects Java registered for the client: the reader hands it to `_release_unread_response`, which queues them for release without wrapping them.
- Because several threads now register objects for one session, `sessionObjectIds` is a concurrent set and the server tracks exactly which IDs each pack registered (`packRecorder`). Do not go back to counting the set before and after, or to ranges of the ID counter: both pick up what another thread registered meanwhile.
- A proxy outlives its session, and its registration, in whatever Java code holds it; called then, it throws `IllegalStateException`. `invokeCallback` checks `SessionContext.closed` and `callbackRegistry` before it packs anything, on both paths: the objects among the arguments are registered for the client while packing, and a client that will not run the callback would never release them. For the same reason `callOnChannel` drops what it registered when the request could not be delivered or the session ended meanwhile. The Python side keeps a callback until `unregister_callback`, or until Java reports that the proxy is gone: `registerCallback` registers each proxy with a `Cleaner` (`PROXY_CLEANER`), and once the client has freed its handle and no Java code holds the proxy, `proxyCollected` unregisters the callback and sends a `Response` with `callback_released` on the callback connection, where the client's reader drops the function. The sync reader hands the function to a thread of its own to die on (`CallbackChannel._let_go`): a finalizer may call Java and wait for the channel, and the reader has to go on reading. Both readers look a call's handler up themselves, before the next frame, which may be the report that the callback is gone. For that to happen nothing in the server may keep the proxy alive but the object registry: `CallbackInfo` does not hold it, and the cleaning action must not capture it.
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
- A channel is busy with the request being cancelled, so `cancel()` opens a short-lived second connection and sends one `Action.Cancel` per channel, with `target_name` set to the channel's SHM path. The server looks the channel up in `GatunServer.channels` and interrupts its thread (`SessionControl.cancel`). `cancel()` names every channel of the client, `cancel(thread)` the one whose `_Channel.thread` is that thread.
- The interrupt is delivered only while the thread runs Java code, never during socket I/O (`beginSocketUse` / `endSocketUse`): interrupting a blocked channel read would close the channel. A cancel that arrives during a callback exchange takes effect when the exchange ends.
- `InterruptedException` maps to `CancelledException`. Java code can poll `GatunServer.checkCancelled()`.
- An interrupt ends a batch, whatever `stop_on_error` says: `runBatch` rethrows the `InterruptedException` instead of packing it as that sub-command's error, drops what the batch had registered, and the batch fails as a whole (`BatchResult.get()` then raises `RuntimeError`). Results would report the commands that never ran as done. The same holds for a batch inside a callback whose Java thread was interrupted.
- Sync client only. With `gatun.limits.max_sessions` reached, the second connection waits two seconds for a slot (`takeSessionSlot`), is refused, and `cancel()` returns False.

### Threads and Channels
`docs/threading-design.md` is the design; this is what the code does.
- A **session** is what a client's threads share on the server: objects, quota, callbacks, the callback connection (`SessionContext`). A **channel** is one connection of it: a socket, an SHM file with all three zones, the JVM thread that serves them, and the Arrow state of that memory (`ChannelContext`, and in `handleClient` a `SessionObserver`, `ArrowLoans` and `ArrowMemoryHandler` per channel). `handleClient(client, id, joined)` runs both kinds; `first` is the channel `connect()` made, and the session ends with it.
- Each Python thread that calls Java has a channel (`_Channel` in `client.py`, found through `GatunClient._local`, a `threading.local`). The first belongs to the client and to the thread that connected. Any other thread joins with its first call (`_open_channel`, the server's `acceptJoins` / `joinSession`, a thread `gatun-channel-N`), so all its calls run on one JVM thread. `_ThreadChannel.__del__` closes the channel when the thread's locals are dropped. A join takes no session slot and counts as activity.
- The connection attributes of the client (`sock`, `shm`, `memory_path`, `_exchange_lock`, `_arena_epoch`, `_arrow_loaded_end`, `_payload_generation`, `_payload_writes`, `_loans`, `_loan_ends`, `_arrow_schema_cache`) are properties that read the calling thread's channel (`_of_channel`). The request path does not use them: `_send_raw` fetches the channel once and hands it to `_write_command` / `_read_response`. Code that must act on another thread's channel does it inside `_borrowing(channel)`, which holds that channel's lock and makes it the calling thread's for the block (ending a loan, an allocation in another thread's arena).
- What stays with the client, shared by all threads: `_pending_frees` (whichever thread sends next carries the releases), the callbacks and the callback connection, the reflection and string caches, `_callback_threads`. The reentrancy check comes before the channel lookup, so a refused call opens nothing.
- A table view and a loan remember their channel (`ArrowTableView`'s third argument, `_Loan.channel`). A view is read from any thread under its channel's lock. A loan is ended on its own channel: by its owner at once, by another thread only if that channel is free (`_end_loan` does not wait for a thread that may be inside a long request), otherwise with the owner's next request. `send_arrow_buffers` refuses an arena whose mapping is not the calling thread's.
- When a channel ends, the server ends its loans (`loans.close()`), gives back the Arrow bytes it had counted for the channel's arena (`arenaBytes` in `handleClient`: `reset_payload_arena` on one channel must not clear another's count) and deletes its SHM file. A response that cannot be written on a joined channel takes what it registered with it (`discardNested`), since the session lives on and nobody would release it.
- `_exchange_lock` is per channel and an `RLock`. Its own thread never waits for it; it is there for borrowers. Commands are built before the lock is taken, so the builder pool is per thread (`_get_builder`), and the methods that use the payload zone around their request hold the lock throughout (`@_holding_channel`). Reentrancy is per thread (`_callback_threads`) and per context (`_callback_scopes`, a `ContextVar` that tasks started by a callback inherit; the scope's `active` flag lets a task that outlives its callback call Java). A plain thread started by a sync callback inherits nothing, so it is a thread like any other: it joins with a channel of its own and its Java calls run while the request waits for the callback.
- Any `BaseException` that interrupts an exchange after the command was written (timeout, `KeyboardInterrupt`) ends the channel: the answer is still coming and the next request would read it (`_mark_dead(channel)`). For a joined channel that is all: the thread's next call opens another, on another JVM thread. Losing the first channel is losing the session, so the client is dead and every channel is closed. A mapped Java exception ends nothing. After `close()` or death, calls raise `DeadConnectionError`.
- `_Channel.close()` shuts the socket down and closes the descriptor and the mapping only if no thread is inside an exchange on the channel; otherwise that thread wakes from the shutdown, fails, and closes them itself. Do not close a descriptor another thread may be reading: on macOS a read that the shutdown has made runnable, and that finds the descriptor closed when it runs, waits out its timeout.
- If the server has no join socket (the path was too long to bind: `FileNotFoundError` on connecting), the client logs one warning and its threads share the first channel, one request at a time, as they did before channels. A refused connection is something else: macOS refuses when the listening socket's queue is full, which a pool of threads making their first calls at once can do. `_connect_to_join_socket` tries again for `_JOIN_RETRY_SECONDS` and then fails the call; the server listens with a backlog of 1024 (`JOIN_BACKLOG`). Never turn a refusal into the shared channel: Java thread-locals would then be shared between Python threads for the rest of the client's life.
- Asking for a channel property gives the calling thread a channel (`_channel()` opens one). Code that may run on a callback's thread must not ask: `free_object` tests `_closed` and `_first`, and the payload arena's hook is handed its channel (`_require_clear_of_loans(..., channel)`, `_note_payload_write(end, channel)`) and takes no lock, because inside a callback the request's thread holds the channel while it waits for that callback.
- `_borrowing(channel)` gives False for a channel that has ended, and while a channel is borrowed `_channel()` does not replace it if it ends (`_local.borrowed`): the replacement would become the borrowing thread's own channel and close the one it had.
- A lent table's object outlives its channel on the server: the server ends the loan with the channel (the table is closed) and keeps the registry entry, as after `reset_payload_arena()`. The client releases it like any other object: `_end_loan` and `_queue_loan_end` put the ID on `_pending_frees` when the loan's channel is dead, and `_Channel.close()` does that for the ends still queued on it (`_Channel._frees` is the client's queue).
- The idle check in the request loop is the reaper's (`isTimedOut` and no request active on any channel and no callback in flight): a request records activity when it ends, so during one thread's long request another thread's call would otherwise find the session "idle" and be refused.
- `SessionQuota.allocateObject` checks and takes a slot in one step (compare-and-set). Channels and callbacks create objects at the same time.
- `openJoinSocket` sets `serverUser` before its accept thread starts: `isSameUser` lets every peer in while it is unset, and the join socket is bound before the server's own.
- The async client has one channel; tasks take turns on it (`_send_raw`'s lock).
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
Execute multiple commands in a single round-trip (100 calls take about an eighth of the time; numbers in `docs/benchmarks.md`):
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
For several operations on the same target, the vectorized APIs take the whole list in one command and return a list. A `batch()` of the same operations is several times faster: batches are packed by hand (`wire.py`) and these commands are not:

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
| `batch` | Mixed operations on **different objects**, and whenever speed matters |

### Low-Level API
For direct control:
```python
client.create_object("java.util.ArrayList")           # No-arg constructor
client.create_object("java.util.ArrayList", 100)      # With initial capacity
client.invoke_method(obj, "methodName", arg1)         # Direct method call (object or ID)
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

with client.arrow_to_java(table, copy=False) as root:   # lent: read in place until the block ends
    client.jvm.com.example.Stats.summarize(root)
batches = client.arrow_to_java_batches(table)   # ArrayList<byte[]> of record batch messages (Spark)

produced = root.slice(1, 2)               # a table made on the Java side
result = client.arrow_from_java(produced)  # pyarrow.Table that owns its data
view = client.arrow_from_java(produced, copy=False)  # ArrowTableView, read in place
produced.close()                          # Java-made tables are the caller's to close
```
- Wire: `Action.ArrowToObject` / `ArrowFromObject`; the table travels as an Arrow IPC stream in the payload zone, located by `arrow_stream` (a `BufferDescriptor`) on `Command` / `Response`. `Command.arrow_form` says what Java makes of it: a table of its own (the default), a lent table, or byte arrays. Both clients implement them; they are not batchable. They are the one Arrow path that works inside a callback from another thread, where the stream is in the call's own memory instead (see Python Callbacks).
- A table made by `arrow_to_java` owns its memory (it does not point into shared memory). `GatunServer.ownedTables` counts the registry entries that refer to it and closes it with the last one, so freeing the handle releases the memory. It is charged to `max_arrow_bytes` until then, separately from the arena's count, which `reset_payload_arena` clears.
- `arrow_to_java(table, copy=False)` lends the table (`ArrowForm.LentTable` in `Command.arrow_form`; sync client, main channel only) and returns an `ArrowLoan`. `ArrowLoans.lend` (one `ArrowLoans` per session) finds the stream's messages without reading their bodies (`ArrowMemoryHandler.ipcMessages`) and loads the one record batch over the bytes in the zone.
  - The buffers do not go through the session's own mapping, which is confined to its thread and unmapped when the session ends: a lent table is an ordinary `VectorSchemaRoot` that Java code may pass to any thread. They go through a second, shared mapping of the same file, made by the first loan from the session's open `FileChannel` (not the path, which may name another file by then) and unmapped once the session has ended and no lent buffer is referenced (`ArrowLoans.close` / `returned`).
  - The data is not charged to `max_arrow_bytes`. What the table allocates beside it is, until the loan ends (`ArrowLoans.allocatedBy`): `VectorLoader` makes a validity bitmap for every column that came without one.
  - Freeing the table's object ends the loan, whatever else refers to the table (`lentTables` in `GatunServer.removeObject`): the reference manager then reports a count of zero, which makes every checked Arrow accessor throw, and the table is closed. Do not make the end depend on reference counts: the client reuses the bytes as soon as it is told.
  - Every Arrow buffer in shared memory, lent or loaded by `send_arrow_buffers`, has the same `ReferenceManager` class (`SharedMemoryBuffers`). Arrow asks for the reference count before each read at one call site, which the JIT inlines for two classes at most; a class of its own for loans made a full read of a lent table take half as long again and slowed reads of other tables in the JVM. Do not add a third.
  - The client must know that the server ended a loan before it writes there again, so a loan's release never rides with the others (`_pending_frees` may go out unanswered on the callback connection): it goes through `_loan_ends`, sent on the main channel by `_flush_loan_ends`, and a loan whose end the server did not confirm keeps its room and is tried again. Open loans are in `_loans`; streams are placed after them (`_send_arrow_stream`), the server writes after them (`ArrowFromObject`), and the transfers with a fixed place (`send_arrow_table`, the arena, `get_arrow_data`) are refused on both sides when they would overlap one. `send_arrow_buffers` checks the whole table before it copies the first buffer: a send refused half way has already overwritten the batch Java holds. `reset_payload_arena` ends them all.
  - Inside a channel callback there is nothing to lend from (a call's memory goes with the call), so the client sends an ordinary copy and the `ArrowLoan` frees it on exit. A copy that ends with the block keeps every promise a loan makes, which is what makes that fallback legal anywhere.
- `arrow_to_java_batches(table)` (`ArrowForm.Batches`; both clients, and inside channel callbacks) returns a `java.util.ArrayList<byte[]>`: `ArrowMemoryHandler.readIpcBatches` copies each record batch message out of the zone as it is. The table keeps its chunks (`_arrow_table_to_send(one_batch=False)`), one batch each, split by `max_rows_per_batch`.
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
- `int` past 64 bits <-> `java.math.BigInteger`, `decimal.Decimal` <-> `java.math.BigDecimal` (both directions)
- numpy scalars and other `numbers.Integral` / `numbers.Real` values -> as `int` / `float`, `numpy.bool_` -> as `bool` (arguments)
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

A `JavaException` carries the thrown exception as a Java object, `java_exception` (Py4J's name):
- The server registers the `Throwable` for the session when it packs the error (`GatunServer.packThrown`, `Response.error_object`), at all three places a command's failure is packed: `handleClient`, a batch's sub-command (`runBatch`) and a nested command on the callback connection (`runNested`). Errors the server writes without an exception (a response that is too large, `CALL_ENDED`) have none.
- It is an ordinary object of the session: counted against `max_objects`, released by the wrapper's finalizer when the Python exception is collected, and its ID goes into the list of what the response registered (`packedIds`, `registered`), so that a response that is never delivered takes it along. A late response nobody reads releases it too (`_release_unread_response`).
- A session at its object limit gets the error without the object (`java_exception` is None): the failure being reported must not become a complaint about the limit.
- The exception must die with its last reference, not with the garbage collector: `_raise_java_exception_impl` raises without binding the exception to a local, which would tie it into a cycle with its own traceback. `tests/test_exception_object.py` raises 10,000 with the collector off and 50 objects allowed. A batch's errors are kept by the `BatchContext` and go with it.
- `CancelledException` and `PayloadTooLargeError` have no place for it; the wrapper is dropped, which releases the object.

Python-side exceptions:
- `ReentrancyError` - Raised when attempting nested Java calls from within a callback
- `PayloadTooLargeError` - Raised when Arrow data exceeds the payload zone, or a command or response the size of the shared memory (`max_message_size`)
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

`allow_all=True` (`connect()`, `aconnect()`, `launch_gateway()`, `GatunAdapter`; `[tool.gatun] allow_all`; `GATUN_ALLOW_ALL`) lifts the list: `ClassAllowlist.isAllowed` answers true for every name, which also opens the instances of `RESTRICTED_TYPES`, since they are restricted only until their type is allowed. `REFUSED_MEMBERS` stay refused. It is one setting, not a list, so it follows the usual precedence (argument > environment > `pyproject.toml`), and anything but true or false is an error in every source: the launcher passes `-Dgatun.allowlist.all=true`, and the server logs one line at start. An empty prefix is still a `ValueError`; its message names `allow_all`.

What the allowlist covers, and what it does not (the README's "What the Allowlist Is and Is Not" is the user-facing version; keep the two in step):
- It gates object creation and static method/field access by class name (`requireClassAllowed`).
- Instance members are open on whatever an allowed call returns, except objects of `ClassAllowlist.RESTRICTED_TYPES` (reflection objects, method handles, class loaders, `Runtime`, `Process*`, `Thread`, `Module`), which need their type in `allowed_classes` (`checkInstanceMember`, called at every instance method/field site). `REFUSED_MEMBERS` (`System.exit` / `load` / `loadLibrary`, `Class.newInstance`) are refused whatever is allowed.
- Class lookup never initializes: `ReflectionCache` loads with `Class.forName(name, false, loader)`, and `Class.forName` called from Python is swapped for `ReflectionCache.classForName` in `CachedMethod`.
- It is a guardrail, not a sandbox. Allowed prefixes and classpath JARs can do anything, and `GetField` / `SetField` reach non-public fields (`get_fields(sb, ["count"])` relies on that). The trust boundary is the OS user: see Session Isolation.

### Resource Limits
Per-session limits (object count, string length, list entries, nesting depth, batch size, Arrow bytes, idle timeout) and the server-wide `max_sessions` are defined in `ResourceLimits.java` as `gatun.limits.*` system properties read once at JVM start. An idle session is closed by a reaper thread when its timeout passes; idle time runs from the end of the last request on any of its channels, and a session with a request running on any channel is not idle (`SessionContext.isRequestActive`). Override them per server with `connect(extra_jvm_flags=["-Dgatun.limits.max_objects=5"])`; see `tests/test_resource_limits.py`. The string, list and nesting limits are checked only where the server converts what Python sent (`convertArgument`: arguments, field values, a callback's answer). The length of `bytes` and of an array is not counted against them (`arrayLength` checks only that it fits the command), but the elements of a `String` or `Object` array go through `convertArgument` like any argument. Values Java returns are not checked against them: they are bounded by the memory size and the session's largest message. Objects Java returns inside a collection or as a callback's arguments count toward `max_objects` without being refused (`SessionQuota.countRegisteredObjects`). The README's Resource Limits section has the table; keep the two in step.

### Session Isolation
- The trust boundary is the OS user: the socket and every SHM file are `0600`, the server checks the peer's UID on accept (`SO_PEERCRED`), and it creates each SHM file itself (`CREATE_NEW`) after deleting whatever was at that path
- Each channel of a session gets its own shared memory file (see Threads and Channels); a client with one thread has one
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
startup_timeout = 30.0           # seconds to wait for the server socket (the default)
allowed_prefixes = ["com.example"]  # added to the built-in class allowlist
allow_all = false                # true lifts the class allowlist
server_jar = "auto"              # "thin" or "full" to choose the server JAR (see Generated Code)
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

# Callbacks (a callable, or an object with the interface's method names).
# Any reference to the callback object unregisters it.
comparator = bridge.register_callback(lambda a, b: a - b, "java.util.Comparator")
bridge.unregister_callback(comparator)
```

`gatun.bridge.JavaException` and its subclasses are the client's classes (imported, not defined again), so the adapter converts nothing: the objects it returns are the client's `JavaObject`s, and a method called on one raises the client's exception whatever the bridge defines.

### PySpark Usage
Nothing in this repo or in stock PySpark switches PySpark over to Gatun; there is no `PYSPARK_USE_GATUN` switch. Running PySpark on Gatun needs a PySpark build ported to the `BridgeAdapter` interface (`docs/bridge_adapter_spec.md`). What this repo tests is driving Spark's JVM classes directly via `connect(classpath=<spark jars>)` (`tests/test_spark_integration.py`, on Spark 4.1.1 and 4.3.0 in CI), which needs a JDK Spark supports: it passes on 22 and fails on 27. The server runs on Spark's own Arrow there (see Generated Code for the two JARs). That file's `dataframe_from_arrow` / `arrow_from_dataframe` helpers show a pyarrow table becoming a Spark DataFrame and a query result coming back as Arrow, through `arrow_to_java_batches` / `arrow_from_java` and Spark-internal converters (`ArrowConverters.toDataFrame`, `Dataset.toArrowBatchRdd`).

## Logging

- Java: `java.util.logging` (Logger for `GatunServer`)
- Python: `logging` module (loggers for `gatun.client` and `gatun.launcher`)

## Method Overload Resolution

Gatun uses specificity scoring to resolve overloaded Java methods:
1. Exact type matches get highest priority
2. Compatible types (e.g., `String` matching `CharSequence`) get medium priority
3. `Object` parameters get lowest priority
4. Non-varargs methods are preferred over varargs
5. A Python `int` matches a `BigInteger` or `BigDecimal` parameter below all of the above
6. A Python `int`, `float` or `bool` matches a parameter typed as a supertype of its wrapper (`Number`, `Comparable`, `Serializable`), as Java's boxing does, but a method that needs this for an argument is chosen only when no method matches without it (`MethodResolver.BoxedTier`): scores add up over the arguments, and otherwise such a match could take a call from the method it resolved to before. Among those methods a tie that no parameter type breaks (`g(Comparable)` beside `g(Serializable)`) is refused as ambiguous
7. A bridge method (the compiler's `compareTo(Object)` beside `compareTo(BigDecimal)`) is called only when nothing else matches: its `Object` parameter would outscore every conversion the real method needs

Note: For methods with `double` parameters (like `Math.pow`), pass Python floats:
```python
Math.pow(2.0, 10.0)  # Works: 1024.0
Math.pow(2, 10)      # May fail: int args don't match double params
```
