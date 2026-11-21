# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- A second server JAR, `gatun-server-thin.jar`, with the server's own classes and nothing else. The launcher starts the server from it when the `classpath` brings its own Apache Arrow, and the server then runs on that Arrow and on the Netty, Jackson and FlatBuffers it came with. Spark 4.3.0 (Arrow 19.0, Jackson 2.21) works: 26 of the 39 Spark tests failed there, because the server's Arrow 18.3 and Jackson 2.20 stood in front of Spark's
- `server_jar="thin"` / `"full"` for `connect()`, `aconnect()`, `launch_gateway()` and `GatunAdapter`, also `server_jar` under `[tool.gatun]` and `GATUN_SERVER_JAR`, decide which server JAR is started. The default, `"auto"`, takes the thin one when the classpath (its entries, and what their manifests add with `Class-Path`) brings all of an Arrow 18.3 or later; an Arrow that is older or incomplete gets the full JAR in front of it, as before, and a warning
- CI runs the Spark tests, and the whole suite with the server on Spark's classpath, against Spark 4.1.1 and 4.3.0
- `benchmarks/benchmark_comparison.py --py4j-nodelay` runs Py4J's server with `TCP_NODELAY` set on the connections it accepts
- A Python `int` that does not fit in 64 bits is sent as a `java.math.BigInteger`, and a `decimal.Decimal` as a `java.math.BigDecimal` with the same digits and scale. Both were refused (`TypeError`). An `int` that fits travels as before, and is converted where the parameter is a `BigInteger` or `BigDecimal`, so that `big.add(5)` works like `big.add(2**70)`; a `float` is never taken for a `BigDecimal`. `Decimal("NaN")` and the infinities raise `ValueError`
- Numbers of other libraries are accepted as arguments: any `numbers.Integral` value is sent as an `int` and any `numbers.Real` value as a `float`, which covers numpy's scalars. numpy is not imported
- `client.register_converter(python_type, fn)` and `unregister_converter`, in both clients: `fn` turns every argument of that type (or a subclass) into a value that can be sent, in single calls, batches and the vectorized calls, also inside a list, tuple or dict. It runs before the command is built, so it may call Java. In the async client it may be a coroutine function
- `return_object_ref=True` on `invoke_method`, `get_field` and `get_static_field` (both clients; `invoke_static_method` had it in the sync client and now has it in the async one), and on a batch's `call`, `call_static` and `get_field`: the result is the Java object, not a converted value
- `allow_all=True` for `connect()`, `aconnect()`, `launch_gateway()` and `GatunAdapter`, also `allow_all = true` under `[tool.gatun]` and `GATUN_ALLOW_ALL=true`. It lifts the class allowlist: every class can be instantiated and its static members used, and reflection objects, class loaders, `Runtime` and threads can be used from Python. `System.exit`, `System.load`, `System.loadLibrary` and `Class.newInstance` stay refused. The default is unchanged. A program that listed every top-level package it needs can say this instead; an empty prefix was, and is, refused. The server logs one line at start when the list is off
- `JavaException.java_exception`: the exception that was thrown, as a Java object (`getCause()`, `getStackTrace()`, the methods of its own class, `is_instance_of`). The name is Py4J's. It is there for single calls, the failed results of a batch, the vectorized calls, both clients and calls made inside a callback, and is released when the Python exception is collected. `java_class`, `message`, `stack_trace` and `str()` are unchanged
- Python 3.11 and 3.12 are supported; the floor was 3.13. CI runs the suite on 3.11, 3.12, 3.13 and 3.14

### Changed
- Jackson is relocated in the server's JAR (`gatun.shaded.com.fasterxml.jackson`), like Netty. It was there under its own name, at the version of Spark 4.1, in front of whatever Jackson the classpath had
- On a classpath with its own Arrow the server's Netty is the classpath's: it reads `-Dio.netty.maxDirectMemory`, and that limit covers the server's Arrow memory and the classpath's Netty buffers together. `-Dgatun.shaded.io.netty.*` flags have no effect there, and the launcher warns about one
- `tests/test_spark_integration.py` skips a Spark older than 4, and fails instead of skipping when `GATUN_REQUIRE_SPARK` names a version that is not the one found
- `startup_timeout` defaults to 30 seconds, from 5. With Spark's 280 JARs on the classpath a server on a small machine has needed more than five seconds to listen. A server that fails to start is still reported at once
- `tests/test_spark_integration.py` finds Spark through `SPARK_HOME`, a distribution extracted in the repository root, or an installed `pyspark`. It looked for `spark-4.1.1-bin-hadoop3/` only
- **A `java.math.BigDecimal` is returned as a `decimal.Decimal` and a `java.math.BigInteger` as an `int`**, as return values, field values, callback arguments and elements of lists and maps. They were `JavaObject`s. Code that called methods on such a result (`value.add(...)`, `value.toString()`) now gets the Python number instead; to keep the Java object, construct it (`client.jvm.java.math.BigDecimal("1.50")` is still a `JavaObject`) or pass `return_object_ref=True`. Subclasses of the two, and other `Number` classes such as `AtomicLong`, are objects as before
- A bridge method is called only when no other method matches. `dec.compareTo(5)` on a `BigDecimal` went to the compiler-made `compareTo(Object)`, whose `Object` parameter scored above the conversion of the int, and failed in its cast
- Protocol version is now 7: two new values, `BigIntVal` and `DecimalVal`, and `Response.error_object`. Client and server must be upgraded together
- Every failed call registers one object for the session, the exception, until the Python exception is collected. A program that keeps its exceptions (in a list, in a log record) keeps those objects, and they count against `gatun.limits.max_objects`. At the limit an error arrives without its object

### Fixed
- README and `docs/benchmarks.md` said a plain instance call is 1.3 times faster through Gatun than through Py4J. Plain instance calls and object creation cost about the same: a second machine had Py4J level or ahead on them. The claims for static calls, `is_instance_of`, collections as arguments and bulk data stand
- The same two documents gave Py4J's 40 ms for returning a value of 8 KB or more without its cause. It is Nagle's algorithm with a delayed acknowledgement: Py4J's server does not set `TCP_NODELAY`. They now say so, and give the comparison with the option set as well, where Gatun is about 3 times faster and not 640 times
- The 0.3.0 notes said a single call through Gatun takes about twice as long as one through Py4J. That was measured before single calls were packed by hand, and stood beside the entry that says the opposite

## [0.3.0] - 2026-11-20

### Added
- `classpath` argument to `connect()`, `aconnect()` and `launch_gateway()` for loading external JARs (e.g. Spark)
- Spark integration tests (run when a Spark distribution is present in the repo root)
- `client.arrow_context()` scoped context manager for Arrow transfers, with an async variant
- `estimate_arrow_size()` and up-front size validation raising `PayloadTooLargeError`
- Direction field on Arrow batch descriptors
- Arrow memory architecture documentation
- `client.arrow_to_java(table)` and `client.arrow_from_java(obj)`, on both clients. A sent table becomes an `org.apache.arrow.vector.VectorSchemaRoot` object that any Java method can receive, and any `VectorSchemaRoot` the JVM holds, including one Java code produced, can be fetched as a `pyarrow.Table`. Before this, sent Arrow data could only be read back by the client that sent it
- `client.arrow_from_java(obj, copy=False)` (sync client) reads a table where Java wrote it in shared memory instead of copying it out. It returns an `ArrowTableView` that is good until the client's next Arrow transfer and raises `StaleArenaError` after it
- `client.arrow_to_java(table, copy=False)` (sync client) lends a table to Java instead of copying it into the JVM: Java reads it where Python wrote it in shared memory. It returns an `ArrowLoan` to use in a `with` block; when the block ends Java can no longer read the table (Arrow raises in Java code that kept it) and the client reuses the memory. Later transfers are written after an open loan, a lent table's data is not counted against `max_arrow_bytes`, and `reset_payload_arena()` ends every loan. Inside a callback that Java made from another thread the table is copied as before
- `client.arrow_to_java_batches(table, max_rows_per_batch=None)`, on both clients: the table's record batches as a `java.util.ArrayList` of `byte[]`, each one a batch's Arrow IPC message. That is the form Spark takes Arrow data in, and it reaches the JVM with one copy and no decoding
- `benchmarks/benchmark_transport.py` measures Arrow transfer through shared memory against a Unix socket and a file, in both directions
- Java can call a Python callback from any thread and at any time, not only from the thread running the client's request: listeners, timers, executors and parallel streams work, several calls at once. These calls travel on a second connection that the client opens with its first `register_callback`. Both clients
- A callback that arrives on that connection can call Java (object creation, method calls, field access, `batch()`, the vectorized calls, `register_callback`, `get_metrics`), also while the client's own channel is busy with a long request or with the request that is waiting for the callback: its calls travel on the callback connection and are run by the Java thread that waits for it. Arrow transfers that use the client's own shared memory (`send_arrow_table`, `send_arrow_buffers`, `get_arrow_data`) are refused there with `ReentrancyError`
- `arrow_to_java()` and `arrow_from_java()` work inside a callback that Java made from another thread (a Spark `foreachBatch` function fetching its batch as Arrow, for example), on both clients. The table goes through shared memory that belongs to that one call, so it does not wait for the request the client is running, and callbacks running at the same time do not share it
- A callback is dropped on its own once nothing can call it any more: when the handle `register_callback()` returned has been garbage collected in Python and no Java code holds the proxy, the JVM's garbage collector reports it and the client lets go of the function. `unregister_callback()` is still the way to stop a callback that Java holds. A callback handed to Java code that keeps it only in a weak reference now lasts only as long as its Python handle; it used to last for the session
- `register_callback()` accepts an object that is not callable, for an interface with several methods (a listener): each call Java makes goes to the object's method of the same name, a default method of the interface included if the object has one for it. A callable still answers every abstract method
- `BridgeAdapter.register_callback(handler, interface)` and `unregister_callback(ref)`, implemented by `GatunAdapter`. Subclasses of `BridgeAdapter` have two more methods to implement
- `allowed_classes` and `allowed_prefixes` arguments to `connect()`, `aconnect()`, `launch_gateway()` and `GatunAdapter`, also settable in `pyproject.toml` and through `GATUN_ALLOWED_CLASSES` / `GATUN_ALLOWED_PREFIXES`. Using a library outside the built-in allowlist no longer needs a source edit and a rebuild
- `socket_timeout` and `callback_timeout` arguments to `connect()`, `aconnect()` and `GatunAdapter`; the handshake uses the configured `connect_timeout`
- `gatun.limits.max_sessions` caps the sessions a server serves at once (default: unlimited). A connection that arrives while every slot is taken waits up to two seconds for one before it is refused
- The async client has `reflect()` and `get_static_field()`, and its JVM view asks the server whether a path is a class or a static method instead of guessing from the spelling
- Nested classes can be named the way Java source names them (`client.jvm.java.util.AbstractMap.SimpleEntry`), in the JVM view and in the allow lists
- `memory` accepts `K` / `M` / `G` suffixes, fractions (`"1.5GB"`), a byte count, or an int

### Changed
- A single call takes about a third of the time. The clients write the commands of single calls (constructors, method calls, static calls, field access, `is_instance_of`) and read plain return values with `struct` instead of Python's FlatBuffers library, when every argument is an `int`, `float`, `bool`, `str`, `None` or a Java object; anything else is built as before. Measured on Linux in alternation with the build before: `list.size()` 130 μs to 44 μs, `list.get(0)` 154 μs to 41 μs, `Math.max(10, 20)` 173 μs to 47 μs. Py4J takes 43, 63 and 115 μs for the same calls on that machine
- A `batch()` is packed the same way when each of its commands can be. 100 method calls in one batch took 4.6 ms and take 0.56 ms, against about 4 ms for the same calls one at a time. The vectorized calls (`invoke_methods`, `create_objects`, `get_fields`) are built as before, which makes a batch several times faster than they are for the same work
- A handle that is garbage collected is released with the client's next call, not with a round trip of its own: creating an `ArrayList` and dropping it took 245 μs and takes 49 μs. `client.free_object()` is still sent at once
- The server runs each session, and the reader and writer of a session's callback connection, on platform threads. They were virtual threads, which are slower to wake when a socket becomes readable: about 30 μs of every request, and a callback from another Java thread takes 0.75 ms where it took 0.94 ms
- Protocol version is now 5: a list whose elements are all integers, all floats, all booleans or all strings travels as one vector instead of a table per element, in both directions, and a map travels as its keys and its values, two such lists. What arrives is unchanged: the same Java element types for a Python list or dict, the same Python values for a Java `List` or `Map`. A list of 500 integers took 6.0 ms as an argument and 5.6 ms as a return value and now takes 0.32 ms and 0.15 ms; a dict of 500 entries took 14.6 ms and 12.9 ms and takes 0.43 ms and 0.34 ms. A list of mixed types is sent as before. A command can also name objects to release before it runs (`Command.free_ids`). Client and server must be upgraded together
- Protocol version 4: `Action.OpenCallbackChannel`, `Action.NestedCommand`, `Action.OpenCallMemory` and a `call_id` on commands and responses, for the callback connection. Client and server must be upgraded together
- A Java object that a callback returns is no longer released before Java has read the callback's answer
- A session can unregister only its own callbacks, and `unregister_callback()` keeps the Python function until the server has dropped the callback
- A callback's arguments that could not be sent (more than 64KB of them) no longer leave their objects registered for the session
- Calling a callback after `unregister_callback` throws `IllegalStateException` in Java before its arguments are sent. The client used to answer "not found", and in the sync client the objects among the arguments stayed registered for the rest of the session
- Protocol version 3: `datetime` / `date` / `time` arguments travel as a new `TemporalVal` value instead of being built with extra Java calls, and `FreeObject` accepts several object IDs. Client and server must be upgraded together
- The async client accepts `datetime` / `date` / `time` arguments
- Executing a batch from inside a callback made by the thread running the client's request raises `ReentrancyError`, like any other call
- `arrow_to_java()` and `send_arrow_table()` serialize the table straight into shared memory instead of building the stream in a buffer and copying that across: about half the time for a 128 MiB table in `arrow_to_java()`, and no buffer the size of the stream. A table that does not fit is refused before its stream is written
- `ArrowTableView.to_pandas()` / `to_pydict()` / `to_pylist()` hold the client's channel while they copy, so a transfer on another thread cannot change the data under them. A view also ends when the transfer that overwrites it starts, not when it succeeds, and when the client's arena allocates over it
- Connecting no longer writes zeros over the whole shared memory file: about 7 ms instead of 260 ms at 64MB, and no longer proportional to the memory size. Disk (or tmpfs) pages are now allocated as they are first used rather than all at connect time
- **`client.cancel()` takes no arguments and interrupts the session's running request** from another thread. The old `cancel(request_id)` could not work: it queued behind the request it was meant to stop. `_get_request_id()` is gone. A cancelled request raises `CancelledException`, and the session stays usable
- Objects belong to the session that created them. Another session that guesses an object ID gets "not found", whether it uses the ID as a target or as an argument
- The server's socket and its shared memory files are readable and writable by their owner only, the server refuses connections from other users, and it creates each shared memory file itself rather than mapping whatever exists at that path
- Reflection is no longer a way around the class allowlist: objects of `java.lang.reflect.Method` / `Constructor` / `Field`, method handles, class loaders, `Runtime`, `Process*`, `Thread` and `Module` cannot be used from Python unless their type is listed in `allowed_classes`; `System.exit`, `System.load`, `System.loadLibrary` and `Class.newInstance` are refused outright; and naming a class, or calling `Class.forName` on it, no longer runs its static initializer
- A stale or foreign `JavaObject` passed as an argument is an error; it used to arrive in Java as `null`
- A type hint that names an unknown class is an error; it used to be ignored
- A Python list of ints becomes a Java list of all `Integer` or all `Long`, never a mix decided value by value
- Python callbacks may only be invoked on the thread running the request, during that request. Java code that calls one from another thread gets an `IllegalStateException` instead of corrupting the request in progress
- The sync client can be shared between threads: requests are serialized, one at a time, and each thread builds its commands in its own buffers. Reentrancy is tracked per thread and per context, so a callback's own thread, and tasks it starts and waits for, are refused, while unrelated threads and tasks wait their turn. A sync callback that waits for another thread calling Java on the same client waits until its callback timeout
- A client used after `close()` raises `DeadConnectionError` rather than failing somewhere inside the transport
- Async `JavaObject` handles are freed when garbage-collected, like sync ones; `detach()` opts out
- The reflection cache behind `client.jvm` belongs to each client instead of the process, and names that did not resolve are asked again
- `send_arrow_table()` accepts a table with no rows ("Received 0 rows"); it used to fail with "Arrow Stream Empty"
- `send_arrow_buffers()` refuses an arena that is not the client's own (`client.get_payload_arena()`), and rejects view types (`string_view`, `list_view`), run-end-encoded and dictionary columns up front; extension types travel as their storage
- `JavaArray(element_type=...)` rejects unknown element types instead of sending them as `Object[]`
- The launched server runs in its own process group, exits when the Python process that launched it is gone, and `launch_gateway()` refuses a fixed `socket_path` that a live server is already using
- Connecting checks that the filesystem has room for the whole shared memory file and refuses the session if not, with the reason in the error. The file is still sparse, so this is a check and not a reservation: it catches a disk that cannot hold the file, not several sessions that later fill it between them
- `arrow_from_java()` copies a table into Arrow's memory pool instead of a `bytes` object. The pool reuses the memory of a table that was dropped, where a `bytes` object of 32 MiB or more was new memory from the operating system each time: fetching a 128 MiB table took about 120 ms and now takes about 25 ms (measured on Linux with pyarrow's default mimalloc pool), and tables under 32 MiB take as long as before. With `ARROW_DEFAULT_MEMORY_POOL=system` nothing changes
- The server keeps the large Arrow buffers it frees and uses them again (`ArrowBlockPool`). Arrow's allocator pools only buffers up to 4 MiB; a larger one, such as the copy `arrow_to_java()` makes of a table, was new memory from the operating system every time, and touching new memory costs several times the copy: `arrow_to_java()` of a 128 MiB table took about 110 ms and now takes about 25 ms (measured on Linux), and tables that fit Netty's own pool take as long as before. The pool keeps at most as many bytes as a client's shared memory (`-Dgatun.arrow.pool.max_bytes`, 0 turns it off) and gives a block back once nobody has asked for it for a minute (`-Dgatun.arrow.pool.idle_ms`, 0 keeps it); a value that is not a whole number stops the server at startup. `get_metrics()` reports what it holds (`pool_*` under `Arrow:`). Arrow buffers between 4 and 16 MiB are no longer rounded up to a power of two
- The server no longer runs with Arrow's debug allocator (`-Darrow.memory.debug.allocator=true`), which records a stack trace for every buffer operation: a small `arrow_to_java()` took more than twice as long with it. A leaked buffer is still reported, without the place it was allocated; pass the flag in `extra_jvm_flags` or `jvm_flags` to get that back

### Fixed
- Project links pointed at the repository's old location
- `get_metrics()` and the server's request log name every action. `Ping` was reported as `Unknown(0)`, and the log showed the same for `GetMetrics`, the Arrow object transfers and everything else added since batches: the two tables of names had each been kept by hand
- `gatun.bridge.JavaException` and its subclasses are the client's exception classes, not a second set with the same names. `except gatun.bridge.JavaException` let through every Java exception that did not come from one of the adapter's own methods: a call through `bridge.jvm`, or a method of an object the bridge returned. `str()` of one raised by an adapter method is now the Java stack trace, as everywhere else, where it was the class and the message
- README: the async client's handles are garbage collected like the sync client's; the static field example used a call that raises; `StaleArenaError` has no `view_epoch` attribute; the payload zone's halves belong to `send_arrow_buffers` / `get_arrow_data` only
- `docs/bridge_adapter_spec.md` described `BridgeAdapter` as a protocol with optional parts and carried a copy of the contract tests that had drifted from `tests/test_bridge_contract.py`
- The performance numbers in the README and `docs/benchmarks.md` dated from 0.1.0 and a different machine. They are measured again, on Linux, with the corrected scripts, and again after single calls were packed by hand (see Changed)
- Benchmark scripts: the Py4J comparison timed Gatun on a cold JVM with a default heap against a warm Py4J JVM with a fixed one, resolved the class path on every Gatun call only, and filled the heap in the byte-array sweep; the other scripts timed their set-up with the case and warmed up for ten calls. `benchmark_comparison.py --json` wrote nothing
- `client.cancel()` during a batch ends the batch. It used to cancel only the command that was running: with `stop_on_error=False` the rest of the batch still ran. The batch now raises `CancelledException`
- An Arrow transfer attempted inside a callback made by the request's own thread raises `ReentrancyError` at once. With a callback timeout set it used to wait for the channel the waiting request held, and failed only when the callback timed out
- `pytest` no longer collects tests from an extracted Spark distribution (`testpaths`)
- `spotlessCheck` no longer fails on generated FlatBuffers code
- CI now runs the Python 3.14 job on Python 3.14
- Nested lists and dicts passed as arguments are no longer corrupted (`[[1, 2], [3, 4]]` arrived as `[[1, 2], 2]`)
- Garbage-collected `JavaObject` wrappers no longer break the connection: frees are queued and sent before the next request instead of from the finalizer. This fixes callbacks that receive Java objects and connections dying after many objects were dropped at once
- `datetime`, `date` and `time` arguments no longer kill the connection, and can be returned from callbacks
- A batch no longer loses a target object whose last reference is dropped before the batch runs
- Objects passed to callbacks are counted against `max_objects`, so freeing them no longer lowers the session's object count below the real one
- Objects nested in a returned collection (for example the elements of `Arrays.asList(obj, obj)`) are now released when their wrappers are dropped; they used to stay on the server for the life of the JVM
- `client.close()` stops the server that `connect()` / `aconnect()` launched for it instead of leaving the JVM running until interpreter exit
- `socket_timeout=None`, `callback_timeout=None` and `handshake_timeout=None` disable the timeout, as documented; they were silently replaced by the defaults
- Sliced pyarrow arrays passed as arguments send their own values rather than the parent buffer from element 0, and primitive pyarrow arrays containing nulls raise `ValueError` instead of sending garbage
- `send_arrow_buffers()` and `estimate_arrow_size()` accept a `RecordBatch`
- A batch or slice of a larger table is sized and copied by its own rows; the first batch of `table.to_batches()` used to copy the whole table's buffers
- The async client can decode returned maps and primitive arrays, and accepts the same argument types as the sync client (`dict`, `bytes`, arrays, `TypeHint`); the two now share one encoder and one decoder
- `char[]` is supported as an argument and a return value (`JavaArray` with `element_type="Char"`); returning one used to fail with `ClassCastException`
- Stopping the server removes its per-session shared memory files instead of leaving them in the temp directory
- Objects in a response that turns out too large to send are released rather than kept until the client disconnects, those a batch's commands created included
- Arrow state (the loaded table, the arena epoch, the schema cache) is kept per session; it was shared by every client of a server, so one client could read another's table and one client's arena reset invalidated the others'
- The server checks that the Arrow buffers a client describes lie inside the payload zone before reading them
- A batch sent without its schema, to a session that does not have it, is an error; it used to be reported as received
- `get_arrow_data()` refuses up front, with `PayloadTooLargeError`, when the data does not fit in the half of the payload zone reserved for it, instead of overwriting part of the table it was copying
- `send_arrow_table()` reports the rows of every chunk of a table, not just the first
- A rejected `send_arrow_buffers()` no longer marks its schema as sent (every later send then omitted it) and no longer counts against `max_arrow_bytes`
- `get_metrics()` reports real object counts, Arrow transfers and callback invocations (these always read 0), counts `Batch` and `GetArrowData` requests, and counts a response that was too large to send as an error
- A constructor or callback registration that fails no longer uses up one of the session's `max_objects` slots, and `create_objects()` / `invoke_methods()` / `get_fields()` release the objects they registered before failing part-way
- `close()` closes the socket even when the shared memory mapping cannot be released yet
- Concurrent `send_arrow_table()` calls on one async client no longer overwrite each other's data: the payload zone is written only once the request holds the channel
- A table from `get_arrow_data()` raises `StaleArenaError` once a later transfer has written over the memory it points at (a larger send, or a newer `get_arrow_data()`), instead of returning that transfer's bytes
- `GatunAdapter(classpath=...)` passes the classpath on
- A callback made during a batch, `invoke_methods()` or `create_objects()` no longer corrupts the rest of that request: the server reads each command from a private copy, since the callback's answer arrives in the same memory
- Callbacks in a batch are served; they used to deadlock the batch
- A table from `get_arrow_data().table` stays valid after `client.close()`: it now keeps the shared memory mapped for as long as it is alive, instead of pointing at unmapped memory
- `Float.valueOf(1.5)`, `Short.valueOf(7)` and other methods taking `float`, `short` or `byte` can be called with Python floats and ints (range-checked); a `TypeHint` converts the value as well as choosing the overload
- `list.stream().count()` and other no-argument methods on objects of non-public classes work; static no-argument methods can be called through an instance
- An array passed for a varargs parameter is used as the varargs array (`String.format("%s-%s", JavaArray(["a", "b"]))`)
- Overloads that score equally are chosen by specificity, not by the order reflection lists them in, and a boxed argument prefers a reference parameter over a primitive one (`list.remove(TypeHint(5, "java.lang.Integer"))` removes the value 5)
- Interface default methods on a callback proxy (`comparator.reversed()`, `predicate.negate()`) run as Java wrote them instead of being sent to Python
- A callback that returns `None` or the wrong type for a primitive return type is reported as such; int results are widened for `long` / `double` methods
- Callback arguments or results too large for the channel produce an error instead of a dead connection
- A Ctrl-C, timeout or task cancellation in the middle of an exchange marks the client dead instead of letting the next request read the previous answer; the async client had no such protection at all
- `socket_timeout=0`, negative values and `float("inf")` mean "no timeout"; they used to fail every read or raise inside the socket
- A failed `connect()` keeps the reason (`client.connect_error`), and `connect()` / `with GatunClient(...)` raise with it as the cause
- A batch that could not be sent can be executed again, and results of a batch that failed as a whole say so
- Maps with `List` keys can be returned (the keys become tuples)
- The server answers a malformed command, an unknown action and a self-containing collection with an error instead of a success, a dropped connection or a stack overflow; long stack traces are shortened to fit the response zone, frame by frame, so every `Caused by` line survives
- An idle session is closed when its timeout passes, not when its next request arrives, and that request gets an error naming the timeout. Idle time is counted from the end of the last request, so a request longer than the timeout no longer ends its own session
- The accept loop survives a failed `accept()` (too many open files, for example)
- Metrics percentiles for a single sample no longer read from an empty bucket
- The server's reflection caches no longer take a lock on every lookup; they could also corrupt their own ordering under concurrent sessions
- Arrow buffer transfer handles sliced nested columns, unions, null columns, tables with no chunks, and all-empty string columns; `arrow_context().receive()` keeps the schema (metadata, nullability)
- `send_arrow_table_batched()` sizes batches from the whole table and splits a batch that still does not fit, instead of sizing from the first 100 rows
- A failed `reset_payload_arena()` no longer leaves the client's epoch ahead of the server's
- The launcher reaps a server it had to kill, leaves ignored signals ignored, does not stop the parent's server from a forked child, keeps only a tail of the server's output, drains that output during startup, and stops the server when startup times out or is interrupted
- `client.jvm.Encoders.STRING()`-style uppercase static methods and lowercase static fields (`System.out`) resolve by what they are, not how they are spelled
- The server's Netty filled the direct memory it allocated with zeros before use: the launcher passed `io.netty.tryReflectionSetAccessible`, but Netty is relocated inside the server JAR and reads `gatun.shaded.io.netty.tryReflectionSetAccessible`. The launcher now passes both. Arrow's memory in the server is then taken without a `DirectByteBuffer` around it: it no longer counts against the JVM's own direct memory limit or shows in the JVM's direct buffer pool statistics. Netty counts it itself, against a limit of the same size (`-XX:MaxDirectMemorySize`, or `-Dgatun.shaded.io.netty.maxDirectMemory` to set it apart), so a server whose NIO buffers and Arrow tables shared one limit can now use up to that much for each

## [0.2.1] - 2026-01-17

### Added
- `ping()` health check method
- Pre-commit hooks and workflow

### Changed
- Moved the Python project to the repository root

## [0.2.0] - 2026-01-16

### Added
- Initial public release on PyPI
- JVM view API for package-style class access (`client.jvm.java.util.ArrayList`)
- Sync and async clients (`GatunClient`, `AsyncGatunClient`)
- `connect()` and `aconnect()` convenience functions with context manager support
- Batch API for executing multiple commands in a single round-trip
- Vectorized APIs: `invoke_methods()`, `create_objects()`, `get_fields()`
- Python callbacks as Java interface implementations
- Arrow data transfer via IPC and zero-copy buffer descriptors
- `JavaArray` for primitive arrays, `JavaObject` for object references
- Request cancellation support
- Observability: trace mode, structured logging, JFR events, metrics
- Security: class allowlist for safe instantiation
- PySpark integration via `BridgeAdapter`

### Fixed
- Stricter mypy type checking with explicit casts

## [0.1.0] - 2026-01-15

### Added
- Initial development release
- Core protocol using FlatBuffers over Unix domain sockets
- Shared memory communication with per-client SHM files
- Basic object creation and method invocation

[Unreleased]: https://github.com/bearing-research/gatun/compare/v0.3.0...HEAD
[0.3.0]: https://github.com/bearing-research/gatun/compare/v0.2.1...v0.3.0
[0.2.1]: https://github.com/bearing-research/gatun/compare/v0.2.0...v0.2.1
[0.2.0]: https://github.com/bearing-research/gatun/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/bearing-research/gatun/releases/tag/v0.1.0
