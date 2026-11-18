# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- `classpath` argument to `connect()`, `aconnect()` and `launch_gateway()` for loading external JARs (e.g. Spark)
- Spark integration tests (run when a Spark distribution is present in the repo root)
- `client.arrow_context()` scoped context manager for Arrow transfers, with an async variant
- `estimate_arrow_size()` and up-front size validation raising `PayloadTooLargeError`
- Direction field on Arrow batch descriptors
- Arrow memory architecture documentation
- `client.arrow_to_java(table)` and `client.arrow_from_java(obj)`, on both clients. A sent table becomes an `org.apache.arrow.vector.VectorSchemaRoot` object that any Java method can receive, and any `VectorSchemaRoot` the JVM holds, including one Java code produced, can be fetched as a `pyarrow.Table`. Before this, sent Arrow data could only be read back by the client that sent it
- `allowed_classes` and `allowed_prefixes` arguments to `connect()`, `aconnect()`, `launch_gateway()` and `GatunAdapter`, also settable in `pyproject.toml` and through `GATUN_ALLOWED_CLASSES` / `GATUN_ALLOWED_PREFIXES`. Using a library outside the built-in allowlist no longer needs a source edit and a rebuild

### Changed
- Protocol version is now 3: `datetime` / `date` / `time` arguments travel as a new `TemporalVal` value instead of being built with extra Java calls, and `FreeObject` accepts several object IDs. Client and server must be upgraded together
- The async client accepts `datetime` / `date` / `time` arguments
- Executing a batch from inside a callback raises `ReentrancyError`, like any other call
- Connecting no longer writes zeros over the whole shared memory file: about 7 ms instead of 260 ms at 64MB, and no longer proportional to the memory size. Disk (or tmpfs) pages are now allocated as they are first used rather than all at connect time

### Fixed
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
- Objects in a response that turns out too large to send are released rather than kept until the client disconnects
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

[Unreleased]: https://github.com/forge-labs-dev/gatun/compare/v0.2.1...HEAD
[0.2.1]: https://github.com/forge-labs-dev/gatun/compare/v0.2.0...v0.2.1
[0.2.0]: https://github.com/forge-labs-dev/gatun/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/forge-labs-dev/gatun/releases/tag/v0.1.0
