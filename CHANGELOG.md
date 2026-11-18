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

### Fixed
- `pytest` no longer collects tests from an extracted Spark distribution (`testpaths`)
- `spotlessCheck` no longer fails on generated FlatBuffers code
- CI now runs the Python 3.14 job on Python 3.14

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
