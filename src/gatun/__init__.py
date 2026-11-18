from gatun.arena import (
    AsyncScopedArenaContext,
    PayloadArena,
    ScopedArenaContext,
    UnsupportedArrowTypeError,
    estimate_arrow_size,
)
from gatun.async_client import (
    AsyncGatunClient,
    AsyncJavaClass,
    AsyncJavaObject,
    AsyncJVMView,
    run_sync,
)
from gatun.client import (
    DEFAULT_CALLBACK_TIMEOUT,
    DEFAULT_SOCKET_TIMEOUT,
    PROTOCOL_VERSION,
    ArrowTableView,
    BatchContext,
    BatchResult,
    CallbackTimeoutError,
    CancelledException,
    DeadConnectionError,
    GatunClient,
    JavaArray,
    JavaClass,
    JavaClassNotFoundException,
    JavaException,
    JavaIllegalArgumentException,
    JavaIndexOutOfBoundsException,
    JavaNoSuchFieldException,
    JavaNoSuchMethodException,
    JavaNullPointerException,
    JavaNumberFormatException,
    JavaObject,
    JavaRuntimeException,
    JavaSecurityException,
    JVMView,
    PayloadTooLargeError,
    ProtocolDesyncError,
    ReentrancyError,
    SocketTimeoutError,
    StaleArenaError,
    TypeHint,
    java_import,
)
from gatun.config import GatunConfig, get_config, load_config, reset_config
from gatun.launcher import GatunSession, launch_gateway

__all__ = [
    "PROTOCOL_VERSION",
    "ArrowTableView",
    "AsyncGatunClient",
    "AsyncJVMView",
    "AsyncJavaClass",
    "AsyncJavaObject",
    "AsyncScopedArenaContext",
    "BatchContext",
    "BatchResult",
    "CallbackTimeoutError",
    "CancelledException",
    "DeadConnectionError",
    "GatunClient",
    "GatunConfig",
    "GatunSession",
    "JVMView",
    "JavaArray",
    "JavaClass",
    "JavaClassNotFoundException",
    "JavaException",
    "JavaIllegalArgumentException",
    "JavaIndexOutOfBoundsException",
    "JavaNoSuchFieldException",
    "JavaNoSuchMethodException",
    "JavaNullPointerException",
    "JavaNumberFormatException",
    "JavaObject",
    "JavaRuntimeException",
    "JavaSecurityException",
    "PayloadArena",
    "PayloadTooLargeError",
    "ProtocolDesyncError",
    "ReentrancyError",
    "ScopedArenaContext",
    "SocketTimeoutError",
    "StaleArenaError",
    "TypeHint",
    "UnsupportedArrowTypeError",
    "aconnect",
    "connect",
    "estimate_arrow_size",
    "get_config",
    "java_import",
    "launch_gateway",
    "load_config",
    "reset_config",
    "run_sync",
]


def _connect_failure(
    session: GatunSession, error: BaseException | None
) -> RuntimeError:
    """Stop a server that could not be connected to and describe the failure.

    When the server refuses a session (no room for its shared memory, too many
    sessions) the client only sees the socket close; the reason is in the
    server's log, so its last lines go into the message.
    """
    session.stop()
    lines = [line.rstrip() for line in session._stderr_lines[-12:] if line.strip()]
    message = f"Failed to connect to Gatun Server: {error}"
    if lines:
        message += "\nServer output:\n" + "\n".join(lines)
    failure = RuntimeError(message)
    failure.__cause__ = error
    return failure


def connect(
    memory: str | None = None,
    socket_path: str | None = None,
    classpath: list[str] | None = None,
    trace: bool = False,
    log_level: str | None = None,
    debug: bool = False,
    extra_jvm_flags: list[str] | None = None,
    allowed_classes: list[str] | None = None,
    allowed_prefixes: list[str] | None = None,
    socket_timeout: float | None = DEFAULT_SOCKET_TIMEOUT,
    callback_timeout: float | None = DEFAULT_CALLBACK_TIMEOUT,
):
    """Convenience: Launches server and returns connected client.

    Args:
        memory: Memory size (e.g., "512MB", "1GB"). Defaults to config value.
        socket_path: Path to Unix socket. Defaults to config value, or a unique
                     path under /tmp so concurrent sessions do not collide.
        classpath: Additional JAR files or directories to add to the classpath.
                   Use this to load external libraries (e.g., Spark JARs).
        trace: Enable trace mode for verbose method resolution logging.
        log_level: Java logging level ("FINE", "FINER", "INFO", etc.).
        debug: If True, Java server logs are printed to stderr in real-time.
        extra_jvm_flags: Additional JVM flags to pass to the server (e.g., for resource limits).
        allowed_classes: Fully qualified class names the client may instantiate or call
                         static members on, in addition to the built-in list.
        allowed_prefixes: Package prefixes (e.g. "com.example") whose classes, and those of
                          their subpackages, are allowed likewise.
        socket_timeout: Seconds to wait for the response to one call. A call that takes
                        longer raises SocketTimeoutError and the client cannot be used
                        again, so raise this for long-running Java calls. None waits
                        without limit.
        callback_timeout: Seconds a Python callback may run before Java is told it
                          failed. None waits without limit.

    Configuration can be set in pyproject.toml:
        [tool.gatun]
        memory = "64MB"
        socket_path = "/tmp/gatun.sock"
        allowed_prefixes = ["com.example"]

    Or via environment variables:
        GATUN_MEMORY=64MB
        GATUN_SOCKET_PATH=/tmp/gatun.sock
        GATUN_ALLOWED_PREFIXES=com.example,org.acme
        GATUN_TRACE=true
        GATUN_LOG_LEVEL=FINE
        GATUN_DEBUG=true

    Example with Spark:
        import glob
        spark_jars = glob.glob("/path/to/spark/jars/*.jar")
        client = connect(memory="512MB", classpath=spark_jars)
        SparkConf = client.jvm.org.apache.spark.SparkConf
        conf = SparkConf().setMaster("local[2]").setAppName("MyApp")
    """
    session = launch_gateway(
        memory=memory,
        socket_path=socket_path,
        classpath=classpath,
        trace=trace,
        log_level=log_level,
        debug=debug,
        extra_jvm_flags=extra_jvm_flags,
        allowed_classes=allowed_classes,
        allowed_prefixes=allowed_prefixes,
    )

    client = GatunClient(
        session.socket_path,
        socket_timeout=socket_timeout,
        callback_timeout=callback_timeout,
        handshake_timeout=get_config().connect_timeout,
    )
    if not client.connect():
        raise _connect_failure(session, client.connect_error)

    # Attach session to client so it doesn't get GC'd
    client._server_session = session
    return client


async def aconnect(
    memory: str | None = None,
    socket_path: str | None = None,
    classpath: list[str] | None = None,
    trace: bool = False,
    log_level: str | None = None,
    debug: bool = False,
    extra_jvm_flags: list[str] | None = None,
    allowed_classes: list[str] | None = None,
    allowed_prefixes: list[str] | None = None,
    socket_timeout: float | None = DEFAULT_SOCKET_TIMEOUT,
    callback_timeout: float | None = DEFAULT_CALLBACK_TIMEOUT,
) -> AsyncGatunClient:
    """Async convenience: Launches server and returns connected async client.

    Args:
        memory: Memory size (e.g., "512MB", "1GB"). Defaults to config value.
        socket_path: Path to Unix socket. Defaults to config value, or a unique
                     path under /tmp so concurrent sessions do not collide.
        classpath: Additional JAR files or directories to add to the classpath.
                   Use this to load external libraries (e.g., Spark JARs).
        trace: Enable trace mode for verbose method resolution logging.
        log_level: Java logging level ("FINE", "FINER", "INFO", etc.).
        debug: If True, Java server logs are printed to stderr in real-time.
        extra_jvm_flags: Additional JVM flags to pass to the server.
        allowed_classes: Fully qualified class names the client may instantiate or call
                         static members on, in addition to the built-in list.
        allowed_prefixes: Package prefixes (e.g. "com.example") whose classes, and those of
                          their subpackages, are allowed likewise.
        socket_timeout: Seconds to wait for the response to one call. A call that takes
                        longer raises SocketTimeoutError and the client cannot be used
                        again, so raise this for long-running Java calls. None waits
                        without limit.
        callback_timeout: Seconds a Python callback may run before Java is told it
                          failed. None waits without limit.

    Example:
        async with await aconnect() as client:
            arr = await client.create_object("java.util.ArrayList")
            await arr.add("hello")

    Configuration can be set in pyproject.toml:
        [tool.gatun]
        memory = "64MB"
        socket_path = "/tmp/gatun.sock"

    Or via environment variables:
        GATUN_MEMORY=64MB
        GATUN_SOCKET_PATH=/tmp/gatun.sock
        GATUN_ALLOWED_PREFIXES=com.example,org.acme
        GATUN_TRACE=true
        GATUN_LOG_LEVEL=FINE
        GATUN_DEBUG=true
    """
    session = launch_gateway(
        memory=memory,
        socket_path=socket_path,
        classpath=classpath,
        trace=trace,
        log_level=log_level,
        debug=debug,
        extra_jvm_flags=extra_jvm_flags,
        allowed_classes=allowed_classes,
        allowed_prefixes=allowed_prefixes,
    )

    client = AsyncGatunClient(
        session.socket_path,
        socket_timeout=socket_timeout,
        callback_timeout=callback_timeout,
        handshake_timeout=get_config().connect_timeout,
    )
    if not await client.connect():
        raise _connect_failure(session, client.connect_error)

    # Attach session to client so it doesn't get GC'd
    client._server_session = session
    return client
