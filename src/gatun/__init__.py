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


def connect(
    memory: str | None = None,
    socket_path: str | None = None,
    classpath: list[str] | None = None,
    trace: bool = False,
    log_level: str | None = None,
    debug: bool = False,
    extra_jvm_flags: list[str] | None = None,
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

    Configuration can be set in pyproject.toml:
        [tool.gatun]
        memory = "64MB"
        socket_path = "/tmp/gatun.sock"

    Or via environment variables:
        GATUN_MEMORY=64MB
        GATUN_SOCKET_PATH=/tmp/gatun.sock
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
    )

    client = GatunClient(session.socket_path)
    if not client.connect():
        session.stop()
        raise RuntimeError("Failed to connect to Gatun Server")

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
    )

    client = AsyncGatunClient(session.socket_path)
    if not await client.connect():
        session.stop()
        raise RuntimeError("Failed to connect to Gatun Server")

    # Attach session to client so it doesn't get GC'd
    client._server_session = session
    return client
