import atexit
import glob
import logging
import os
import re
import secrets
import signal
import socket
import subprocess
import threading
import time
import weakref
from pathlib import Path

from gatun.config import get_config

logger = logging.getLogger(__name__)

# --- Configuration ---
MODULE_DIR = Path(__file__).parent.resolve()
JAR_PATH = MODULE_DIR / "jars" / "gatun-server-all.jar"

# Track all active sessions for cleanup on signals
_active_sessions: weakref.WeakSet["GatunSession"] = weakref.WeakSet()
_signal_handlers_installed = False


def _cleanup_all_sessions():
    """Stop all active sessions."""
    for session in list(_active_sessions):
        try:
            session.stop()
        except Exception:  # noqa: BLE001, S110 - runs from signal/atexit; one failure must not block the rest
            pass


def _signal_handler(signum, frame):
    """Handle SIGTERM/SIGINT by cleaning up sessions."""
    _cleanup_all_sessions()
    # Re-raise the signal to exit
    signal.signal(signum, signal.SIG_DFL)
    os.kill(os.getpid(), signum)


def _install_signal_handlers():
    """Install signal handlers for cleanup (once per process)."""
    global _signal_handlers_installed
    if _signal_handlers_installed:
        return
    _signal_handlers_installed = True

    # Only install handlers if we're not already handling these signals
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            current = signal.getsignal(sig)
            if current in (signal.SIG_DFL, signal.SIG_IGN, None):
                signal.signal(sig, _signal_handler)
        except (OSError, ValueError):
            pass  # Can't set signal handler (e.g., not main thread)


# JVM Flags required for Apache Arrow & Netty (Java 22+)
# Also includes flags needed for Spark's Kryo serialization
DEFAULT_JVM_FLAGS = [
    # Arrow/Netty memory requirements
    "--add-opens=java.base/java.nio=ALL-UNNAMED",
    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
    "--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED",
    # Spark Kryo serialization requirements (Java 17+)
    "--add-opens=java.base/java.lang=ALL-UNNAMED",
    "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
    "--add-opens=java.base/java.util=ALL-UNNAMED",
    "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
    "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
    "--add-opens=java.base/java.net=ALL-UNNAMED",
    "--add-opens=java.base/java.io=ALL-UNNAMED",
    "--add-opens=java.base/java.security=ALL-UNNAMED",
    # Netty reflective access
    "-Dio.netty.tryReflectionSetAccessible=true",
    "-Darrow.memory.debug.allocator=true",
]


# A dotted Java name: identifiers separated by single dots ('$' for nested classes)
_JAVA_NAME = re.compile(r"[A-Za-z_$][A-Za-z0-9_$]*(\.[A-Za-z_$][A-Za-z0-9_$]*)*")


def _checked_names(names: list[str], option: str) -> list[str]:
    """Validate allow-list entries and drop duplicates, keeping their order.

    A prefix may be written "com.example", "com.example." or "com.example.*". The
    server validates again; checking here reports a mistake as a ValueError at
    the call site instead of as a server that failed to start.
    """
    checked: list[str] = []
    for name in names:
        if not isinstance(name, str):
            raise TypeError(
                f"{option} entries must be strings, got {type(name).__name__}"
            )
        bare = name
        if option == "allowed_prefixes":
            # One suffix only, as ClassAllowlist.normalizePrefix does
            if bare.endswith(".*"):
                bare = bare[:-2]
            elif bare.endswith("."):
                bare = bare[:-1]
        if not _JAVA_NAME.fullmatch(bare):
            raise ValueError(
                f"Invalid entry in {option}: {name!r}. Expected a dotted Java name "
                "such as 'com.example.Thing' (a class) or 'com.example' (a package prefix)."
            )
        if name not in checked:
            checked.append(name)
    return checked


def _name_list(value: object, option: str) -> list[str]:
    """Check that an allow-list option is a list, not a bare string.

    A string is iterable, so without this "com.example" would be taken as the
    eleven one-letter names it is made of.
    """
    if value is None:
        return []
    if isinstance(value, str) or not isinstance(value, (list, tuple, set, frozenset)):
        raise TypeError(
            f"{option} must be a list of strings, got {type(value).__name__}: {value!r}"
        )
    return list(value)


def _take_property(jvm_flags: list[str], name: str) -> list[str]:
    """Remove every -D<name>=... flag from jvm_flags and return the names in them.

    The JVM keeps only the last of several -D flags for one property, so a list
    the caller set directly has to be merged into the launcher's own flag.
    """
    prefix = f"-D{name}="
    names: list[str] = []
    for flag in [flag for flag in jvm_flags if flag.startswith(prefix)]:
        jvm_flags.remove(flag)
        names += [
            item.strip() for item in flag[len(prefix) :].split(",") if item.strip()
        ]
    return names


def _drain_pipe(pipe, storage: list):
    """Drain a pipe to prevent buffer fill-up. Runs in background thread."""
    try:
        storage.extend(pipe)
    except (ValueError, OSError):
        pass  # Pipe closed


class GatunSession:
    def __init__(self, process, socket_path, memory_bytes):
        self.process = process
        self.socket_path = socket_path
        self.memory_bytes = memory_bytes
        self._stdout_lines: list[str] = []
        self._stderr_lines: list[str] = []
        self._drain_threads: list[threading.Thread] = []

    def _start_drain_threads(self):
        """Start background threads to drain stdout/stderr pipes."""
        if self.process.stdout:
            t = threading.Thread(
                target=_drain_pipe,
                args=(self.process.stdout, self._stdout_lines),
                daemon=True,
            )
            t.start()
            self._drain_threads.append(t)
        if self.process.stderr:
            t = threading.Thread(
                target=_drain_pipe,
                args=(self.process.stderr, self._stderr_lines),
                daemon=True,
            )
            t.start()
            self._drain_threads.append(t)

    def stop(self):
        if self.process:
            logger.debug("Stopping Java server...")
            self.process.terminate()
            try:
                self.process.wait(timeout=2)
            except subprocess.TimeoutExpired:
                self.process.kill()
            self.process = None

        # Clean up the socket and the per-session shared memory files
        # (<socket>.<session id>.shm). The server removes them itself on a clean
        # shutdown; this covers a server that was killed.
        shm_files = glob.glob(f"{glob.escape(self.socket_path)}.*.shm")
        for path in [self.socket_path, *shm_files]:
            try:
                os.unlink(path)
            except OSError:
                pass  # Already gone, or not ours to remove

        # Remove from active sessions
        _active_sessions.discard(self)
        # Stopped for good: drop the atexit entry so it does not keep this session
        # (and its captured server output) alive until the interpreter exits
        atexit.unregister(self.stop)


def launch_gateway(
    memory: str | None = None,
    socket_path: str | None = None,
    classpath: list[str] | None = None,
    trace: bool = False,
    log_level: str | None = None,
    debug: bool = False,
    extra_jvm_flags: list[str] | None = None,
    allowed_classes: list[str] | None = None,
    allowed_prefixes: list[str] | None = None,
):
    """
    Launches the embedded Java server.

    Args:
        memory: Memory size (e.g., "512MB", "1GB"). Defaults to config value.
        socket_path: Path to the Unix socket. If not specified, generates a unique
                     path in the system temp directory to allow concurrent sessions.
        classpath: Additional JAR files or directories to add to the classpath.
                   This allows loading external classes (e.g., Spark JARs).
        trace: Enable trace mode for verbose method resolution logging.
               Useful for debugging "wrong method called" issues.
        log_level: Java logging level. Options: "FINE", "FINER", "FINEST", "INFO",
                   "WARNING". Default is "INFO". Use "FINE" for request/response
                   logging, "FINER" for object registry changes.
        debug: If True, Java server output is printed to stderr instead of captured.
               Useful for seeing logs in real-time during development.
        allowed_classes: Fully qualified class names the client may instantiate or
                         call static members on, in addition to the built-in list.
        allowed_prefixes: Package prefixes (e.g. "com.example") whose classes, and
                          those of their subpackages, are allowed likewise.

    The allow lists from the arguments, the environment and pyproject.toml are
    combined; everything else follows arguments > environment > pyproject.toml.

    Configuration can be set in pyproject.toml:
        [tool.gatun]
        memory = "64MB"
        socket_path = "/tmp/gatun.sock"  # Optional: fixed path for single session
        jvm_flags = ["-Xmx512m"]
        allowed_classes = ["java.util.concurrent.atomic.AtomicLong"]
        allowed_prefixes = ["com.example"]

    Environment variables for observability:
        GATUN_TRACE=true     - Enable trace mode
        GATUN_LOG_LEVEL=FINE - Set log level
        GATUN_DEBUG=true     - Pass through Java server output
    """
    config = get_config()

    if not JAR_PATH.exists():
        raise RuntimeError(f"Gatun JAR not found at {JAR_PATH}. Did you run 'uv sync'?")

    # 1. Apply config defaults, then parse memory
    if memory is None:
        memory = config.memory

    size_str = memory.upper()
    if size_str.endswith("GB"):
        mem_bytes = int(float(size_str[:-2]) * 1024 * 1024 * 1024)
    elif size_str.endswith("MB"):
        mem_bytes = int(float(size_str[:-2]) * 1024 * 1024)
    else:
        mem_bytes = int(size_str)  # Assume bytes

    # 2. Setup Paths - use random temp file if not specified
    if socket_path is None:
        socket_path = config.socket_path
    if socket_path is None:
        # Generate unique socket path to allow multiple concurrent sessions
        # Always use /tmp to avoid Unix domain socket path length limits (~108 chars)
        # Don't use tempfile.gettempdir() as it may return a longer path based on TMPDIR
        random_suffix = secrets.token_hex(8)
        socket_path = f"/tmp/gatun_{os.getpid()}_{random_suffix}.sock"

    # 3. Construct Command with config JVM flags
    jvm_flags = DEFAULT_JVM_FLAGS + config.jvm_flags
    if extra_jvm_flags:
        jvm_flags.extend(extra_jvm_flags)

    # 3a. Add observability flags
    # Check environment variables first, then use function arguments
    if trace or os.environ.get("GATUN_TRACE", "").lower() == "true":
        jvm_flags.append("-Dgatun.trace=true")

    effective_log_level = log_level or os.environ.get("GATUN_LOG_LEVEL")
    if effective_log_level:
        # Pass the log level to Java - it will configure logging programmatically
        jvm_flags.append(f"-Dgatun.log.level={effective_log_level}")

    # 3b. Classes allowed beyond the server's built-in list
    all_classes = _checked_names(
        [
            *config.allowed_classes,
            *_take_property(jvm_flags, "gatun.allowlist.classes"),
            *_name_list(allowed_classes, "allowed_classes"),
        ],
        "allowed_classes",
    )
    all_prefixes = _checked_names(
        [
            *config.allowed_prefixes,
            *_take_property(jvm_flags, "gatun.allowlist.prefixes"),
            *_name_list(allowed_prefixes, "allowed_prefixes"),
        ],
        "allowed_prefixes",
    )
    if all_classes:
        jvm_flags.append(f"-Dgatun.allowlist.classes={','.join(all_classes)}")
    if all_prefixes:
        jvm_flags.append(f"-Dgatun.allowlist.prefixes={','.join(all_prefixes)}")

    # Find Java executable - prefer JAVA_HOME, fall back to PATH
    java_home = os.environ.get("JAVA_HOME")
    if java_home:
        java_cmd = os.path.join(java_home, "bin", "java")
    else:
        java_cmd = "java"

    if classpath:
        # Use -cp with main class to allow additional classpath entries
        # java [FLAGS] -cp [CLASSPATH] org.gatun.server.GatunServer [MEM_SIZE] [SOCKET_PATH]
        all_jars = [str(JAR_PATH)] + classpath
        cp = os.pathsep.join(all_jars)
        cmd = (
            [java_cmd]
            + jvm_flags
            + ["-cp", cp, "org.gatun.server.GatunServer", str(mem_bytes), socket_path]
        )
    else:
        # Use -jar for simplicity when no extra classpath needed
        # java [FLAGS] -jar [JAR] [MEM_SIZE] [SOCKET_PATH]
        cmd = (
            [java_cmd]
            + jvm_flags
            + ["-jar", str(JAR_PATH), str(mem_bytes), socket_path]
        )

    logger.info("Launching Java server: %s @ %s", memory, socket_path)
    logger.debug("JVM command: %s", " ".join(cmd))

    # Check if debug mode is requested via env var or parameter
    import sys

    debug_mode = debug or os.environ.get("GATUN_DEBUG", "").lower() == "true"

    if debug_mode:
        # Pass through output for debugging - logs appear in real-time
        process = subprocess.Popen(cmd, stdout=sys.stderr, stderr=sys.stderr, text=True)
    else:
        # Capture output (default) - quieter but startup errors are still reported
        process = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
        )

    # 4. Wait for Socket to be ready (not just file existence, but connectable)
    max_retries = int(config.startup_timeout / 0.1)
    retries = max_retries
    while retries > 0:
        if process.poll() is not None:
            # Process died
            if debug_mode:
                # Output was already printed to stderr
                raise RuntimeError("Java Server failed to start (see output above)")
            else:
                stdout, stderr = process.communicate()
                raise RuntimeError(
                    f"Java Server failed to start:\nstdout: {stdout}\nstderr: {stderr}"
                )

        # Check if socket file exists AND is connectable
        if os.path.exists(socket_path):
            try:
                # Try to connect to verify server is ready
                sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                sock.settimeout(0.1)
                sock.connect(socket_path)
                sock.close()
                break  # Server is ready
            except (ConnectionRefusedError, OSError):
                pass  # Socket exists but server not ready yet

        time.sleep(0.1)
        retries -= 1

    if retries == 0:
        process.terminate()
        raise RuntimeError("Timed out waiting for Java Server socket.")

    # 5. Register Cleanup
    session = GatunSession(process, socket_path, mem_bytes)

    # Start background threads to drain stdout/stderr pipes.
    # This prevents the Java process from blocking when the pipe buffer fills up
    # (default ~64KB on most systems). Without draining, the server hangs after
    # producing enough log output.
    if not debug_mode:
        session._start_drain_threads()

    _active_sessions.add(session)
    atexit.register(session.stop)
    _install_signal_handlers()

    return session
