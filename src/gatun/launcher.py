import atexit
import glob
import logging
import os
import re
import secrets
import signal
import socket
import stat
import struct
import subprocess
import sys
import threading
import time
import urllib.parse
import weakref
import zipfile
from pathlib import Path

from gatun.client import COMMAND_ZONE_SIZE, RESPONSE_ZONE_SIZE
from gatun.config import get_config, parse_switch

logger = logging.getLogger(__name__)

# --- Configuration ---
MODULE_DIR = Path(__file__).parent.resolve()
JAR_PATH = MODULE_DIR / "jars" / "gatun-server-all.jar"
# The server without Arrow, Netty and Jackson: see _server_jar
THIN_JAR_PATH = MODULE_DIR / "jars" / "gatun-server-thin.jar"

# One class from each library the server's own code uses and the thin JAR
# leaves out: Arrow's vectors, its message format, its memory, its Netty
# allocator and the classes that one puts into Netty's package, Netty's buffers
# and what they stand on, and FlatBuffers
_HOST_ARROW_CLASSES = (
    b"org/apache/arrow/vector/VectorSchemaRoot.class",
    b"org/apache/arrow/flatbuf/Message.class",
    b"org/apache/arrow/memory/BufferAllocator.class",
    b"org/apache/arrow/memory/netty/NettyAllocationManager.class",
    b"io/netty/buffer/PooledByteBufAllocatorL.class",
    b"io/netty/buffer/PooledByteBufAllocator.class",
    b"io/netty/util/internal/PlatformDependent.class",
    b"com/google/flatbuffers/Table.class",
)

# The oldest Arrow the server runs on: the one it is compiled against
# (gatun-core/build.gradle.kts). An older one on the classpath gets the fat JAR
# in front of it, as every Arrow did before there was a thin one.
MIN_HOST_ARROW = (18, 3)
_ARROW_POM = "META-INF/maven/org.apache.arrow/arrow-vector/pom.properties"

# What `server_jar` may be: the launcher's own choice, or one of the two JARs
SERVER_JAR_CHOICES = ("auto", "thin", "full")

# The shared memory holds the command zone, the payload zone and the response
# zone. A size that leaves no payload zone makes the response zone overlap the
# command zone, so require room for a small payload as well.
MIN_PAYLOAD_SIZE = 65536
MIN_MEMORY_BYTES = COMMAND_ZONE_SIZE + MIN_PAYLOAD_SIZE + RESPONSE_ZONE_SIZE

# Lines of server output kept per stream. The tail is what a startup failure
# reports; a server logging at FINE for days must not grow the client's memory.
OUTPUT_TAIL_LINES = 1000

# How long a server gets to exit after SIGTERM before it is killed
STOP_GRACE_SECONDS = 2.0

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

    # Only take over a signal that would otherwise kill the process without any
    # cleanup. An ignored signal stays ignored: a process started with nohup, or
    # as a background job of a non-interactive shell, must not start dying of
    # it because it launched a server. Any other handler is the application's.
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            if signal.getsignal(sig) is signal.SIG_DFL:
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
    # Netty reflective access, without which it zero-fills the direct memory it
    # allocates. The server's own Netty is relocated in the JAR, and the names
    # of its system properties with it; the plain name is for a Netty on the
    # classpath (Spark's).
    "-Dio.netty.tryReflectionSetAccessible=true",
    "-Dgatun.shaded.io.netty.tryReflectionSetAccessible=true",
]


_LIKELY_JAR = re.compile("arrow|netty|flatbuffers", re.IGNORECASE)


def _central_directory(path: str) -> bytes:
    """The central directory of a JAR: the part that lists its entries.

    Read whole and searched as bytes (`_jar_has`). Going through `zipfile`
    builds an object per entry, and a Spark classpath has some 280 JARs with
    a quarter of a million entries between them.
    """
    with open(path, "rb") as f:
        size = f.seek(0, os.SEEK_END)
        # The record that ends the file, behind a comment of at most 64 KB
        tail_size = min(size, 22 + 65535)
        f.seek(size - tail_size)
        tail = f.read(tail_size)
        end = tail.rfind(b"PK\x05\x06")
        if end < 0 or end + 22 > len(tail):
            return b""
        cd_size, cd_offset = struct.unpack_from("<II", tail, end + 12)
        if cd_size == 0xFFFFFFFF or cd_offset == 0xFFFFFFFF:
            # Zip64: the real numbers are in a record that a locator, right
            # before this one, points to
            locator = end - 20
            if locator < 0 or tail[locator : locator + 4] != b"PK\x06\x07":
                return b""
            (record_offset,) = struct.unpack_from("<Q", tail, locator + 8)
            f.seek(record_offset)
            record = f.read(56)
            if len(record) < 56 or record[:4] != b"PK\x06\x06":
                return b""
            cd_size, cd_offset = struct.unpack_from("<QQ", record, 40)
        f.seek(cd_offset)
        return f.read(cd_size)


def _jar_has(directory: bytes, name: bytes) -> bool:
    """Whether a central directory lists exactly this entry."""
    at = directory.find(name)
    while at >= 0:
        # An entry's name follows its 46-byte header, which gives its length;
        # without this a relocated copy (shaded/org/apache/arrow/...) would do
        header = at - 46
        if (
            header >= 0
            and directory[header : header + 4] == b"PK\x01\x02"
            and struct.unpack_from("<H", directory, header + 28)[0] == len(name)
        ):
            return True
        at = directory.find(name, at + 1)
    return False


def _classpath_entries(classpath: list[str]) -> list[str]:
    """The files and directories a classpath names, the way the JVM reads it:
    `dir/*` is every JAR in `dir`."""
    entries: list[str] = []
    for item in classpath:
        for entry in item.split(os.pathsep):
            if entry.endswith(os.sep + "*") or entry == "*":
                folder = entry[:-1] or "."
                entries += sorted(
                    path
                    for path in glob.glob(os.path.join(glob.escape(folder), "*"))
                    if path.lower().endswith(".jar")
                )
            elif entry:
                entries.append(entry)
    return entries


def _manifest_class_path(jar: str) -> list[str]:
    """The entries a JAR's manifest adds to the classpath (`Class-Path`), which
    the JVM follows: relative ones are beside the JAR."""
    try:
        with zipfile.ZipFile(jar) as archive:
            manifest = archive.read("META-INF/MANIFEST.MF").decode("utf-8", "replace")
    except (OSError, KeyError, ValueError, zipfile.BadZipFile):
        return []
    # A line that starts with a space continues the one before
    text = manifest.replace("\r\n", "\n").replace("\r", "\n").replace("\n ", "")
    for line in text.split("\n"):
        name, _, value = line.partition(":")
        if name.strip().lower() == "class-path":
            base = os.path.dirname(jar)
            entries = []
            for url in value.split():
                path = urllib.parse.unquote(urllib.parse.urlparse(url).path)
                if path:
                    entries.append(os.path.normpath(os.path.join(base, path)))
            return entries
    return []


def _host_arrow(classpath: list[str]) -> tuple[set[bytes], str | None]:
    """What the classpath lacks of the Arrow the server needs (see
    _HOST_ARROW_CLASSES), and the entry that has Arrow's vectors, or None."""
    missing = set(_HOST_ARROW_CLASSES)
    vectors: str | None = None
    entries = _classpath_entries(classpath)
    # The likely ones first, so that a long classpath is not read to its end
    entries.sort(key=lambda entry: not _LIKELY_JAR.search(os.path.basename(entry)))
    seen = set(entries)
    followed = False
    while entries:
        for entry in entries:
            if os.path.isdir(entry):
                found = {
                    name
                    for name in missing
                    if os.path.exists(os.path.join(entry, os.fsdecode(name)))
                }
            else:
                try:
                    directory = _central_directory(entry)
                except (OSError, ValueError, OverflowError, struct.error):
                    # Not there, or no JAR: the JVM skips such an entry, too
                    continue
                found = {name for name in missing if _jar_has(directory, name)}
            if _HOST_ARROW_CLASSES[0] in found:
                vectors = entry
            missing -= found
            if not missing:
                return missing, vectors
        if followed:
            break
        # Not all on the classpath as given. A JAR can name more of it in its
        # manifest (a pathing JAR, Maven's addClasspath), and the JVM follows
        # those. Read only now: a manifest costs opening the JAR as an archive.
        followed = True
        queue = [entry for entry in seen if not os.path.isdir(entry)]
        entries = []
        while queue and len(seen) < 4096:
            for more in _manifest_class_path(queue.pop()):
                if more not in seen:
                    seen.add(more)
                    entries.append(more)
                    if not os.path.isdir(more):
                        queue.append(more)
    return missing, vectors


def _arrow_version(entry: str) -> tuple[int, int] | None:
    """The version of the Arrow in a classpath entry, from the description
    Maven puts into arrow-vector's JAR (and an uber JAR usually keeps). None if
    it does not say."""
    text = ""
    try:
        if os.path.isdir(entry):
            with open(os.path.join(entry, _ARROW_POM), encoding="utf-8") as f:
                text = f.read()
        else:
            with zipfile.ZipFile(entry) as archive:
                text = archive.read(_ARROW_POM).decode("utf-8", "replace")
    except (OSError, KeyError, ValueError, zipfile.BadZipFile):
        pass
    found = re.search(r"^version=(\d+)\.(\d+)", text, re.MULTILINE) or re.match(
        r"arrow-vector-(\d+)\.(\d+)", os.path.basename(entry)
    )
    return (int(found[1]), int(found[2])) if found else None


def _server_jar(classpath: list[str] | None, choice: str = "auto") -> Path:
    """The JAR to start the server from.

    The fat JAR has Arrow in it (and Arrow's Netty and Jackson, both relocated).
    In front of a classpath with an Arrow of its own it would shadow that Arrow
    class by class, and code on the classpath built for another version breaks:
    Spark 4.3 ships Arrow 19. Relocating the server's Arrow is no way out,
    because that code has to receive the VectorSchemaRoot `arrow_to_java()`
    makes, as its own class. So where the classpath brings Arrow, the server
    is started from the thin JAR, which has the server's classes and nothing
    else, and uses that Arrow like everything else there.

    Not an Arrow older than the one the server is compiled against, though
    (MIN_HOST_ARROW): the server may call what that one does not have. It gets
    the fat JAR, as before. `choice` ("thin" or "full") overrides all of this.
    """
    if choice == "full":
        return JAR_PATH
    if choice == "auto":
        if not classpath:
            return JAR_PATH
        missing, vectors = _host_arrow(classpath)
        if vectors is None:
            return JAR_PATH
        if missing:
            logger.warning(
                "The classpath has Arrow but not all of it the server needs "
                "(missing: %s), so the server's own Arrow goes in front of it",
                ", ".join(sorted(os.fsdecode(name) for name in missing)),
            )
            return JAR_PATH
        version = _arrow_version(vectors)
        if version is not None and version < MIN_HOST_ARROW:
            logger.warning(
                "The classpath has Arrow %d.%d (%s) and the server needs %d.%d or "
                "later, so the server's own Arrow goes in front of it. "
                "server_jar='thin' runs the server on the classpath's Arrow anyway.",
                *version,
                vectors,
                *MIN_HOST_ARROW,
            )
            return JAR_PATH
    if not THIN_JAR_PATH.exists() and JAR_PATH.exists() and choice == "auto":
        # A checkout that was built before there was a thin JAR
        logger.warning(
            "The classpath has its own Arrow, but %s is not there, so the "
            "server's own Arrow goes in front of it. Build it with "
            "'uv sync --reinstall-package gatun'.",
            THIN_JAR_PATH,
        )
        return JAR_PATH
    return THIN_JAR_PATH


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
            hint = (
                " To allow every class, pass allow_all=True."
                if option == "allowed_prefixes" and bare in ("", "*")
                else ""
            )
            raise ValueError(
                f"Invalid entry in {option}: {name!r}. Expected a dotted Java name "
                "such as 'com.example.Thing' (a class) or 'com.example' (a package prefix)."
                + hint
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


_MEMORY_UNITS = {"K": 1024, "M": 1024**2, "G": 1024**3}
_MEMORY_SIZE = re.compile(r"(\d+(?:\.\d+)?)\s*(?:([KMG])B?)?", re.IGNORECASE)


def _parse_memory_size(memory: str | int) -> int:
    """Convert a memory size such as "64MB" to a number of bytes.

    Accepts a number followed by KB, MB or GB (or K, M, G; any case), a bare
    number of bytes, or a positive int of bytes. Units are powers of 1024.

    Raises:
        ValueError: if the value is not a size, or is too small for a server
            to run with.
    """
    mem_bytes: int | None = None
    # bool is an int, but True is not a size
    if isinstance(memory, int) and not isinstance(memory, bool):
        mem_bytes = memory if memory > 0 else None
    elif isinstance(memory, str):
        match = _MEMORY_SIZE.fullmatch(memory.strip())
        if match:
            number, unit = match.groups()
            if unit is not None:
                mem_bytes = int(float(number) * _MEMORY_UNITS[unit.upper()])
            elif number.isdigit():  # no such thing as half a byte
                mem_bytes = int(number)

    if mem_bytes is None:
        raise ValueError(
            f"Invalid memory size: {memory!r}. Expected a number with a unit, such "
            "as '512KB', '64MB' or '1GB' (K, M and G also work, in any case), or a "
            "positive whole number of bytes."
        )
    if mem_bytes < MIN_MEMORY_BYTES:
        raise ValueError(
            f"Memory size {memory!r} is too small: {mem_bytes:,} bytes, and the "
            f"minimum is {MIN_MEMORY_BYTES:,} bytes ({MIN_MEMORY_BYTES // 1024}KB), "
            "which holds the command zone, the response zone and a small payload "
            "zone. Sizes are written like '512KB', '64MB' or '1GB'."
        )
    return mem_bytes


def _accepts_connections(socket_path: str, timeout: float) -> bool:
    """Whether something is listening on the Unix socket at socket_path."""
    probe = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    probe.settimeout(timeout)
    try:
        probe.connect(socket_path)
    except TimeoutError:
        # Nobody refused the connection, so there is a listener whose accept
        # queue is full
        return True
    except OSError:
        # No file, a socket file nobody listens on, or not a socket at all
        return False
    finally:
        probe.close()
    return True


def _drain_pipe(pipe, storage: list, limit: int = OUTPUT_TAIL_LINES):
    """Read a pipe to its end, keeping the most recent lines in storage.

    Runs in a background thread: a pipe nobody reads fills up and blocks the
    server on its next line of output. storage holds between limit and
    2 * limit lines once that many were read; trimming at twice the limit
    avoids shifting the whole list for every line.
    """
    try:
        for line in pipe:
            storage.append(line)
            if len(storage) >= 2 * limit:
                del storage[:-limit]
    except (ValueError, OSError):
        pass  # Pipe closed


def _terminate(process: subprocess.Popen) -> None:
    """Stop a process and reap it: SIGTERM, then SIGKILL if that is not enough."""
    if process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(timeout=STOP_GRACE_SECONDS)
    except subprocess.TimeoutExpired:
        process.kill()
        # Without this the killed process stays a zombie until the interpreter
        # exits, and returncode is never set
        process.wait()


class GatunSession:
    def __init__(self, process, socket_path, memory_bytes):
        self.process = process
        self.socket_path = socket_path
        self.memory_bytes = memory_bytes
        self._stdout_lines: list[str] = []
        self._stderr_lines: list[str] = []
        self._drain_threads: list[threading.Thread] = []
        # A fork copies this object, the atexit entry and the signal handler
        # into the child, where they would stop a server the child does not own
        self._owner_pid = os.getpid()
        self._stopped = False

    def _start_drain_threads(self):
        """Start background threads to drain stdout/stderr pipes."""
        for pipe, storage in (
            (self.process.stdout, self._stdout_lines),
            (self.process.stderr, self._stderr_lines),
        ):
            if pipe:
                t = threading.Thread(
                    target=_drain_pipe, args=(pipe, storage), daemon=True
                )
                t.start()
                self._drain_threads.append(t)

    def _finish_draining(self, process) -> None:
        """Collect the last output of a process that has exited, then close its pipes."""
        for t in self._drain_threads:
            # The pipes reach end-of-file when the process exits, unless a
            # process it started still holds them open
            t.join(timeout=1)
        if any(t.is_alive() for t in self._drain_threads):
            # Closing a pipe that a thread is blocked reading would block too
            return
        for pipe in (process.stdout, process.stderr):
            if pipe:
                try:
                    pipe.close()
                except OSError:
                    pass

    def _captured_output(self) -> str:
        """The tail of the server's output, for error messages."""
        return (
            f"stdout: {''.join(self._stdout_lines[-OUTPUT_TAIL_LINES:])}\n"
            f"stderr: {''.join(self._stderr_lines[-OUTPUT_TAIL_LINES:])}"
        )

    def stop(self):
        """Stop the server and remove its files. Safe to call more than once.

        Does nothing in a process other than the one that launched the server.
        """
        if os.getpid() != self._owner_pid or self._stopped:
            return
        # Set first: a signal handler may call stop() again while this call is
        # waiting for the server to exit
        self._stopped = True
        process, self.process = self.process, None

        try:
            if process:
                logger.debug("Stopping Java server...")
                _terminate(process)
                self._finish_draining(process)
        finally:
            # Clean up the socket and the per-session files: shared memory
            # (<socket>.<session id>.shm) and the socket a callback connection
            # is made to (<socket>.<session id>.cb). The server removes them
            # itself on a clean shutdown; this covers a server that was killed.
            base = glob.escape(self.socket_path)
            for path in (*glob.glob(f"{base}.*.shm"), *glob.glob(f"{base}.*.cb")):
                try:
                    os.unlink(path)
                except OSError:
                    pass  # Already gone, or not ours to remove
            try:
                # Only if it is a socket. The path is the caller's choice, and a
                # server that failed before binding it has left whatever was
                # there in place.
                if stat.S_ISSOCK(os.lstat(self.socket_path).st_mode):
                    os.unlink(self.socket_path)
            except OSError:
                pass

            # Remove from active sessions
            _active_sessions.discard(self)
            # Stopped for good: drop the atexit entry so it does not keep this
            # session (and its captured server output) alive until the
            # interpreter exits
            atexit.unregister(self.stop)


def _wait_until_ready(session: GatunSession, timeout: float, debug_mode: bool) -> None:
    """Wait until the server accepts connections on its socket.

    Raises RuntimeError if the server exits first or the timeout passes; on a
    timeout the server has been stopped by the time this returns.
    """
    process = session.process
    deadline = time.monotonic() + timeout
    while True:
        returncode = process.poll()
        if returncode is not None:
            if debug_mode:
                # Output was already printed to stderr
                raise RuntimeError(
                    f"Java Server failed to start (exit code {returncode}, "
                    "see output above)"
                )
            session._finish_draining(process)
            raise RuntimeError(
                f"Java Server failed to start (exit code {returncode}):\n"
                f"{session._captured_output()}"
            )

        # The file existing is not enough: it may be left over from an earlier
        # server, with this one yet to replace it
        if _accepts_connections(session.socket_path, timeout=0.1):
            return
        if time.monotonic() >= deadline:
            break
        time.sleep(0.1)

    # Stop it before reporting, so the message has all of its output
    session.stop()
    message = (
        f"Timed out after {timeout:g}s waiting for Java Server socket at "
        f"{session.socket_path}."
    )
    if not debug_mode:
        message += f" Server output:\n{session._captured_output()}"
    raise RuntimeError(message)


def launch_gateway(
    memory: str | int | None = None,
    socket_path: str | None = None,
    classpath: list[str] | None = None,
    trace: bool = False,
    log_level: str | None = None,
    debug: bool = False,
    extra_jvm_flags: list[str] | None = None,
    allowed_classes: list[str] | None = None,
    allowed_prefixes: list[str] | None = None,
    allow_all: bool | None = None,
    server_jar: str | None = None,
):
    """
    Launches the embedded Java server.

    Args:
        memory: Shared memory size per client: a number with a unit ("512KB",
                "64MB", "1GB") or a number of bytes. Defaults to config value.
        socket_path: Path to the Unix socket. If not specified, generates a unique
                     path in the system temp directory to allow concurrent sessions.
                     A fixed path can serve one server at a time: launching fails
                     if another server is already listening on it.
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
        allow_all: True lifts the class allowlist: every class can be used, and
                   so can reflection objects, class loaders, `Runtime` and the
                   other types that are otherwise restricted. `System.exit`,
                   `System.load`, `System.loadLibrary` and `Class.newInstance`
                   stay refused. False keeps the list whatever the environment
                   or pyproject.toml say; None (the default) leaves it to them.
        server_jar: Which of the two server JARs to start. "auto" (the default,
                    also GATUN_SERVER_JAR and [tool.gatun] server_jar) takes the
                    thin one, which has no Arrow of its own, when the classpath
                    brings an Arrow the server can run on, and the full one
                    otherwise. "thin" and "full" decide it.

    The allow lists from the arguments, the environment and pyproject.toml are
    combined; everything else follows arguments > environment > pyproject.toml.

    Configuration can be set in pyproject.toml:
        [tool.gatun]
        memory = "64MB"
        socket_path = "/tmp/gatun.sock"  # Optional: fixed path for single session
        jvm_flags = ["-Xmx512m"]
        allowed_classes = ["java.util.concurrent.atomic.AtomicLong"]
        allowed_prefixes = ["com.example"]
        allow_all = false

    Environment variables for observability:
        GATUN_TRACE=true     - Enable trace mode
        GATUN_LOG_LEVEL=FINE - Set log level
        GATUN_DEBUG=true     - Pass through Java server output
    """
    config = get_config()

    choice = config.server_jar if server_jar is None else server_jar
    if choice not in SERVER_JAR_CHOICES:
        raise ValueError(
            f"server_jar must be one of {', '.join(SERVER_JAR_CHOICES)}, got {choice!r}"
        )
    jar = _server_jar(classpath, choice)
    if not jar.exists():
        raise RuntimeError(
            f"Gatun JAR not found at {jar}. "
            "Did you run 'uv sync --reinstall-package gatun'?"
        )

    # 1. Apply config defaults, then parse memory
    if memory is None:
        memory = config.memory

    mem_bytes = _parse_memory_size(memory)

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
    if jar == THIN_JAR_PATH:
        # The server's Netty is the classpath's there, and reads its settings
        # under their own names. Not passed on under them: that Netty is also
        # the classpath's own code's, which the setting was not meant for.
        for flag in (*config.jvm_flags, *(extra_jvm_flags or ())):
            if flag.startswith("-Dgatun.shaded.io.netty."):
                logger.warning(
                    "%s has no effect: the server runs on the classpath's Arrow "
                    "and Netty, which reads -D%s",
                    flag.partition("=")[0],
                    flag.partition("=")[0][len("-Dgatun.shaded.") :],
                )

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

    # Or no list at all. A flag the caller set directly counts like the
    # environment: the argument, when given, decides.
    if allow_all is not None and not isinstance(allow_all, bool):
        raise TypeError(
            f"allow_all must be True or False, got {type(allow_all).__name__}: "
            f"{allow_all!r}"
        )
    lift_list = config.allow_all
    for value in _take_property(jvm_flags, "gatun.allowlist.all"):
        lift_list = parse_switch(value, "-Dgatun.allowlist.all")
    if allow_all is not None:
        lift_list = allow_all
    if lift_list:
        jvm_flags.append("-Dgatun.allowlist.all=true")

    # 3c. The process the server belongs to. The server runs in its own session
    # (see Popen below), so nothing else ends it when this process is killed
    # without running its cleanup; the server watches this pid and exits when
    # it is gone. A value the caller set directly ties it to that process
    # instead.
    parent_pid = _take_property(jvm_flags, "gatun.parent.pid")
    jvm_flags.append(
        f"-Dgatun.parent.pid={parent_pid[-1] if parent_pid else os.getpid()}"
    )

    # Find Java executable - prefer JAVA_HOME, fall back to PATH
    java_home = os.environ.get("JAVA_HOME")
    if java_home:
        java_cmd = os.path.join(java_home, "bin", "java")
    else:
        java_cmd = "java"

    if classpath:
        # Use -cp with main class to allow additional classpath entries
        # java [FLAGS] -cp [CLASSPATH] org.gatun.server.GatunServer [MEM_SIZE] [SOCKET_PATH]
        all_jars = [str(jar)] + classpath
        cp = os.pathsep.join(all_jars)
        cmd = (
            [java_cmd]
            + jvm_flags
            + ["-cp", cp, "org.gatun.server.GatunServer", str(mem_bytes), socket_path]
        )
    else:
        # Use -jar for simplicity when no extra classpath needed
        # java [FLAGS] -jar [JAR] [MEM_SIZE] [SOCKET_PATH]
        cmd = [java_cmd] + jvm_flags + ["-jar", str(jar), str(mem_bytes), socket_path]

    logger.info("Launching Java server: %s @ %s", memory, socket_path)
    logger.debug("JVM command: %s", " ".join(cmd))

    # A server already listening here would answer the readiness check below,
    # so the caller would end up connected to it (with whatever classpath and
    # memory size it was started with) while the new server deleted its socket
    # file and truncated its shared memory files. A socket file left behind by
    # a dead server refuses connections and is simply replaced.
    if _accepts_connections(socket_path, timeout=1.0):
        raise RuntimeError(
            f"A server is already listening on {socket_path}; launching another "
            "one on the same socket path would break it. Stop that server first, "
            "pass a different socket_path, or leave socket_path unset (also in "
            "[tool.gatun] and GATUN_SOCKET_PATH) to get a unique path for each "
            "server. To use the server that is already running, connect to it "
            "with GatunClient(socket_path) instead of launching."
        )

    # Check if debug mode is requested via env var or parameter
    debug_mode = debug or os.environ.get("GATUN_DEBUG", "").lower() == "true"
    # Pass output through in debug mode, so logs appear in real time. Otherwise
    # capture it: quieter, and a startup failure can still report it.
    output = sys.stderr if debug_mode else subprocess.PIPE

    session = GatunSession(None, socket_path, mem_bytes)
    try:
        session.process = subprocess.Popen(
            cmd,
            stdout=output,
            stderr=output,
            text=True,
            # Undecodable output must not end the thread that drains the pipe
            errors="replace",
            # Its own session, and so its own process group: Ctrl-C in a
            # terminal signals the whole foreground group, which would take the
            # server down under a program that handles KeyboardInterrupt
            start_new_session=True,
        )
        # 4. Drain the pipes from the start: a server that writes more than a
        # pipe buffer (~64KB) while starting up would otherwise block before it
        # ever listens.
        if not debug_mode:
            session._start_drain_threads()

        # 5. Wait for the socket to be connectable, not just for the file
        _wait_until_ready(session, config.startup_timeout, debug_mode)

        # 6. Register cleanup
        _active_sessions.add(session)
        atexit.register(session.stop)
        _install_signal_handlers()
    except BaseException:
        # Nothing else refers to the server yet, so whatever interrupted the
        # startup (KeyboardInterrupt included) would leave it running
        if session.process is not None:
            session.stop()
        raise

    return session
