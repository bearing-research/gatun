import logging
import tempfile
import time
from pathlib import Path

import pytest

from gatun.client import GatunClient
from gatun.launcher import launch_gateway

# --- Logging Setup ---
# Pytest captures this automatically. Run with -o log_cli=true to see it live.
logger = logging.getLogger("gatun.tests")


@pytest.fixture(scope="session")
def java_gateway():
    """
    Launches the Java Server once for the entire test session.
    Uses the packed JAR via the new 'launch_gateway' API.
    """
    # 1. Setup specific test paths (use tempfile for cross-platform compatibility)
    socket_path = Path(tempfile.gettempdir()) / "gatun_test.sock"

    # Cleanup stale socket from previous runs
    # (unlink(missing_ok=True) requires Python 3.8+)
    if socket_path.exists():
        socket_path.unlink()

    logger.info(f"Launching Gatun Server (64MB) at {socket_path}...")

    # 2. Launch (Configuration Phase)
    # We pass the path as a string because the Popen/Java args expect strings
    session = launch_gateway(memory="64MB", socket_path=str(socket_path))

    yield session

    # 3. Teardown
    logger.info("Stopping Gatun Server...")
    session.stop()

    # Final cleanup
    if socket_path.exists():
        socket_path.unlink()


def _create_client(session) -> GatunClient:
    """Connect a client to the session's server, retrying a few times.

    Each attempt uses a new client: a failed connect() leaves its socket
    connected or closed, so retrying on the same client can never succeed.
    """
    socket_path = str(session.socket_path)
    for _ in range(10):
        c = GatunClient(socket_path)
        if c.connect():
            break
        c.close()
        time.sleep(0.1)
    else:
        server_output = "".join(session._stderr_lines[-40:])
        raise RuntimeError(
            f"Client failed to connect to Gateway at {socket_path}. "
            f"Server output:\n{server_output}"
        )

    # Verify Handshake (Sanity Check)
    # We don't enforce exact size - just verify connection worked
    if c.memory_size <= 0:
        raise RuntimeError(f"Invalid memory size: {c.memory_size}")

    return c


@pytest.fixture(scope="function")
def client(java_gateway):
    """
    Provides a connected GatunClient for each test function.
    """
    logger.debug(f"Connecting client to {java_gateway.socket_path}")

    c = _create_client(java_gateway)
    yield c

    # Releases the shared memory mapping too, not just the socket: an unlinked
    # file keeps its disk blocks for as long as a mapping of it is alive.
    c.close()


@pytest.fixture(scope="function")
def make_client(java_gateway):
    """
    Factory fixture for creating fresh clients.

    Use this for tests where each call should get a fresh connection.
    The factory automatically closes clients after each use via a finalizer,
    preventing resource exhaustion.

    Usage:
        def test_something(make_client):
            client = make_client()
            # ... test code ...
            # client is automatically closed when the next make_client() is called
            # or when the test function ends
    """
    current_client = [None]  # Use list to allow mutation in closure

    def _make():
        # Close the previous client before creating a new one
        if current_client[0] is not None:
            current_client[0].close()

        c = _create_client(java_gateway)
        current_client[0] = c
        return c

    yield _make

    # Cleanup the last client
    if current_client[0] is not None:
        current_client[0].close()
