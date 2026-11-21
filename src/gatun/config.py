"""Configuration system for Gatun.

Reads configuration from pyproject.toml under the [tool.gatun] section.

Example pyproject.toml:
    [tool.gatun]
    memory = "64MB"
    socket_path = "/tmp/gatun.sock"
    jvm_flags = ["-Xmx512m"]
    allowed_classes = ["java.util.concurrent.atomic.AtomicLong"]
    allowed_prefixes = ["com.example"]
    allow_all = false

Configuration precedence (highest to lowest):
    1. Function arguments (e.g., connect(memory="32MB"))
    2. Environment variables (e.g., GATUN_MEMORY=32MB)
    3. pyproject.toml [tool.gatun] section
    4. Built-in defaults

The allow lists are the exception: allowed_classes and allowed_prefixes from every
source (arguments, GATUN_ALLOWED_CLASSES / GATUN_ALLOWED_PREFIXES, pyproject.toml)
are combined and added to the server's built-in list. allow_all (GATUN_ALLOW_ALL),
which lifts the list, follows the precedence above like any other setting.
"""

import os
import tomllib
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class GatunConfig:
    """Configuration for Gatun client and server."""

    # Server settings
    memory: str = "16MB"
    socket_path: str | None = None

    # JVM settings
    jvm_flags: list[str] = field(default_factory=list)

    # Classes and package prefixes allowed in addition to the server's built-in list
    allowed_classes: list[str] = field(default_factory=list)
    allowed_prefixes: list[str] = field(default_factory=list)
    # Allow every class, lifting that list
    allow_all: bool = False

    # Timeouts
    connect_timeout: float = 5.0  # seconds
    # Seconds to wait for the server's socket. It only bounds a server that
    # neither listens nor exits: one that exits is reported at once. A JVM with
    # a few hundred JARs on its classpath (Spark's) needs more than five seconds
    # to get there on a small machine.
    startup_timeout: float = 30.0

    def __post_init__(self) -> None:
        # Expand ~ in socket_path
        if self.socket_path:
            self.socket_path = os.path.expanduser(self.socket_path)


def find_pyproject_toml(start_path: Path | None = None) -> Path | None:
    """Find pyproject.toml by searching up from start_path.

    Args:
        start_path: Directory to start searching from. Defaults to cwd.

    Returns:
        Path to pyproject.toml if found, None otherwise.
    """
    if start_path is None:
        start_path = Path.cwd()

    current = start_path.resolve()
    while current != current.parent:
        pyproject = current / "pyproject.toml"
        if pyproject.exists():
            return pyproject
        current = current.parent

    # Check root as well
    pyproject = current / "pyproject.toml"
    if pyproject.exists():
        return pyproject

    return None


def _split_names(value: str) -> list[str]:
    """Split a comma-separated list of names, dropping blanks."""
    return [name.strip() for name in value.split(",") if name.strip()]


def parse_switch(value: str, source: str) -> bool:
    """Read "true" or "false" (in any case) from an environment variable or a flag.

    Anything else is an error: for a setting that opens the server up, "1" or
    "yes" must not be quietly read as one or the other.
    """
    lowered = value.strip().lower()
    if lowered == "true":
        return True
    if lowered == "false":
        return False
    raise ValueError(f"Invalid value for {source}: {value!r}. Expected true or false.")


def load_config(pyproject_path: Path | None = None) -> GatunConfig:
    """Load configuration from pyproject.toml.

    Args:
        pyproject_path: Explicit path to pyproject.toml. If None, searches
                       up from current directory.

    Returns:
        GatunConfig with values from pyproject.toml merged with defaults.
    """
    config = GatunConfig()

    # 1. Load from pyproject.toml
    if pyproject_path is None:
        pyproject_path = find_pyproject_toml()

    if pyproject_path and pyproject_path.exists():
        with open(pyproject_path, "rb") as f:
            data = tomllib.load(f)

        gatun_config = data.get("tool", {}).get("gatun", {})

        if "memory" in gatun_config:
            config.memory = gatun_config["memory"]
        if "socket_path" in gatun_config:
            config.socket_path = gatun_config["socket_path"]
        if "jvm_flags" in gatun_config:
            config.jvm_flags = gatun_config["jvm_flags"]
        if "connect_timeout" in gatun_config:
            config.connect_timeout = gatun_config["connect_timeout"]
        if "startup_timeout" in gatun_config:
            config.startup_timeout = gatun_config["startup_timeout"]
        for option in ("allowed_classes", "allowed_prefixes"):
            if option in gatun_config:
                value = gatun_config[option]
                # A bare string would be read as a list of its characters
                if not isinstance(value, list):
                    raise TypeError(
                        f"[tool.gatun] {option} in {pyproject_path} must be a list of "
                        f"strings, got {type(value).__name__}: {value!r}"
                    )
                setattr(config, option, list(value))
        if "allow_all" in gatun_config:
            value = gatun_config["allow_all"]
            # A string is true whatever it says
            if not isinstance(value, bool):
                raise TypeError(
                    f"[tool.gatun] allow_all in {pyproject_path} must be true or "
                    f"false, got {type(value).__name__}: {value!r}"
                )
            config.allow_all = value

    # 2. Override with environment variables
    if env_memory := os.environ.get("GATUN_MEMORY"):
        config.memory = env_memory
    if env_socket := os.environ.get("GATUN_SOCKET_PATH"):
        config.socket_path = env_socket
    if env_timeout := os.environ.get("GATUN_CONNECT_TIMEOUT"):
        config.connect_timeout = float(env_timeout)
    if env_startup := os.environ.get("GATUN_STARTUP_TIMEOUT"):
        config.startup_timeout = float(env_startup)
    # Allow lists accumulate across sources instead of overriding: each source
    # names libraries it needs, and none can take another's away.
    if env_classes := os.environ.get("GATUN_ALLOWED_CLASSES"):
        config.allowed_classes += _split_names(env_classes)
    if env_prefixes := os.environ.get("GATUN_ALLOWED_PREFIXES"):
        config.allowed_prefixes += _split_names(env_prefixes)
    if env_allow_all := os.environ.get("GATUN_ALLOW_ALL"):
        config.allow_all = parse_switch(env_allow_all, "GATUN_ALLOW_ALL")

    # Reprocess paths after env override
    config.__post_init__()

    return config


# Global config instance, lazily loaded
_config: GatunConfig | None = None


def get_config() -> GatunConfig:
    """Get the global configuration, loading it if necessary."""
    global _config
    if _config is None:
        _config = load_config()
    return _config


def reset_config() -> None:
    """Reset the global configuration. Useful for testing."""
    global _config
    _config = None
