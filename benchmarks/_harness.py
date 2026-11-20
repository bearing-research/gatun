"""Timing shared by the benchmark scripts.

Three things the numbers depend on, done one way everywhere:

- Warm-up runs for a time as well as a count. The server's JIT compiles a method
  after thousands of calls; a case timed after ten of them measures the
  interpreter.
- What a case needs before each run (a fresh list to sort, an arena reset) is
  done by `setup`, outside the timed part.
- The figure reported is the median. On a shared machine a few runs in a hundred
  take many times longer than the rest, and a mean follows them.
"""

from __future__ import annotations

import json
import os
import platform
import statistics
import subprocess
import time
from collections.abc import Callable
from typing import Any

WARMUP_SECONDS = 1.0
WARMUP_MIN_RUNS = 300
RUNS = 300
QUICK = {"warmup_seconds": 0.05, "warmup_min_runs": 20, "runs": 30}


def measure(
    func: Callable[..., Any],
    *,
    setup: Callable[[], Any] | None = None,
    warmup_seconds: float = WARMUP_SECONDS,
    warmup_min_runs: int = WARMUP_MIN_RUNS,
    runs: int = RUNS,
    budget: float | None = None,
) -> dict:
    """Time `func`, in microseconds.

    `setup`, if given, runs before every call, untimed, and its result is passed
    to `func`. `budget` ends the timed runs after that many seconds, for cases
    where one run takes long (at least seven are made).
    """

    def once() -> float:
        if setup is None:
            start = time.perf_counter_ns()
            func()
        else:
            state = setup()
            start = time.perf_counter_ns()
            func(state)
        return (time.perf_counter_ns() - start) / 1_000

    warmed = 0
    warm_until = time.perf_counter() + warmup_seconds
    while warmed < warmup_min_runs or time.perf_counter() < warm_until:
        once()
        warmed += 1

    times: list[float] = []
    deadline = None if budget is None else time.perf_counter() + budget
    while len(times) < runs:
        times.append(once())
        if deadline is not None and len(times) >= 7 and time.perf_counter() >= deadline:
            break

    ordered = sorted(times)
    return {
        "median_us": statistics.median(ordered),
        "p95_us": ordered[min(len(ordered) - 1, int(len(ordered) * 0.95))],
        "min_us": ordered[0],
        "runs": len(ordered),
        "warmup_runs": warmed,
    }


class Report:
    """Collects the cases of one script, prints them, and writes them as JSON."""

    def __init__(self, title: str, *, quick: bool = False):
        self.title = title
        self.options: dict = dict(QUICK) if quick else {}
        self.cases: list[dict] = []
        print(f"{title}{' [quick: numbers are not meaningful]' if quick else ''}")
        print(environment_line())

    def section(self, name: str) -> None:
        print(f"\n{name}")

    def case(
        self,
        name: str,
        func: Callable[..., Any],
        *,
        setup: Callable[[], Any] | None = None,
        budget: float | None = None,
        **extra: Any,
    ) -> float:
        """Measure one case and return its median in microseconds."""
        options = dict(self.options)
        if budget is not None:
            # A long case is bounded by time, in quick mode by a short one
            options.pop("runs", None)
            options["budget"] = 0.2 if self.options else budget
            options["warmup_min_runs"] = 3
        stats = measure(func, setup=setup, **options)
        stats.update(name=name, **extra)
        self.cases.append(stats)
        note = ""
        if "bytes" in extra:
            mb_per_s = extra["bytes"] / stats["median_us"]
            stats["mb_per_s"] = mb_per_s
            note = f"  {mb_per_s:9.0f} MB/s"
        print(
            f"  {name:<52}{format_us(stats['median_us']):>10}"
            f"  (p95 {format_us(stats['p95_us'])}, {stats['runs']} runs){note}"
        )
        return stats["median_us"]

    def note(self, text: str) -> None:
        print(f"    {text}")

    def write(self, path: str | None) -> None:
        if not path:
            return
        with open(path, "w") as out:
            json.dump(
                {
                    "title": self.title,
                    "environment": environment(),
                    "cases": self.cases,
                },
                out,
                indent=1,
            )
        print(f"\nwrote {path}")


def format_us(microseconds: float) -> str:
    if microseconds >= 10_000:
        return f"{microseconds / 1000:.1f} ms"
    if microseconds >= 1_000:
        return f"{microseconds / 1000:.2f} ms"
    return f"{microseconds:.1f} us"


def java_version() -> str:
    java = "java"
    if os.environ.get("JAVA_HOME"):
        java = os.path.join(os.environ["JAVA_HOME"], "bin", "java")
    try:
        out = subprocess.run(
            [java, "-version"], capture_output=True, text=True, check=False, timeout=20
        )
    except (OSError, subprocess.TimeoutExpired):
        return "unknown"
    lines = (out.stderr or out.stdout).splitlines()
    return lines[0] if lines else "unknown"


def environment() -> dict:
    return {
        "machine": platform.machine(),
        "system": platform.platform(),
        "cpus": os.cpu_count(),
        "python": platform.python_version(),
        "java": java_version(),
    }


def environment_line() -> str:
    env = environment()
    return (
        f"{env['system']}, {env['machine']}, {env['cpus']} CPUs, "
        f"Python {env['python']}, {env['java']}"
    )
