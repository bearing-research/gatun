"""Does gatun's shared memory beat a socket or a tmpfs file for moving Arrow data?

Moves one table between Python and the JVM, in both directions, five ways:

  arena      gatun's buffer-descriptor path: send_arrow_buffers() / get_arrow_data().
             One copy into the shared region, read in place on the other side.
             Java code cannot receive or produce these tables.
  shm_ipc    gatun's object path: arrow_to_java() / arrow_from_java(). An Arrow IPC
             stream written into the shared region and copied out on the other side.
  shm_view   arrow_from_java(copy=False): the same stream, read where Java wrote it.
             Java to Python only.
  socket     An Arrow IPC stream over a Unix domain socket, stock pyarrow and Arrow Java.
  file       An Arrow IPC stream file in --file-dir (tmpfs by default). Python reads it
             through a memory map (in place); Arrow Java's stock reader copies it.

Each is timed twice: "handoff" (the receiver holds a usable table) and "scan" (handoff,
then the receiver reads every data byte), because a path that reads in place has done
no work at handoff.

Usage, from the repository root (the helper needs a JDK 22+ and the built gatun jar):

    javac -cp src/gatun/jars/gatun-server-all.jar -d benchmarks/java/out \
        benchmarks/java/bench/TransportBench.java
    uv run python benchmarks/benchmark_transport.py --classes benchmarks/java/out

gatun's shared memory file and the file baseline both go to /dev/shm where there is one.
On macOS there is none, so both fall back to /tmp, which is on disk.
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import socket
import statistics
import threading
import time
from collections.abc import Callable
from functools import partial
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc

from gatun import connect

ROW_BYTES = 36  # int64 + float64 + 16 bytes of string data + a 4-byte offset


def make_table(rows: int) -> pa.Table:
    ids = pa.array(range(rows), type=pa.int64())
    value = pc.multiply(pc.cast(ids, pa.float64()), 0.5)
    text = pc.utf8_lpad(pc.cast(ids, pa.string()), width=16, padding="0")
    return pa.table({"id": ids, "value": value, "s": text}).combine_chunks()


def scan(table: pa.Table) -> tuple[int, int]:
    """Read every data byte of the table. Returns (rows, id sum)."""
    id_sum = pc.sum(table.column("id")).as_py() or 0
    pc.sum(table.column("value"))
    pc.sum(pc.utf8_length(table.column("s")))
    return table.num_rows, id_sum


def measure(run: Callable[[], float], *, min_reps: int, budget: float) -> dict:
    """run() performs one transfer and returns the seconds it took."""
    for _ in range(3):
        run()
    times: list[float] = []
    deadline = time.perf_counter() + budget
    while len(times) < min_reps or (time.perf_counter() < deadline and len(times) < 40):
        times.append(run())
    return {
        "median_s": statistics.median(times),
        "min_s": min(times),
        "reps": len(times),
    }


class Bench:
    def __init__(self, client, tb, table: pa.Table, file_dir: Path, sock_dir: Path):
        self.client = client
        self.tb = tb
        self.table = table
        self.rows = table.num_rows
        self.id_sum = self.rows * (self.rows - 1) // 2
        self.file_path = str(file_dir / f"gatun_bench_{os.getpid()}.arrows")
        self.sock_path = str(sock_dir / f"gatun_bench_{os.getpid()}.data.sock")
        self.arena = client.get_payload_arena()
        self.schema_cache: dict[int, bool] = {}
        self.jroot = tb.make(self.rows)

    def close(self) -> None:
        self.jroot.close()
        self._reset_arena()
        self.arena.close()
        for path in (self.file_path, self.sock_path):
            try:
                os.unlink(path)
            except FileNotFoundError:
                pass

    def _reset_arena(self) -> None:
        self.arena.reset()
        self.client.reset_payload_arena()

    def _check(self, rows: int, id_sum: int | None) -> None:
        if rows != self.rows or (id_sum is not None and id_sum != self.id_sum):
            raise AssertionError(
                f"transfer corrupted: got rows={rows} id_sum={id_sum}, "
                f"expected rows={self.rows} id_sum={self.id_sum}"
            )

    # ---- Python -> Java ---------------------------------------------------------------

    def p2j_arena(self, do_scan: bool) -> float:
        # Java code cannot reach this table, so there is nothing to scan it with
        start = time.perf_counter()
        self._reset_arena()
        self.client.send_arrow_buffers(self.table, self.arena, self.schema_cache)
        return time.perf_counter() - start

    def p2j_shm_ipc(self, do_scan: bool) -> float:
        start = time.perf_counter()
        root = self.client.arrow_to_java(self.table)
        if do_scan:
            rows, id_sum, _ = self.tb.checksum(root)
            elapsed = time.perf_counter() - start
            self._check(rows, id_sum)
        else:
            elapsed = time.perf_counter() - start
        # The handle's finalizer queues the free; it goes out with the next request
        del root
        return elapsed

    def p2j_socket(self, do_scan: bool) -> float:
        self.tb.startSocketReader(self.sock_path, do_scan)
        start = time.perf_counter()
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as conn:
            conn.connect(self.sock_path)
            with (
                conn.makefile("wb") as raw,
                pa.ipc.new_stream(pa.PythonFile(raw, mode="w"), self.table.schema) as w,
            ):
                w.write_table(self.table)
        rows, id_sum, _ = self.tb.awaitSocketReader()
        elapsed = time.perf_counter() - start
        self._check(rows, id_sum if do_scan else None)
        return elapsed

    def p2j_file(self, do_scan: bool) -> float:
        start = time.perf_counter()
        with (
            pa.OSFile(self.file_path, "wb") as sink,
            pa.ipc.new_stream(sink, self.table.schema) as w,
        ):
            w.write_table(self.table)
        rows, id_sum, _ = self.tb.readFile(self.file_path, do_scan)
        elapsed = time.perf_counter() - start
        self._check(rows, id_sum if do_scan else None)
        return elapsed

    # ---- Java -> Python ---------------------------------------------------------------

    def j2p_arena(self, do_scan: bool) -> float:
        # get_arrow_data() can only return the table this client last sent
        start = time.perf_counter()
        view = self.client.get_arrow_data()
        table = view.table
        if do_scan:
            rows, id_sum = scan(table)
            elapsed = time.perf_counter() - start
            self._check(rows, id_sum)
        else:
            elapsed = time.perf_counter() - start
        return elapsed

    def j2p_shm_ipc(self, do_scan: bool) -> float:
        start = time.perf_counter()
        table = self.client.arrow_from_java(self.jroot)
        if do_scan:
            rows, id_sum = scan(table)
            elapsed = time.perf_counter() - start
            self._check(rows, id_sum)
        else:
            elapsed = time.perf_counter() - start
            self._check(table.num_rows, None)
        return elapsed

    def j2p_shm_view(self, do_scan: bool) -> float:
        start = time.perf_counter()
        table = self.client.arrow_from_java(self.jroot, copy=False).table
        if do_scan:
            rows, id_sum = scan(table)
            elapsed = time.perf_counter() - start
            self._check(rows, id_sum)
        else:
            elapsed = time.perf_counter() - start
            self._check(table.num_rows, None)
        return elapsed

    def j2p_socket(self, do_scan: bool) -> float:
        try:
            os.unlink(self.sock_path)
        except FileNotFoundError:
            pass
        received: list[pa.Table] = []

        def read() -> None:
            conn, _ = server.accept()
            with conn, conn.makefile("rb") as raw:
                received.append(
                    pa.ipc.open_stream(pa.PythonFile(raw, mode="r")).read_all()
                )

        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as server:
            server.bind(self.sock_path)
            server.listen(1)
            reader = threading.Thread(target=read)
            reader.start()
            start = time.perf_counter()
            self.tb.writeSocket(self.jroot, self.sock_path)
            reader.join()
        table = received[0]
        if do_scan:
            rows, id_sum = scan(table)
            elapsed = time.perf_counter() - start
            self._check(rows, id_sum)
        else:
            elapsed = time.perf_counter() - start
            self._check(table.num_rows, None)
        return elapsed

    def j2p_file(self, do_scan: bool) -> float:
        start = time.perf_counter()
        self.tb.writeFile(self.jroot, self.file_path)
        with pa.memory_map(self.file_path, "r") as source:
            table = pa.ipc.open_stream(source).read_all()
            if do_scan:
                rows, id_sum = scan(table)
                elapsed = time.perf_counter() - start
                self._check(rows, id_sum)
            else:
                elapsed = time.perf_counter() - start
                self._check(table.num_rows, None)
            del table
        return elapsed


def run_size(client, tb, mib: int, args) -> list[dict]:
    rows = mib * 1024 * 1024 // ROW_BYTES
    table = make_table(rows)
    bench = Bench(client, tb, table, Path(args.file_dir), Path(args.socket_dir))
    data_bytes = table.nbytes
    results: list[dict] = []

    def record(direction: str, path: str, fn: Callable[[bool], float]) -> None:
        for mode, do_scan in (("handoff", False), ("scan", True)):
            if path == "arena" and direction == "py_to_java" and do_scan:
                continue
            stats = measure(
                partial(fn, do_scan), min_reps=args.min_reps, budget=args.budget
            )
            stats.update(
                direction=direction,
                path=path,
                mode=mode,
                mib=mib,
                data_bytes=data_bytes,
                mb_per_s=data_bytes / stats["median_s"] / 1e6,
            )
            results.append(stats)
            print(
                f"{mib:>5} MiB  {direction:<10} {path:<8} {mode:<8}"
                f"{stats['median_s'] * 1e3:>10.2f} ms {stats['mb_per_s']:>9.0f} MB/s"
                f"  ({stats['reps']} reps)",
                flush=True,
            )

    try:
        record("py_to_java", "arena", bench.p2j_arena)
        # Leave one table loaded so get_arrow_data() has something to return
        bench._reset_arena()
        client.send_arrow_buffers(table, bench.arena, bench.schema_cache)
        record("java_to_py", "arena", bench.j2p_arena)
        bench._reset_arena()

        record("py_to_java", "shm_ipc", bench.p2j_shm_ipc)
        record("py_to_java", "socket", bench.p2j_socket)
        record("py_to_java", "file", bench.p2j_file)

        record("java_to_py", "shm_ipc", bench.j2p_shm_ipc)
        record("java_to_py", "shm_view", bench.j2p_shm_view)
        record("java_to_py", "socket", bench.j2p_socket)
        record("java_to_py", "file", bench.j2p_file)
    finally:
        bench.close()
    return results


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument(
        "--classes", required=True, help="directory with bench/TransportBench.class"
    )
    parser.add_argument(
        "--sizes", default="1,16,128", help="table sizes in MiB, comma separated"
    )
    # The same place for both, so that gatun's shared memory file and the file
    # baseline sit on the same kind of storage
    memory_dir = "/dev/shm" if os.path.isdir("/dev/shm") else "/tmp"
    parser.add_argument(
        "--shm-dir",
        default=memory_dir,
        help="where gatun puts its socket and shared memory file "
        "(tmpfs where there is one)",
    )
    parser.add_argument(
        "--file-dir",
        default=memory_dir,
        help="where the file baseline writes (tmpfs where there is one)",
    )
    parser.add_argument(
        "--socket-dir", default="/tmp", help="where the socket baseline binds"
    )
    parser.add_argument("--min-reps", type=int, default=7)
    parser.add_argument(
        "--budget", type=float, default=3.0, help="seconds of timed runs per case"
    )
    parser.add_argument("--json", help="write results to this file")
    args = parser.parse_args()

    sizes = [int(s) for s in args.sizes.split(",")]
    memory_mib = max(sizes) * 5 + 64
    socket_path = f"{args.shm_dir}/gatun_bench_{os.getpid()}.sock"
    client = connect(
        memory=f"{memory_mib}MB",
        socket_path=socket_path,
        classpath=[args.classes],
        allowed_prefixes=["bench"],
        extra_jvm_flags=["-Dgatun.limits.max_arrow_bytes=17179869184"],
        socket_timeout=300,
    )
    results: list[dict] = []
    try:
        tb = client.jvm.bench.TransportBench
        for mib in sizes:
            results.extend(run_size(client, tb, mib, args))
    finally:
        client.close()

    if args.json:
        meta = {
            "machine": platform.machine(),
            "system": platform.platform(),
            "cpus": os.cpu_count(),
            "python": platform.python_version(),
            "pyarrow": pa.__version__,
            "shm_dir": args.shm_dir,
            "file_dir": args.file_dir,
            "socket_dir": args.socket_dir,
            "gatun_memory_mib": memory_mib,
        }
        Path(args.json).write_text(
            json.dumps({"meta": meta, "results": results}, indent=2)
        )


if __name__ == "__main__":
    main()
