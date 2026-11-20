# Gatun Performance Benchmarks

What Gatun's operations cost, measured, and how single calls compare with Py4J (the bridge PySpark uses).

## What the numbers say

- **A single call is slower than Py4J's.** An instance method call takes about 150 μs through Gatun and about 77 μs through Py4J on the test machine, and a loop of calls runs at about 5,500 a second against 9,500 to 11,000. Creating an object and dropping it costs Gatun two round trips (the second releases the object), which is where its largest deficit is.
- **Gatun is faster when a call carries a collection or a large return value.** A Python list of 100 integers becomes an `ArrayList` in 0.55 ms against 6.2 ms. `is_instance_of` is one round trip, not several (145 μs against 519 μs). Py4J takes about 40 ms to return a string of 8 KB or a byte array of 16 KB; Gatun takes 0.2 ms.
- **Round trips are the cost, so sending several operations together pays.** A hundred method calls take 19 ms one at a time, 5.1 ms in a `batch()` and 4.3 ms as one `invoke_methods()`.
- **Bulk data belongs in Arrow.** 16 MB of columns reaches Java in 1.3 to 3.6 ms depending on the method, and 128 MiB in 8 to 28 ms, where an Arrow stream over a Unix socket takes 106 ms. Getting 500 integers into Java takes 78 ms as 500 calls, 0.35 ms as one Python list and 0.19 ms as one `pyarrow.Array` argument.
- **A callback costs about 390 μs** with the default callback timeout, and 230 μs with `callback_timeout=None`.

## Test environment

- 8 vCPUs of an AMD EPYC 7713 on a shared cloud host (a container; the host's other tenants are a source of noise), 32 GB of memory
- Ubuntu 24.04, Linux 6.8, glibc 2.39
- OpenJDK 22.0.2 (Temurin), Python 3.13.12, pyarrow 26.0.1, Py4J 0.10.9.9
- Gatun built from commit `aeafc4a`, the code of 0.3.0, with 16 MB of shared memory unless a script says otherwise
- November 2026

For scale: on this machine one blocking round trip between two processes over a Unix socket pair, four bytes each way, takes 28 μs. Times on other hardware will differ, and so may the comparison with Py4J: it rests on what a Unix socket round trip and a TCP loopback round trip cost, which is a property of the operating system.

## Method

- **Warm-up by time as well as by count.** The server's JIT compiles a method after thousands of calls. The comparison warms each case for 2 seconds and at least 1,000 calls; the other scripts for 1 second and at least 300.
- **Medians.** On a shared machine a few runs in a hundred take many times longer than the rest, and a mean follows them. The tables give the median; the comparison also records the mean, p95 and p99.
- **Set-up is not timed.** A case that needs a fresh list to sort, or an empty arena, gets it before the clock starts.
- **The comparison treats both bridges alike.** Both JVMs run with `-Xms512m -Xmx512m`. Classes are bound once on both sides (`Math = jvm.java.lang.Math`), with one section that times the full path on every call. Every case, the throughput loops included, runs on a JVM that is warm. A trial runs Gatun and then Py4J, three trials in a row, and each figure is the median of the three trials' medians. Py4J is measured with `auto_convert=True`, as PySpark uses it, and with `auto_convert=False` ("thin" below), where it cannot take Python lists and dicts as arguments.
- **Everything ran twice.** For the scripts other than the comparison, the tables give the mean of the two runs' medians. The two runs agree within 25% for every case but two, which are named where they appear. The comparison's tables are from its second run; the first gave the same picture (an instance call at 154 μs against 77 μs).

## Running the benchmarks

From the repository root, with `JAVA_HOME` pointing at a JDK 22 or later:

```bash
# Gatun against Py4J: about 35 minutes. --quick is a smoke run, --gatun-only skips Py4J
uv run python benchmarks/benchmark_comparison.py --json comparison.json

# Batches and vectorized calls, callbacks, Arrow transfer: a few minutes each
uv run python benchmarks/benchmark_vectorized.py --json vectorized.json
uv run python benchmarks/benchmark_callbacks.py --json callbacks.json
uv run python benchmarks/benchmark_arrow.py --json arrow.json

# Arrow tables through shared memory against a Unix socket and a file.
# Needs its Java helper compiled first
javac -cp src/gatun/jars/gatun-server-all.jar -d benchmarks/java/out \
    benchmarks/java/bench/TransportBench.java
uv run python benchmarks/benchmark_transport.py --classes benchmarks/java/out \
    --json transport.json
```

Every script takes `--quick` for a run that checks it works and whose numbers mean nothing.

## Results

### Single calls: Gatun and Py4J

Median microseconds unless marked. The last column compares Gatun with Py4J as PySpark configures it.

| Case | Gatun | Gatun p99 | Py4J | Py4J p99 | Py4J thin | Gatun is |
|---|---:|---:|---:|---:|---:|---:|
| 1.1 `ArrayList()` | 359 μs | 419 μs | 87.5 μs | 115 μs | 88.5 μs | 4.1x slower |
| 1.2 constructor, int argument | 399 μs | 462 μs | 106 μs | 136 μs | 89.7 μs | 3.8x slower |
| 1.3 constructor, string argument | 393 μs | 516 μs | 185 μs | 215 μs | 170 μs | 2.1x slower |
| 1.4 `HashMap(int)` | 370 μs | 481 μs | 104 μs | 144 μs | 88.6 μs | 3.5x slower |
| 2.1 static call, no arguments | 145 μs | 193 μs | 172 μs | 210 μs | 172 μs | 1.2x faster |
| 2.2 static call, int arguments | 291 μs | 354 μs | 204 μs | 239 μs | 177 μs | 1.4x slower |
| 2.3 static call, double arguments | 210 μs | 271 μs | 206 μs | 250 μs | 181 μs | the same |
| 2.4 static call, string argument | 191 μs | 248 μs | 184 μs | 217 μs | 174 μs | the same |
| 2.5 static call, string to string | 188 μs | 241 μs | 191 μs | 236 μs | 177 μs | the same |
| 3.1 instance call, returns int | 149 μs | 164 μs | 77.1 μs | 80.6 μs | 82.4 μs | 1.9x slower |
| 3.2 instance call, int argument | 200 μs | 257 μs | 96.0 μs | 116 μs | 88.2 μs | 2.1x slower |
| 3.3 instance call, string argument | 203 μs | 246 μs | 95.5 μs | 113 μs | 86.1 μs | 2.1x slower |
| 3.4 instance call, returns an element | 185 μs | 242 μs | 93.3 μs | 117 μs | 86.8 μs | 2.0x slower |
| 3.5 instance call, returns a string | 148 μs | 193 μs | 77.9 μs | 99.6 μs | 84.2 μs | 1.9x slower |
| 3.6 two instance calls | 536 μs | 651 μs | 267 μs | 320 μs | 257 μs | 2.0x slower |
| 4.1 static field, int | 158 μs | 181 μs | 75.4 μs | 83.0 μs | 80.7 μs | 2.1x slower |
| 4.2 static field, object | 323 μs | 381 μs | 163 μs | 177 μs | 163 μs | 2.0x slower |
| 5.1 list of 10 as an argument, then two calls | 528 μs | 624 μs | 809 μs | 1.17 ms | n/a | 1.5x faster |
| 5.2 list of 100 as an argument, then two calls | 554 μs | 700 μs | 6.19 ms | 7.48 ms | n/a | 11x faster |
| 5.3 dict of 3 as an argument, then two calls | 651 μs | 789 μs | 601 μs | 753 μs | n/a | 1.1x slower |
| 5.4 110 bytes in and out, four calls | 968 μs | 1.17 ms | 553 μs | 723 μs | 477 μs | 1.8x slower |
| 5.5 1 KB in and out, four calls | 1.00 ms | 1.30 ms | 625 μs | 731 μs | 545 μs | 1.6x slower |
| 6.1 returns an int | 147 μs | 194 μs | 79.4 μs | 101 μs | 79.1 μs | 1.8x slower |
| 6.2 returns a string | 183 μs | 243 μs | 102 μs | 124 μs | 86.4 μs | 1.8x slower |
| 6.3 returns an object reference | 319 μs | 390 μs | 92.0 μs | 117 μs | 88.6 μs | 3.5x slower |
| 7.1 a call that throws, caught in Python | 245 μs | 297 μs | 301 μs | 496 μs | 274 μs | 1.2x faster |
| 7.2 the same call succeeding | 187 μs | 240 μs | 207 μs | 244 μs | 177 μs | 1.1x faster |
| 8.1 `is_instance_of`, true | 145 μs | 183 μs | 519 μs | 581 μs | 483 μs | 3.6x faster |
| 8.2 `is_instance_of`, false | 138 μs | 173 μs | 521 μs | 567 μs | 468 μs | 3.8x faster |
| 9.1 `jvm.java.lang.Math.max(10, 20)`, full path | 206 μs | 232 μs | 512 μs | 581 μs | 450 μs | 2.5x faster |
| 9.2 `jvm.java.util.ArrayList()`, full path | 300 μs | 371 μs | 402 μs | 498 μs | 368 μs | 1.3x faster |

Rows 5.1 to 5.3 are from a later run, on the build that sends a list as one vector (protocol version 5), and both of their bridges were timed in it. That run was on another host of the same kind, where every round trip was shorter (an instance call took 112 μs through Gatun and 44 μs through Py4J), so compare those rows within themselves and not with their neighbours.

Three things explain most of the table:

- **A Gatun round trip costs about twice a Py4J round trip here** (sections 3, 4 and 6). See [Where a call's time goes](#where-a-calls-time-goes).
- **A handle that is dropped is released with a round trip of its own.** The cases that make an object and drop it (section 1, and 6.3) pay for two. A program that keeps its objects pays less: handles that die between two requests are released together, in one message. Py4J releases in bursts too, which a median hides: its `ArrayList()` case has a median of 88 μs and a mean of 168 μs (Gatun's are 359 and 361).
- **A static call through Py4J is two round trips**, one to look the method up and one to call it, and a class reached through `jvm.` costs several more (section 9). Gatun asks the server what a dotted name is once and remembers.

### Calls in a loop

Calls a second, with classes and methods bound before the loop. The median of three passes after one that is not timed.

| Loop | Gatun | Py4J | Py4J thin | Gatun is |
|---|---:|---:|---:|---:|
| 10,000 static calls (`Math.abs(i)`) | 5,468 | 9,192 | 11,526 | 1.7x slower |
| 10,000 objects created | 3,305 | 5,651 | 6,224 | 1.7x slower |
| 10,000 instance calls (`list.add(i)`) | 5,516 | 9,462 | 12,386 | 1.7x slower |
| 1,000 times: a `HashMap`, two puts, a get, a size | 4,658 | 7,932 | 9,703 | 1.7x slower |

The first run had Gatun at 5,200 to 5,400 calls a second and Py4J at 10,400 to 11,000 for the call loops, a factor of 2.

### Payload size

One call each, by the size of what it carries.

| Case | Size | Gatun | Py4J | Gatun is |
|---|---:|---:|---:|---:|
| String as a constructor argument | 0 B | 410 μs | 198 μs | 2.1x slower |
| | 128 B | 368 μs | 200 μs | 1.8x slower |
| | 2 KB | 383 μs | 225 μs | 1.7x slower |
| | 8 KB | 390 μs | 258 μs | 1.5x slower |
| String returned | 0 B | 172 μs | 81.3 μs | 2.1x slower |
| | 128 B | 159 μs | 92.5 μs | 1.7x slower |
| | 2 KB | 157 μs | 96.1 μs | 1.6x slower |
| | 8 KB | 169 μs | 41.0 ms | 242x faster |
| `byte[]` as an argument | 0 B | 221 μs | 201 μs | 1.1x slower |
| | 1 KB | 223 μs | 214 μs | the same |
| | 4 KB | 235 μs | 236 μs | the same |
| | 16 KB | 266 μs | 345 μs | 1.3x faster |
| | 32 KB | 293 μs | 499 μs | 1.7x faster |
| `byte[]` returned | 0 B | 205 μs | 103 μs | 2.0x slower |
| | 1 KB | 190 μs | 182 μs | the same |
| | 4 KB | 192 μs | 339 μs | 1.8x faster |
| | 16 KB | 193 μs | 42.0 ms | 218x faster |
| | 32 KB | 204 μs | 44.0 ms | 216x faster |

Gatun's time does not depend on the size in this range: the value travels through shared memory. Py4J's grows with it, and a value of 8 KB (a string) or 16 KB (a byte array) returned from Java takes about 40 ms. That figure is the same in both runs and does not grow with the size, which is what a delayed acknowledgement on the TCP connection looks like; this benchmark does not establish the cause.

The string argument cases create a `StringBuilder` and drop it, so Gatun's times include the release.

### Batches and vectorized calls

The same work sent one operation at a time, in a `batch()`, and as one vectorized call. In brackets: against one at a time.

| Work | One at a time | `batch()` | Vectorized call |
|---|---:|---:|---:|
| 3 method calls | 708 μs | 373 μs (1.9x) | 391 μs (1.8x) |
| 10 method calls | 2.05 ms | 809 μs (2.5x) | 677 μs (3.0x) |
| 100 method calls | 19.2 ms | 5.14 ms (3.7x) | 4.28 ms (4.5x) |
| 3 objects created | 650 μs | 479 μs (1.4x) | 461 μs (1.4x) |
| 10 objects created | 1.77 ms | 810 μs (2.2x) | 744 μs (2.4x) |
| 100 objects created | 16.4 ms | 4.39 ms (3.7x) | 3.85 ms (4.3x) |

The vectorized calls are `invoke_methods()` and `create_objects()`. From ten operations up they are a tenth to a fifth faster than a batch of the same size; for three there is nothing between them. The two runs disagreed by a third on "3 method calls, `invoke_methods()`" (447 and 335 μs).

| Field reads on one object | Median |
|---|---:|
| 2 fields, one `get_field()` each | 308 μs |
| 2 fields, `get_fields()` | 226 μs |
| 1 field, `get_field()` | 167 μs |
| 1 field, `get_fields()` | 203 μs |

### Callbacks

Callbacks made by the thread that is running the request, which travel on the request's channel. The cost of one callback is what a Python comparator adds to the same sort with Java's own ordering, divided by the number of comparisons.

| Case | Median | Callbacks | Each |
|---|---:|---:|---:|
| Sort 10, Java ordering | 231 μs | | |
| Sort 10, Python comparator | 8.85 ms | 22 | 392 μs |
| Sort 100, Java ordering | 226 μs | | |
| Sort 100, Python comparator | 208 ms | 535 | 387 μs |
| Sort 1,000, Java ordering | 315 μs | | |
| Sort 1,000, Python comparator | 3.37 s | 8,670 | 389 μs |
| Sort 100, Python comparator, `callback_timeout=None` | 122 ms | 535 | 228 μs |
| `HashMap.put` (no callback) | 225 μs | | |
| `HashMap.merge` with a Python `BiFunction` | 658 μs | 1 | 433 μs more than `put` |

With a callback timeout, 30 seconds by default, the client runs each callback on a thread of its own so that it can stop waiting for it. `connect(callback_timeout=None)` runs it on the thread that is reading, which takes the cost of a callback from 390 μs to 230 μs.

Callbacks from another Java thread travel on the callback connection:

| Case | Median |
|---|---:|
| `CompletableFuture.completedFuture(7).join()` (no callback) | 518 μs |
| `CompletableFuture.supplyAsync(python).join()` | 1.27 ms |
| The same, with the Python function making one Java call | 1.65 ms |

The first two differ by 750 μs, which is the callback and the hand-over to the pool's thread together. A Java call from inside the callback adds 380 μs.

### Arrow: the handoff, by size

The time for the call to return with the table on the other side. Two columns, an `int64` and a `float64`, 16 bytes a row, 256 MB of shared memory.

| Method | 1K rows (16 KB) | 10K (160 KB) | 100K (1.6 MB) | 1M (16 MB) | Rate at 16 MB |
|---|---:|---:|---:|---:|---:|
| `send_arrow_table` | 318 μs | 249 μs | 339 μs | 2.26 ms | 7.1 GB/s |
| `send_arrow_buffers` | 466 μs | 450 μs | 499 μs | 1.34 ms | 12.0 GB/s |
| `arrow_to_java` | 466 μs | 457 μs | 572 μs | 2.61 ms | 6.1 GB/s |
| `arrow_to_java(copy=False)` | 498 μs | 473 μs | 512 μs | 1.29 ms | 12.4 GB/s |
| `arrow_to_java_batches` | 467 μs | 442 μs | 1.07 ms | 3.59 ms | 4.5 GB/s |
| `arrow_from_java` | 212 μs | 216 μs | 323 μs | 2.18 ms | 7.4 GB/s |
| `arrow_from_java(copy=False)` | 204 μs | 205 μs | 260 μs | 1.06 ms | 15.0 GB/s |
| `get_arrow_data` | 278 μs | 266 μs | 326 μs | 1.15 ms | 13.9 GB/s |

Below a megabyte the time is mostly round trips, not data: each `arrow_to_java` form makes two (the second releases the table's handle or ends the loan), `arrow_from_java` one. From 16 MB the copy shows: the methods that read in place (`send_arrow_buffers`, both `copy=False` forms, `get_arrow_data`) take half the time of those that copy.

The two runs disagreed on `arrow_to_java_batches` at 100,000 rows: 0.66 ms and 1.49 ms. The table has their mean.

### Arrow: shared memory against a socket and a file

One table of an `int64`, a `float64` and a 16-character string column moved every way Gatun has, and as an Arrow IPC stream over a Unix socket and through a file in `/dev/shm` with stock pyarrow and Arrow Java. "Handoff" ends when the receiver holds a usable table; "read" also has the receiver read every byte, since a path that reads in place has done no work at handoff.

Python to Java:

| Path | 1 MiB | 16 MiB | 128 MiB | 128 MiB, read |
|---|---:|---:|---:|---:|
| `send_arrow_buffers` | 1.63 ms | 2.36 ms | 7.99 ms | |
| `arrow_to_java` | 1.33 ms | 3.34 ms | 24.5 ms | 55.3 ms |
| `arrow_to_java(copy=False)` | 1.25 ms | 2.06 ms | 11.7 ms | 43.7 ms |
| `arrow_to_java_batches` | 1.11 ms | 4.51 ms | 28.0 ms | 67.6 ms |
| Unix socket | 0.81 ms | 4.03 ms | 106 ms | 137 ms |
| File in `/dev/shm` | 1.61 ms | 15.0 ms | 193 ms | 222 ms |

Java to Python:

| Path | 1 MiB | 16 MiB | 128 MiB | 128 MiB, read |
|---|---:|---:|---:|---:|
| `get_arrow_data` | 0.99 ms | 1.80 ms | 13.2 ms | 32.6 ms |
| `arrow_from_java` | 0.48 ms | 2.61 ms | 19.9 ms | 38.8 ms |
| `arrow_from_java(copy=False)` | 0.40 ms | 1.37 ms | 12.7 ms | 32.3 ms |
| Unix socket | 0.94 ms | 3.87 ms | 96.2 ms | 117 ms |
| File in `/dev/shm` | 1.52 ms | 12.1 ms | 95.8 ms | 127 ms |

Java code cannot read a table sent with `send_arrow_buffers`, which is why that row has no read time. At 1 MiB a socket is as fast as anything; at 128 MiB shared memory is four to thirteen times faster at the handoff and two to four times faster with the read. This script ran once.

### Values in bulk

Getting 500 integers into Java:

| How | Median | Against 500 calls |
|---|---:|---:|
| 500 `add()` calls | 77.7 ms | |
| 500 `add()` calls in a `batch()` | 22.4 ms | 3.5x faster |
| A Python list as a constructor argument | 353 μs | 220x faster |
| An `int32` `pyarrow.Array` as an argument (arrives as `int[]`) | 193 μs | 404x faster |
| A table with `arrow_to_java()` | 357 μs | 217x faster |

Collections and arrays as arguments, by size:

| Argument | 100 elements | 500 elements |
|---|---:|---:|
| `list` of ints to `ArrayList` | 316 μs | 318 μs |
| `list` of strings to `ArrayList` | 343 μs | 385 μs |
| `list` of ints and strings to `ArrayList` | 1.74 ms | 6.97 ms |
| `dict` of string keys and int values to `HashMap` | 365 μs | 434 μs |
| `pyarrow` `int32` array to `int[]` | 180 μs | 173 μs |
| `pyarrow` `float64` array to `double[]` | 175 μs | 193 μs |

Collections as return values:

| Returned | 100 elements | 500 elements |
|---|---:|---:|
| `ArrayList` of ints to `list` | 120 μs | 153 μs |
| `ArrayList` of strings to `list` | 160 μs | 214 μs |
| `HashMap` of string keys and int values to `dict` | 216 μs | 339 μs |

A list whose elements are all ints, all floats, all bools or all strings travels as one vector, and a dict as two such lists, so the length hardly shows: the 300 μs and more of the argument rows is mostly the two round trips of creating the collection and releasing it. A list of mixed types is encoded one element at a time, at about 14 μs each. Until protocol version 5 every list and dict was: measured on the same machine in alternation with the figures above, 500 ints took 5.97 ms as an argument and 5.58 ms as a return value, and a dict of 500 took 14.6 ms and 12.9 ms. A command holds 64 KB, which is about 8,000 ints or a dict of 3,000 short entries; beyond that, use Arrow.

These three tables are from a later build than the rest of this document (the commit that introduced protocol version 5), in two runs of their own.

## Where a call's time goes

For `list.size()`, 20,000 calls on a warm server:

| | |
|---|---:|
| The call, as timed without a profiler | 162 μs |
| A bare round trip between two processes on this machine | 28 μs |
| The server's own time for the request (`get_metrics()`, median) | about 10 μs |
| Under cProfile: waiting in `recv` for the response | 62 μs |
| Under cProfile: `sendall` | 12 μs |

The rest, about half of the call, is Python: building the command and reading the response with the FlatBuffers library, which is pure Python. cProfile slows that part down and not the two system calls, so the split is approximate. The wait for the response is 62 μs where a bare round trip and the server's own time add up to about 38; what the remainder is made of was not measured.

## Benchmark files

| File | What it measures |
|---|---|
| `benchmark_comparison.py` | Gatun against Py4J: single calls, loops, payload sizes |
| `benchmark_vectorized.py` | One operation at a time against `batch()` and the vectorized calls |
| `benchmark_callbacks.py` | Python callbacks, on the request's thread and from other Java threads |
| `benchmark_arrow.py` | Every Arrow transfer by size, and bulk values the other ways |
| `benchmark_transport.py` | Arrow through shared memory against a Unix socket and a file (Java half in `benchmarks/java/`) |
| `_harness.py` | The timing the three middle scripts share |
| `profile_hotspots.py` | A cProfile run of the client's common operations, for finding where time goes |
