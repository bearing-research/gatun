# Gatun Performance Benchmarks

What Gatun's operations cost, measured, and how single calls compare with Py4J (the bridge PySpark uses).

## What the numbers say

- **A call with plain arguments takes 31 to 39 μs. For a plain instance call and for creating an object, that is about what Py4J takes.** An instance method call took 33 μs through Gatun and 42 μs through Py4J on the test machine, and a loop of calls ran at about 27,000 a second against 18,000 to 23,000; on a second machine Py4J was level or ahead on the same cases (see [A second machine](#a-second-machine)). A bare round trip between two processes takes 21 μs on the test machine.
- **Gatun is ahead where Py4J makes more round trips.** A static call takes 39 μs against 113 μs (Py4J makes two round trips for it), and `is_instance_of` 31 μs against 275 μs. A Python list of 100 integers as a constructor argument, with two calls on the result, takes 0.28 ms against 5.6 ms.
- **Stock Py4J takes about 40 ms to return a value of 8 KB or more, and the cause is Nagle's algorithm.** Its server does not set `TCP_NODELAY`. With the option set, Py4J returns such a value in 0.2 to 0.3 ms, about 3 times Gatun's time (see [Payload size](#payload-size)).
- **An operation in a `batch()` costs about 5 μs.** A hundred method calls take 4.2 ms one at a time, 0.56 ms in a batch and 2.7 ms as one `invoke_methods()`.
- **Bulk data belongs in Arrow.** 16 MB of columns reaches Java in 1.0 to 3.2 ms depending on the method, and 128 MiB in 8 to 27 ms, where an Arrow stream over a Unix socket takes 102 ms. Getting 500 integers into Java takes 21 ms as 500 calls, 2.4 ms as a batch of them, 0.22 ms as one Python list and 0.17 ms as one `pyarrow.Array` argument.
- **A callback costs about 330 μs** with the default callback timeout, and 160 μs with `callback_timeout=None`.

## Test environment

- 8 vCPUs of an AMD EPYC 7713 on a shared cloud host (a container; the host's other tenants are a source of noise), 32 GB of memory
- Ubuntu 24.04, Linux 6.8, glibc 2.39
- OpenJDK 22.0.2 (Temurin), Python 3.13.12, pyarrow 26.0.1, Py4J 0.10.9.9
- Gatun at commit `c42ed2c`, with 16 MB of shared memory unless a script says otherwise. The comparison's first run and the transport script's 1 MiB and 16 MiB columns are from the commit before it, `e49df2a`, which differs only in how a `batch()` is encoded; neither script uses one
- November 2026

For scale: on this machine one blocking round trip between two processes over a Unix socket pair, four bytes each way, takes 21 μs. Times on other hardware will differ, and so may the comparison with Py4J: it rests on what a Unix socket round trip and a TCP loopback round trip cost, which is a property of the operating system.

## Method

- **Warm-up by time as well as by count.** The server's JIT compiles a method after thousands of calls. The comparison warms each case for 2 seconds and at least 1,000 calls; the other scripts for 1 second and at least 300.
- **Medians.** On a shared machine a few runs in a hundred take many times longer than the rest, and a mean follows them. The tables give the median; the comparison also records the mean, p95 and p99.
- **Set-up is not timed.** A case that needs a fresh list to sort, or an empty arena, gets it before the clock starts.
- **The comparison treats both bridges alike.** Both JVMs run with `-Xms512m -Xmx512m`. Classes are bound once on both sides (`Math = jvm.java.lang.Math`), with one section that times the full path on every call. Every case, the throughput loops included, runs on a JVM that is warm. A trial runs Gatun and then Py4J, three trials in a row, and each figure is the median of the three trials' medians. Py4J is measured with `auto_convert=True`, as PySpark uses it, and with `auto_convert=False` ("thin" below), where it cannot take Python lists and dicts as arguments.
- **Everything ran twice**, but for the transport script, whose section says what its table holds. For the scripts other than the comparison, the tables give the mean of the two runs' medians. A call is short enough now for the host's noise to show: the two runs differ by a third to a half on the cases that make calls one at a time, and by less than a quarter on most others; the cases that differ by more are named where they appear. The comparison's tables are from its second run. About three quarters of its rows are within a tenth of the first run's, for both bridges, and a few differ by as much as 40% (Gatun's 1.4 was 61 μs in the first run and its 3.3 was 48 μs; Py4J's 2.3 was 143 μs and its 7.1 was 219 μs). A difference of a third between the two bridges in one row is therefore within what the machine does on its own; the pattern over a section is what to read.

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

`benchmark_comparison.py --py4j-nodelay` runs Py4J's server with `TCP_NODELAY` set on the connections it accepts, which Py4J does not do itself (see [Payload size](#payload-size)). It compiles `benchmarks/java/bench/NoDelayGateway.java` against the installed Py4J jar, so it needs `javac`.

## Results

### Single calls: Gatun and Py4J

Median microseconds unless marked. The last column compares Gatun with Py4J as PySpark configures it.

| Case | Gatun | Gatun p99 | Py4J | Py4J p99 | Py4J thin | Gatun is |
|---|---:|---:|---:|---:|---:|---:|
| 1.1 `ArrayList()` | 46.3 μs | 59.4 μs | 47.1 μs | 61.6 μs | 44.9 μs | the same |
| 1.2 constructor, int argument | 51.2 μs | 66.6 μs | 61.8 μs | 82.0 μs | 47.1 μs | 1.2x faster |
| 1.3 constructor, string argument | 63.9 μs | 80.1 μs | 98.9 μs | 127 μs | 88.5 μs | 1.5x faster |
| 1.4 `HashMap(int)` | 44.8 μs | 61.5 μs | 60.5 μs | 83.8 μs | 47.5 μs | 1.4x faster |
| 2.1 static call, no arguments | 37.6 μs | 50.0 μs | 97.4 μs | 122 μs | 95.0 μs | 2.6x faster |
| 2.2 static call, int arguments | 39.0 μs | 51.5 μs | 113 μs | 131 μs | 92.8 μs | 2.9x faster |
| 2.3 static call, double arguments | 37.4 μs | 51.5 μs | 109 μs | 130 μs | 94.7 μs | 2.9x faster |
| 2.4 static call, string argument | 39.4 μs | 52.7 μs | 103 μs | 161 μs | 90.7 μs | 2.6x faster |
| 2.5 static call, string to string | 37.5 μs | 49.3 μs | 136 μs | 162 μs | 104 μs | 3.6x faster |
| 3.1 instance call, returns int | 33.0 μs | 50.2 μs | 41.9 μs | 45.0 μs | 41.4 μs | 1.3x faster |
| 3.2 instance call, int argument | 36.0 μs | 45.2 μs | 67.0 μs | 81.3 μs | 42.5 μs | 1.9x faster |
| 3.3 instance call, string argument | 34.0 μs | 47.4 μs | 54.4 μs | 76.2 μs | 41.8 μs | 1.6x faster |
| 3.4 instance call, returns an element | 34.4 μs | 45.8 μs | 55.6 μs | 73.4 μs | 45.0 μs | 1.6x faster |
| 3.5 instance call, returns a string | 32.8 μs | 45.0 μs | 53.7 μs | 64.8 μs | 53.2 μs | 1.6x faster |
| 3.6 two instance calls | 104 μs | 164 μs | 146 μs | 192 μs | 163 μs | 1.4x faster |
| 4.1 static field, int | 36.1 μs | 53.9 μs | 40.9 μs | 51.9 μs | 40.5 μs | 1.1x faster |
| 4.2 static field, object | 44.7 μs | 76.7 μs | 85.8 μs | 109 μs | 84.2 μs | 1.9x faster |
| 5.1 list of 10 as an argument, then two calls | 309 μs | 360 μs | 817 μs | 1.19 ms | n/a | 2.6x faster |
| 5.2 list of 100 as an argument, then two calls | 282 μs | 318 μs | 5.64 ms | 7.48 ms | n/a | 20x faster |
| 5.3 dict of 3 as an argument, then two calls | 333 μs | 416 μs | 479 μs | 698 μs | n/a | 1.4x faster |
| 5.4 110 bytes in and out, four calls | 344 μs | 522 μs | 373 μs | 436 μs | 322 μs | 1.1x faster |
| 5.5 1 KB in and out, four calls | 362 μs | 437 μs | 355 μs | 472 μs | 320 μs | the same |
| 6.1 returns an int | 32.8 μs | 42.6 μs | 40.7 μs | 52.6 μs | 41.5 μs | 1.2x faster |
| 6.2 returns a string | 36.0 μs | 48.1 μs | 56.1 μs | 85.1 μs | 44.0 μs | 1.6x faster |
| 6.3 returns an object reference | 52.4 μs | 79.0 μs | 46.8 μs | 72.0 μs | 50.2 μs | 1.1x slower |
| 7.1 a call that throws, caught in Python | 99.3 μs | 119 μs | 174 μs | 215 μs | 146 μs | 1.8x faster |
| 7.2 the same call succeeding | 39.4 μs | 50.3 μs | 106 μs | 124 μs | 118 μs | 2.7x faster |
| 8.1 `is_instance_of`, true | 31.3 μs | 48.8 μs | 275 μs | 355 μs | 269 μs | 8.8x faster |
| 8.2 `is_instance_of`, false | 31.3 μs | 42.0 μs | 276 μs | 362 μs | 263 μs | 8.8x faster |
| 9.1 `jvm.java.lang.Math.max(10, 20)`, full path | 42.0 μs | 54.0 μs | 277 μs | 308 μs | 255 μs | 6.6x faster |
| 9.2 `jvm.java.util.ArrayList()`, full path | 48.0 μs | 61.5 μs | 198 μs | 292 μs | 207 μs | 4.1x faster |

Three things explain most of the table:

- **A call with plain arguments is one round trip on both bridges** (3.1 to 3.5, 4.1, 6.1 and 6.2): 33 to 36 μs against 41 to 67 μs here. The round trip itself is 21 μs; the rest is the client's Python and the server's work. See [Where a call's time goes](#where-a-calls-time-goes). These rows are the ones a different machine can turn around: see [A second machine](#a-second-machine).
- **A static call through Py4J is two round trips**, one to look the method up and one to call it (section 2), and a class reached through `jvm.` costs several more (section 9). Gatun asks the server what a dotted name is once and remembers.
- **A collection as an argument is one message for Gatun and a round trip for each element for Py4J** (5.1 to 5.3). Those three cases, and the two with `bytes`, are the ones whose commands Gatun still writes with the FlatBuffers library; each also makes two or three calls on the result.

The cases that make an object and drop it (section 1, and 6.3) are level at the median. Their means are not: Py4J releases dropped objects in bursts, so its `ArrayList()` case has a median of 47 μs and a mean of 92 μs, where Gatun's are 46 and 47. Gatun sends a release with the next call.

### A second machine

The comparison of plain calls rests on what a Unix socket round trip and a TCP loopback round trip cost, which the operating system and the machine decide. An independent run on a machine with 2 vCPUs had Py4J level with Gatun, or up to 1.4 times ahead, on `list.size()` (3.1) and on `ArrayList()` created and dropped (1.1). The same call on that machine took up to three times as long in one state of the scheduler as in another, which is more than the difference between the bridges.

So the table above does not show that a plain instance call or an object creation is faster through Gatun: they cost about the same. What the second machine reproduced is the rest: static calls (section 2), `is_instance_of` (section 8), the full `jvm.` path (section 9), collections as arguments (5.1 to 5.3) and bulk data.

### Calls in a loop

Calls a second, with classes and methods bound before the loop. The median of three passes after one that is not timed.

| Loop | Gatun | Py4J | Py4J thin | Gatun is |
|---|---:|---:|---:|---:|
| 10,000 static calls (`Math.abs(i)`) | 26,796 | 18,394 | 22,710 | 1.5x faster |
| 10,000 objects created | 24,182 | 11,513 | 11,808 | 2.1x faster |
| 10,000 instance calls (`list.add(i)`) | 28,525 | 18,841 | 23,425 | 1.5x faster |
| 1,000 times: a `HashMap`, two puts, a get, a size | 25,409 | 15,211 | 18,044 | 1.7x faster |

28,500 calls a second is 35 μs a call. The first run agrees within a tenth, but for the loop that creates objects (18,100 a second for Gatun there) and Py4J's last row (12,700).

### Payload size

One call each, by the size of what it carries.

| Case | Size | Gatun | Py4J | Gatun is |
|---|---:|---:|---:|---:|
| String as a constructor argument | 0 B | 74.2 μs | 103 μs | 1.4x faster |
| | 128 B | 66.4 μs | 102 μs | 1.5x faster |
| | 2 KB | 45.6 μs | 115 μs | 2.5x faster |
| | 8 KB | 53.9 μs | 151 μs | 2.8x faster |
| String returned | 0 B | 33.5 μs | 41.1 μs | 1.2x faster |
| | 128 B | 31.4 μs | 43.6 μs | 1.4x faster |
| | 2 KB | 49.7 μs | 49.5 μs | the same |
| | 8 KB | 64.7 μs | 41.0 ms | 634x faster |
| `byte[]` as an argument | 0 B | 107 μs | 110 μs | the same |
| | 1 KB | 110 μs | 119 μs | 1.1x faster |
| | 4 KB | 121 μs | 138 μs | 1.1x faster |
| | 16 KB | 164 μs | 264 μs | 1.6x faster |
| | 32 KB | 185 μs | 384 μs | 2.1x faster |
| `byte[]` returned | 0 B | 60.2 μs | 69.8 μs | 1.2x faster |
| | 1 KB | 62.5 μs | 114 μs | 1.8x faster |
| | 4 KB | 63.6 μs | 280 μs | 4.4x faster |
| | 16 KB | 65.7 μs | 42.0 ms | 640x faster |
| | 32 KB | 70.3 μs | 43.0 ms | 612x faster |

Gatun's time grows slowly with the size, since the value travels through shared memory. Py4J's grows faster, and a value of 8 KB (a string) or 16 KB (a byte array) returned from Java takes about 40 ms, the same in both runs and whatever the size.

That 40 ms is Nagle's algorithm meeting a delayed acknowledgement, and not the cost of Py4J's protocol. Py4J's server never sets `TCP_NODELAY` on the sockets it accepts (Py4J issue 516; pull request 517, which sets it, is open). A large return value leaves the server as more than one write, and the last of them waits until the client acknowledges the ones before, which the client's kernel puts off for about 40 ms. With the option set on the accepted socket, stock Py4J 0.10.9.9 returned strings of 8 to 32 KB in 0.2 to 0.3 ms, measured on the second machine described below. Against that, Gatun is about 3 times faster on these rows, not 600 times. Both figures are true of something: a program on Py4J as released, PySpark included, waits the 40 ms; the protocol itself does not need it. `benchmark_comparison.py --py4j-nodelay` runs the comparison with the option set.

The string argument cases create a `StringBuilder` and drop it. A `byte[]` costs Gatun more than a string of the same size in both directions: `bytes` is not among the values packed by hand, so those commands and responses go through the FlatBuffers library.

The rows under 100 μs move by tens of microseconds between runs. The first run had a string argument of 0 B and of 128 B at 45 μs, and a returned string of 2 KB and of 8 KB at 37 and 46 μs.

### Batches and vectorized calls

The same work sent one operation at a time, in a `batch()`, and as one vectorized call. In brackets: against one at a time.

| Work | One at a time | `batch()` | Vectorized call |
|---|---:|---:|---:|
| 3 method calls | 136 μs | 99.7 μs (1.4x) | 235 μs (0.6x) |
| 10 method calls | 502 μs | 132 μs (3.8x) | 423 μs (1.2x) |
| 100 method calls | 4.20 ms | 560 μs (7.5x) | 2.68 ms (1.6x) |
| 3 objects created | 159 μs | 96.5 μs (1.6x) | 270 μs (0.6x) |
| 10 objects created | 434 μs | 128 μs (3.4x) | 392 μs (1.1x) |
| 100 objects created | 4.22 ms | 573 μs (7.4x) | 2.31 ms (1.8x) |

A batch costs about 85 μs and 5 μs for each operation in it, so it is ahead of single calls from three operations up. The vectorized calls, `invoke_methods()` and `create_objects()`, are behind a batch at every size, and behind single calls for three operations: they still write their commands and read their results with the FlatBuffers library, at about 25 μs an operation, where single calls and batches are packed by hand (see [Where a call's time goes](#where-a-calls-time-goes)). What they keep is their form: one call that returns a list.

The "one at a time" figures are the ones the two runs disagree on: 100 method calls took 4.84 ms in one and 3.56 ms in the other, 100 objects 4.99 ms and 3.46 ms. The batch and vectorized figures agree within a fifth.

| Field reads on one object | Median |
|---|---:|
| 2 fields, one `get_field()` each | 86.5 μs |
| 2 fields, `get_fields()` | 131 μs |
| 1 field, `get_field()` | 47.0 μs |
| 1 field, `get_fields()` | 124 μs |

### Callbacks

Callbacks made by the thread that is running the request, which travel on the request's channel. The cost of one callback is what a Python comparator adds to the same sort with Java's own ordering, divided by the number of comparisons.

| Case | Median | Callbacks | Each |
|---|---:|---:|---:|
| Sort 10, Java ordering | 87.1 μs | | |
| Sort 10, Python comparator | 8.00 ms | 22 | 360 μs |
| Sort 100, Java ordering | 75.8 μs | | |
| Sort 100, Python comparator | 175 ms | 535 | 326 μs |
| Sort 1,000, Java ordering | 118 μs | | |
| Sort 1,000, Python comparator | 2.84 s | 8,670 | 327 μs |
| Sort 100, Python comparator, `callback_timeout=None` | 87.2 ms | 535 | 163 μs |
| `HashMap.put` (no callback) | 71.8 μs | | |
| `HashMap.merge` with a Python `BiFunction` | 405 μs | 1 | 333 μs more than `put` |

With a callback timeout, 30 seconds by default, the client runs each callback on a thread of its own so that it can stop waiting for it. `connect(callback_timeout=None)` runs it on the thread that is reading, which takes the cost of a callback from 330 μs to 160 μs. A callback's request and its answer still go through the FlatBuffers library on the Python side.

Callbacks from another Java thread travel on the callback connection:

| Case | Median |
|---|---:|
| `CompletableFuture.completedFuture(7).join()` (no callback) | 141 μs |
| `CompletableFuture.supplyAsync(python).join()` | 710 μs |
| The same, with the Python function making one Java call | 810 μs |

The first two differ by 570 μs, which is the callback and the hand-over to the pool's thread together. A Java call from inside the callback adds 100 μs.

### Arrow: the handoff, by size

The time for the call to return with the table on the other side. Two columns, an `int64` and a `float64`, 16 bytes a row, 256 MB of shared memory.

| Method | 1K rows (16 KB) | 10K (160 KB) | 100K (1.6 MB) | 1M (16 MB) | Rate at 16 MB |
|---|---:|---:|---:|---:|---:|
| `send_arrow_table` | 177 μs | 174 μs | 232 μs | 1.91 ms | 8.4 GB/s |
| `send_arrow_buffers` | 367 μs | 333 μs | 379 μs | 1.13 ms | 14.2 GB/s |
| `arrow_to_java` | 261 μs | 248 μs | 384 μs | 2.06 ms | 7.8 GB/s |
| `arrow_to_java(copy=False)` | 268 μs | 242 μs | 346 μs | 957 μs | 16.7 GB/s |
| `arrow_to_java_batches` | 232 μs | 268 μs | 454 μs | 3.20 ms | 5.0 GB/s |
| `arrow_from_java` | 167 μs | 160 μs | 240 μs | 1.93 ms | 8.3 GB/s |
| `arrow_from_java(copy=False)` | 193 μs | 138 μs | 175 μs | 881 μs | 18.2 GB/s |
| `get_arrow_data` | 257 μs | 198 μs | 231 μs | 1.08 ms | 14.9 GB/s |

Below a megabyte the time is mostly round trips and encoding, not data: each `arrow_to_java` form makes two round trips (the second releases the table's handle or ends the loan), `arrow_from_java` one. From 16 MB the copy shows: the methods that read in place (`send_arrow_buffers`, both `copy=False` forms, `get_arrow_data`) take half the time of those that copy.

Eight of these cells differ by more than a quarter between the two runs, by 43% at most (`arrow_to_java` at 100,000 rows, 316 and 453 μs; `send_arrow_buffers` at a million, 0.93 and 1.33 ms).

### Arrow: shared memory against a socket and a file

One table of an `int64`, a `float64` and a 16-character string column moved every way Gatun has, and as an Arrow IPC stream over a Unix socket and through a file in `/dev/shm` with stock pyarrow and Arrow Java. "Handoff" ends when the receiver holds a usable table; "read" also has the receiver read every byte, since a path that reads in place has done no work at handoff.

Python to Java:

| Path | 1 MiB | 16 MiB | 128 MiB | 128 MiB, read |
|---|---:|---:|---:|---:|
| `send_arrow_buffers` | 1.43 ms | 1.89 ms | 7.83 ms | |
| `arrow_to_java` | 1.08 ms | 2.78 ms | 23.6 ms | 44.3 ms |
| `arrow_to_java(copy=False)` | 0.82 ms | 1.87 ms | 10.8 ms | 39.0 ms |
| `arrow_to_java_batches` | 0.73 ms | 4.05 ms | 26.9 ms | 59.4 ms |
| Unix socket | 0.99 ms | 4.75 ms | 102 ms | 125 ms |
| File in `/dev/shm` | 1.31 ms | 15.2 ms | 189 ms | 213 ms |

Java to Python:

| Path | 1 MiB | 16 MiB | 128 MiB | 128 MiB, read |
|---|---:|---:|---:|---:|
| `get_arrow_data` | 0.80 ms | 1.76 ms | 12.8 ms | 31.4 ms |
| `arrow_from_java` | 0.45 ms | 2.13 ms | 18.3 ms | 37.0 ms |
| `arrow_from_java(copy=False)` | 0.29 ms | 1.16 ms | 11.7 ms | 30.8 ms |
| Unix socket | 0.69 ms | 3.52 ms | 91.6 ms | 111 ms |
| File in `/dev/shm` | 1.15 ms | 11.6 ms | 95.3 ms | 123 ms |

Java code cannot read a table sent with `send_arrow_buffers`, which is why that row has no read time. At 1 MiB a socket is in the range of the shared memory paths; at 128 MiB shared memory is four to thirteen times faster at the handoff and two to four times faster with the read.

The 1 MiB and 16 MiB columns are from one run. The 128 MiB columns are the median of three runs: one run in three had a path take half as long again as in the others (`arrow_from_java` took 17.9, 18.3 and 26.2 ms), on this build and on the one before it alike.

### Values in bulk

Getting 500 integers into Java:

| How | Median | Against 500 calls |
|---|---:|---:|
| 500 `add()` calls | 21.2 ms | |
| 500 `add()` calls in a `batch()` | 2.36 ms | 9.0x faster |
| A Python list as a constructor argument | 219 μs | 97x faster |
| An `int32` `pyarrow.Array` as an argument (arrives as `int[]`) | 170 μs | 125x faster |
| A table with `arrow_to_java()` | 243 μs | 87x faster |

The two runs gave 18.3 and 24.1 ms for the 500 calls, and 124 and 216 μs for the `pyarrow.Array`.

Collections and arrays as arguments, by size:

| Argument | 100 elements | 500 elements |
|---|---:|---:|
| `list` of ints to `ArrayList` | 202 μs | 181 μs |
| `list` of strings to `ArrayList` | 185 μs | 239 μs |
| `list` of ints and strings to `ArrayList` | 1.59 ms | 7.20 ms |
| `dict` of string keys and int values to `HashMap` | 226 μs | 329 μs |
| `pyarrow` `int32` array to `int[]` | 140 μs | 131 μs |
| `pyarrow` `float64` array to `double[]` | 133 μs | 147 μs |

Collections as return values:

| Returned | 100 elements | 500 elements |
|---|---:|---:|
| `ArrayList` of ints to `list` | 80.5 μs | 90.6 μs |
| `ArrayList` of strings to `list` | 100 μs | 230 μs |
| `HashMap` of string keys and int values to `dict` | 146 μs | 293 μs |

A list whose elements are all ints, all floats, all bools or all strings travels as one vector, and a dict as two such lists, so the length hardly shows. Most of an argument row's time is not the elements: a command that carries a collection is written with the FlatBuffers library, and the collection made by the run before is released in a round trip of its own, since releases ride only on commands that are packed by hand. A list of mixed types is encoded one element at a time, at about 14 μs each. Until protocol version 5 every list and dict was: when that changed, measured in alternation on a host of the same kind, 500 ints went from 5.97 ms to 0.32 ms as an argument and from 5.58 ms to 0.15 ms as a return value, and a dict of 500 from 14.6 ms to 0.43 ms and from 12.9 ms to 0.34 ms. A command holds 64 KB, which is about 8,000 ints or a dict of 3,000 short entries; beyond that, use Arrow.

The two runs disagree on three of these rows: a list of 100 ints (143 and 260 μs), an `ArrayList` of 500 strings returned (172 and 288 μs) and a `HashMap` of 100 returned (127 and 165 μs).

## Where a call's time goes

For `list.size()`, 20,000 calls on a warm server, with a timestamp taken at each of the client's steps. "Before" is the build in which every command and response went through the FlatBuffers library for Python and the server's sessions ran on virtual threads. It was run three times and the current build six, on the same machine within two hours; the figures are means of the runs.

| Step | Before | Now |
|---|---:|---:|
| Building the command | 32 μs | 3.5 μs |
| The checks before a request, the write to shared memory, four bytes to the socket | 13 μs | 10 μs |
| Waiting for the response | 59 μs | 28 μs |
| Reading the response | 25 μs | 4.6 μs |
| The call | 130 μs | 47 μs |

Taking the timestamps costs 6 to 8 μs: the same call takes 39 μs without them. Single runs of the current build gave 35 to 53 μs for the call.

- **Building and reading** were the FlatBuffers library, which is pure Python and costs several microseconds for each table: 32 μs for a command with no arguments, 59 μs with one, 78 μs with two (now 3.5, 5.1 and 7.7 μs). The client now writes the commands of single calls and of batches with `struct` when their arguments are plain values (`int`, `float`, `bool`, `str`, `None`, a Java object), and reads a result that is one such value the same way. Everything else still goes through the library: a list, a `bytes` or an array as an argument, the vectorized calls, callbacks, Arrow commands.
- **The wait** is a bare round trip between two processes, 21 μs on this machine, and the server's own time for the request, which `get_metrics()` puts in the lowest bucket of its histogram (under 10 μs). It was twice that while a session ran on a virtual thread, which the JDK wakes from a socket read in two steps (a poller thread, then a carrier thread); a platform thread is woken by the kernel.
- **A handle that is dropped** used to be released in a round trip of its own before the next call. Its ID now travels with the next packed command, so creating an object and dropping it went from 245 μs to 49 μs, with the two builds run in alternation.

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
