<p align="center">
  <img src="assets/logo.png" alt="Gatun Logo" width="400">
</p>

# Gatun

> **⚠️ Alpha Status**: This project is experimental and under active development. APIs may change without notice. Not recommended for production use.

High-performance Python-to-Java bridge using shared memory and Unix domain sockets.

## Features

- **Shared Memory IPC**: Zero-copy data transfer via mmap
- **FlatBuffers Protocol**: Efficient binary serialization
- **Apache Arrow Integration**: Zero-copy array/table transfer
- **Sync & Async Clients**: Both blocking and asyncio support
- **Python Callbacks**: Register Python functions as Java interfaces
- **Request Cancellation**: Cancel long-running operations
- **Threads**: The threads of a client call Java at the same time, each on a JVM thread of its own, on the same objects
- **JVM View API**: Pythonic package-style navigation (`client.jvm.java.util.ArrayList`)
- **Spark from Python**: Load Spark's JARs and drive its JVM classes directly; a `BridgeAdapter` interface for porting PySpark
- **Pythonic JavaObjects**: Iteration, indexing, and len() support on Java collections
- **Batch API**: Execute multiple commands in a single round-trip
- **Vectorized APIs**: `invoke_methods`, `create_objects`, `get_fields` for several operations on one target in one round-trip
- **Observability**: Server metrics, structured logging, and JFR events for debugging and monitoring

## Performance

Measured on Linux (8 vCPUs of a shared AMD EPYC 7713, OpenJDK 22.0.2, Python 3.13.12); every figure is a median. [docs/benchmarks.md](docs/benchmarks.md) has all the cases, the method, and the commands to run them. Times depend on the machine, and so may the comparison with Py4J.

### Single Calls

A call with plain arguments is one round trip and takes 31 to 39 μs. A plain instance call and creating an object cost about the same through Py4J (PySpark's bridge), which makes two round trips for a static call. The gap is wide where a call carries a collection or returns something large:

| Operation | Gatun | Py4J | |
|-----------|------:|-----:|---|
| Instance method call (`list.size()`) | 33 μs | 42 μs | about the same |
| Create an object and drop it (`ArrayList()`) | 46 μs | 47 μs | about the same |
| Static method call (`Math.max(10, 20)`) | 39 μs | 113 μs | Gatun 2.9x faster |
| Python list of 100 as an argument | 0.28 ms | 5.6 ms | Gatun 20x faster |
| `is_instance_of` | 31 μs | 275 μs | Gatun 8.8x faster |
| Return a 16 KB `byte[]` | 66 μs | 42 ms | Gatun 640x faster, see below |

Both bridges were given the same JVM settings and a warm JVM, with classes bound once (`Math = jvm.java.lang.Math`). A handle that is dropped is released with the next call, not with a round trip of its own.

Two things to know before reading the table:

- **The first two rows are not a lead.** Two runs on this shared machine differ by up to a third on a single row, and a run on another machine (2 vCPUs) had Py4J level with Gatun on both, or up to 1.4 times ahead. What reproduced there is the rest: static calls, `is_instance_of`, collections as arguments and bulk data.
- **The last row is Nagle's algorithm, not Py4J's protocol.** Py4J's server does not set `TCP_NODELAY`, so a value of 8 KB or more that it returns waits about 40 ms for a delayed acknowledgement. With the option set on the server's connections, the same Py4J returned strings of 8 to 32 KB in 0.2 to 0.3 ms, which leaves Gatun about 3 times faster. A program on stock Py4J sees the 40 ms.

### Fewer Round Trips

A call sent on its own costs a round trip. In a `batch()` it costs about 5 μs:

| Work | One at a Time | `batch()` | Vectorized Call |
|------|--------------:|----------:|----------------:|
| 10 method calls on one object | 502 μs | 132 μs | 423 μs |
| 100 method calls on one object | 4.20 ms | 560 μs | 2.68 ms |
| 100 objects created | 4.22 ms | 573 μs | 2.31 ms |

The vectorized calls (`invoke_methods`, `create_objects`) are slower than a batch of the same size: their commands are still built with the FlatBuffers library, where single calls and batches are packed by hand.

### Arrow Data Transfer

Time for the call to return with the table on the other side:

| Method | 16 KB | 1.6 MB | 16 MB | |
|--------|------:|-------:|------:|---|
| `arrow_to_java` | 261 μs | 384 μs | 2.06 ms | Java gets its own copy |
| `arrow_to_java(copy=False)` | 268 μs | 346 μs | 957 μs | Java reads in place |
| `send_arrow_buffers` | 367 μs | 379 μs | 1.13 ms | Loaded in place, not visible to Java code |
| `arrow_from_java` | 167 μs | 240 μs | 1.93 ms | Python gets its own copy |
| `arrow_from_java(copy=False)` | 193 μs | 175 μs | 881 μs | Python reads in place |

At 128 MiB, `arrow_to_java` takes 24 ms and `arrow_to_java(copy=False)` 11 ms, where an Arrow IPC stream over a Unix socket takes 102 ms.

Next to the other ways of getting 500 integers into Java:

| How | Time |
|-----|-----:|
| 500 `add()` calls | 21 ms |
| 500 `add()` calls in a `batch()` | 2.4 ms |
| A Python list as a constructor argument | 0.22 ms |
| A `pyarrow.Array` argument (arrives as `int[]`) | 0.17 ms |

### Callbacks

A callback made during a request (a Python comparator in `Collections.sort`) costs about 330 μs. With `connect(callback_timeout=None)` it costs 160 μs: a timeout, 30 seconds by default, has each callback run on a thread of its own. Sorting 1,000 elements took about 8,700 of them and 2.8 seconds, so a comparator in Python is for short lists.

### When to Use Gatun vs Py4J

| Use Case | Recommendation |
|----------|----------------|
| Many small calls, one at a time | Either: plain instance calls and object creation cost about the same; static calls are one round trip for Gatun and two for Py4J |
| Bulk data | Gatun: Arrow tables through shared memory |
| Collections as arguments, large return values | Gatun |
| Many operations that can be sent together | Gatun: `batch()` and the vectorized calls |

## Installation

```bash
pip install gatun
```

## Requirements

- **Python**: 3.11+
- **Java**: 22+
- **OS**: Linux, macOS (Windows is not supported - Unix domain sockets required)

## Quick Start

```python
from gatun import connect

# Auto-launch server and connect
client = connect()

# Create Java objects via JVM view
ArrayList = client.jvm.java.util.ArrayList
my_list = ArrayList()
my_list.add("hello")
my_list.add("world")
print(my_list.size())  # 2

# Call static methods
result = client.jvm.java.lang.Integer.parseInt("42")  # 42
result = client.jvm.java.lang.Math.max(10, 20)        # 20

# Clean up
client.close()
```

## Examples

### java_import for Shorter Paths

```python
from gatun import connect, java_import

client = connect()

# Wildcard import
java_import(client.jvm, "java.util.*")
arr = client.jvm.ArrayList()  # instead of client.jvm.java.util.ArrayList()
arr.add("hello")

# Single class import
java_import(client.jvm, "java.lang.StringBuilder")
sb = client.jvm.StringBuilder("hello")
print(sb.toString())  # "hello"
```

### Collections

```python
from gatun import connect, java_import

client = connect()

# HashMap
hm = client.jvm.java.util.HashMap()
hm.put("key1", "value1")
hm.put("key2", 42)
print(hm.get("key1"))  # "value1"
print(hm.size())       # 2

# TreeMap (sorted keys)
tm = client.jvm.java.util.TreeMap()
tm.put("zebra", 1)
tm.put("apple", 2)
tm.put("mango", 3)
print(tm.firstKey())  # "apple"
print(tm.lastKey())   # "zebra"

# HashSet (no duplicates)
hs = client.jvm.java.util.HashSet()
hs.add("a")
hs.add("b")
hs.add("a")  # duplicate ignored
print(hs.size())        # 2
print(hs.contains("a")) # True

# Collections utility methods
java_import(client.jvm, "java.util.*")
arr = client.jvm.ArrayList()
arr.add("banana")
arr.add("apple")
arr.add("cherry")
client.jvm.Collections.sort(arr)     # ["apple", "banana", "cherry"]
client.jvm.Collections.reverse(arr)  # ["cherry", "banana", "apple"]

# Arrays.asList (returns Python list)
result = client.jvm.java.util.Arrays.asList("a", "b", "c")  # ['a', 'b', 'c']
```

### String Operations

```python
from gatun import connect

client = connect()

# StringBuilder
sb = client.jvm.java.lang.StringBuilder("Hello")
sb.append(" ")
sb.append("World!")
print(sb.toString())  # "Hello World!"

# String static methods
result = client.jvm.java.lang.String.valueOf(123)  # "123"
result = client.jvm.java.lang.String.format("Hello %s, you have %d messages", "Alice", 5)
# "Hello Alice, you have 5 messages"
```

### Math Operations

```python
from gatun import connect

client = connect()

Math = client.jvm.java.lang.Math
print(Math.abs(-42))        # 42
print(Math.min(5, 3))       # 3
print(Math.max(10, 20))     # 20
print(Math.pow(2.0, 10.0))  # 1024.0 (note: use floats for double params)
print(Math.sqrt(16.0))      # 4.0
```

### Integer Utilities

```python
from gatun import connect

client = connect()

Integer = client.jvm.java.lang.Integer
print(Integer.parseInt("42"))        # 42
print(Integer.valueOf("123"))        # 123
print(Integer.toBinaryString(255))   # "11111111"
print(Integer.MAX_VALUE)             # 2147483647 (static field)
```

### Passing Python Collections

Python lists and dicts are automatically converted to Java collections:

```python
from gatun import connect

client = connect()

arr = client.jvm.java.util.ArrayList()
arr.add([1, 2, 3])                    # Converted to Java List
arr.add({"name": "Alice", "age": 30}) # Converted to Java Map
print(arr.size())  # 2
```

### Async Client

```python
from gatun import aconnect
import asyncio

async def main():
    client = await aconnect()

    # All operations are async
    arr = await client.jvm.java.util.ArrayList()
    await arr.add("hello")
    await arr.add("world")
    size = await arr.size()  # 2

    # Static methods
    result = await client.jvm.java.lang.Integer.parseInt("42")  # 42

    await client.close()

asyncio.run(main())
```

### Python Callbacks

Register Python functions as Java interface implementations:

```python
from gatun import connect

client = connect()

def compare(a, b):
    return -1 if a < b else (1 if a > b else 0)

comparator = client.register_callback(compare, "java.util.Comparator")

arr = client.jvm.java.util.ArrayList()
arr.add(3)
arr.add(1)
arr.add(2)
client.jvm.java.util.Collections.sort(arr, comparator)
# arr is now [1, 2, 3]
```

An interface with several methods takes an object instead of a function. Each call goes to the object's method of the same name. That includes a default method of the interface, if the object has a method for it; the other default methods run in Java:

```python
class Countdown:
    def __init__(self, start):
        self.left = start

    def hasMoreElements(self):
        return self.left > 0

    def nextElement(self):
        self.left -= 1
        return self.left

enumeration = client.register_callback(Countdown(3), "java.util.Enumeration")
client.jvm.java.util.Collections.list(enumeration)  # [2, 1, 0]
```

A function, or any other callable, answers every method of its interface.

Async callbacks work too:

```python
from gatun import aconnect
import asyncio

async def main():
    client = await aconnect()

    async def async_compare(a, b):
        await asyncio.sleep(0.01)  # Simulate async work
        return -1 if a < b else (1 if a > b else 0)

    comparator = await client.register_callback(async_compare, "java.util.Comparator")

asyncio.run(main())
```

Java can call a callback from any thread and at any time, including when the client is doing nothing: a listener, a timer, an executor's worker, a parallel stream. How the call reaches Python depends on the thread that makes it:

- **The thread running your request** (a comparator passed to `Collections.sort`) calls back on the request's own channel. Such a callback cannot call Java (`ReentrancyError`), and neither can a task it awaits: the request is waiting for it on the only channel the call could use. Work queued by a callback and run after the request is fine. So is, in the sync client, work the callback hands to another *thread*: a thread has a connection of its own ([Threads](#threads)), and its calls run while the request waits for the callback.
- **Any other thread** calls on a second connection, which the client opens with its first `register_callback`. Each of these calls runs on a thread of its own (a task of its own in the async client), so several run at the same time, alongside your own code; what they touch has to be safe for that. These callbacks can call Java: create objects, call methods, read and write fields, use the objects they were passed. Their calls travel on the same connection and are run by the Java thread that is waiting for the callback, so they do not wait for the client's own channel, which may be busy with a long request (a stream's `awaitTermination()`) or with the very request that is waiting for the callback (`executor.submit(callback).get()`).

  They can also move Arrow tables with `arrow_to_java` and `arrow_from_java` (a `foreachBatch` function that fetches its batch, for example). The client's shared memory belongs to its own channel, so each such call gets shared memory of its own, as large as the client's, when it first moves a table; the limit on one table is the same `memory` setting. A table read in place there (`copy=False`) is good until the callback's next transfer.

  `batch()`, the vectorized calls (`invoke_methods`, `create_objects`, `get_fields`), `register_callback` and `get_metrics` work there too. What such a callback cannot do is whatever goes through the client's own shared memory: the other Arrow transfers (`send_arrow_table`, `send_arrow_buffers`, `get_arrow_data`, `arrow_context`) raise `ReentrancyError`. A command or a result may be as large there as anywhere else (see [Large Values](#large-values)). In the sync client a plain thread the callback starts does not inherit the route; its calls travel on a connection of its own, like any thread's ([Threads](#threads)), as do those of a callback Java has stopped waiting for because it timed out.

A callback stays registered, and its function alive, for as long as it can still be called: until `unregister_callback`, the end of the session, or the moment nothing holds the proxy any more. That last one means the object `register_callback` returned has been garbage collected in Python and Java code has let go of the proxy too; the JVM's garbage collector then reports it and the client drops the function, some time later. So keep the returned object for as long as the callback should work when the Java code you give it to holds it only weakly (a registry of weak listeners). A proxy called after that throws `IllegalStateException` in the calling Java thread.

### Cancelling a Request

`client.cancel()` interrupts the requests that are running on the client, from another thread:

```python
import threading
from gatun import CancelledException, connect

client = connect(allowed_classes=["java.lang.Thread"])

threading.Timer(0.5, client.cancel).start()
try:
    client.jvm.java.lang.Thread.sleep(60_000)
except CancelledException:
    print("cancelled")

client.ping()  # the client is still usable
```

It works by interrupting the Java thread, so it stops code that responds to interruption (blocking calls, `GatunServer.checkCancelled()`), and returns `False` when nothing was running. A request that never checks for interruption runs to its end. `cancel()` makes a short connection of its own, so on a server with `gatun.limits.max_sessions` set it needs a free slot.

When several threads of the client are inside a request, `cancel()` interrupts them all and `cancel(thread)` the request of that one thread.

### Threads

A `GatunClient` can be shared between threads, and the threads do not wait for each other. Each thread that calls Java gets a connection of its own to the client's session, with its first call, and a JVM thread of its own that runs every call it makes:

```python
import threading
from gatun import connect

client = connect(allowed_classes=["java.lang.Thread"])
items = client.jvm.java.util.ArrayList()

def slow():
    client.jvm.java.lang.Thread.sleep(1500)

worker = threading.Thread(target=slow)
worker.start()

items.add("x")        # returns at once; the other thread is still asleep in Java
items.size()          # 1

worker.join()
```

- **Objects belong to the client, not to a thread.** An object made on one thread can be used, passed and released on any other, and the resource limits count what all threads hold together.
- **One Python thread, one JVM thread**, for the life of the Python thread. Java code that keeps state in a `ThreadLocal` (a Spark session's active session, a transaction, a logging context) finds it again on the next call from the same Python thread, and never sees another thread's. This holds for a thread pool's workers too.
- **When a Python thread ends, its JVM thread and its connection go with it.** The thread that called `connect()` is the exception: its connection is the session and stays until `close()`.
- **Callbacks** work from every thread. A callback made by the thread that runs a request runs during that request, whichever thread registered it.
- **Arrow** transfers of two threads do not disturb each other: each connection has shared memory of its own. A table view (`copy=False`) can be read from any thread and ends with the next transfer of the thread that made it, or with that thread. A lent table (`arrow_to_java(table, copy=False)`) can be used and ended from any thread, and ends at the latest with the thread that lent it. A payload arena belongs to the thread that asked for it.
- **A timeout or an interrupted call** on a thread other than the one that connected costs that thread its connection, not the client: its next call opens a new one, on a new JVM thread. On the thread that connected it ends the client, as before.
- `close()` closes every thread's connection. A thread that is inside a call gets `DeadConnectionError`.

The async client has one connection. Calls awaited together (`asyncio.gather`) run one after the other on one JVM thread, as before.

### Type Checking with is_instance_of

```python
from gatun import connect

client = connect()

arr = client.create_object("java.util.ArrayList")
print(client.is_instance_of(arr, "java.util.List"))       # True
print(client.is_instance_of(arr, "java.util.Collection")) # True
print(client.is_instance_of(arr, "java.util.Map"))        # False
```

### Pythonic Java Collections

JavaObject wrappers support iteration, indexing, and length:

```python
from gatun import connect

client = connect()

arr = client.jvm.java.util.ArrayList()
arr.add("a")
arr.add("b")
arr.add("c")

# Iterate
for item in arr:
    print(item)  # "a", "b", "c"

# Index access
print(arr[0])  # "a"
print(arr[1])  # "b"

# Length
print(len(arr))  # 3

# Convert to Python list
items = list(arr)  # ["a", "b", "c"]
```

### Batch API

Execute multiple commands in a single round-trip to reduce per-call overhead:

```python
from gatun import connect

client = connect()

arr = client.create_object("java.util.ArrayList")

# Batch 100 operations in one round-trip
with client.batch() as b:
    for i in range(100):
        b.call(arr, "add", i)
    size_result = b.call(arr, "size")

print(size_result.get())  # 100

# Mix different operation types
with client.batch() as b:
    obj = b.create("java.util.HashMap")
    r1 = b.call_static("java.lang.Integer", "parseInt", "42")
    r2 = b.call_static("java.lang.Math", "max", 10, 20)

print(r1.get())  # 42
print(r2.get())  # 20

# Error handling: continue on error (default) or stop on first error
with client.batch(stop_on_error=True) as b:
    r1 = b.call(arr, "add", "valid")
    r2 = b.call_static("java.lang.Integer", "parseInt", "invalid")  # Will error
    r3 = b.call(arr, "size")  # Skipped when stop_on_error=True
```

### Vectorized APIs

For several calls on one object, several objects, or several fields of one object, there are calls that take the whole list:

```python
from gatun import connect

client = connect()

# invoke_methods - Multiple calls on same object in one round-trip
arr = client.create_object("java.util.ArrayList")
results = client.invoke_methods(arr, [
    ("add", ("a",)),
    ("add", ("b",)),
    ("add", ("c",)),
    ("size", ()),
])
# results = [True, True, True, 3]

# create_objects - Create multiple objects in one round-trip
list1, map1, set1 = client.create_objects([
    ("java.util.ArrayList", ()),
    ("java.util.HashMap", ()),
    ("java.util.HashSet", ()),
])

# get_fields - Read multiple fields from one object
sb = client.create_object("java.lang.StringBuilder", "hello")
values = client.get_fields(sb, ["count"])  # [5]
```

**When to use which API:**
| API | Best For |
|-----|----------|
| `invoke_methods` | Multiple method calls on **same object** |
| `create_objects` | Creating multiple objects at startup |
| `get_fields` | Reading multiple fields from one object |
| `batch` | Mixed operations on **different objects**, and whenever speed matters |

A `batch()` is the fastest of the four, several times faster than the vectorized call for the same work (see [Performance](#performance)).

### JavaArray for Primitive Arrays

Primitive arrays (`int[]`, `long[]`, `double[]`, etc.) are returned as `JavaArray`:

```python
from gatun import connect, JavaArray
import pyarrow as pa

client = connect()

# Primitive arrays from Java are JavaArray instances
original = pa.array([1, 2, 3], type=pa.int32())
int_array = client.jvm.java.util.Arrays.copyOf(original, 3)
print(isinstance(int_array, JavaArray))  # True
print(int_array.element_type)  # "Int"
print(list(int_array))  # [1, 2, 3]

# Create typed arrays manually for passing to Java
int_array = JavaArray([1, 2, 3], element_type="Int")
str_array = JavaArray(["a", "b"], element_type="String")
result = client.jvm.java.util.Arrays.toString(int_array)  # "[1, 2, 3]"
```

### Object Arrays as JavaObject

Object arrays (`Object[]`, `String[]`) are returned as `JavaObject` references:

```python
from gatun import connect

client = connect()

# Object arrays from toArray() are JavaObject (not JavaArray)
arr = client.jvm.java.util.ArrayList()
arr.add("x")
arr.add("y")
java_array = arr.toArray()  # Returns JavaObject

# Use len() and iteration (not .size() or .length)
print(len(java_array))    # 2
print(java_array[0])      # "x"
print(list(java_array))   # ["x", "y"]

# Can still pass back to Java methods
result = client.jvm.java.util.Arrays.toString(java_array)  # "[x, y]"
```

This distinction exists because Object arrays are kept as references on the Java side, allowing `Array.set()` and `Array.get()` to modify them directly.

### Arrow Data Transfer

Gatun supports multiple methods for transferring Arrow data between Python and Java. To exchange tables with your own Java code, see [Arrow Tables as Java Objects](#arrow-tables-as-java-objects) below.

```python
from gatun import connect
import pyarrow as pa

client = connect()

# Method 1: IPC Format (simple, good for small data)
table = pa.table({"x": [1, 2, 3], "y": ["a", "b", "c"]})
result = client.send_arrow_table(table)  # "Received 3 rows"

# Method 2: Scoped Context Manager (recommended for most use cases)
# Handles arena lifecycle automatically with proper cleanup
with client.arrow_context() as ctx:
    ctx.send(table)              # Auto-resets arena between sends
    ctx.send(another_table)      # Safe to send multiple tables
    result = ctx.receive()       # Get data back as PyArrow table
# Arena automatically closed on exit

# Method 3: Zero-Copy Buffer Transfer (manual control for advanced use)
table = pa.table({"name": ["Alice", "Bob"], "age": [25, 30]})
arena = client.get_payload_arena()
schema_cache = {}
client.send_arrow_buffers(table, arena, schema_cache)

# Get data back from Java
result_view = client.get_arrow_data()
print(result_view.num_rows)  # 2
print(result_view.to_pydict())  # {'name': ['Alice', 'Bob'], 'age': [25, 30]}

arena.close()
```

#### Arrow Tables as Java Objects

To hand a table to Java code, or to get one that Java code produced, use `arrow_to_java()` and `arrow_from_java()`. A sent table becomes an `org.apache.arrow.vector.VectorSchemaRoot` in the JVM, and you get an ordinary object handle to it:

```python
import pyarrow as pa

table = pa.table({"x": [1, 2, 3], "name": ["a", "b", "c"]})

# Python -> Java: the handle is a VectorSchemaRoot
root = client.arrow_to_java(table)
root.getRowCount()                 # 3
root.getVector("x").get(2)         # 3

# Pass it to any Java method that takes a VectorSchemaRoot
# client.jvm.com.example.Stats.summarize(root)

# Java -> Python: works for any VectorSchemaRoot the JVM holds,
# including ones Java code created
produced = root.slice(1, 2)        # a new table, made on the Java side
result = client.arrow_from_java(produced)
result.to_pydict()                 # {'x': [2, 3], 'name': ['b', 'c']}

# Read a result once without copying it out of shared memory
view = client.arrow_from_java(produced, copy=False)
view.to_pydict()                   # good until the next Arrow transfer
produced.close()                   # Java-made tables are yours to close
```

- A table made by `arrow_to_java()` owns its memory in the JVM and is released when its handle is garbage collected or the connection closes. Java code that keeps it for longer must copy it. To release it at a known point, use `client.free_object(root.object_id)`, or in the async client `async with await client.arrow_to_java(table) as root:`.
- The memory of a freed table is kept by the server for the next one, because memory that is new from the operating system is slow to touch. It keeps buffers over 4 MiB, at most as many bytes as the `memory` setting, and gives one back after a minute in which nobody asked for it. `extra_jvm_flags=["-Dgatun.arrow.pool.max_bytes=0"]` turns that off, `-Dgatun.arrow.pool.idle_ms=...` changes the minute (0 keeps the memory until the server exits), and `client.get_metrics()` shows what is held. Both take whole numbers (bytes, milliseconds); the server does not start with anything else.
- `arrow_from_java()` returns a `pyarrow.Table` that owns its data, so it stays valid after later transfers and after `close()`.
- `arrow_from_java(obj, copy=False)` skips that copy (sync client only). It returns an `ArrowTableView` over the bytes Java wrote into shared memory, which is good until the client's next Arrow transfer of any kind and raises `StaleArenaError` after it. Use it to read a result once (aggregate it, call `to_pandas()`); keep the default for a table you hold on to. A `pyarrow.Table` taken out of the view with `view.table` is not checked.
- Each transfer must fit in the shared memory payload zone (`PayloadTooLargeError` otherwise). Connect with more memory, or move a large table in pieces with `table.slice()` / `root.slice(start, length)`.
- Dictionary-encoded columns are not supported; decode them first.

##### Lending a Table Instead of Copying It

By default Java copies a sent table into memory of its own. With `copy=False` it reads the table where Python wrote it, in shared memory, for as long as a `with` block lasts:

```python
with client.arrow_to_java(table, copy=False) as root:
    root.getRowCount()             # 3
    # client.jvm.com.example.Stats.summarize(root)
# The loan is over: Java can no longer read the table
```

- `arrow_to_java(table, copy=False)` returns an `ArrowLoan`. Entering it gives the `VectorSchemaRoot` handle; leaving it (or `loan.close()`) ends the loan. A loan that is dropped without either ends with the client's next request after it was garbage collected.
- The table is Java's to read during the loan and not after. Java code that kept it gets an exception from Arrow when it reads (an empty vector, or `IllegalStateException: Ref count should be >= 1`), not the bytes Python wrote there next. Code that needs the data for longer must copy it during the loan. Two things are not caught: a raw memory address taken from a buffer, and a JVM started with Arrow's bounds checks switched off.
- While a loan is open its bytes stay put, and later transfers are written after them: an open loan takes room in the payload zone away from other transfers. `send_arrow_table()`, `send_arrow_buffers()` and `get_arrow_data()` use fixed places in the zone and raise `RuntimeError` if a loan is in the way. `reset_payload_arena()` ends every loan.
- The data of a lent table takes no memory in the JVM and is not counted against `max_arrow_bytes`. One thing is: Arrow leaves the validity bitmap of a column without nulls out of the stream, and Java allocates one when it loads the column (one bit a row).
- If Java code still holds buffers of a lent table when the connection closes, the server keeps that session's shared memory mapped until they are released, and logs a warning.
- Sync client, on its own channel. Inside a callback that Java made from another thread the table is copied as usual, and the block frees the copy when it ends (`loan.in_place` is `False`).

##### Record Batches as Byte Arrays

Some Java code takes Arrow data as serialized record batches and not as a table. Spark does (`ArrowConverters.toDataFrame`). `arrow_to_java_batches()` hands the table over in that form, without building a table in the JVM first:

```python
batches = client.arrow_to_java_batches(table, max_rows_per_batch=2)
batches.size()                     # 2: a java.util.ArrayList of byte[]
```

- Each element is one record batch as its Arrow IPC message. Each chunk of the table becomes a batch; `max_rows_per_batch` splits larger ones. A Java array holds less than 2 GiB, so a table with larger chunks needs it.
- The arrays are ordinary Java objects on the heap. Nothing is decoded on the way, and nothing is counted against `max_arrow_bytes`.
- Both clients have it, and it works inside a callback that Java made from another thread.

The other methods on this page do not give Java code access to the data: `send_arrow_table()` reads and counts a table, and `send_arrow_buffers()` / `get_arrow_data()` hold one batch that can only be read back.

#### Size Validation

Gatun validates data size before transfer and provides informative errors:

```python
from gatun import connect, PayloadTooLargeError, estimate_arrow_size
import pyarrow as pa

client = connect(memory="16MB")
arena = client.get_payload_arena()

# Check size before sending
large_table = pa.table({"data": list(range(1_000_000))})
estimated_size = estimate_arrow_size(large_table)
print(f"Estimated size: {estimated_size:,} bytes")
print(f"Available: {arena.bytes_available():,} bytes")

# If too large, get a clear error
try:
    client.send_arrow_buffers(large_table, arena, {})
except PayloadTooLargeError as e:
    print(f"Table too large: {e.payload_size:,} > {e.max_size:,} bytes")
    print("Consider: reset arena, use batching, or increase memory")

arena.close()
```

## Arrow Memory Architecture

Gatun's Arrow integration uses shared memory for high-performance data transfer with a carefully designed memory safety model.

### Shared Memory Layout

```
┌─────────────────────────────────────────────────────────────────┐
│                     Shared Memory Region                         │
├─────────────────────────────────────────────────────────────────┤
│ Command Zone (64KB)    │ Python writes commands, Java reads     │
├─────────────────────────────────────────────────────────────────┤
│ Payload Zone           │ Arrow data buffers                      │
│   ├── First Half       │   Python → Java transfers               │
│   └── Second Half      │   Java → Python transfers               │
├─────────────────────────────────────────────────────────────────┤
│ Response Zone (64KB)   │ Java writes responses, Python reads    │
└─────────────────────────────────────────────────────────────────┘
```

The halves are how `send_arrow_buffers()` and `get_arrow_data()` share the zone, so that data can come back while the batch that was sent is still loaded:
- **Python → Java**: `send_arrow_buffers()` writes from the start of the zone
- **Java → Python**: `get_arrow_data()` writes to the second half `[size/2, size)`

A batch may be larger than half the zone. Data cannot be returned while such a batch is loaded, and sending one ends the views that `get_arrow_data()` returned earlier. `arrow_to_java()` and `arrow_from_java()` do not use the halves: each writes its stream into the free part of the zone, which is why one table may take all of it.

### Memory Safety: The Epoch System

Gatun uses an **arena epoch** system to prevent use-after-free and stale data access:

```python
from gatun import connect, StaleArenaError
import pyarrow as pa

client = connect()
arena = client.get_payload_arena()

# Send data (epoch = 0)
table = pa.table({"id": [1, 2, 3]})
client.send_arrow_buffers(table, arena, {})

# Get data back - view is bound to current epoch
view = client.get_arrow_data()  # the view belongs to epoch 0

# Reset arena - epoch increments to 1
arena.reset()
client.reset_payload_arena()

# Accessing stale view raises StaleArenaError
try:
    data = view.to_pydict()  # Raises StaleArenaError!
except StaleArenaError as e:
    print(e)  # Arrow table from epoch 0 is stale (current epoch: 1). ...

arena.close()
```

**How epochs work:**

1. **Initial state**: Both Python and Java start with epoch 0
2. **On data transfer**: The `ArrowBatchDescriptor` includes the current epoch
3. **On validation**: Java rejects data if descriptor epoch doesn't match its epoch
4. **On reset**: Both sides increment their epoch, invalidating all previous views
5. **On access**: `ArrowTableView` checks epoch before returning data

This prevents:
- **Use-after-reset**: Accessing data after arena memory is reused
- **Stale reads**: Reading outdated data from a previous transfer
- **Cross-session corruption**: Data from one transfer corrupting another

### Data Flow: Python → Java

```
1. Python: Copy Arrow buffers to shared memory (first half)
   ┌──────────────┐     memcpy      ┌──────────────────────┐
   │ PyArrow Table│ ───────────────>│ Shared Memory [0,N/2)│
   └──────────────┘                 └──────────────────────┘

2. Python: Send ArrowBatchDescriptor via FlatBuffers
   - Buffer offsets and lengths
   - Schema (or schema hash if cached)
   - Current epoch

3. Java: Validate epoch, wrap buffers as ArrowBuf (zero-copy read)
   ┌──────────────────────┐  wrap   ┌──────────────────────┐
   │ Shared Memory [0,N/2)│ ───────>│ VectorSchemaRoot     │
   └──────────────────────┘         └──────────────────────┘
```

### Data Flow: Java → Python

```
1. Python: Request data via GetArrowData command

2. Java: Write Arrow buffers to shared memory (second half)
   ┌──────────────────────┐  memcpy ┌────────────────────────┐
   │ VectorSchemaRoot     │ ───────>│ Shared Memory [N/2, N) │
   └──────────────────────┘         └────────────────────────┘

3. Java: Send ArrowBatchDescriptor with buffer offsets + epoch

4. Python: Wrap buffers as PyArrow arrays (zero-copy read)
   ┌────────────────────────┐  wrap  ┌──────────────┐
   │ Shared Memory [N/2, N) │ ──────>│ ArrowTableView│
   └────────────────────────┘        └──────────────┘
```

### Best Practices

#### Recommended: Use the Context Manager

```python
from gatun import connect
import pyarrow as pa

client = connect(memory="256MB")

# Simple case: send and receive with automatic cleanup
with client.arrow_context() as ctx:
    ctx.send(my_table)
    result = ctx.receive()
    # Process result...
# Arena automatically cleaned up, even on exceptions
```

#### For Batch Processing

```python
from gatun import connect, estimate_arrow_size
import pyarrow as pa

client = connect(memory="256MB")

# Process large dataset in batches
with client.arrow_context() as ctx:
    for batch in large_table.to_batches(max_chunksize=100_000):
        ctx.send(batch)  # Auto-resets arena between sends
        # Process batch in Java...
```

The async client has the same context manager:

```python
async with client.arrow_context() as ctx:
    await ctx.send(table)
```

#### Manual Control (Advanced)

```python
from gatun import connect
import pyarrow as pa

client = connect(memory="256MB")

# For fine-grained control over arena lifecycle
arena = client.get_payload_arena()
schema_cache = {}  # Reuse cache for schema deduplication

for batch in large_table.to_batches(max_chunksize=100_000):
    arena.reset()
    client.reset_payload_arena()
    client.send_arrow_buffers(batch, arena, schema_cache)
    # Process batch in Java...

# Always close arena when done
arena.close()
```

**Guidelines:**
- Use `arrow_context()` for most use cases - handles cleanup automatically
- Use `send_arrow_table()` for small data (< 1K rows) - simpler API
- Use `send_arrow_buffers()` with manual arena for maximum control
- Use `estimate_arrow_size()` to check size before large transfers
- Keep `schema_cache` across transfers to avoid re-serializing schema

### Low-Level API

For direct control:

```python
from gatun import connect

client = connect()

# Create objects
obj = client.create_object("java.util.ArrayList")
obj = client.create_object("java.util.ArrayList", 100)  # with capacity

# Invoke methods
client.invoke_method(obj, "add", "item")  # the object or its object_id
result = client.invoke_static_method("java.lang.Math", "max", 10, 20)

# Access static fields
max_int = client.get_static_field("java.lang.Integer", "MAX_VALUE")

# Vectorized operations (single round-trip for multiple operations)
client.invoke_methods(obj, [("add", ("a",)), ("add", ("b",)), ("size", ())])
client.create_objects([("java.util.ArrayList", ()), ("java.util.HashMap", ())])
```

### Observability

Get server metrics for debugging and monitoring:

```python
from gatun import connect

client = connect()

# Get server metrics report
metrics = client.get_metrics()
print(metrics)
# === Gatun Server Metrics ===
# Global:
#   total_requests: 150
#   total_errors: 0
#   requests_per_sec: 45.23
#   current_sessions: 1
#   current_objects: 12
#   peak_objects: 25
# ...
```

Enable trace mode for method resolution debugging:

```python
from gatun import connect

# Enable trace mode
client = connect(trace=True)

# Enable verbose logging
client = connect(log_level="FINE")
```

Or via environment variables:

```bash
export GATUN_TRACE=true
export GATUN_LOG_LEVEL=FINE
```

## PySpark Integration

Gatun can load and drive Spark's JVM classes directly (pass the Spark JARs via `connect(classpath=...)`), and ships a `BridgeAdapter` abstraction designed as the contract between PySpark and a JVM backend. Stock PySpark releases talk to the JVM through Py4J and do not use Gatun; running PySpark itself on Gatun requires a PySpark build ported to the `BridgeAdapter` interface (see [docs/bridge_adapter_spec.md](docs/bridge_adapter_spec.md)).

### The Classpath's Arrow

Spark ships its own Apache Arrow, and so may your application. A JVM has room for one: with two on the classpath, whichever comes first shadows the other class by class, and code built for the other version breaks. So the server comes as two JARs, and the launcher picks one from the `classpath` you pass:

- **No Arrow on the classpath** (or no classpath): the server runs from the JAR that has everything, Arrow included. Arrow's two large dependencies, Netty and Jackson, are in there under other package names, so they are never in front of yours. The small ones are there under their own (`slf4j-api`, `commons-codec`, `flatbuffers-java`) and do come first; put your own copy of the server's Arrow on the classpath if that matters.
- **Arrow on the classpath**: the server runs from a JAR with its own classes and nothing else, and uses your Arrow, with the Netty, Jackson and FlatBuffers that Arrow came with. `arrow_to_java()` then returns a `VectorSchemaRoot` of your Arrow, which your Java code can take as an argument.

Nothing has to be set. The classpath must bring all of Arrow that the server uses (`arrow-vector`, `arrow-format`, `arrow-memory-core`, `arrow-memory-netty` and its buffer patch, Netty's `netty-buffer` and `netty-common`, `flatbuffers-java`), which Spark's does, in JARs it names or in ones their manifests name (`Class-Path`). It must also be Arrow 18.3 or later, the one the server is built with. A classpath whose Arrow is older or incomplete gets the first JAR, as every classpath did before, and a warning that says why. The server is tested on Spark 4.1.1 (Arrow 18.3) and Spark 4.3.0 (Arrow 19.0).

To decide it yourself, pass `server_jar="thin"` or `server_jar="full"` to `connect()` (also `GATUN_SERVER_JAR`, and `server_jar` under `[tool.gatun]`); the default is `"auto"`. On your Arrow the server's Netty is yours too, and reads its settings under their own names: `-Dio.netty.maxDirectMemory`, not `-Dgatun.shaded.io.netty.maxDirectMemory`, which the launcher warns about there. That limit is then one limit for the server's Arrow memory and your code's Netty buffers together.

The BridgeAdapter API can be used directly:

```python
from gatun.bridge_adapters import GatunAdapter

# Create bridge (launches JVM)
bridge = GatunAdapter(memory="256MB")

# Use bridge API
obj = bridge.new("java.util.ArrayList")
bridge.call(obj, "add", "hello")
result = bridge.call_static("java.lang.Math", "max", 10, 20)

# Array operations
arr = bridge.new_array("java.lang.String", 3)
bridge.array_set(arr, 0, "hello")
bridge.array_get(arr, 0)  # "hello"

# Callbacks: a Python function (or object) as a Java interface
by_length = bridge.register_callback(lambda a, b: len(a) - len(b), "java.util.Comparator")
bridge.call_static("java.util.Collections", "sort", obj, by_length)
bridge.unregister_callback(by_length)

bridge.close()
```

## Configuration

Configure via `pyproject.toml`:

```toml
[tool.gatun]
memory = "64MB"
socket_path = "/tmp/gatun.sock"  # Optional: uses random path by default
```

Or environment variables:

```bash
export GATUN_MEMORY=64MB
export GATUN_SOCKET_PATH=/tmp/gatun.sock
```

The launcher waits `startup_timeout` seconds (default 30; `GATUN_STARTUP_TIMEOUT`) for the server to listen. A server that fails to start is reported at once with its output; the timeout is only for one that neither starts nor exits. A JVM with a few hundred JARs on its classpath can take several seconds on a small machine.

`memory` takes `KB` / `MB` / `GB` (or `K` / `M` / `G`), a fraction such as `"1.5GB"`, or a number of bytes; the minimum is 192KB. With a fixed `socket_path`, launching fails if a live server is already listening there.

Timeouts are per client:

```python
client = connect(
    socket_timeout=120.0,   # seconds to wait for a response (default 30)
    callback_timeout=None,  # seconds a callback may run (default 30); None, 0 or inf: no limit
)
```

After a timeout the client is dead (`DeadConnectionError` on the next call): the late answer is still on its way and would be read as the next request's. Connect a new client.

### Allowing Your Own Classes

The server only lets a client create objects of, or call static members on, allowlisted classes. The built-in list covers common JDK collections, strings and boxed types, plus `org.apache.spark.`, `org.apache.log4j.` and `scala.`. Anything else raises `JavaSecurityException` until you allow it:

```python
from gatun import connect

client = connect(
    classpath=["/path/to/mylib.jar"],
    allowed_classes=["java.util.concurrent.atomic.AtomicLong"],  # exact class names
    allowed_prefixes=["com.example"],  # a package and all its subpackages
)

counter = client.jvm.java.util.concurrent.atomic.AtomicLong(41)
counter.incrementAndGet()  # 42
```

The lists can also be set once per project or environment. Entries from all three places are combined and added to the built-in list:

```toml
[tool.gatun]
allowed_classes = ["java.util.concurrent.atomic.AtomicLong"]
allowed_prefixes = ["com.example"]
```

```bash
export GATUN_ALLOWED_PREFIXES=com.example,org.acme
```

A nested class is named either way: `java.util.AbstractMap.SimpleEntry` or `java.util.AbstractMap$SimpleEntry`.

To allow every class, as Py4J does, lift the list:

```python
from gatun import connect

client = connect(allow_all=True)

client.jvm.java.util.concurrent.atomic.AtomicLong(41).incrementAndGet()  # 42
client.jvm.java.lang.Runtime.getRuntime().availableProcessors()
```

`allow_all=True` allows every class, and with it the objects the list otherwise keeps out of reach (see the next section): reflection objects, class loaders, `Runtime`, threads. `System.exit`, `System.load`, `System.loadLibrary` and `Class.newInstance` stay refused. It is off unless you turn it on: with the argument, with `allow_all = true` under `[tool.gatun]`, or with `GATUN_ALLOW_ALL=true`. An argument, `allow_all=False` included, wins over the environment, which wins over `pyproject.toml`. The server writes one line to its log at start when the list is off.

### What the Allowlist Is and Is Not

The server trusts the operating-system user it runs as, and nobody else:

- Its socket and shared memory files are owner-only (`0600`), and it refuses connections from other users.
- Each session reaches only the objects it created. Another session cannot call, read, free or pass an object by guessing its ID.

Inside that boundary, the allowlist is a guardrail against accidents and against class names that come from somewhere you do not control. It is not a sandbox, and with `allow_all=True` it is not there at all: what stands then is the boundary above and the four refused members.

- It decides which classes can be instantiated and which static members can be used. Instance methods are open on whatever an allowed call returns, with these exceptions: objects that would undo the list (`java.lang.reflect.Method` / `Constructor` / `Field`, method handles, class loaders, `Runtime`, `Process`, `ProcessBuilder`, `Thread`, `Module`) cannot be used from Python unless you put their type in `allowed_classes`, and `System.exit`, `System.load`, `System.loadLibrary` and `Class.newInstance` are always refused.
- Naming a class does not run its static initializer, and neither does `Class.forName` called from Python, in either form: `Class.forName(name, true, loader)` loads the class without initializing it too. A class is initialized when something allowed first uses it: an instance is created, a static method called or a static field read or set, from Python or by Java code. A class that registers itself in its initializer is therefore not registered by `Class.forName` (a JDBC driver without a `META-INF/services/java.sql.Driver` entry, from before JDBC 4); with the class allowed, create an instance of it (`client.jvm.com.example.Driver()`), which runs the initializer.
- Anything you allow can do what it does. The built-in `org.apache.spark.` and `scala.` prefixes, and any JAR you add, include classes that read files and start processes. Field access (`get_field`, `get_fields`) reaches non-public fields.

With `allow_all=True` the first point does not apply: every class can be instantiated and every static member used, the listed types included, and only `System.exit`, `System.load`, `System.loadLibrary` and `Class.newInstance` are refused. Those four are no protection against code that means harm (`Runtime.getRuntime().halt(0)` ends the server as well); they catch the call made by mistake.

Do not expose a Gatun socket to code or users you do not trust with the JVM.

### Resource Limits

Each session is limited in what it may hold and send. The limits are system properties of the server, read once when it starts, so they are set per server:

```python
client = connect(extra_jvm_flags=["-Dgatun.limits.max_objects=500000"])
```

| Property (`gatun.limits.`) | Default | Limits |
|---|---|---|
| `max_objects` | 100,000 | Java objects a session holds at once (handles not yet freed). Making one past it is refused; objects that arrive inside a returned collection or as a callback's arguments are counted without being refused, so they can take a session past it, and calls that make an object then fail until handles are freed |
| `max_string_length` | 10,485,760 | Characters (UTF-16 units) of one string or decimal sent from Python; bytes of one big integer |
| `max_list_entries` | 100,000 | Entries of one list or map sent from Python |
| `max_nesting_depth` | 100 | Lists and maps inside each other, sent from Python |
| `max_batch_size` | 10,000 | Commands in one `batch()` |
| `max_method_calls` / `max_create_objects` | 10,000 | Entries of one `invoke_methods` / `create_objects` |
| `max_arrow_bytes` | 1 GB | Arrow memory charged to a session: tables made by `arrow_to_java` until their handles are freed, and the payload arena since its last reset. The data of a lent table (`copy=False`) is not charged |
| `max_sessions` | 0 (no limit) | Sessions the server serves at once |
| `session_idle_timeout_ms` | 0 (none) | Time without activity after which a session is closed. Activity is a request ending on any of the client's channels, and an object or Arrow memory taken or released |

The string, list and nesting limits apply to what Python sends: arguments, values set into fields, and what a callback answers with. The length of `bytes` and of an array is not checked against them, though each element of a `String` or `Object` array is checked like any other argument. Values Java returns are not checked against them either; they are bounded by the memory size and the largest message a session may receive. A list of a million numbers that a Java method builds comes back whole, and the same list passed back to Java is refused unless `max_list_entries` is raised.

## Supported Types

| Python | Java |
|--------|------|
| `int` | `int`, `long`; `java.math.BigInteger` when it does not fit in 64 bits |
| `float` | `double` |
| `bool` | `boolean` |
| `decimal.Decimal` | `java.math.BigDecimal`, with the same digits and scale |
| `str` | `String` |
| `list` | `List` (ArrayList) |
| `dict` | `Map` (HashMap) |
| `bytes` | `byte[]` |
| `JavaArray` | Primitive arrays (`int[]`, `double[]`, etc.) |
| `pyarrow.Array` | Typed arrays (`int32` → `int[]`, `int64` → `long[]`, `float64` → `double[]`, `bool` → `boolean[]`, `string` → `String[]`) |
| `array.array` | `int[]` (`'i'`), `long[]` (`'q'`), `double[]` (`'d'`) |
| `datetime.datetime`, `date`, `time` | `java.sql.Timestamp`, `Date`, `Time` (as arguments; a naive datetime is local time) |
| `None` | `null` |
| `JavaObject` | Object reference (including Object arrays) |
| numpy scalars and other `numbers.Integral` / `numbers.Real` values | as `int` / `float`; `numpy.bool_` as `bool` |

### Big Numbers

A Python `int` that does not fit in 64 bits arrives as a `BigInteger`, and a `decimal.Decimal` as a `BigDecimal` with the same digits and the same scale. They come back the same way: a `BigInteger` is a Python `int` and a `BigDecimal` a `Decimal`, as return values, as fields and inside lists and maps.

```python
from decimal import Decimal

BigInteger = client.jvm.java.math.BigInteger    # needs allowed_classes, or allow_all=True
BigDecimal = client.jvm.java.math.BigDecimal

BigInteger("1").shiftLeft(70)              # 1180591620717411303424, an int
BigInteger("1").add(2**70)                 # 1180591620717411303425
BigDecimal("1.50").add(Decimal("0.25"))    # Decimal('1.75')
```

- An `int` that fits in 64 bits travels as an `int` or `long`, as before. Where the parameter is a `BigInteger` or `BigDecimal` it is converted, so `big.add(5)` works like `big.add(2**70)`. A `float` is never taken for a `BigDecimal`: pass a `Decimal`.
- `Decimal("NaN")` and the infinities raise `ValueError`: `BigDecimal` has neither. `Decimal("-0")` arrives as `0`.
- To get the Java object instead of the value, construct it (`BigDecimal("1.50")` is a `JavaObject`) or ask for it: `client.invoke_method(dec, "add", x, return_object_ref=True)`. `invoke_static_method`, `get_field`, `get_static_field` and a batch's `call`, `call_static` and `get_field` take the same argument. It works for every value that is otherwise converted, a `String` or a `List` included.
- numpy is not needed and not imported. Its scalars are sent as what they say they are: `numpy.int64(5)` as `5`, `numpy.float32(1.5)` as `1.5`. `numpy.bool_` registers with neither number class and is known by its dtype: it is sent as a `bool`, and so is a 0-d array of bools.

### Converters for Your Own Types

A converter turns every argument of one of your types into something Gatun can send. It runs before the call is built, so it may call Java:

```python
from decimal import Decimal

class Money:
    def __init__(self, amount, currency):
        self.amount, self.currency = amount, currency

def to_java(money):
    return client.jvm.java.util.AbstractMap.SimpleEntry(money.currency, Decimal(money.amount))

client.register_converter(Money, to_java)

prices = client.jvm.java.util.ArrayList()
prices.add(Money("9.99", "EUR"))            # arrives as the SimpleEntry
prices.add([Money("1.50", "USD")])          # also inside a list, a tuple or a dict
prices.get(0).getValue()                    # Decimal('9.99')
```

- A converter is used for its type and for subclasses; the nearest registered class wins. It applies to the arguments of single calls, batches and the vectorized calls, in both clients. With the async client it may be a coroutine function.
- What it returns is sent as it is, without another round of conversion, and the calls a converter makes while it converts a value do not convert that type again: a converter for `dict` can pass the dict itself to `LinkedHashMap(d)`.
- `int`, `float`, `bool`, `str`, `None` and Java objects take no converter. Any other type does, including one Gatun already knows (`dict`, `datetime`).
- Return values, and the values a callback returns, are not converted. `client.unregister_converter(Money)` removes one.

### Large Values

A string, a `bytes`, a list or a dict of any size is an ordinary argument and an ordinary return value, up to the size of the shared memory the client connected with (`memory=`, 16 MB by default):

```python
client = connect(memory="64MB")

text = "x" * 5_000_000
builder = client.jvm.java.lang.StringBuilder(text)   # 5 MB as an argument
builder.toString() == text                           # and as a return value

client.max_message_size                              # 67108864: the memory size
```

- A command or a response of up to 64 KB is written to shared memory, as before. A larger one follows its length on the socket. Nothing else changes for the caller, and it holds for single calls, a batch (its commands taken together, and its results), the vectorized calls, callback arguments and results, both clients, and calls made inside a callback.
- Beyond `max_message_size` a command raises `PayloadTooLargeError` before it is sent, and a response arrives as that error; the client stays usable. Connect with more memory, split the call, or move the data as Arrow.
- The server's limits on what Python sends apply as they did: 10 MB per string (`gatun.limits.max_string_length`) and 100,000 entries per list or map (`gatun.limits.max_list_entries`) by default, raised with `extra_jvm_flags=["-Dgatun.limits.max_string_length=67108864"]`. What Java returns is bounded by the memory size alone.
- A large value takes no room in the payload zone, so it cannot disturb an Arrow table that is lent or read in place. For bulk data Arrow remains several times faster: a value travels through the socket and is decoded element by element.
- A Java stack trace arrives whole. It is shortened only past a megabyte, or a quarter of the memory if that is less.

## Exception Handling

Java exceptions are mapped to Python exceptions:

```python
from gatun import (
    connect,
    JavaException,
    JavaSecurityException,
    JavaIllegalArgumentException,
    JavaNoSuchMethodException,
    JavaClassNotFoundException,
    JavaNullPointerException,
    JavaIndexOutOfBoundsException,
    JavaNumberFormatException,
)

client = connect()

try:
    client.jvm.java.lang.Integer.parseInt("not_a_number")
except JavaNumberFormatException as e:
    print(f"Parse error: {e}")
```

`JavaNoSuchFieldException` and `JavaRuntimeException` are mapped the same way, a Java `InterruptedException` arrives as `CancelledException` (see [Cancelling a Request](#cancelling-a-request)), and any other Java exception as the base `JavaException`.

A `JavaException` has `java_class`, `message` and `stack_trace` (which is also its `str()`), and `java_exception`: the exception that was thrown, as a Java object.

```python
try:
    client.jvm.java.lang.Integer.parseInt("not_a_number")
except JavaException as e:
    thrown = e.java_exception
    thrown.getMessage()                        # 'For input string: "not_a_number"'
    thrown.getCause()                          # None here; a JavaObject when there is one
    thrown.getStackTrace()[0].getMethodName()  # 'forInputString'
    client.is_instance_of(thrown, "java.lang.IllegalArgumentException")  # True
```

- Methods of the exception's own class work as well (`thrown.getErrorCode()` on an `SQLException`), and it can be passed back to Java like any object.
- It is an object of the session and is released when the Python exception is collected. To use it for longer, keep the exception or the object.
- It is there for single calls, each failed result of a batch, the vectorized calls, both clients, and calls made inside a callback. It is `None` for an exception that was not thrown in Java (`JavaException(...)` made in Python), and when the session is at `gatun.limits.max_objects`: the error then arrives without it.

## Architecture

Gatun uses a client-server architecture with shared memory for high-performance IPC:

```
┌───────────────────────────────────────────────────────────────┐
│                        Python Client                          │
│  ┌─────────────┐  ┌─────────────┐  ┌───────────────────────┐  │
│  │ GatunClient │  │ AsyncClient │  │    BridgeAdapter      │  │
│  └──────┬──────┘  └──────┬──────┘  └───────────┬───────────┘  │
│         └────────────────┼─────────────────────┘              │
│                          ▼                                    │
│  ┌─────────────────────────────────────────────────────────┐  │
│  │              FlatBuffers Serialization                  │  │
│  └─────────────────────────────────────────────────────────┘  │
└──────────────────────────┬────────────────────────────────────┘
                           │ Unix Domain Socket (length prefix)
                           │ + Shared Memory (command/response)
┌──────────────────────────▼────────────────────────────────────┐
│                         Java Server                           │
│  ┌─────────────────────────────────────────────────────────┐  │
│  │                     GatunServer                         │  │
│  │  - Command dispatch (create, invoke, field access)      │  │
│  │  - Object registry and session management               │  │
│  │  - Security allowlist enforcement                       │  │
│  └─────────────────────────────────────────────────────────┘  │
│  ┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐  │
│  │ ReflectionCache │ │ MethodResolver  │ │ ArrowHandler    │  │
│  │ - Method cache  │ │ - Overload res. │ │ - Arrow IPC     │  │
│  │ - Constructor   │ │ - Varargs       │ │ - Zero-copy     │  │
│  │ - Field cache   │ │ - Type compat.  │ │                 │  │
│  └─────────────────┘ └─────────────────┘ └─────────────────┘  │
└───────────────────────────────────────────────────────────────┘
```

### Communication Flow

1. Python serializes command to FlatBuffers, writes to shared memory
2. Length prefix sent over Unix socket signals Java to process
3. Java reads command from shared memory, executes, writes response
4. Response length sent back over socket
5. Python reads response from shared memory

A command or a response larger than its 64 KB zone is not written to shared memory: its bytes follow the length on the socket ([Large Values](#large-values)).

Each thread of a client that calls Java has such a connection of its own, with its own shared memory and its own thread on the server ([Threads](#threads)). They share the session: the objects, the limits and the callbacks.

### Memory Layout

```
Offset 0          64KB                            size-64KB        size
   │               │                                  │              │
   ▼               ▼                                  ▼              ▼
   ┌───────────────┬──────────────────────────────────┬──────────────┐
   │ Command Zone  │         Payload Zone             │Response Zone │
   │ (Python→Java) │  [First half]  │  [Second half]  │ (Java→Python)│
   └───────────────┴───────────────┴──────────────────┴──────────────┘
                    send_arrow_      get_arrow_data
                    buffers
```

The halves belong to `send_arrow_buffers()` and `get_arrow_data()`. The other Arrow transfers write wherever the zone is free.

See [Arrow Memory Architecture](#arrow-memory-architecture) for details on the epoch-based memory safety model.

## Development

```bash
JAVA_HOME=/opt/homebrew/opt/openjdk uv sync  # Install deps and build JAR
uv run pytest              # Run tests
uv run ruff check .        # Lint
uv run ruff format .       # Format
```

The `uv sync` command automatically builds the Java JAR via the custom build backend. It needs `flatc` (the FlatBuffers compiler) on `PATH` and a JDK 22+; `JAVA_HOME` is also used at runtime to find `java`.

### Project Structure

```
gatun/
├── src/gatun/             # Python client library
│   ├── client.py          # Sync client
│   ├── async_client.py    # Async client
│   ├── callback_channel.py# The connection Java calls Python on
│   ├── launcher.py        # Server process management
│   ├── bridge.py          # BridgeAdapter interface
│   └── bridge_adapters.py # GatunAdapter
├── tests/                 # pytest suite (exercises the Java server too)
├── benchmarks/            # Benchmark scripts (see docs/benchmarks.md)
├── docs/                  # Benchmark results, BridgeAdapter specification
├── gatun-core/
│   └── src/main/java/org/gatun/server/
│       ├── GatunServer.java       # Main server
│       ├── ReflectionCache.java   # Caching layer
│       ├── MethodResolver.java    # Method resolution
│       ├── ArrowMemoryHandler.java# Arrow transfer
│       ├── ArrowLoans.java        # Tables lent in place
│       ├── ArrowBlockPool.java    # Reuse of large Arrow buffers
│       └── CallbackChannel.java   # The connection Java calls Python on
└── schemas/
    └── commands.fbs       # FlatBuffers protocol schema
```

## License

Apache 2.0
