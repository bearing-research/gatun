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
- **JVM View API**: Pythonic package-style navigation (`client.jvm.java.util.ArrayList`)
- **Spark from Python**: Load Spark's JARs and drive its JVM classes directly; a `BridgeAdapter` interface for porting PySpark
- **Pythonic JavaObjects**: Iteration, indexing, and len() support on Java collections
- **Batch API**: Execute multiple commands in a single round-trip
- **Vectorized APIs**: `invoke_methods`, `create_objects`, `get_fields` for several operations on one target in one round-trip
- **Observability**: Server metrics, structured logging, and JFR events for debugging and monitoring

## Performance

Measured on Linux (8 vCPUs of a shared AMD EPYC 7713, OpenJDK 22.0.2, Python 3.13.12); every figure is a median. [docs/benchmarks.md](docs/benchmarks.md) has all the cases, the method, and the commands to run them. Times depend on the machine, and so may the comparison with Py4J.

### Single Calls

A call is a round trip, and on this machine a round trip through Py4J (PySpark's bridge) takes about half the time of one through Gatun. Gatun is ahead where a call carries a collection or returns something large:

| Operation | Gatun | Py4J | |
|-----------|------:|-----:|---|
| Instance method call (`list.size()`) | 149 μs | 77 μs | Py4J 1.9x faster |
| Static method call (`Math.max(10, 20)`) | 291 μs | 204 μs | Py4J 1.4x faster |
| Create an object and drop it (`ArrayList()`) | 359 μs | 88 μs | Py4J 4.1x faster |
| Calls in a loop | 5,500 /s | 9,500 /s | Py4J 1.7x faster |
| Python list of 100 as an argument | 2.1 ms | 10.8 ms | Gatun 5.2x faster |
| `is_instance_of` | 145 μs | 519 μs | Gatun 3.6x faster |
| Return a 16 KB `byte[]` | 193 μs | 42 ms | Gatun 218x faster |

Both bridges were given the same JVM settings and a warm JVM, with classes bound once (`Math = jvm.java.lang.Math`). Creating an object and dropping it costs Gatun a second round trip, to release it.

### Fewer Round Trips

Since the round trip is the cost, sending operations together is what helps most:

| Work | One at a Time | `batch()` | Vectorized Call |
|------|--------------:|----------:|----------------:|
| 10 method calls on one object | 2.05 ms | 809 μs | 677 μs |
| 100 method calls on one object | 19.2 ms | 5.14 ms | 4.28 ms |
| 100 objects created | 16.4 ms | 4.39 ms | 3.85 ms |

### Arrow Data Transfer

Time for the call to return with the table on the other side:

| Method | 16 KB | 1.6 MB | 16 MB | |
|--------|------:|-------:|------:|---|
| `arrow_to_java` | 466 μs | 572 μs | 2.61 ms | Java gets its own copy |
| `arrow_to_java(copy=False)` | 498 μs | 512 μs | 1.29 ms | Java reads in place |
| `send_arrow_buffers` | 466 μs | 499 μs | 1.34 ms | Loaded in place, not visible to Java code |
| `arrow_from_java` | 212 μs | 323 μs | 2.18 ms | Python gets its own copy |
| `arrow_from_java(copy=False)` | 204 μs | 260 μs | 1.06 ms | Python reads in place |

At 128 MiB, `arrow_to_java` takes 24 ms and `arrow_to_java(copy=False)` 12 ms, where an Arrow IPC stream over a Unix socket takes 106 ms.

Next to the other ways of getting 500 integers into Java:

| How | Time |
|-----|-----:|
| 500 `add()` calls | 92 ms |
| 500 `add()` calls in a `batch()` | 24 ms |
| A Python list as a constructor argument | 6.7 ms |
| A `pyarrow.Array` argument (arrives as `int[]`) | 0.25 ms |

### Callbacks

A callback made during a request (a Python comparator in `Collections.sort`) costs about 390 μs. With `connect(callback_timeout=None)` it costs 230 μs: a timeout, 30 seconds by default, has each callback run on a thread of its own. Sorting 1,000 elements took about 8,700 of them and 3.4 seconds, so a comparator in Python is for short lists.

### When to Use Gatun vs Py4J

| Use Case | Recommendation |
|----------|----------------|
| Many small calls, one at a time | Py4J: about twice the calls per second |
| Bulk data | Gatun: Arrow tables through shared memory |
| Collections as arguments, large return values | Gatun |
| Many operations that can be sent together | Gatun: `batch()` and the vectorized calls |

## Installation

```bash
pip install gatun
```

## Requirements

- **Python**: 3.13+
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

- **The thread running your request** (a comparator passed to `Collections.sort`) calls back on the request's own channel. Such a callback cannot call Java (`ReentrancyError`), and neither can a task it awaits: the request is waiting for it on the only channel the call could use. In the sync client, do not make it wait for another *thread* that calls Java on the same client either: that thread waits for the request to finish, and the request is waiting for the callback. The callback timeout ends the wait with `CallbackTimeoutError`; with `callback_timeout=None` nothing does. Work queued by a callback and run after the request is fine.
- **Any other thread** calls on a second connection, which the client opens with its first `register_callback`. Each of these calls runs on a thread of its own (a task of its own in the async client), so several run at the same time, alongside your own code; what they touch has to be safe for that. These callbacks can call Java: create objects, call methods, read and write fields, use the objects they were passed. Their calls travel on the same connection and are run by the Java thread that is waiting for the callback, so they do not wait for the client's own channel, which may be busy with a long request (a stream's `awaitTermination()`) or with the very request that is waiting for the callback (`executor.submit(callback).get()`).

  They can also move Arrow tables with `arrow_to_java` and `arrow_from_java` (a `foreachBatch` function that fetches its batch, for example). The client's shared memory belongs to its own channel, so each such call gets shared memory of its own, as large as the client's, when it first moves a table; the limit on one table is the same `memory` setting. A table read in place there (`copy=False`) is good until the callback's next transfer.

  `batch()`, the vectorized calls (`invoke_methods`, `create_objects`, `get_fields`), `register_callback` and `get_metrics` work there too. What such a callback cannot do is whatever goes through the client's own shared memory: the other Arrow transfers (`send_arrow_table`, `send_arrow_buffers`, `get_arrow_data`, `arrow_context`) raise `ReentrancyError`. A command or a result is limited to 64KB, a batch's results taken together; a table is not, since it does not travel in them. In the sync client a plain thread the callback starts does not inherit the route; its calls take their turn on the client's channel, as do those of a callback Java has stopped waiting for because it timed out.

A callback stays registered, and its function alive, for as long as it can still be called: until `unregister_callback`, the end of the session, or the moment nothing holds the proxy any more. That last one means the object `register_callback` returned has been garbage collected in Python and Java code has let go of the proxy too; the JVM's garbage collector then reports it and the client drops the function, some time later. So keep the returned object for as long as the callback should work when the Java code you give it to holds it only weakly (a registry of weak listeners). A proxy called after that throws `IllegalStateException` in the calling Java thread.

### Cancelling a Request

`client.cancel()` interrupts the request that is running on the client, from another thread:

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

### Threads

A `GatunClient` can be shared between threads. Requests are serialized: one runs at a time, and the others wait. For parallel work, give each thread its own client (`GatunClient(client.socket_path)` connects another session to the same server).

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
| `batch` | Mixed operations on **different objects** |

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
client.invoke_method(obj.object_id, "add", "item")
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

### What the Allowlist Is and Is Not

The server trusts the operating-system user it runs as, and nobody else:

- Its socket and shared memory files are owner-only (`0600`), and it refuses connections from other users.
- Each session reaches only the objects it created. Another session cannot call, read, free or pass an object by guessing its ID.

Inside that boundary, the allowlist is a guardrail against accidents and against class names that come from somewhere you do not control. It is not a sandbox:

- It decides which classes can be instantiated and which static members can be used. Instance methods are open on whatever an allowed call returns, with these exceptions: objects that would undo the list (`java.lang.reflect.Method` / `Constructor` / `Field`, method handles, class loaders, `Runtime`, `Process`, `ProcessBuilder`, `Thread`, `Module`) cannot be used from Python unless you put their type in `allowed_classes`, and `System.exit`, `System.load`, `System.loadLibrary` and `Class.newInstance` are always refused.
- Naming a class does not run its static initializer, and neither does `Class.forName` called from Python. A class is initialized when something allowed first uses it.
- Anything you allow can do what it does. The built-in `org.apache.spark.` and `scala.` prefixes, and any JAR you add, include classes that read files and start processes. Field access (`get_field`, `get_fields`) reaches non-public fields.

Do not expose a Gatun socket to code or users you do not trust with the JVM.

## Supported Types

| Python | Java |
|--------|------|
| `int` | `int`, `long` |
| `float` | `double` |
| `bool` | `boolean` |
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
