# PySpark Bridge Adapter Specification

## Overview

This document defines the minimal bridge contract between PySpark and a JVM backend (Py4J or Gatun).
Instead of making Gatun emulate Py4J, we define a clean abstraction that both can implement.

## BridgeAdapter

`gatun.bridge.BridgeAdapter` is an abstract base class: an implementation subclasses it, and
every method below is abstract. `GatunAdapter` (`gatun.bridge_adapters`) is the one
implementation in this repository; there is no Py4J adapter yet.

```python
from abc import ABC
from typing import Any, Protocol, runtime_checkable

@runtime_checkable
class JVMRef(Protocol):
    """Opaque reference to a JVM object."""

    @property
    def object_id(self) -> int:
        """Unique identifier for this object in the JVM."""
        ...

class BridgeAdapter(ABC):
    """Minimal bridge contract for PySpark <-> JVM communication."""

    # === Object Lifecycle ===

    def new(self, class_name: str, *args: Any) -> JVMRef:
        """Create a new JVM object.

        Args:
            class_name: Fully qualified class name (e.g., "java.util.ArrayList")
            *args: Constructor arguments (Python types auto-converted)

        Returns:
            Reference to the created object
        """
        ...

    def close(self) -> None:
        """Close the bridge and release all resources."""
        ...

    def detach(self, ref: JVMRef) -> None:
        """Prevent automatic cleanup of this object reference.

        Used when passing objects to long-lived Java structures.
        """
        ...

    # === Method Calls ===

    def call(self, ref: JVMRef, method: str, *args: Any) -> Any:
        """Call an instance method on a JVM object.

        Args:
            ref: Object reference
            method: Method name
            *args: Method arguments

        Returns:
            Method result (JVMRef for objects, Python types for primitives)
        """
        ...

    def call_static(self, class_name: str, method: str, *args: Any) -> Any:
        """Call a static method on a JVM class.

        Args:
            class_name: Fully qualified class name
            method: Static method name
            *args: Method arguments

        Returns:
            Method result
        """
        ...

    # === Field Access ===

    def get_field(self, ref: JVMRef, name: str) -> Any:
        """Get an instance field value."""
        ...

    def set_field(self, ref: JVMRef, name: str, value: Any) -> None:
        """Set an instance field value."""
        ...

    def get_static_field(self, class_name: str, name: str) -> Any:
        """Get a static field value."""
        ...

    def set_static_field(self, class_name: str, name: str, value: Any) -> None:
        """Set a static field value."""
        ...

    # === Type Checking ===

    def is_instance_of(self, ref: JVMRef, class_name: str) -> bool:
        """Check if object is instance of class (supports interfaces)."""
        ...

    # === Arrays ===

    def new_array(self, element_class: str, length: int) -> JVMRef:
        """Create a new JVM array.

        Args:
            element_class: Element type (e.g., "java.lang.String", "int", "long")
            length: Array length

        Returns:
            Reference to the array (supports indexing via array_get/array_set)
        """
        ...

    def array_get(self, array_ref: JVMRef, index: int) -> Any:
        """Get element at index from JVM array."""
        ...

    def array_set(self, array_ref: JVMRef, index: int, value: Any) -> None:
        """Set element at index in JVM array."""
        ...

    def array_length(self, array_ref: JVMRef) -> int:
        """Get length of JVM array."""
        ...

    # === Callbacks ===

    def register_callback(self, handler: Any, interface: str) -> JVMRef:
        """Make a JVM object that implements a Java interface in Python.

        Args:
            handler: A callable, which is given the arguments of whichever
                method of the interface the JVM calls and returns its result.
                Or an object that is not callable, for an interface with
                several methods: each call goes to its method of the same name.
            interface: Fully qualified interface name (e.g., "java.util.Comparator")

        Returns:
            Reference to an object that implements the interface
        """
        ...

    def unregister_callback(self, ref: JVMRef) -> None:
        """Stop serving a callback and let go of its handler."""
        ...

    # === JVM View ===

    @property
    def jvm(self) -> "JVMView":
        """Get JVM view for navigating classes.

        Allows: bridge.jvm.java.util.ArrayList()
        Instead of: bridge.new("java.util.ArrayList")
        """
        ...

    def java_import(self, package: str) -> None:
        """Import package for shorter class names.

        Args:
            package: Package path with optional wildcard (e.g., "java.util.*")
        """
        ...
```

## Type Conversion Rules

### Python → JVM

| Python Type | JVM Type |
|-------------|----------|
| `int` (fits in 32 bits) | `int` |
| `int` (larger) | `long` |
| `float` | `double` |
| `bool` | `boolean` |
| `str` | `String` |
| `bytes` | `byte[]` |
| `None` | `null` |
| `list` | `ArrayList` |
| `dict` | `HashMap` |
| `JVMRef` | (pass through) |
| `datetime.date` | `java.sql.Date` |
| `datetime.datetime` | `java.sql.Timestamp` |

### JVM → Python

| JVM Type | Python Type |
|----------|-------------|
| `int`, `short`, `byte` | `int` |
| `long` | `int` |
| `float`, `double` | `float` |
| `boolean` | `bool` |
| `char` | `str` |
| `String` | `str` |
| `byte[]` | `bytes` |
| `null` | `None` |
| `List`, `ArrayList` | `list` |
| `Map`, `HashMap` | `dict` |
| Arrays (primitive) | `list` |
| Arrays (object) | `JVMRef` |
| Other objects | `JVMRef` |

## Callbacks

`register_callback` is how the JVM calls Python: a foreachBatch function, a streaming query
listener, a comparator. The rules an implementation must keep:

- The returned reference is an ordinary JVM object that implements the interface. It can be
  passed to any method that takes one and stored by JVM code.
- A callable handler answers every abstract method of the interface. Any other handler
  answers each call with its method of the same name, and a call to a method it lacks fails in
  the JVM caller with an exception that names the method. A default method of the interface
  runs in the JVM unless such a handler has a method of that name, which then overrides it (a
  listener interface whose methods all have empty default bodies is implemented that way). The
  methods of `java.lang.Object` run in the JVM and do not reach Python.
- Arguments and the return value follow the type conversion rules above. JVM objects among the
  arguments arrive as `JVMRef`.
- An exception raised by the handler is thrown in the JVM thread that made the call. If it
  reaches Python again through the call that led to it, it is a `JavaException`.
- The JVM may call from any thread and at any time, also while no call from Python is running
  and several at once. Handlers must be safe for that.
- A callback made by a thread other than the one serving the current call from Python may
  call back into the JVM through the bridge. Whether one made by that thread itself may do so
  is up to the implementation (Py4J allows it; Gatun raises `ReentrancyError`).
- An implementation may limit how long a handler runs. `GatunAdapter` does: after
  `callback_timeout` seconds (30 by default) the JVM caller gets an exception while the
  handler is still running. Pass `callback_timeout=None` when handlers run long, as a
  foreachBatch function does.
- The handler stays registered, and referenced, for as long as the JVM can call it: until
  `unregister_callback` or `close()`, or until nothing refers to the callback object any
  more, in Python or in the JVM. JVM code may hold the object indefinitely, but an
  implementation may let go of a handler whose object the JVM has garbage collected (Gatun
  does), so keep the returned reference if the JVM holds the object only weakly. After
  `unregister_callback` a call fails in the JVM with an exception and does not reach Python.
- `unregister_callback` takes any reference to the callback object: the one
  `register_callback` returned, or one that came back from the JVM later (read out of a
  collection of listeners, for example).

With Py4J the handler corresponds to an object with `class Java: implements = [interface]`
passed through the gateway's callback server.

## Exception Handling

```python
class JavaException(Exception):
    """Base class for Java exceptions raised in Python."""

    def __init__(self, java_class: str, message: str, stack_trace: str = ""):
        self.java_class = java_class
        self.message = message
        self.stack_trace = stack_trace
        super().__init__(stack_trace or f"{java_class}: {message}")
```

Subclasses:
- `JavaSecurityException`
- `JavaIllegalArgumentException`
- `JavaNoSuchMethodException`
- `JavaNoSuchFieldException`
- `JavaClassNotFoundException`
- `JavaNullPointerException`
- `JavaIndexOutOfBoundsException`
- `JavaNumberFormatException`
- `JavaRuntimeException`

`gatun.bridge` and the package's top level export the same classes, so
`except gatun.bridge.JavaException` catches a Java exception however the call was made: through
an adapter method (`new`, `call`, `call_static`, the field and array methods), through
`bridge.jvm`, or as a method of an object the bridge returned. `str()` of one is the Java
stack trace, whose first line is the class and the message.

## Contract Tests

The required behaviour is defined by `tests/test_bridge_contract.py`. `BridgeContractTests`
there takes any adapter through a `bridge` fixture; an implementation subclasses it and
provides the fixture, as `TestGatunAdapter` does:

```python
class TestGatunAdapter(BridgeContractTests):
    @pytest.fixture
    def bridge(self):
        from gatun.bridge_adapters import GatunAdapter

        adapter = GatunAdapter()
        yield adapter
        adapter.close()
```

It covers object creation, instance and static calls, static fields, `is_instance_of`,
arrays, type conversion in both directions, exceptions, `detach` and `close`, callbacks
(a function, an object with named methods, a default method overridden, a missing method, an
exception in the handler, `unregister_callback`) and the JVM view with `java_import`.

Where implementations may differ, the tests accept either answer. A class that does not
exist is one case: Gatun checks its allowlist before it looks the class up, so the error for
an unknown class is a `JavaSecurityException`, where another bridge would report
`JavaClassNotFoundException`.

## PySpark Integration Points

### Current Usage → Bridge Adapter Migration

| Current Pattern | Bridge Adapter Pattern |
|-----------------|----------------------|
| `sc._jvm.ClassName()` | `bridge.jvm.ClassName()` or `bridge.new("ClassName")` |
| `sc._jvm.Class.staticMethod()` | `bridge.jvm.Class.staticMethod()` or `bridge.call_static()` |
| `obj.method(args)` | `bridge.call(obj, "method", args)` |
| `gateway.new_array(cls, n)` | `bridge.new_array("cls", n)` |
| `is_instance_of(obj, cls)` | `bridge.is_instance_of(obj, "cls")` |
| `java_import(jvm, "pkg.*")` | `bridge.java_import("pkg.*")` |
| Object with `class Java: implements = ["Iface"]` | `bridge.register_callback(obj, "Iface")` |
| `ensure_callback_server_started(gw)` | Not needed: the adapter serves callbacks once one is registered |

### Key Files to Modify in PySpark

1. `pyspark/java_gateway.py` - Entry point, create adapter
2. `pyspark/core/context.py` - SparkContext uses gateway
3. `pyspark/sql/session.py` - SparkSession uses _jvm
4. `pyspark/sql/types.py` - Type conversions
5. `pyspark/sql/utils.py` - to_java_array function
6. `pyspark/ml/wrapper.py` - ML wrappers use new_array
7. `pyspark/ml/pipeline.py` - Pipeline stages use new_array
8. `pyspark/sql/utils.py` - `ForeachBatchFunction`, a callback object
9. `pyspark/sql/streaming/listener.py` - the streaming query listener, a callback object with several methods
