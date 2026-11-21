"""Bridge adapter implementations.

This module provides BridgeAdapter implementations for different backends.
"""

from __future__ import annotations

from typing import Any, ClassVar, cast

from gatun import connect as gatun_connect
from gatun.bridge import BridgeAdapter, JVMRef, JVMView
from gatun.client import (
    DEFAULT_CALLBACK_TIMEOUT,
    DEFAULT_SOCKET_TIMEOUT,
    GatunClient,
    JavaObject,
)
from gatun.client import (
    JVMView as GatunJVMView,
)
from gatun.client import (
    java_import as gatun_java_import,
)


class GatunAdapter(BridgeAdapter):
    """BridgeAdapter implementation backed by Gatun.

    This adapter wraps Gatun's native client and exposes it through
    the standard BridgeAdapter interface.
    """

    def __init__(
        self,
        memory: str = "64MB",
        classpath: list[str] | None = None,
        allowed_classes: list[str] | None = None,
        allowed_prefixes: list[str] | None = None,
        socket_timeout: float | None = DEFAULT_SOCKET_TIMEOUT,
        callback_timeout: float | None = DEFAULT_CALLBACK_TIMEOUT,
        allow_all: bool | None = None,
        server_jar: str | None = None,
    ):
        """Create a new GatunAdapter.

        Args:
            memory: Shared memory size (e.g., "64MB", "256MB")
            classpath: Additional JAR files for the classpath
            allowed_classes: Class names allowed beyond the server's built-in list
            allowed_prefixes: Package prefixes allowed beyond the built-in list
            socket_timeout: Seconds to wait for one call's response; None for no limit
            callback_timeout: Seconds a Python callback may run; None for no limit
            allow_all: True lifts the class allowlist: every class can be used
            server_jar: "thin" or "full" to choose the server JAR; by default the
                        launcher takes the thin one on a classpath with its own Arrow
        """
        # connect() launches the server and returns a connected client
        self._client: GatunClient | None = gatun_connect(
            memory=memory,
            classpath=classpath,
            allowed_classes=allowed_classes,
            allowed_prefixes=allowed_prefixes,
            socket_timeout=socket_timeout,
            callback_timeout=callback_timeout,
            allow_all=allow_all,
            server_jar=server_jar,
        )

    # === Object Lifecycle ===

    def new(self, class_name: str, *args: Any) -> JVMRef:
        """Create a new JVM object."""
        assert self._client is not None, "Bridge is closed"
        result: JVMRef = self._client.create_object(class_name, *args)
        return result

    def close(self) -> None:
        """Close the bridge and release all resources."""
        if self._client:
            self._client.close()
            self._client = None

    def detach(self, ref: JVMRef) -> None:
        """Prevent automatic cleanup of this object reference."""
        if isinstance(ref, JavaObject):
            ref.detach()

    # === Method Calls ===

    def call(self, ref: JVMRef, method: str, *args: Any) -> Any:
        """Call an instance method on a JVM object."""
        obj_id = ref.object_id if hasattr(ref, "object_id") else ref
        return self._client.invoke_method(obj_id, method, *args)

    def call_static(self, class_name: str, method: str, *args: Any) -> Any:
        """Call a static method on a JVM class."""
        return self._client.invoke_static_method(class_name, method, *args)

    # === Field Access ===

    def get_field(self, ref: JVMRef, name: str) -> Any:
        """Get an instance field value."""
        obj_id = ref.object_id if hasattr(ref, "object_id") else ref
        return self._client.get_field(obj_id, name)

    def set_field(self, ref: JVMRef, name: str, value: Any) -> None:
        """Set an instance field value."""
        obj_id = ref.object_id if hasattr(ref, "object_id") else ref
        self._client.set_field(obj_id, name, value)

    def get_static_field(self, class_name: str, name: str) -> Any:
        """Get a static field value."""
        return self._client.get_static_field(class_name, name)

    def set_static_field(self, class_name: str, name: str, value: Any) -> None:
        """Set a static field value."""
        self._client.set_static_field(class_name, name, value)

    # === Type Checking ===

    def is_instance_of(self, ref: JVMRef, class_name: str) -> bool:
        """Check if object is instance of class."""
        return self._client.is_instance_of(ref, class_name)

    # === Arrays ===

    # Mapping of primitive type names to their wrapper class TYPE field
    _PRIMITIVE_TYPES: ClassVar[dict[str, tuple[str, str]]] = {
        "int": ("java.lang.Integer", "TYPE"),
        "long": ("java.lang.Long", "TYPE"),
        "double": ("java.lang.Double", "TYPE"),
        "float": ("java.lang.Float", "TYPE"),
        "boolean": ("java.lang.Boolean", "TYPE"),
        "byte": ("java.lang.Byte", "TYPE"),
        "short": ("java.lang.Short", "TYPE"),
        "char": ("java.lang.Character", "TYPE"),
    }

    def new_array(self, element_class: str, length: int) -> JVMRef:
        """Create a new JVM array."""
        # Get the Class object for the element type
        if element_class in self._PRIMITIVE_TYPES:
            wrapper_class, field = self._PRIMITIVE_TYPES[element_class]
            class_obj = self._client.get_static_field(wrapper_class, field)
        else:
            class_obj = self._client.invoke_static_method(
                "java.lang.Class", "forName", element_class
            )

        # Create the array using reflection
        # Use return_object_ref=True to get an ObjectRef instead of auto-converted array
        return cast(
            JVMRef,
            self._client.invoke_static_method(
                "java.lang.reflect.Array",
                "newInstance",
                class_obj,
                length,
                return_object_ref=True,
            ),
        )

    def array_get(self, array_ref: JVMRef, index: int) -> Any:
        """Get element at index from JVM array."""
        return self._client.invoke_static_method(
            "java.lang.reflect.Array", "get", array_ref, index
        )

    def array_set(self, array_ref: JVMRef, index: int, value: Any) -> None:
        """Set element at index in JVM array."""
        self._client.invoke_static_method(
            "java.lang.reflect.Array", "set", array_ref, index, value
        )

    def array_length(self, array_ref: JVMRef) -> int:
        """Get length of JVM array."""
        return cast(
            int,
            self._client.invoke_static_method(
                "java.lang.reflect.Array", "getLength", array_ref
            ),
        )

    # === Callbacks ===

    def register_callback(self, handler: Any, interface: str) -> JVMRef:
        """Make a JVM object that implements a Java interface in Python."""
        assert self._client is not None, "Bridge is closed"
        result: JVMRef = self._client.register_callback(handler, interface)
        return result

    def unregister_callback(self, ref: JVMRef) -> None:
        """Stop serving a callback and let go of its handler."""
        assert self._client is not None, "Bridge is closed"
        self._client.unregister_callback(cast(Any, ref))

    # === JVM View ===

    @property
    def jvm(self) -> JVMView:
        """Get JVM view for navigating classes."""
        assert self._client is not None, "Bridge is closed"
        return _GatunJVMViewAdapter(self._client.jvm, self._client)

    def java_import(self, package: str) -> None:
        """Import package for shorter class names."""
        gatun_java_import(self._client.jvm, package)


class _GatunJVMViewAdapter(JVMView):
    """Adapter that wraps Gatun's JVMView to implement the bridge JVMView."""

    def __init__(self, jvm_view: GatunJVMView, client: GatunClient):
        self._jvm_view = jvm_view
        self._client = client

    def __getattr__(self, name: str) -> _GatunJVMViewAdapter | JVMRef | Any:
        """Navigate to package, class, or access static member."""
        result = getattr(self._jvm_view, name)

        # A JavaObject is a value (a static field that holds an object). It is tested
        # first because it answers every attribute name, "_path" included.
        if isinstance(result, JavaObject):
            return result

        # If it's a JVMView node, wrap it
        if hasattr(result, "_path"):
            return _GatunJVMViewAdapter(result, self._client)

        # Otherwise return the value (primitives, strings, etc.)
        return result

    def __call__(self, *args: Any) -> JVMRef:
        """Create instance of this class."""
        return cast(JVMRef, self._jvm_view(*args))  # type: ignore[operator]
