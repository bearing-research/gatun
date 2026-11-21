"""Choosing a Java method for Python arguments, and converting the arguments."""

import os
import subprocess

import pytest

from gatun import (
    JavaArray,
    JavaIllegalArgumentException,
    TypeHint,
    connect,
)


class TestPythonNumbers:
    """Python has one int and one float; Java has several of each."""

    def test_float_for_a_float_parameter(self, client):
        assert client.jvm.java.lang.Float.valueOf(1.5) == 1.5
        assert client.jvm.java.lang.Float.toString(1.5) == "1.5"

    def test_int_for_a_short_or_byte_parameter(self, client):
        assert client.jvm.java.lang.Short.valueOf(7) == 7
        assert client.jvm.java.lang.Byte.valueOf(-3) == -3

    @pytest.mark.parametrize(
        ("class_name", "value", "java_type"),
        [
            ("java.lang.Short", 70_000, "short"),
            ("java.lang.Short", -32_769, "short"),
            ("java.lang.Byte", 300, "byte"),
        ],
    )
    def test_int_that_does_not_fit_is_refused(
        self, client, class_name, value, java_type
    ):
        """It must not wrap around to a different number."""
        with pytest.raises(
            JavaIllegalArgumentException, match=f"does not fit in a Java {java_type}"
        ):
            client.invoke_static_method(class_name, "valueOf", value)

    def test_wider_overloads_are_still_preferred(self, client):
        """Narrowing is the last resort, behind every exact or widening match."""
        Math = client.jvm.java.lang.Math
        assert Math.max(1, 2) == 2
        assert isinstance(Math.max(1, 2), int)
        assert Math.abs(-2.5) == 2.5
        assert client.jvm.java.lang.String.valueOf(1.5) == "1.5"
        assert client.jvm.java.lang.String.valueOf(7) == "7"

    def test_reference_overload_wins_for_a_boxed_argument(self, client):
        """`remove(Object)`, not `remove(int)`, when the hint says Integer."""
        items = client.jvm.java.util.ArrayList()
        for value in (5, 6, 7):
            items.add(value)

        assert items.remove(0) == 5  # by index
        assert items.remove(TypeHint(7, "java.lang.Integer")) is True  # by value
        assert list(items) == [6]

    def test_mixed_ints_in_a_list_are_one_type(self, client):
        """A list is all Integer or all Long, never a mix decided value by value."""
        boxed = client.jvm.java.util.ArrayList([1, 2**40])
        assert client.jvm.java.lang.String.valueOf(boxed) == "[1, 1099511627776]"
        first = boxed.get(0)
        assert first == 1
        # Long.equals(Integer) is false in Java: a mix made such lookups miss
        assert boxed.contains(TypeHint(1, "java.lang.Long")) is True


_SUPERTYPES = """
import java.io.Serializable;

public class Supertypes {
  public final String made;

  public Supertypes(Number n) { made = describe(n); }

  static String describe(Object o) { return o.getClass().getSimpleName() + ":" + o; }

  public static String number(Number n) { return describe(n); }
  public static String comparable(Comparable<?> c) { return describe(c); }
  public static String serializable(Serializable s) { return describe(s); }
  public static String numbers(Number... ns) { return String.valueOf(ns.length); }

  public static String which(Number n) { return "Number"; }
  public static String which(Object o) { return "Object"; }

  public static String closer(long x) { return "long"; }
  public static String closer(Number n) { return "Number"; }
}
"""


@pytest.fixture(scope="module")
def supertypes(tmp_path_factory):
    out = tmp_path_factory.mktemp("supertypes")
    source = out / "Supertypes.java"
    source.write_text(_SUPERTYPES)
    java_home = os.environ.get("JAVA_HOME")
    javac = os.path.join(java_home, "bin", "javac") if java_home else "javac"
    subprocess.run([javac, "--release", "22", "-d", str(out), str(source)], check=True)
    client = connect(
        memory="16MB", classpath=[str(out)], allowed_classes=["Supertypes"]
    )
    try:
        yield client
    finally:
        client.close()


class TestBoxingToASupertype:
    """Java boxes a primitive and then widens the box: an int reaches a `Number`,
    `Comparable` or `Serializable` parameter as an Integer."""

    def test_number_parameter(self, supertypes):
        Supertypes = supertypes.jvm.Supertypes
        assert Supertypes.number(5) == "Integer:5"
        assert Supertypes.number(2**40) == "Long:1099511627776"
        assert Supertypes.number(2.5) == "Double:2.5"

    def test_interface_parameters(self, supertypes):
        Supertypes = supertypes.jvm.Supertypes
        assert Supertypes.comparable(5) == "Integer:5"
        assert Supertypes.comparable(True) == "Boolean:true"
        assert Supertypes.serializable(2.5) == "Double:2.5"

    def test_constructor_and_varargs(self, supertypes):
        made = supertypes.jvm.Supertypes(7)
        assert supertypes.get_field(made, "made") == "Integer:7"
        assert supertypes.jvm.Supertypes.numbers(1, 2.5, 3) == "3"

    def test_in_a_batch(self, supertypes):
        with supertypes.batch() as b:
            result = b.call_static("Supertypes", "number", 5)
        assert result.get() == "Integer:5"

    def test_the_box_is_preferred_to_object(self, supertypes):
        assert supertypes.jvm.Supertypes.which(5) == "Number"

    def test_a_primitive_parameter_is_preferred_to_the_box(self, supertypes):
        """Java tries widening before boxing: `closer(long)` is chosen for an int."""
        assert supertypes.jvm.Supertypes.closer(5) == "long"


class TestTypeHints:
    def test_hint_converts_the_value(self, client):
        """The hinted type used to pick the overload and then fail to receive it."""
        assert client.jvm.java.lang.Float.valueOf(TypeHint(1.5, "float")) == 1.5
        assert client.jvm.java.lang.Short.valueOf(TypeHint(7, "short")) == 7
        assert client.jvm.java.lang.Long.valueOf(TypeHint(5, "long")) == 5
        assert client.jvm.java.lang.Double.valueOf(TypeHint(2, "double")) == 2.0

    def test_hint_for_a_boxed_type(self, client):
        items = client.jvm.java.util.ArrayList()
        items.add(TypeHint(5, "java.lang.Long"))
        assert items.contains(TypeHint(5, "java.lang.Long")) is True
        assert items.contains(5) is False  # an Integer is not that Long

    def test_hint_out_of_range_is_refused(self, client):
        with pytest.raises(JavaIllegalArgumentException, match="does not fit"):
            client.jvm.java.lang.Short.valueOf(TypeHint(70_000, "short"))

    def test_unknown_type_in_a_hint_is_an_error(self, client):
        """A misspelt type used to be dropped, and some overload chosen anyway."""
        items = client.jvm.java.util.ArrayList()
        with pytest.raises(
            JavaIllegalArgumentException, match="unknown class: no.such.Type"
        ):
            items.add(TypeHint(1, "no.such.Type"))
        assert items.size() == 0


class TestVarargs:
    def test_array_passed_for_the_varargs_parameter(self, client):
        """An array given as the last argument is the varargs array itself."""
        String = client.jvm.java.lang.String
        packed = JavaArray(["a", "b"], element_type="Object")
        assert String.format("%s-%s", packed) == "a-b"
        assert String.format("%s-%s", "a", "b") == "a-b"

    def test_typed_array_for_generic_varargs(self, client):
        names = JavaArray(["a", "b"], element_type="String")
        assert client.jvm.java.util.Arrays.asList(names) == ["a", "b"]

    def test_no_values_for_the_varargs_parameter(self, client):
        assert client.jvm.java.lang.String.format("plain") == "plain"


class TestMethodsOfNonPublicClasses:
    """The object's class is often private; the method is public on an interface."""

    def test_no_argument_method_on_a_private_implementation(self, client):
        items = client.jvm.java.util.ArrayList()
        for value in (1, 2, 3):
            items.add(value)

        # stream() returns a package-private ReferencePipeline
        assert items.stream().count() == 3
        # Arrays.asList's result is a private nested class; so is its iterator
        assert client.jvm.java.util.Collections.unmodifiableList(items).size() == 3

    def test_static_method_called_through_an_instance(self):
        client = connect(memory="16MB", allowed_classes=["java.util.Optional"])
        try:
            items = client.jvm.java.util.ArrayList()
            items.add("x")
            first = items.stream().findFirst()
            assert first.isPresent() is True
            # Optional.empty() is static and takes no arguments
            assert first.empty().isPresent() is False
        finally:
            client.close()
