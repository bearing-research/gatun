"""Tests for auto-convert between Java collections and Python list/dict."""

import enum

import pytest

from gatun import JavaArray, JavaException, connect


def test_java_list_to_python_list(client):
    """Test that Java List returned from method is auto-converted to Python list."""
    # Create an ArrayList and use subList which returns a List
    arr = client.jvm.java.util.ArrayList()
    arr.add("one")
    arr.add("two")
    arr.add("three")

    # subList returns a List - should be auto-converted to Python list
    sub = arr.subList(0, 2)
    assert isinstance(sub, list)
    assert sub == ["one", "two"]


def test_java_map_to_python_dict(client):
    """Test that Java HashMap returned from method is auto-converted."""
    # Create a map and return it via Collections method
    Collections = client.jvm.java.util.Collections

    # singletonMap returns a Map
    result = Collections.singletonMap("key", "value")
    assert isinstance(result, dict)
    assert result == {"key": "value"}


def test_java_empty_list(client):
    """Test empty list conversion."""
    Collections = client.jvm.java.util.Collections
    result = Collections.emptyList()
    assert isinstance(result, list)
    assert result == []


def test_java_empty_map(client):
    """Test empty map conversion."""
    Collections = client.jvm.java.util.Collections
    result = Collections.emptyMap()
    assert isinstance(result, dict)
    assert result == {}


def test_nested_list(client):
    """Test nested list conversion via subList."""
    # Create two ArrayLists and add them to an outer ArrayList
    outer = client.jvm.java.util.ArrayList()

    inner1 = client.jvm.java.util.ArrayList()
    inner1.add("a")
    inner1.add("b")

    inner2 = client.jvm.java.util.ArrayList()
    inner2.add("c")
    inner2.add("d")

    outer.add(inner1)
    outer.add(inner2)

    # subList returns a List view which should be auto-converted
    result = outer.subList(0, 2)
    assert isinstance(result, list)
    assert len(result) == 2
    # The inner lists should also be auto-converted
    assert result[0] == ["a", "b"]
    assert result[1] == ["c", "d"]


def test_list_with_mixed_types(client):
    """Test list with mixed primitive types."""
    arr = client.jvm.java.util.ArrayList()
    arr.add("hello")
    arr.add(42)
    arr.add(3.14)

    # subList returns a List that should be auto-converted
    result = arr.subList(0, 3)
    assert isinstance(result, list)
    assert result[0] == "hello"
    assert result[1] == 42
    assert abs(result[2] - 3.14) < 0.001


def test_map_with_multiple_entries(client):
    """Test map with multiple entries."""
    HashMap = client.jvm.java.util.HashMap
    my_map = HashMap()
    my_map.put("one", 1)
    my_map.put("two", 2)
    my_map.put("three", 3)

    Collections = client.jvm.java.util.Collections

    # Create an unmodifiable map from our map
    unmod = Collections.unmodifiableMap(my_map)
    # This should return as dict since it implements Map
    assert isinstance(unmod, dict)
    assert unmod == {"one": 1, "two": 2, "three": 3}


def test_list_singleton(client):
    """Test Collections.singletonList returns a Python list."""
    Collections = client.jvm.java.util.Collections
    result = Collections.singletonList("only")
    assert isinstance(result, list)
    assert result == ["only"]


def test_map_with_list_values(client):
    """Test map containing list values."""
    HashMap = client.jvm.java.util.HashMap
    my_map = HashMap()

    list1 = client.jvm.java.util.ArrayList()
    list1.add(1)
    list1.add(2)

    my_map.put("numbers", list1)
    my_map.put("name", "test")

    Collections = client.jvm.java.util.Collections
    result = Collections.unmodifiableMap(my_map)

    assert isinstance(result, dict)
    assert result["name"] == "test"
    assert result["numbers"] == [1, 2]


@pytest.mark.parametrize(
    "value",
    [
        [[1, 2], [3, 4]],
        [["a", "b"], ["c", "d"]],
        [[1], [2], [3]],
        [[], [1], []],
        [[1, "a"], [2.5, True]],
        [[[1, 2], [3]], [[4]]],
        {"a": {"x": 1, "y": 5}, "b": 2},
        {"a": {"x": 1}, "b": 2},
        [{"k": [1, 2]}, {"k": [3]}],
        {"outer": [[1, 2], [3]], "n": 1},
    ],
    ids=repr,
)
def test_nested_collection_argument_roundtrip(client, value):
    """Nested lists/dicts passed as arguments arrive intact.

    Regression: the server reused one FlatBuffers wrapper per container type
    across recursion, so after converting the first inner collection the outer
    loop kept reading from it ([[1, 2], [3, 4]] arrived as [[1, 2], 2]).
    """
    holder = client.jvm.java.util.ArrayList()
    holder.add(value)
    assert holder.get(0) == value


def test_nested_array_argument(client):
    """An Object[] whose elements are themselves arrays converts element by element."""
    nested = JavaArray(
        [
            JavaArray([1, 2], element_type="Int"),
            JavaArray([3, 4], element_type="Int"),
        ],
        element_type="Object",
    )
    assert client.jvm.java.util.Arrays.deepToString(nested) == "[[1, 2], [3, 4]]"


def test_map_with_a_list_key(client):
    """A Java List is a fine map key; a Python list is not hashable."""
    by_pair = client.jvm.java.util.HashMap()
    by_pair.put([1, 2], "pair")
    by_pair.put("plain", "text")

    copy = client.invoke_method(by_pair.object_id, "clone")
    assert copy == {(1, 2): "pair", "plain": "text"}


def element_classes(client, collection):
    """The Java class name of each element of a Java collection."""
    as_array = client.invoke_method(collection.object_id, "toArray")
    return [
        client.invoke_static_method(
            "java.lang.reflect.Array", "get", as_array, index, return_object_ref=True
        )
        .getClass()
        .getName()
        for index in range(len(as_array))
    ]


@pytest.mark.parametrize(
    ("value", "java_class"),
    [
        ([1, 2, 3], "java.lang.Integer"),
        ([-(2**31), 2**31 - 1], "java.lang.Integer"),
        ([1, 2**31], "java.lang.Long"),
        ([-(2**63), 2**63 - 1], "java.lang.Long"),
        ([1.5, -0.0, float("inf")], "java.lang.Double"),
        ([True, False, True], "java.lang.Boolean"),
        (["a", "", "héllo", "日本語", "\U0001f600"], "java.lang.String"),
        (["", ""], "java.lang.String"),
        ((1, 2), "java.lang.Integer"),
        (range(4), "java.lang.Integer"),
    ],
    ids=repr,
)
def test_list_of_one_kind_arrives_as_that_type(client, value, java_class):
    """These lists travel as one vector; Java sees the elements it always did."""
    received = client.jvm.java.util.ArrayList(value)

    assert element_classes(client, received) == [java_class] * len(value)
    # and back, where Java packs them the same way
    assert client.invoke_method(received.object_id, "clone") == list(value)


def test_list_of_nan_roundtrip(client):
    received = client.jvm.java.util.ArrayList([float("nan"), 1.0])
    back = client.invoke_method(received.object_id, "clone")
    assert back[0] != back[0]
    assert back[1] == 1.0


def test_list_of_int_subclasses_is_one_type(client):
    """Integers that are not plain `int` go element by element, under the same rule."""

    class Size(enum.IntEnum):
        SMALL = 1
        LARGE = 2**40

    received = client.jvm.java.util.ArrayList([Size.SMALL, Size.LARGE])
    assert element_classes(client, received) == ["java.lang.Long"] * 2

    received = client.jvm.java.util.ArrayList([Size.SMALL, Size.SMALL])
    assert element_classes(client, received) == ["java.lang.Integer"] * 2


def test_list_of_bools_and_ints_keeps_both(client):
    """`True` is an `int` in Python; a mixed list must not turn it into 1."""
    received = client.jvm.java.util.ArrayList([True, 2, False])
    assert element_classes(client, received) == [
        "java.lang.Boolean",
        "java.lang.Integer",
        "java.lang.Boolean",
    ]
    back = client.invoke_method(received.object_id, "clone")
    assert back == [True, 2, False]
    assert [type(item) for item in back] == [bool, int, bool]


def test_list_with_an_integer_past_64_bits_is_refused(client):
    with pytest.raises(Exception, match="(?i)int|range|format|overflow"):
        client.jvm.java.util.ArrayList([1, 2**64])
    # the half-built command was dropped, and the client still works
    assert client.jvm.java.util.ArrayList([1, 2]).size() == 2


@pytest.mark.parametrize(
    "value",
    [
        {"a": 1, "b": 2},
        {"a": "x", "b": ""},
        {1: 1.5, 2: 2.5},
        {"flag": True, "other": False},
        {"mixed": 1, "values": "two", "none": None},
        {1: "a", "b": 2},
        {},
        {"": ""},
    ],
    ids=repr,
)
def test_dict_roundtrip(client, value):
    """A dict travels as its keys and its values, each as a list."""
    received = client.jvm.java.util.HashMap(value)

    assert received.size() == len(value)
    assert client.invoke_method(received.object_id, "clone") == value


def test_dict_integers_are_typed_one_by_one(client):
    """In a map each integer is an Integer if it fits: `get(1)` has to find the key 1."""
    received = client.jvm.java.util.HashMap({1: 10, 2**40: 2**41})

    assert received.get(1) == 10
    assert received.get(2**40) == 2**41
    keys = client.jvm.java.util.ArrayList(received.keySet())
    assert sorted(element_classes(client, keys)) == [
        "java.lang.Integer",
        "java.lang.Long",
    ]


def test_java_lists_of_one_kind_to_python(client):
    """What Java returns takes the same forms, whatever the boxed type."""
    jvm = client.jvm
    assert jvm.java.util.Arrays.asList("x", "", "é") == ["x", "", "é"]
    assert jvm.java.util.Arrays.asList(1.5, 2.5) == [1.5, 2.5]
    assert jvm.java.util.Arrays.asList(True, False) == [True, False]

    longs = jvm.java.util.ArrayList()
    longs.add(2**53 + 1)  # a Long
    longs.add(1)  # an Integer
    assert client.invoke_method(longs.object_id, "clone") == [2**53 + 1, 1]

    linked = jvm.java.util.LinkedList(["b", "a"])
    assert client.invoke_method(linked.object_id, "clone") == ["b", "a"]


def test_string_in_a_list_is_checked_against_the_string_limit():
    limited = connect(extra_jvm_flags=["-Dgatun.limits.max_string_length=100"])
    try:
        with pytest.raises(JavaException, match="String too long"):
            limited.jvm.java.util.ArrayList(["short", "x" * 500])
        assert limited.jvm.java.util.ArrayList(["short", "x" * 50]).size() == 2
    finally:
        limited.close()
