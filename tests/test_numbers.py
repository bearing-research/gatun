"""Numbers that fit no Java primitive, the numbers of other libraries, and
converters for a program's own types."""

import ast
import asyncio
import decimal
import fractions
import os
import pathlib
import subprocess
from decimal import Decimal
from numbers import Integral

import numpy as np
import pytest

import gatun
from gatun import (
    JavaException,
    JavaNoSuchMethodException,
    JavaNumberFormatException,
    JavaObject,
    TypeHint,
    aconnect,
    connect,
)
from gatun.async_client import AsyncJavaObject

NUMBERS_JAVA = """
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class Numbers {
    public static final BigInteger HUGE = BigInteger.ONE.shiftLeft(100);
    public static BigDecimal rate = new BigDecimal("0.0725");
    public BigDecimal amount = new BigDecimal("9.99");
    public Object held;

    public static String which(long x) { return "long"; }
    public static String which(BigInteger x) { return "BigInteger"; }
    public static String which(double x) { return "double"; }
    public static String which(BigDecimal x) { return "BigDecimal"; }

    public static String onlyBigInteger(BigInteger x) { return x.toString(); }
    public static String onlyBigDecimal(BigDecimal x) {
        return x.unscaledValue() + " scale " + x.scale();
    }
    public static String intOrObject(int x) { return "int"; }
    public static String intOrObject(Object x) { return "Object"; }
    public static String number(Number x) { return x.getClass().getName(); }

    public static String describe(Object x) {
        return x == null ? "null" : x.getClass().getName() + " " + x;
    }
    public static Object echo(Object x) { return x; }
    public static BigInteger big(String digits) { return new BigInteger(digits); }
    public static BigInteger shifted(int bits) { return BigInteger.ONE.shiftLeft(bits); }
    public static BigDecimal decimal(String digits) { return new BigDecimal(digits); }
    public static List<Object> several() {
        List<Object> out = new ArrayList<>();
        out.add(BigInteger.ONE.shiftLeft(70));
        out.add(new BigDecimal("1.50"));
        out.add(7);
        return out;
    }
    public static Map<String, BigDecimal> prices() {
        Map<String, BigDecimal> out = new LinkedHashMap<>();
        out.put("tea", new BigDecimal("3.20"));
        out.put("cake", new BigDecimal("4.75"));
        return out;
    }
    public static BigDecimal[] array() {
        return new BigDecimal[] {new BigDecimal("1.1"), new BigDecimal("2.2")};
    }
    public static Object apply(Function<Object, Object> f, Object x) { return f.apply(x); }

    public BigDecimal plus(BigDecimal other) { return amount.add(other); }
    public String hold(Object x) { held = x; return describe(x); }

    public static class Money {
        public final BigDecimal amount;
        public final String currency;
        public Money(BigDecimal amount, String currency) {
            this.amount = amount;
            this.currency = currency;
        }
        @Override public String toString() { return amount + " " + currency; }
    }
    public static String show(Money m) { return m.toString(); }
    public static String showAll(List<Money> all) { return all.toString(); }
    public static String showMap(Map<String, Money> all) { return all.toString(); }
}
"""

ALLOWED = [
    "Numbers",
    "Numbers.Money",
    "java.math.BigInteger",
    "java.math.BigDecimal",
    "java.util.LinkedHashMap",
]


@pytest.fixture(scope="module")
def numbers_classes(tmp_path_factory):
    out = tmp_path_factory.mktemp("numbers")
    java_home = os.environ.get("JAVA_HOME")
    javac = os.path.join(java_home, "bin", "javac") if java_home else "javac"
    source = out / "Numbers.java"
    source.write_text(NUMBERS_JAVA)
    subprocess.run([javac, "--release", "22", "-d", str(out), str(source)], check=True)
    return str(out)


@pytest.fixture(scope="module")
def client(numbers_classes):
    client = connect(
        memory="16MB", classpath=[numbers_classes], allowed_classes=ALLOWED
    )
    yield client
    client.close()


@pytest.fixture
def numbers(client):
    return client.jvm.Numbers


class TestBigIntegerArguments:
    @pytest.mark.parametrize(
        "value", [2**63, -(2**63) - 1, 2**70, -(2**70), 2**64 - 1, 10**40]
    )
    def test_an_int_past_64_bits_arrives_as_a_big_integer(self, numbers, value):
        assert numbers.number(value) == "java.math.BigInteger"
        assert numbers.describe(value) == f"java.math.BigInteger {value}"
        assert numbers.echo(value) == value

    def test_more_digits_than_python_prints(self, numbers):
        value = 2**20000 + 1
        assert numbers.echo(value) == value
        assert numbers.echo(-value) == -value

    @pytest.mark.parametrize("value", [2**63 - 1, -(2**63), 2**40, 5, 0, -1])
    def test_an_int_that_fits_travels_as_before(self, numbers, value):
        expected = "Integer" if -(2**31) <= value < 2**31 else "Long"
        assert numbers.describe(value) == f"java.lang.{expected} {value}"

    def test_the_digits_arrive(self, numbers):
        assert numbers.describe(2**70) == f"java.math.BigInteger {2**70}"
        assert numbers.describe(-(10**30)) == f"java.math.BigInteger -{10**30}"

    def test_as_a_method_argument(self, client):
        big = client.create_object("java.math.BigInteger", "1")
        assert big.add(2**70) == 2**70 + 1

    def test_in_a_list_and_a_dict(self, numbers):
        assert numbers.echo([1, 2**70, -(2**90)]) == [1, 2**70, -(2**90)]
        assert numbers.echo({"a": 2**70, 2**80: "b"}) == {"a": 2**70, 2**80: "b"}

    def test_a_subclass_of_int(self, numbers):
        class Count(int):
            pass

        assert numbers.echo(Count(2**70)) == 2**70

    def test_a_type_hint_makes_a_small_int_a_big_integer(self, numbers):
        hinted = TypeHint(5, "java.math.BigInteger")
        assert numbers.describe(hinted) == "java.math.BigInteger 5"


class TestDecimalArguments:
    @pytest.mark.parametrize(
        ("text", "unscaled", "scale"),
        [
            ("1.5", 15, 1),
            ("1.50", 150, 2),
            ("0.1", 1, 1),
            ("-0.000001", -1, 6),
            ("1E+3", 1, -3),
            ("12345678901234567890.123456789012345678901234567890", None, 30),
            ("0", 0, 0),
            ("100", 100, 0),
        ],
    )
    def test_a_decimal_arrives_exactly(self, numbers, text, unscaled, scale):
        if unscaled is None:
            unscaled = int(text.replace(".", ""))
        assert numbers.onlyBigDecimal(Decimal(text)) == f"{unscaled} scale {scale}"

    def test_precision_beyond_the_context(self, numbers):
        """The context's precision rounds arithmetic, not what is sent."""
        digits = "1." + "123456789" * 20
        with decimal.localcontext() as context:
            context.prec = 5
            assert numbers.echo(Decimal(digits)) == Decimal(digits)

    @pytest.mark.parametrize("text", ["NaN", "sNaN", "Infinity", "-Infinity"])
    def test_what_big_decimal_cannot_hold_is_refused(self, numbers, client, text):
        with pytest.raises(ValueError, match="no NaN and no infinity"):
            numbers.describe(Decimal(text))
        assert client.ping() is True

    def test_an_exponent_java_cannot_hold(self, numbers, client):
        with pytest.raises(JavaNumberFormatException):
            numbers.describe(Decimal("1E+99999999999"))
        assert client.ping() is True

    def test_a_subclass_of_decimal(self, numbers):
        class Price(Decimal):
            pass

        assert numbers.onlyBigDecimal(Price("2.50")) == "250 scale 2"

    def test_in_a_list_and_a_dict(self, numbers):
        values = [Decimal("1.10"), Decimal(-2), None]
        assert numbers.echo(values) == values
        assert numbers.echo({"price": Decimal("3.20")}) == {"price": Decimal("3.20")}

    def test_as_a_constructor_argument_and_a_field(self, client, numbers):
        money = client.create_object("Numbers.Money", Decimal("9.99"), "EUR")
        assert numbers.show(money) == "9.99 EUR"

        holder = numbers()
        client.set_field(holder, "amount", Decimal("12.345"))
        assert client.get_field(holder, "amount") == Decimal("12.345")
        client.set_static_field("Numbers", "rate", Decimal("0.0800"))
        try:
            assert client.get_static_field("Numbers", "rate") == Decimal("0.0800")
        finally:
            client.set_static_field("Numbers", "rate", Decimal("0.0725"))


class TestReturnValues:
    def test_a_big_integer_comes_back_as_an_int(self, numbers):
        result = numbers.big(str(2**70))
        assert type(result) is int
        assert result == 2**70
        assert numbers.big("-123456789012345678901234567890") == -(
            123456789012345678901234567890
        )

    def test_a_big_integer_that_would_fit_is_an_int_too(self, numbers):
        result = numbers.big("42")
        assert type(result) is int
        assert result == 42
        assert numbers.big("0") == 0
        assert numbers.big("-1") == -1

    def test_more_digits_than_python_prints(self, numbers):
        """Text would not do: Python refuses to parse or print an int of more
        than 4300 digits."""
        assert numbers.shifted(40_000) == 1 << 40_000

    @pytest.mark.parametrize(
        "text", ["1.5", "1.50", "0.0725", "-3", "1E+3", "1E-10", "0.00"]
    )
    def test_a_big_decimal_comes_back_as_a_decimal(self, numbers, text):
        result = numbers.decimal(text)
        assert type(result) is Decimal
        assert result.as_tuple() == Decimal(text).as_tuple()

    def test_inside_a_list_a_map_and_an_array(self, numbers, client):
        assert numbers.several() == [2**70, Decimal("1.50"), 7]
        assert numbers.prices() == {"tea": Decimal("3.20"), "cake": Decimal("4.75")}
        assert list(numbers.array()) == [Decimal("1.1"), Decimal("2.2")]

    def test_fields(self, client, numbers):
        assert client.get_static_field("Numbers", "HUGE") == 2**100
        assert numbers.HUGE == 2**100
        holder = numbers()
        assert client.get_field(holder, "amount") == Decimal("9.99")
        assert client.get_fields(holder, ["amount"]) == [Decimal("9.99")]

    def test_instance_method(self, numbers):
        assert numbers().plus(Decimal("0.01")) == Decimal("10.00")

    def test_vectorized_calls(self, client, numbers):
        holder = numbers()
        assert client.invoke_methods(
            holder, [("plus", (Decimal(1),)), ("plus", (Decimal("0.001"),))]
        ) == [Decimal("10.99"), Decimal("9.991")]

    def test_batch(self, client):
        with client.batch() as b:
            big = b.call_static("Numbers", "echo", 2**70)
            dec = b.call_static("Numbers", "echo", Decimal("1.50"))
            small = b.call_static("Numbers", "big", "7")
        assert big.get() == 2**70
        assert dec.get().as_tuple() == Decimal("1.50").as_tuple()
        assert small.get() == 7

    def test_other_numbers_stay_objects(self, client):
        """Only the two exact classes are values; an AtomicLong is a thing with
        an identity."""
        client_with_atomic = connect(
            memory="16MB",
            allowed_classes=["java.util.concurrent.atomic.AtomicLong"],
        )
        try:
            counter = client_with_atomic.jvm.java.util.concurrent.atomic.AtomicLong(5)
            holder = client_with_atomic.jvm.java.util.ArrayList()
            holder.add(counter)
            assert isinstance(holder.get(0), JavaObject)
        finally:
            client_with_atomic.close()


class TestKeepingTheReference:
    def test_a_constructor_returns_the_object(self, client):
        dec = client.jvm.java.math.BigDecimal("1.50")
        assert isinstance(dec, JavaObject)
        assert dec.scale() == 2
        big = client.create_object("java.math.BigInteger", "12")
        assert isinstance(big, JavaObject)
        assert big.bitLength() == 4

    def test_instance_method(self, client):
        dec = client.create_object("java.math.BigDecimal", "1.50")
        assert dec.add(Decimal(1)) == Decimal("2.50")

        total = client.invoke_method(
            dec.object_id, "add", Decimal(1), return_object_ref=True
        )
        assert isinstance(total, JavaObject)
        assert total.scale() == 2
        assert total.toPlainString() == "2.50"

    def test_static_method(self, client):
        ref = client.invoke_static_method(
            "Numbers", "decimal", "1.50", return_object_ref=True
        )
        assert isinstance(ref, JavaObject)
        assert ref.precision() == 3

    def test_fields(self, client, numbers):
        huge = client.get_static_field("Numbers", "HUGE", return_object_ref=True)
        assert isinstance(huge, JavaObject)
        assert huge.bitLength() == 101

        holder = numbers()
        amount = client.get_field(holder, "amount", return_object_ref=True)
        assert isinstance(amount, JavaObject)
        assert amount.scale() == 2

    def test_no_argument_method(self, client):
        """The path with no arguments is a separate one in the server."""
        dec = client.create_object("java.math.BigDecimal", "-1.5")
        assert dec.negate() == Decimal("1.5")
        negated = client.invoke_method(dec.object_id, "negate", return_object_ref=True)
        assert isinstance(negated, JavaObject)
        assert negated.signum() == 1

    def test_other_converted_values_too(self, client):
        items = client.create_object("java.util.ArrayList")
        items.add("x")
        view = client.invoke_method(
            items.object_id, "subList", 0, 1, return_object_ref=True
        )
        assert isinstance(view, JavaObject)
        assert view.size() == 1

    def test_batch(self, client, numbers):
        dec = client.create_object("java.math.BigDecimal", "1.50")
        holder = numbers()
        with client.batch() as b:
            value = b.call(dec, "add", Decimal(1))
            ref = b.call(dec, "add", Decimal(1), return_object_ref=True)
            static_ref = b.call_static(
                "Numbers", "decimal", "7.5", return_object_ref=True
            )
            field_ref = b.get_field(holder, "amount", return_object_ref=True)
            field = b.get_field(holder, "amount")
        assert value.get() == Decimal("2.50")
        assert ref.get().toPlainString() == "2.50"
        assert static_ref.get().scale() == 1
        assert field_ref.get().scale() == 2
        assert field.get() == Decimal("9.99")

    def test_batch_that_is_packed_by_hand(self, client):
        """No argument here needs the builder, so this is the other encoding."""
        with client.batch() as b:
            value = b.call_static("Numbers", "decimal", "1.50")
            ref = b.call_static("Numbers", "decimal", "1.50", return_object_ref=True)
        assert value.get() == Decimal("1.50")
        assert isinstance(ref.get(), JavaObject)


class TestOverloads:
    """One method name taking long, BigInteger, double and BigDecimal."""

    @pytest.mark.parametrize(
        ("value", "chosen"),
        [
            (5, "long"),
            (-1, "long"),
            (2**40, "long"),
            (2**63 - 1, "long"),
            (2**63, "BigInteger"),
            (-(2**70), "BigInteger"),
            (1.5, "double"),
            (float(2**70), "double"),
            (Decimal("1.5"), "BigDecimal"),
            (Decimal(5), "BigDecimal"),
            (True, None),
        ],
    )
    def test_each_python_number_finds_its_overload(self, numbers, value, chosen):
        if chosen is None:
            with pytest.raises(JavaNoSuchMethodException):
                numbers.which(value)
        else:
            assert numbers.which(value) == chosen

    def test_type_hints_choose(self, numbers):
        assert numbers.which(TypeHint(5, "java.math.BigInteger")) == "BigInteger"
        assert numbers.which(TypeHint(5, "java.math.BigDecimal")) == "BigDecimal"
        assert numbers.which(TypeHint(5, "double")) == "double"

    def test_a_small_int_for_a_big_integer_parameter(self, numbers, client):
        """Which Java type an int arrives as depends on its size; whether a
        call works must not."""
        assert numbers.onlyBigInteger(5) == "5"
        assert numbers.onlyBigInteger(2**40) == str(2**40)
        assert numbers.onlyBigInteger(2**70) == str(2**70)
        big = client.create_object("java.math.BigInteger", "10")
        assert big.add(5) == 15
        assert big.multiply(2**70) == 10 * 2**70

    def test_an_int_for_a_big_decimal_parameter(self, numbers, client):
        assert numbers.onlyBigDecimal(5) == "5 scale 0"
        assert numbers.onlyBigDecimal(2**70) == f"{2**70} scale 0"
        dec = client.create_object("java.math.BigDecimal", "1.5")
        assert dec.add(1) == Decimal("2.5")

    def test_compare_to(self, client):
        """compareTo(BigDecimal) has a twin that takes Object and casts, which
        an int must not be given to."""
        dec = client.create_object("java.math.BigDecimal", "1.5")
        assert dec.compareTo(Decimal("1.5")) == 0
        assert dec.compareTo(1) == 1
        assert dec.compareTo(2**70) == -1
        big = client.create_object("java.math.BigInteger", "10")
        assert big.compareTo(5) == 1
        assert big.compareTo(2**70) == -1

    def test_the_twin_is_still_called_when_nothing_else_fits(self, client):
        dec = client.create_object("java.math.BigDecimal", "1.5")
        with pytest.raises(JavaException, match="ClassCastException"):
            dec.compareTo("text")
        assert dec.equals(Decimal("1.5")) is True
        assert dec.equals(1) is False

    def test_a_float_is_not_a_decimal(self, numbers):
        """A double has no one decimal it stands for, so none is picked."""
        with pytest.raises(JavaNoSuchMethodException):
            numbers.onlyBigDecimal(1.5)
        with pytest.raises(JavaNoSuchMethodException):
            numbers.onlyBigInteger(1.0)

    def test_a_primitive_parameter_is_preferred(self, numbers):
        assert numbers.intOrObject(5) == "int"
        assert numbers.intOrObject(2**70) == "Object"
        assert numbers.intOrObject(Decimal(5)) == "Object"

    def test_batch(self, client):
        with client.batch() as b:
            results = [
                b.call_static("Numbers", "which", value)
                for value in (5, 2**70, 1.5, Decimal("1.5"))
            ]
        assert [r.get() for r in results] == [
            "long",
            "BigInteger",
            "double",
            "BigDecimal",
        ]


class _Level(Integral):
    """An integer that is not an int: it only says so, as numpy's do."""

    def __init__(self, value):
        self._value = value

    def __int__(self):
        return self._value

    def _same(self, *args):
        raise NotImplementedError

    __abs__ = __add__ = __and__ = __ceil__ = __eq__ = __floor__ = _same
    __floordiv__ = __invert__ = __le__ = __lshift__ = __lt__ = __mod__ = _same
    __mul__ = __neg__ = __or__ = __pos__ = __pow__ = __radd__ = __rand__ = _same
    __rfloordiv__ = __rlshift__ = __rmod__ = __rmul__ = __ror__ = __round__ = _same
    __rpow__ = __rrshift__ = __rshift__ = __rtruediv__ = __rxor__ = _same
    __truediv__ = __trunc__ = __xor__ = _same
    __hash__ = None


class TestNumbersOfOtherLibraries:
    def test_anything_registered_as_an_integer(self, numbers):
        assert numbers.which(_Level(7)) == "long"
        assert numbers.echo(_Level(2**70)) == 2**70

    @pytest.mark.parametrize(
        ("value", "java_class", "expected"),
        [
            (np.int8(-3), "java.lang.Integer", -3),
            (np.int32(7), "java.lang.Integer", 7),
            (np.int64(2**40), "java.lang.Long", 2**40),
            (np.uint8(200), "java.lang.Integer", 200),
            (np.uint64(2**64 - 1), "java.math.BigInteger", 2**64 - 1),
            (np.float32(1.5), "java.lang.Double", 1.5),
            (np.float64(2.25), "java.lang.Double", 2.25),
            (np.float16(0.5), "java.lang.Double", 0.5),
        ],
    )
    def test_numpy_scalars(self, numbers, value, java_class, expected):
        assert numbers.describe(value) == f"{java_class} {expected}"
        result = numbers.echo(value)
        assert result == expected
        assert type(result) in (int, float)

    def test_numpy_scalars_choose_overloads_like_python_numbers(self, numbers):
        assert numbers.which(np.int64(5)) == "long"
        assert numbers.which(np.uint64(2**63)) == "BigInteger"
        assert numbers.which(np.float32(1.5)) == "double"

    def test_numpy_scalars_in_collections(self, numbers):
        assert numbers.echo([np.int64(1), np.int64(2)]) == [1, 2]
        assert numbers.echo({"n": np.int32(3), "x": np.float64(0.5)}) == {
            "n": 3,
            "x": 0.5,
        }
        assert numbers.echo(list(np.arange(4))) == [0, 1, 2, 3]

    def test_a_fraction_is_a_real_number(self, numbers):
        assert numbers.which(fractions.Fraction(1, 2)) == "double"
        assert numbers.echo(fractions.Fraction(1, 4)) == 0.25

    def test_in_a_batch_and_a_method_call(self, client):
        items = client.create_object("java.util.ArrayList")
        with client.batch() as b:
            b.call(items, "add", np.int64(7))
            b.call(items, "add", np.float32(0.5))
        items.add(np.int16(9))
        assert list(items) == [7, 0.5, 9]

    def test_a_complex_number_is_still_refused(self, numbers):
        with pytest.raises(TypeError, match="Unsupported argument type"):
            numbers.describe(1 + 2j)
        with pytest.raises(TypeError, match="Unsupported argument type"):
            numbers.describe(np.complex128(1 + 2j))

    def test_gatun_does_not_import_numpy(self):
        """It reads what a number says of itself; no library is named. (The
        FlatBuffers library and pyarrow use numpy when it is installed, so what
        the process has loaded says nothing.)"""
        package = pathlib.Path(gatun.__file__).parent
        for source in package.glob("*.py"):
            for node in ast.walk(ast.parse(source.read_text())):
                if isinstance(node, ast.Import):
                    names = [alias.name for alias in node.names]
                elif isinstance(node, ast.ImportFrom):
                    names = [node.module or ""]
                else:
                    continue
                assert not [n for n in names if n.split(".")[0] == "numpy"], source


class Money:
    def __init__(self, amount: str, currency: str):
        self.amount = amount
        self.currency = currency


class Euro(Money):
    def __init__(self, amount: str):
        super().__init__(amount, "EUR")


class TestConverters:
    @pytest.fixture
    def client(self, numbers_classes):
        """A client of its own: converters stay registered."""
        client = connect(
            memory="16MB", classpath=[numbers_classes], allowed_classes=ALLOWED
        )
        calls = []

        def to_money(value):
            calls.append(value)
            return client.jvm.Numbers.Money(Decimal(value.amount), value.currency)

        client.register_converter(Money, to_money)
        client.converted = calls
        yield client
        client.close()

    def test_a_converter_may_call_java(self, client):
        assert client.jvm.Numbers.show(Money("9.99", "EUR")) == "9.99 EUR"
        assert len(client.converted) == 1

    def test_every_kind_of_call(self, client):
        money = Money("1.50", "USD")
        assert client.invoke_static_method("Numbers", "show", money) == "1.50 USD"

        holder = client.create_object("Numbers")
        held = client.invoke_method(holder.object_id, "hold", money)
        assert held.endswith("1.50 USD")
        client.set_field(holder, "held", Money("2", "GBP"))
        assert client.get_field(holder, "held").toString() == "2 GBP"

        results = client.invoke_methods(
            holder, [("hold", (Money("3", "CHF"),)), ("hold", (Money("4", "SEK"),))]
        )
        assert [r.split(" ", 1)[1] for r in results] == ["3 CHF", "4 SEK"]

        lists = client.create_objects([("java.util.ArrayList", ([Money("5", "NOK")],))])
        assert lists[0].get(0).toString() == "5 NOK"

    def test_constructor_argument(self, client):
        items = client.create_object("java.util.ArrayList", [Money("1", "EUR")])
        assert items.get(0).toString() == "1 EUR"

    def test_inside_collections(self, client):
        show_all = client.jvm.Numbers.showAll
        assert show_all([Money("1", "EUR"), Money("2", "USD")]) == "[1 EUR, 2 USD]"
        assert show_all((Money("3", "EUR"),)) == "[3 EUR]"
        assert client.jvm.Numbers.showMap({"a": Money("4", "EUR")}) == "{a=4 EUR}"
        assert client.jvm.Numbers.describe([[Money("5", "EUR")]]).endswith("[[5 EUR]]")

    def test_batch(self, client):
        holder = client.create_object("Numbers")
        with client.batch() as b:
            shown = b.call_static("Numbers", "show", Money("6", "EUR"))
            held = b.call(holder, "hold", Money("7", "EUR"))
            b.set_field(holder, "held", Money("8", "EUR"))
            made = b.create("java.util.ArrayList", [Money("9", "EUR")])
        assert shown.get() == "6 EUR"
        assert held.get().endswith("7 EUR")
        assert client.get_field(holder, "held").toString() == "8 EUR"
        assert made.get().get(0).toString() == "9 EUR"

    def test_a_subclass_uses_the_nearest_converter(self, client):
        assert client.jvm.Numbers.show(Euro("1")) == "1 EUR"

        client.register_converter(
            Euro, lambda value: client.jvm.Numbers.Money(Decimal(0), "euro")
        )
        assert client.jvm.Numbers.show(Euro("1")) == "0 euro"
        assert client.jvm.Numbers.show(Money("1", "USD")) == "1 USD"

    def test_a_type_gatun_does_not_know(self, client):
        client.register_converter(set, lambda value: sorted(value))
        assert client.jvm.Numbers.describe({3, 1, 2}).endswith("[1, 2, 3]")
        client.register_converter(complex, lambda value: [value.real, value.imag])
        assert client.jvm.Numbers.echo(1 + 2j) == [1.0, 2.0]

    def test_a_type_gatun_knows_can_be_given_another_meaning(self, client):
        """A dict becomes a HashMap; this program wants its order kept. The
        converter hands the dict itself to Java, which must not call it again."""
        client.register_converter(
            dict, lambda value: client.jvm.java.util.LinkedHashMap(value)
        )
        described = client.jvm.Numbers.describe({"b": 1, "a": 2})
        assert described.startswith("java.util.LinkedHashMap")

    def test_the_result_is_sent_as_it_is(self, client):
        client.register_converter(frozenset, lambda value: {1 + 2j})
        with pytest.raises(TypeError, match="Unsupported argument type"):
            client.jvm.Numbers.describe(frozenset())
        assert client.ping() is True

    def test_a_failing_converter(self, client):
        def broken(value):
            raise RuntimeError("no rate for today")

        client.register_converter(bytearray, broken)
        with pytest.raises(RuntimeError, match="no rate for today"):
            client.jvm.Numbers.describe(bytearray(b"x"))
        with client.batch() as b:
            ok = b.call_static("Numbers", "big", "1")
        assert ok.get() == 1
        assert client.ping() is True

    def test_plain_values_are_never_asked_about(self, client):
        assert client.jvm.Numbers.describe("text") == "java.lang.String text"
        assert client.jvm.Numbers.which(5) == "long"
        assert client.converted == []

    def test_unregister(self, client):
        client.unregister_converter(Money)
        client.unregister_converter(Money)  # ignored
        with pytest.raises(TypeError, match="Unsupported argument type"):
            client.jvm.Numbers.show(Money("1", "EUR"))

    @pytest.mark.parametrize("plain", [int, float, bool, str, type(None), object])
    def test_plain_types_take_no_converter(self, client, plain):
        with pytest.raises(ValueError, match="takes no converter"):
            client.register_converter(plain, str)

    def test_java_objects_take_no_converter(self, client):
        with pytest.raises(ValueError, match="takes no converter"):
            client.register_converter(JavaObject, str)

    def test_bad_registrations(self, client):
        with pytest.raises(TypeError, match="takes a class"):
            client.register_converter("Money", str)
        with pytest.raises(TypeError, match="must be callable"):
            client.register_converter(Money, "not a function")

        async def later(value):
            return value

        with pytest.raises(TypeError, match="cannot await a converter"):
            client.register_converter(Money, later)

    def test_a_subclass_of_a_plain_type(self, client):
        class UserId(int):
            pass

        client.register_converter(UserId, lambda value: f"user-{int(value)}")
        assert client.jvm.Numbers.describe(UserId(7)) == "java.lang.String user-7"
        assert client.jvm.Numbers.describe(7) == "java.lang.Integer 7"

    def test_a_type_hint_around_a_converted_value(self, client):
        hinted = TypeHint(Money("1", "EUR"), "java.lang.Object")
        assert client.jvm.Numbers.describe(hinted).endswith("1 EUR")


class TestCallbacks:
    def test_arguments_and_results(self, client, numbers):
        seen = []

        def double(value):
            seen.append(value)
            return value * 2

        function = client.register_callback(double, "java.util.function.Function")
        try:
            assert numbers.apply(function, Decimal("1.25")) == Decimal("2.50")
            assert numbers.apply(function, 2**70) == 2**71
            assert seen == [Decimal("1.25"), 2**70]
            assert type(seen[0]) is Decimal
        finally:
            client.unregister_callback(function)


class TestAsyncClient:
    @pytest.fixture
    async def aclient(self, numbers_classes):
        client = await aconnect(
            memory="16MB", classpath=[numbers_classes], allowed_classes=ALLOWED
        )
        yield client
        await client.close()

    async def test_arguments_and_results(self, aclient):
        invoke = aclient.invoke_static_method
        assert await invoke("Numbers", "echo", 2**70) == 2**70
        assert await invoke("Numbers", "which", 2**70) == "BigInteger"
        assert await invoke("Numbers", "which", Decimal("1.5")) == "BigDecimal"
        assert await invoke("Numbers", "which", np.int64(5)) == "long"
        result = await invoke("Numbers", "decimal", "1.50")
        assert result.as_tuple() == Decimal("1.50").as_tuple()
        assert await invoke("Numbers", "several") == [2**70, Decimal("1.50"), 7]
        assert await aclient.get_static_field("Numbers", "HUGE") == 2**100

    async def test_keeping_the_reference(self, aclient):
        dec = await aclient.create_object("java.math.BigDecimal", "1.50")
        assert isinstance(dec, AsyncJavaObject)
        assert await aclient.invoke_method(dec, "add", Decimal(1)) == Decimal("2.50")

        total = await aclient.invoke_method(
            dec, "add", Decimal(1), return_object_ref=True
        )
        assert isinstance(total, AsyncJavaObject)
        assert await aclient.invoke_method(total, "scale") == 2

        ref = await aclient.invoke_static_method(
            "Numbers", "decimal", "7.5", return_object_ref=True
        )
        assert isinstance(ref, AsyncJavaObject)
        huge = await aclient.get_static_field("Numbers", "HUGE", return_object_ref=True)
        assert isinstance(huge, AsyncJavaObject)
        holder = await aclient.create_object("Numbers")
        amount = await aclient.get_field(holder, "amount", return_object_ref=True)
        assert isinstance(amount, AsyncJavaObject)
        assert await aclient.get_field(holder, "amount") == Decimal("9.99")

    async def test_converters(self, aclient):
        async def to_money(value):
            await asyncio.sleep(0)
            return await aclient.create_object(
                "Numbers.Money", Decimal(value.amount), value.currency
            )

        aclient.register_converter(Money, to_money)
        invoke = aclient.invoke_static_method
        assert await invoke("Numbers", "show", Money("9.99", "EUR")) == "9.99 EUR"
        assert (
            await invoke("Numbers", "showAll", [Money("1", "EUR"), Money("2", "USD")])
            == "[1 EUR, 2 USD]"
        )
        holder = await aclient.create_object("Numbers")
        await aclient.set_field(holder, "held", Money("3", "EUR"))
        held = await aclient.get_field(holder, "held")
        assert await aclient.invoke_method(held, "toString") == "3 EUR"
        shown = await aclient.invoke_methods(holder, [("hold", (Money("4", "EUR"),))])
        assert shown[0].endswith("4 EUR")
        made = await aclient.create_objects(
            [("java.util.ArrayList", ([Money("5", "EUR")],))]
        )
        first = await aclient.invoke_method(made[0], "get", 0)
        assert await aclient.invoke_method(first, "toString") == "5 EUR"

        # A plain function works too
        aclient.register_converter(set, lambda value: sorted(value))
        assert await invoke("Numbers", "echo", {2, 1}) == [1, 2]
