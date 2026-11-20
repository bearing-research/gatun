"""Commands packed by hand, responses read by hand, and releases that ride along.

`gatun.wire` writes the bytes of the commonest commands itself instead of going
through the FlatBuffers builder. The first half reads those bytes back with the
generated classes, which is what the server does; the second half checks what
the clients do with them against a server.
"""

import enum
import gc
import threading

import flatbuffers
import pytest

from gatun import (
    CancelledException,
    JavaNumberFormatException,
    PayloadTooLargeError,
    aconnect,
    connect,
    wire,
)
from gatun.client import JavaObject, TypeHint
from gatun.generated.org.gatun.protocol import (
    Action,
    BoolVal,
    Command,
    DoubleVal,
    IntVal,
    ListVal,
    NullVal,
    ObjectRef,
    Response,
    StringVal,
    Value,
)

INVOKE = Action.Action.InvokeMethod
V = Value.Value


class Handle:
    """Stands in for a JavaObject: all the packer reads is `object_id`."""

    def __init__(self, object_id):
        self.object_id = object_id


def read_command(data):
    return Command.Command.GetRootAsCommand(data, 0)


def argument_of(command, index):
    """(value type, value) of one argument, read with the generated classes."""
    arg = command.Args(index)
    kind = arg.ValType()
    table = arg.Val()
    readers = {
        V.IntVal: (IntVal.IntVal, "V"),
        V.DoubleVal: (DoubleVal.DoubleVal, "V"),
        V.BoolVal: (BoolVal.BoolVal, "V"),
        V.StringVal: (StringVal.StringVal, "V"),
        V.ObjectRef: (ObjectRef.ObjectRef, "Id"),
    }
    if kind == V.NullVal:
        return kind, None
    reader_class, field = readers[kind]
    reader = reader_class()
    reader.Init(table.Bytes, table.Pos)
    value = getattr(reader, field)()
    return kind, value.decode("utf-8") if isinstance(value, bytes) else value


class TestPackCommand:
    def test_no_arguments(self):
        command = read_command(wire.pack_command(INVOKE, "size", 42, (), ()))

        assert command.Action() == INVOKE
        assert command.TargetName() == b"size"
        assert command.TargetId() == 42
        assert command.ArgsLength() == 0
        assert command.FreeIdsLength() == 0
        assert command.ReturnObjectRef() is False
        assert command.AnswersSent() == 0
        # Fields the packer never writes read as absent
        assert command.RequestId() == 0
        assert command.Batch() is None
        assert command.ArrowStream() is None
        assert command.NestedCommandLength() == 0

    @pytest.mark.parametrize(
        ("value", "kind"),
        [
            (7, V.IntVal),
            (0, V.IntVal),
            (-1, V.IntVal),
            (2**63 - 1, V.IntVal),
            (-(2**63), V.IntVal),
            (1.5, V.DoubleVal),
            (0.0, V.DoubleVal),
            (float("inf"), V.DoubleVal),
            (True, V.BoolVal),
            (False, V.BoolVal),
            ("hello", V.StringVal),
            ("", V.StringVal),
            ("wörld ☃ 𝄞", V.StringVal),
            ("a\0b", V.StringVal),
            (None, V.NullVal),
        ],
    )
    def test_one_argument(self, value, kind):
        command = read_command(wire.pack_command(INVOKE, "add", 1, (value,), ()))

        assert command.ArgsLength() == 1
        assert argument_of(command, 0) == (kind, value)
        assert command.Args(0).TypeHint() is None

    def test_object_argument(self):
        data = wire.pack_command(INVOKE, "add", 1, (Handle(99),), (Handle,))

        assert argument_of(read_command(data), 0) == (V.ObjectRef, 99)

    def test_arguments_of_every_kind_keep_their_order(self):
        # Strings of every length up to a few words: each one ends at a
        # different distance from the next argument's start
        args = (1, "a", 2.5, None, True, Handle(5), *("x" * n for n in range(20)), -3)
        command = read_command(
            wire.pack_command(INVOKE, "méthode", 2**40, args, (Handle,))
        )

        assert command.TargetName() == "méthode".encode()
        assert command.TargetId() == 2**40
        assert command.ArgsLength() == len(args)
        for index, value in enumerate(args):
            expected = value.object_id if isinstance(value, Handle) else value
            assert argument_of(command, index)[1] == expected

    def test_names_of_every_length(self):
        for length in range(40):
            name = "n" * length
            command = read_command(wire.pack_command(INVOKE, name, 1, (length,), ()))
            assert command.TargetName() == name.encode()
            assert argument_of(command, 0) == (V.IntVal, length)

    def test_return_object_ref(self):
        data = wire.pack_command(
            Action.Action.InvokeStaticMethod, "a.B.c", 0, (), (), True
        )

        command = read_command(data)
        assert command.ReturnObjectRef() is True
        assert command.Action() == Action.Action.InvokeStaticMethod

    @pytest.mark.parametrize(
        "value",
        [
            2**63,  # past 64 bits
            -(2**63) - 1,
            [1, 2],
            (1, 2),
            {"a": 1},
            b"bytes",
            TypeHint(1, "long"),
            enum.IntEnum("Colour", "RED").RED,  # a subclass of int
            "\ud800",  # not valid Unicode
            Handle(1),  # not among the types given as object references
        ],
    )
    def test_what_it_leaves_to_the_builder(self, value):
        assert wire.pack_command(INVOKE, "add", 1, (value,), ()) is None

    def test_a_name_or_an_id_of_another_type_is_left_to_the_builder(self):
        assert wire.pack_command(INVOKE, b"size", 1, (), ()) is None
        assert wire.pack_command(INVOKE, "size", "1", (), ()) is None
        assert wire.pack_command(INVOKE, "size", 2**63, (), ()) is None
        assert wire.pack_command(INVOKE, "size", True, (), ()) is None


class TestWithFrees:
    def test_ids_ride_on_a_command(self):
        plain = wire.pack_command(INVOKE, "indexOf", 7, ("world", 2), ())
        ids = [3, 2**40, 5]

        command = read_command(wire.with_frees(plain, ids, 9))

        assert [command.FreeIds(i) for i in range(command.FreeIdsLength())] == ids
        assert command.AnswersSent() == 9
        # The command itself is as it was
        assert command.Action() == INVOKE
        assert command.TargetName() == b"indexOf"
        assert command.TargetId() == 7
        assert command.ArgsLength() == 2
        assert argument_of(command, 0) == (V.StringVal, "world")
        assert argument_of(command, 1) == (V.IntVal, 2)

    def test_on_a_command_with_no_arguments(self):
        plain = wire.pack_command(INVOKE, "size", 7, (), ())

        command = read_command(wire.with_frees(plain, [1]))

        assert command.FreeIdsLength() == 1
        assert command.FreeIds(0) == 1
        assert command.ArgsLength() == 0
        assert command.AnswersSent() == 0

    def test_size_stays_within_what_was_reserved(self):
        for name_length in range(9):
            plain = wire.pack_command(INVOKE, "n" * name_length, 7, ("abc",), ())
            for count in (1, 2, 511, 512):
                packed = wire.with_frees(plain, list(range(count)))
                assert len(packed) - len(plain) <= wire.free_ids_size(count)

    def test_free_command(self):
        command = read_command(wire.pack_free([4, 5, 6], 2))

        assert command.Action() == Action.Action.FreeObject
        assert [command.FreeIds(i) for i in range(command.FreeIdsLength())] == [4, 5, 6]
        assert command.AnswersSent() == 2
        assert command.ArgsLength() == 0
        assert command.TargetId() == 0


def response_with(kind, add_value, force_defaults=False):
    """A Response holding one value, built the way the generated code builds it."""
    builder = flatbuffers.Builder(256)
    builder.ForceDefaults(force_defaults)
    value = add_value(builder)
    Response.Start(builder)
    Response.AddReturnValType(builder, kind)
    Response.AddReturnVal(builder, value)
    builder.Finish(Response.End(builder))
    return bytes(builder.Output())


def int_value(number):
    def add(builder):
        IntVal.Start(builder)
        IntVal.AddV(builder, number)
        return IntVal.End(builder)

    return add


def double_value(number):
    def add(builder):
        DoubleVal.Start(builder)
        DoubleVal.AddV(builder, number)
        return DoubleVal.End(builder)

    return add


def bool_value(flag):
    def add(builder):
        BoolVal.Start(builder)
        BoolVal.AddV(builder, flag)
        return BoolVal.End(builder)

    return add


def string_value(text):
    def add(builder):
        offset = builder.CreateString(text)
        StringVal.Start(builder)
        StringVal.AddV(builder, offset)
        return StringVal.End(builder)

    return add


def object_value(object_id):
    def add(builder):
        ObjectRef.Start(builder)
        ObjectRef.AddId(builder, object_id)
        return ObjectRef.End(builder)

    return add


def null_value(builder):
    NullVal.Start(builder)
    return NullVal.End(builder)


def as_pair(client, object_id):
    return ("object", client, object_id)


class TestReadPlain:
    # A field at its default value is left out of the table unless the writer
    # forces it in; both forms have to read the same
    @pytest.mark.parametrize("force_defaults", [False, True])
    @pytest.mark.parametrize(
        ("kind", "add_value", "expected"),
        [
            (V.IntVal, int_value(7), 7),
            (V.IntVal, int_value(0), 0),
            (V.IntVal, int_value(-(2**63)), -(2**63)),
            (V.DoubleVal, double_value(2.5), 2.5),
            (V.DoubleVal, double_value(0.0), 0.0),
            (V.BoolVal, bool_value(True), True),
            (V.BoolVal, bool_value(False), False),
            (V.StringVal, string_value("hello"), "hello"),
            (V.StringVal, string_value(""), ""),
            (V.StringVal, string_value("wörld ☃ 𝄞"), "wörld ☃ 𝄞"),
            (V.NullVal, null_value, None),
            (V.ObjectRef, object_value(12), ("object", "client", 12)),
        ],
    )
    def test_values(self, kind, add_value, expected, force_defaults):
        data = response_with(kind, add_value, force_defaults)

        value = wire.read_plain(data, "client", as_pair)

        assert value == expected
        assert type(value) is type(expected)

    def test_a_list_is_not_plain(self):
        def empty_list(builder):
            ListVal.Start(builder)
            return ListVal.End(builder)

        data = response_with(V.ListVal, empty_list)

        assert wire.read_plain(data, None, as_pair) is wire.NOT_PLAIN

    def test_an_error_is_not_plain(self):
        builder = flatbuffers.Builder(256)
        message = builder.CreateString("boom")
        error_type = builder.CreateString("java.lang.RuntimeException")
        Response.Start(builder)
        Response.AddIsError(builder, True)
        Response.AddErrorMsg(builder, message)
        Response.AddErrorType(builder, error_type)
        builder.Finish(Response.End(builder))

        assert wire.read_plain(bytes(builder.Output()), None, as_pair) is wire.NOT_PLAIN

    def test_a_callback_request_is_not_plain(self):
        builder = flatbuffers.Builder(256)
        method = builder.CreateString("compare")
        Response.Start(builder)
        Response.AddIsCallback(builder, True)
        Response.AddCallbackId(builder, 3)
        Response.AddCallbackMethod(builder, method)
        builder.Finish(Response.End(builder))

        assert wire.read_plain(bytes(builder.Output()), None, as_pair) is wire.NOT_PLAIN

    def test_a_value_with_anything_after_it_is_not_plain(self):
        builder = flatbuffers.Builder(256)
        value = int_value(7)(builder)
        Response.Start(builder)
        Response.AddReturnValType(builder, V.IntVal)
        Response.AddReturnVal(builder, value)
        Response.AddCallId(builder, 4)
        builder.Finish(Response.End(builder))

        assert wire.read_plain(bytes(builder.Output()), None, as_pair) is wire.NOT_PLAIN

    def test_a_response_with_no_value_is_not_plain(self):
        builder = flatbuffers.Builder(64)
        Response.Start(builder)
        builder.Finish(Response.End(builder))

        assert wire.read_plain(bytes(builder.Output()), None, as_pair) is wire.NOT_PLAIN


class TestPackedCalls:
    """The same answers whichever way a command was built."""

    def test_arguments_and_results_of_every_plain_kind(self, client):
        items = client.create_object("java.util.ArrayList")
        values = ["text", 7, 2**40, -1.5, True, False, None, "", 0, 0.0, "wörld ☃ 𝄞"]
        for value in values:
            assert items.add(value) is True

        assert items.size() == len(values)
        for index, value in enumerate(values):
            got = items.get(index)
            assert got == value
            assert type(got) is type(value)
        assert items.indexOf("text") == 0
        assert items.contains(2**40) is True
        assert items.contains("absent") is False

    def test_object_as_argument_and_result(self, client):
        outer = client.create_object("java.util.ArrayList")
        inner = client.create_object("java.lang.StringBuilder", "inside")

        outer.add(inner)

        assert isinstance(outer.get(0), JavaObject)
        assert outer.get(0).toString() == "inside"
        assert outer.contains(inner) is True

    def test_static_call_field_and_instance_check(self, client):
        assert client.invoke_static_method("java.lang.Math", "max", 10, 20) == 20
        assert client.invoke_static_method("java.lang.Math", "sqrt", 16.0) == 4.0
        assert client.get_static_field("java.lang.Integer", "MAX_VALUE") == 2**31 - 1
        text = client.create_object("java.lang.StringBuilder", "hello")
        assert client.get_field(text, "count") == 5
        assert client.is_instance_of(text, "java.lang.CharSequence") is True
        assert client.is_instance_of(text, "java.util.List") is False
        boxed = client.invoke_static_method(
            "java.lang.Integer", "valueOf", 5, return_object_ref=True
        )
        assert isinstance(boxed, JavaObject)
        assert boxed.intValue() == 5

    def test_an_error_arrives_as_its_exception(self, client):
        with pytest.raises(JavaNumberFormatException, match="not a number"):
            client.invoke_static_method("java.lang.Integer", "parseInt", "not a number")

    def test_arguments_the_packer_leaves_go_through_the_builder(self, client):
        items = client.create_object("java.util.ArrayList", [1, 2, 3])
        items.add({"a": 1})
        items.add(TypeHint(5, "long"))

        assert items.size() == 5
        assert items.get(3) == {"a": 1}
        assert items.get(4) == 5
        # The builder's own refusal of an integer past 64 bits
        with pytest.raises(TypeError, match="bad number"):
            items.add(2**70)

    def test_a_command_that_does_not_fit_is_refused(self, client):
        items = client.create_object("java.util.ArrayList")

        with pytest.raises(PayloadTooLargeError):
            items.add("x" * 70_000)

        assert items.size() == 0

    def test_mixed_with_builder_commands_on_one_client(self, client):
        items = client.create_object("java.util.ArrayList")
        for round_number in range(20):
            items.add(round_number)  # packed
            items.add([round_number])  # built
        with client.batch() as batch:
            size = batch.call(items, "size")

        assert size.get() == 40
        assert items.get(39) == [19]

    async def test_async_client(self):
        client = await aconnect()
        try:
            items = await client.create_object("java.util.ArrayList")
            for value in ("text", 7, -1.5, True, None):
                assert await items.add(value) is True
            assert await items.size() == 5
            assert await items.get(0) == "text"
            assert await items.get(4) is None
            assert await client.invoke_static_method("java.lang.Math", "max", 3, 9) == 9
            assert await client.is_instance_of(items, "java.util.List") is True
            with pytest.raises(JavaNumberFormatException):
                await client.invoke_static_method("java.lang.Integer", "parseInt", "x")
        finally:
            await client.close()


def requests_of(client, action):
    """How many requests of one action the server has counted."""
    for line in client.get_metrics().splitlines():
        if line.strip().startswith(f"{action}:"):
            return int(line.split("count=")[1].split()[0])
    return 0


def live_objects(client):
    return int(client.get_metrics().split("current_objects:")[1].split()[0])


@pytest.fixture
def own_client():
    """A client with a server to itself, so that its request counts are its own."""
    client = connect(allowed_classes=["java.lang.Thread"])
    yield client
    client.close()


class TestRidingReleases:
    """A dropped handle is released by the next command, not by a round trip."""

    def test_dropped_objects_are_released_without_a_request_of_their_own(
        self, own_client
    ):
        client = own_client
        kept = client.create_object("java.util.ArrayList")
        baseline = live_objects(client)

        for _ in range(100):
            client.create_object("java.util.ArrayList")
        gc.collect()
        assert kept.size() == 0

        assert live_objects(client) == baseline
        assert requests_of(client, "FreeObject") == 0

    def test_released_before_the_command_runs(self):
        """Each create is within the limit only once the one before is gone."""
        client = connect(extra_jvm_flags=["-Dgatun.limits.max_objects=2"])
        try:
            kept = client.create_object("java.util.ArrayList")
            for _ in range(20):
                client.create_object("java.util.ArrayList")

            assert kept.size() == 0
        finally:
            client.close()

    def test_more_than_one_chunk(self, own_client):
        client = own_client
        kept = client.create_object("java.util.ArrayList")
        baseline = live_objects(client)
        dropped = [
            client.create_objects([("java.util.ArrayList", ())] * 400) for _ in range(3)
        ]
        assert live_objects(client) == baseline + 1200

        del dropped
        gc.collect()
        # Each command takes a chunk of 512 with it
        for _ in range(3):
            kept.size()

        assert live_objects(client) == baseline
        assert requests_of(client, "FreeObject") == 0

    def test_a_command_with_no_room_sends_them_first(self, own_client):
        client = own_client
        kept = client.create_object("java.util.ArrayList")
        baseline = live_objects(client)
        dropped = client.create_objects([("java.util.ArrayList", ())] * 300)
        del dropped
        gc.collect()

        # 300 IDs take 2.4 KB; this command leaves less than that
        kept.add("x" * 64_000)

        assert live_objects(client) == baseline
        assert requests_of(client, "FreeObject") == 1
        assert kept.size() == 1

    def test_a_builder_command_sends_them_first(self, own_client):
        client = own_client
        kept = client.create_object("java.util.ArrayList")
        baseline = live_objects(client)
        client.create_object("java.util.ArrayList")
        gc.collect()

        kept.add([1, 2])  # a list argument is built, not packed

        assert live_objects(client) == baseline
        assert requests_of(client, "FreeObject") == 1

    def test_releases_on_a_cancelled_command_are_sent_again(self, own_client):
        """The client cannot tell whether the server got to them, so it repeats them."""
        client = own_client
        kept = client.create_object("java.util.ArrayList")
        baseline = live_objects(client)
        for _ in range(10):
            client.create_object("java.util.ArrayList")
        gc.collect()

        timer = threading.Timer(0.3, client.cancel)
        timer.start()
        try:
            with pytest.raises(CancelledException):
                client.invoke_static_method("java.lang.Thread", "sleep", 30_000)
        finally:
            timer.cancel()
        kept.size()

        assert live_objects(client) == baseline

    def test_explicit_free_is_still_immediate(self, own_client):
        client = own_client
        baseline = live_objects(client)
        handle = client.create_object("java.util.ArrayList")
        handle.detach()

        client.free_object(handle.object_id)

        assert requests_of(client, "FreeObject") == 1
        assert live_objects(client) == baseline

    async def test_async_client(self):
        client = await aconnect()
        try:
            kept = await client.create_object("java.util.ArrayList")
            metrics = await client.get_metrics()
            baseline = int(metrics.split("current_objects:")[1].split()[0])

            for _ in range(50):
                await client.create_object("java.util.ArrayList")
            gc.collect()
            assert await kept.size() == 0

            metrics = await client.get_metrics()
            assert int(metrics.split("current_objects:")[1].split()[0]) == baseline
            assert "FreeObject:" not in metrics
        finally:
            await client.close()
