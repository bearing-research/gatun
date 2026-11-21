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
from gatun.callback_channel import MAX_FRAME_SIZE
from gatun.client import JavaObject, TypeHint, _nested_command
from gatun.generated.org.gatun.protocol import (
    Action,
    BatchResponse,
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

    def test_a_command_past_the_command_zone_is_left_to_the_builder(self):
        """The builder writes fewer bytes for the same command, and may fit it in."""
        fits = wire.pack_command(INVOKE, "add", 1, ("x" * 64_500,), ())
        assert fits is not None and len(fits) <= wire.COMMAND_ZONE_SIZE
        assert wire.pack_command(INVOKE, "add", 1, ("x" * 65_400,), ()) is None

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


BATCH_COMMANDS = [
    (Action.Action.CreateObject, 0, "java.util.ArrayList", (), False),
    (INVOKE, 7, "add", ("héllo",), False),
    (INVOKE, 2**40, "set", (0, 1.5), True),
    (Action.Action.InvokeStaticMethod, 0, "java.lang.Math.max", (10, 20), True),
    (Action.Action.GetField, 7, "size", (), False),
    (Action.Action.SetField, 7, "flag", (True,), False),
    (INVOKE, 7, "add", (None,), False),
    (INVOKE, 7, "add", (Handle(99),), False),
]


class TestPackBatch:
    @pytest.mark.parametrize("stop_on_error", [False, True])
    def test_every_command_reads_back(self, stop_on_error):
        outer = read_command(wire.pack_batch(BATCH_COMMANDS, (Handle,), stop_on_error))

        assert outer.Action() == Action.Action.Batch
        assert outer.TargetName() is None
        assert outer.ArgsLength() == 0
        assert outer.FreeIdsLength() == 0
        batch = outer.Batch()
        assert batch.StopOnError() is stop_on_error
        assert batch.CommandsLength() == len(BATCH_COMMANDS)
        for index, (action, target_id, name, args, ref) in enumerate(BATCH_COMMANDS):
            command = batch.Commands(index)
            assert command.Action() == action
            assert command.ReturnObjectRef() is ref
            assert command.TargetId() == target_id
            assert command.TargetName() == name.encode()
            assert command.ArgsLength() == len(args)
            assert command.Batch() is None
            for position, value in enumerate(args):
                expected = value.object_id if isinstance(value, Handle) else value
                assert argument_of(command, position)[1] == expected

    def test_any_number_of_commands(self):
        # The vector of commands is padded to a multiple of 8 for odd counts
        for count in range(1, 12):
            commands = [(INVOKE, n, "n" * n, (n,), False) for n in range(count)]
            batch = read_command(wire.pack_batch(commands, (), False)).Batch()
            assert batch.CommandsLength() == count
            for n in range(count):
                assert batch.Commands(n).TargetId() == n
                assert batch.Commands(n).TargetName() == b"n" * n
                assert argument_of(batch.Commands(n), 0) == (V.IntVal, n)

    def test_ids_ride_on_a_batch(self):
        packed = wire.pack_batch(BATCH_COMMANDS, (Handle,), True)

        outer = read_command(wire.with_frees(packed, [3, 4], 6))

        assert [outer.FreeIds(i) for i in range(outer.FreeIdsLength())] == [3, 4]
        assert outer.AnswersSent() == 6
        assert outer.Action() == Action.Action.Batch
        assert outer.Batch().StopOnError() is True
        assert outer.Batch().CommandsLength() == len(BATCH_COMMANDS)
        assert outer.Batch().Commands(1).TargetName() == b"add"

    @pytest.mark.parametrize(
        "command",
        [
            (INVOKE, 7, "add", ([1, 2],), False),
            (INVOKE, 7, "add", (2**70,), False),
            (INVOKE, 7, None, (), False),
            (INVOKE, 7, "add", (Handle(1),), False),  # not given as an object type
        ],
    )
    def test_one_command_for_the_builder_leaves_it_the_batch(self, command):
        commands = [BATCH_COMMANDS[1], command, BATCH_COMMANDS[2]]

        assert wire.pack_batch(commands, (), False) is None


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
        Response.AddCallbackId(builder, 4)
        builder.Finish(Response.End(builder))

        assert wire.read_plain(bytes(builder.Output()), None, as_pair) is wire.NOT_PLAIN

    def test_a_reply_on_the_callback_connection_is_a_value_with_its_call_id(self):
        builder = flatbuffers.Builder(256)
        value = int_value(7)(builder)
        Response.Start(builder)
        Response.AddReturnValType(builder, V.IntVal)
        Response.AddReturnVal(builder, value)
        Response.AddCallId(builder, 4)
        builder.Finish(Response.End(builder))

        assert wire.read_plain(bytes(builder.Output()), None, as_pair) == 7

    @pytest.mark.parametrize(
        "add",
        [
            lambda b: Response.AddIsCallback(b, True),
            lambda b: Response.AddCallbackId(b, 9),
            lambda b: Response.AddIsError(b, True),
            lambda b: Response.AddCallbackReleased(b, True),
        ],
        ids=["is_callback", "callback_id", "is_error", "callback_released"],
    )
    def test_a_call_id_with_anything_else_is_not_plain(self, add):
        builder = flatbuffers.Builder(256)
        value = int_value(7)(builder)
        Response.Start(builder)
        Response.AddReturnValType(builder, V.IntVal)
        Response.AddReturnVal(builder, value)
        add(builder)
        Response.AddCallId(builder, 4)
        builder.Finish(Response.End(builder))

        assert wire.read_plain(bytes(builder.Output()), None, as_pair) is wire.NOT_PLAIN

    def test_a_response_with_no_value_is_not_plain(self):
        builder = flatbuffers.Builder(64)
        Response.Start(builder)
        builder.Finish(Response.End(builder))

        assert wire.read_plain(bytes(builder.Output()), None, as_pair) is wire.NOT_PLAIN


class TestBatchValues:
    def batch_response(self, add_responses):
        """A Response holding a BatchResponse; returns the bytes and its table."""
        builder = flatbuffers.Builder(1024)
        responses = add_responses(builder)
        BatchResponse.StartResponsesVector(builder, len(responses))
        for offset in reversed(responses):
            builder.PrependUOffsetTRelative(offset)
        vector = builder.EndVector()
        BatchResponse.Start(builder)
        BatchResponse.AddResponses(builder, vector)
        batch = BatchResponse.End(builder)
        Response.Start(builder)
        Response.AddBatch(builder, batch)
        builder.Finish(Response.End(builder))
        data = bytes(builder.Output())
        return data, Response.Response.GetRootAsResponse(data, 0).Batch()._tab.Pos

    def test_each_response_is_read_or_left(self):
        def add_responses(builder):
            offsets = []
            for kind, add_value in (
                (V.IntVal, int_value(7)),
                (V.StringVal, string_value("wörld")),
                (V.BoolVal, bool_value(False)),
                (V.NullVal, null_value),
                (V.ObjectRef, object_value(12)),
                (V.DoubleVal, double_value(2.5)),
            ):
                value = add_value(builder)
                Response.Start(builder)
                Response.AddReturnValType(builder, kind)
                Response.AddReturnVal(builder, value)
                offsets.append(Response.End(builder))
            message = builder.CreateString("boom")
            Response.Start(builder)
            Response.AddIsError(builder, True)
            Response.AddErrorMsg(builder, message)
            offsets.append(Response.End(builder))
            offsets.append(offsets[0])  # the same table twice
            return offsets

        data, batch = self.batch_response(add_responses)

        assert wire.batch_values(data, batch, "client", as_pair) == [
            7,
            "wörld",
            False,
            None,
            ("object", "client", 12),
            2.5,
            wire.NOT_PLAIN,
            7,
        ]

    def test_no_responses(self):
        data, batch = self.batch_response(lambda builder: [])

        assert wire.batch_values(data, batch, None, as_pair) == []


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
        # An integer past 64 bits is the builder's too: a BigInteger
        items.add(2**70)
        assert items.get(5) == 2**70

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

    def test_batch_of_plain_commands(self, client):
        items = client.create_object("java.util.ArrayList")
        text = client.create_object("java.lang.StringBuilder", "hello")
        with client.batch() as batch:
            made = batch.create("java.util.ArrayList", 16)
            adds = [batch.call(items, "add", value) for value in ("a", 7, 1.5, None)]
            added = batch.call(items, "add", text)
            size = batch.call(items, "size")
            first = batch.call(items, "get", 0)
            last = batch.call(items, "get", 4)
            largest = batch.call_static("java.lang.Math", "max", 10, 20)
            count = batch.get_field(text, "count")
            cleared = batch.call(items, "clear")

        assert isinstance(made.get(), JavaObject)
        assert made.get().size() == 0
        assert [add.get() for add in adds] == [True] * 4
        assert added.get() is True
        assert size.get() == 5
        assert first.get() == "a"
        assert last.get().toString() == "hello"
        assert largest.get() == 20
        assert count.get() == 5
        assert cleared.get() is None
        assert items.size() == 0

    def test_batch_with_an_error_in_the_middle(self, client):
        items = client.create_object("java.util.ArrayList")
        with client.batch() as batch:
            before = batch.call(items, "add", "first")
            failed = batch.call_static("java.lang.Integer", "parseInt", "no")
            after = batch.call(items, "size")

        assert before.get() is True
        assert failed.is_error
        with pytest.raises(JavaNumberFormatException):
            failed.get()
        assert after.get() == 1

        with client.batch(stop_on_error=True) as batch:
            before = batch.call(items, "add", "second")
            failed = batch.call_static("java.lang.Integer", "parseInt", "no")
            after = batch.call(items, "add", "never")

        assert before.get() is True
        assert failed.is_error
        assert after.get() is None
        assert items.size() == 2

    def test_batch_with_a_command_for_the_builder(self, client):
        items = client.create_object("java.util.ArrayList")
        with client.batch() as batch:
            batch.call(items, "add", 1)
            batch.call(items, "add", [2, 3])  # a list: the whole batch is built
            batch.call(items, "add", TypeHint(4, "long"))
            size = batch.call(items, "size")

        assert size.get() == 3
        assert items.get(1) == [2, 3]

    def test_batch_of_many_commands(self, client):
        items = client.create_object("java.util.ArrayList")
        with client.batch() as batch:
            results = [batch.call(items, "add", f"item {n}") for n in range(500)]
            size = batch.call(items, "size")

        assert all(result.get() is True for result in results)
        assert size.get() == 500
        assert items.get(499) == "item 499"

    def test_a_string_that_only_the_builder_fits_in(self, client):
        items = client.create_object("java.util.ArrayList")
        text = "x" * 65_400

        items.add(text)

        assert items.get(0) == text

    def test_a_packed_command_fits_the_frame_it_gets_inside_a_callback(self):
        """There it travels wrapped, in a frame with the command zone's limit."""
        for length in range(64_800, 65_536, 7):
            data = wire.pack_command(INVOKE, "add", 5, ("x" * length,), (JavaObject,))
            if data is not None:
                assert len(_nested_command(1, data)) <= MAX_FRAME_SIZE

    def test_a_batch_that_only_the_builder_fits_in(self, client):
        items = client.create_object("java.util.ArrayList")
        commands = [(INVOKE, items.object_id, "add", (n,), False) for n in range(800)]
        assert wire.pack_batch(commands, (JavaObject,), False) is None

        with client.batch() as batch:
            for n in range(800):
                batch.call(items, "add", n)

        assert items.size() == 800

    def test_batch_too_large_for_a_command_is_refused(self, client):
        items = client.create_object("java.util.ArrayList")
        batch = client.batch()
        for _ in range(100):
            batch.call(items, "add", "x" * 1000)

        with pytest.raises(PayloadTooLargeError):
            batch.execute()

        assert items.size() == 0

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
        # More than one command carries: sent ahead of it, all of them
        kept.size()

        assert live_objects(client) == baseline

    def test_a_request_that_is_not_sent_keeps_them_queued(
        self, own_client, monkeypatch
    ):
        """Ending a loan comes first and can fail the request: cancel() may
        catch that exchange."""
        client = own_client
        kept = client.create_object("java.util.ArrayList")
        baseline = live_objects(client)
        for _ in range(3):
            client.create_object("java.util.ArrayList")
        gc.collect()
        kept.size()
        dropped = client.create_objects([("java.util.ArrayList", ())] * 3)
        del dropped
        gc.collect()
        assert len(client._pending_frees) == 3

        def cancelled():
            raise CancelledException("cancelled")

        with monkeypatch.context() as patched:
            patched.setattr(client, "_flush_loan_ends", cancelled)
            client._loan_ends.append(object())
            with pytest.raises(CancelledException):
                kept.size()
            client._loan_ends.clear()

        assert len(client._pending_frees) == 3
        assert kept.size() == 0
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

    def test_a_batch_takes_them_too(self, own_client):
        client = own_client
        kept = client.create_object("java.util.ArrayList")
        baseline = live_objects(client)
        for _ in range(10):
            client.create_object("java.util.ArrayList")
        gc.collect()

        with client.batch() as batch:
            size = batch.call(kept, "size")

        assert size.get() == 0
        assert live_objects(client) == baseline
        assert requests_of(client, "FreeObject") == 0

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

    async def test_async_client_more_than_one_chunk(self):
        client = await aconnect()
        try:
            kept = await client.create_object("java.util.ArrayList")
            metrics = await client.get_metrics()
            baseline = int(metrics.split("current_objects:")[1].split()[0])
            dropped = [
                await client.create_objects([("java.util.ArrayList", ())] * 400)
                for _ in range(3)
            ]
            del dropped
            gc.collect()

            assert await kept.size() == 0

            metrics = await client.get_metrics()
            assert int(metrics.split("current_objects:")[1].split()[0]) == baseline
        finally:
            await client.close()

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
