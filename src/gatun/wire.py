"""The commonest commands written, and the commonest responses read, with `struct`.

The FlatBuffers library for Python is pure Python and costs several microseconds
per table: a method call with one argument took about 60 microseconds to build
and 16 to read the answer of, against some 30 for the server to run it and the
two processes to wake each other. What is here writes the same bytes, so the
server reads them like any other command, and anything it does not cover goes
through the builder as before:

- `pack_command`: a command that is an action, a name, an object ID and
  arguments that are each an `int`, a `float`, a `bool`, a `str`, `None` or a
  Java object. That is every method call, static call and constructor call
  whose arguments are plain.
- `with_frees`: the same command with object IDs to release added to it
  (`Command.free_ids`), so that dropping an object costs no round trip.
- `read_plain`: the value of a response that is one such value and nothing else.

The layout below is written out by hand from `schemas/commands.fbs`. A vtable
slot is 4 + 2 * (the field's position in its table), and a union field takes two
positions, its type and its value. Change the order of the fields of `Command`,
`Argument`, `Response` or the value tables there and this file must follow;
`tests/test_wire.py` reads what is packed here back with the generated classes.
"""

from __future__ import annotations

import struct
from typing import Any

from gatun.generated.org.gatun.protocol import Action, Value

_INT = Value.Value.IntVal
_STRING = Value.Value.StringVal
_DOUBLE = Value.Value.DoubleVal
_BOOL = Value.Value.BoolVal
_NULL = Value.Value.NullVal
_OBJECT = Value.Value.ObjectRef

# Every packed command starts with the same bytes: the offset of the Command
# table and the vtables of all the tables it can hold, each at a fixed place.
#
#   Command   18 fields; those used: action (0), target_name (1), target_id (2),
#             args (3), return_object_ref (6), answers_sent (14), free_ids (17)
#   Argument  val_type, val
#   a value   one field of 8 bytes (IntVal, DoubleVal, ObjectRef), one of up to
#             4 bytes (BoolVal, StringVal), or none (NullVal)
_VT_COMMAND = 4
_VT_ARGUMENT = 44
_VT_WORD = 52
_VT_SMALL = 60
_VT_EMPTY = 68
_COMMAND = 72

# Where the Command table keeps its fields, from the table's start
_AT_NAME = 4
_AT_TARGET_ID = 8
_AT_ANSWERS_SENT = 16
_AT_ARGS = 24
_AT_FREE_IDS = 28
_AT_ACTION = 32
_AT_RETURN_OBJECT_REF = 33
_COMMAND_SIZE = 40

_command_slots = [0] * 18
_command_slots[0] = _AT_ACTION
_command_slots[1] = _AT_NAME
_command_slots[2] = _AT_TARGET_ID
_command_slots[3] = _AT_ARGS
_command_slots[6] = _AT_RETURN_OBJECT_REF
_command_slots[14] = _AT_ANSWERS_SENT
_command_slots[17] = _AT_FREE_IDS

_HEADER = (
    struct.pack("<I", _COMMAND)
    + struct.pack("<20H", 40, _COMMAND_SIZE, *_command_slots)
    + struct.pack("<4H", 8, 12, 8, 4)
    + struct.pack("<3H2x", 6, 16, 8)
    + struct.pack("<3H2x", 6, 8, 4)
    + struct.pack("<2H", 4, 4)
)
assert len(_HEADER) == _COMMAND

# A vector with nothing in it, right after the Command table: what `args` and
# `free_ids` point to when there are none
_EMPTY_VECTOR = _COMMAND + _COMMAND_SIZE
_NAME = _EMPTY_VECTOR + 8

# The Command table and the empty vector: the vtable's distance, the name's,
# the object ID, answers_sent, the two vectors' distances, the action,
# return_object_ref
_pack_command_table = struct.Struct("<iIqqIIBB6x4x4x").pack
assert struct.calcsize("<iIqqIIBB6x4x4x") == _COMMAND_SIZE + 8
_NAME_DISTANCE = _NAME - (_COMMAND + _AT_NAME)
_NO_ARGS = _EMPTY_VECTOR - (_COMMAND + _AT_ARGS)
_NO_FREE_IDS = _EMPTY_VECTOR - (_COMMAND + _AT_FREE_IDS)

# An argument is an Argument table and its value's table 16 bytes further on,
# each starting with the distance back to its vtable. All of it is a multiple
# of 8 bytes long and starts at one, which keeps the 8-byte values aligned.
_pack_word_argument = struct.Struct("<iIB7xi4xq").pack
_pack_double_argument = struct.Struct("<iIB7xi4xd").pack
_pack_bool_argument = struct.Struct("<iIB7xi?3x").pack
_pack_null_argument = struct.Struct("<iIB7xi4x").pack
_pack_string_argument = struct.Struct("<iIB7xiII").pack
_VALUE_DISTANCE = 12

_PADDING = [bytes(n) for n in range(8)]

_u32 = struct.Struct("<I")
_i64 = struct.Struct("<q")


def pack_command(
    action: int,
    name: str,
    target_id: int,
    args: tuple[Any, ...],
    ref_types: tuple[type, ...],
    return_object_ref: bool = False,
) -> bytes | None:
    """The bytes of a command, or None if it has to go through the builder.

    The exact type of an argument decides. A subclass of `int` or `str`, a
    `TypeHint`, a collection, an integer that does not fit in 64 bits: all of
    those get None, and the builder treats them as it always has.
    """
    if type(name) is not str or type(target_id) is not int:
        return None
    try:
        text = name.encode("utf-8")
        string = _u32.pack(len(text)) + text + b"\0"
        string += _PADDING[-len(string) % 8]
        count = len(args)
        if not count:
            return b"".join(
                (
                    _HEADER,
                    _pack_command_table(
                        _COMMAND - _VT_COMMAND,
                        _NAME_DISTANCE,
                        target_id,
                        0,
                        _NO_ARGS,
                        _NO_FREE_IDS,
                        action,
                        return_object_ref,
                    ),
                    string,
                )
            )

        vector = _NAME + len(string)
        # The vector is a count and one distance per argument, each from its
        # own place in the vector to the argument's table
        at = vector + 4 + 4 * count
        skip = -at % 8
        at += skip
        distances = []
        blocks = []
        entry = vector + 4
        for value in args:
            kind = type(value)
            if kind is int:
                block = _pack_word_argument(
                    at - _VT_ARGUMENT,
                    _VALUE_DISTANCE,
                    _INT,
                    at + 16 - _VT_WORD,
                    value,
                )
            elif kind is str:
                data = value.encode("utf-8")
                block = (
                    _pack_string_argument(
                        at - _VT_ARGUMENT,
                        _VALUE_DISTANCE,
                        _STRING,
                        at + 16 - _VT_SMALL,
                        4,
                        len(data),
                    )
                    + data
                    + b"\0"
                )
                block += _PADDING[-len(block) % 8]
            elif kind is float:
                block = _pack_double_argument(
                    at - _VT_ARGUMENT,
                    _VALUE_DISTANCE,
                    _DOUBLE,
                    at + 16 - _VT_WORD,
                    value,
                )
            elif kind is bool:
                block = _pack_bool_argument(
                    at - _VT_ARGUMENT,
                    _VALUE_DISTANCE,
                    _BOOL,
                    at + 16 - _VT_SMALL,
                    value,
                )
            elif value is None:
                block = _pack_null_argument(
                    at - _VT_ARGUMENT, _VALUE_DISTANCE, _NULL, at + 16 - _VT_EMPTY
                )
            elif kind in ref_types:
                block = _pack_word_argument(
                    at - _VT_ARGUMENT,
                    _VALUE_DISTANCE,
                    _OBJECT,
                    at + 16 - _VT_WORD,
                    value.object_id,
                )
            else:
                return None
            distances.append(at - entry)
            entry += 4
            blocks.append(block)
            at += len(block)
        return b"".join(
            (
                _HEADER,
                _pack_command_table(
                    _COMMAND - _VT_COMMAND,
                    _NAME_DISTANCE,
                    target_id,
                    0,
                    vector - (_COMMAND + _AT_ARGS),
                    _NO_FREE_IDS,
                    action,
                    return_object_ref,
                ),
                string,
                struct.pack(f"<{count + 1}I", count, *distances),
                _PADDING[skip],
                *blocks,
            )
        )
    except (struct.error, UnicodeError):
        # An integer past 64 bits, a string that is not valid Unicode: the
        # builder's error for it is the one callers know
        return None


def with_frees(command: bytes, ids: list[int], answers_sent: int = 0) -> bytes:
    """A command from `pack_command` that also releases the objects `ids` name.

    The server releases them before it runs the command. `answers_sent` is what
    a FreeObject command carries: the number of callback answers the client had
    sent on its callback connection, which the server reads before it releases
    anything.
    """
    count = len(ids)
    # A count and then the IDs, which are 8 bytes each and so start at a
    # multiple of 8: the count goes 4 bytes before one
    skip = (4 - len(command)) % 8
    vector = len(command) + skip
    patched = bytearray(command)
    _i64.pack_into(patched, _COMMAND + _AT_ANSWERS_SENT, answers_sent)
    _u32.pack_into(patched, _COMMAND + _AT_FREE_IDS, vector - (_COMMAND + _AT_FREE_IDS))
    return b"".join((patched, _PADDING[skip], struct.pack(f"<I{count}q", count, *ids)))


def free_ids_size(count: int) -> int:
    """At most how many bytes `with_frees` adds for `count` IDs."""
    return 12 + 8 * count


def _free_nothing() -> bytes:
    command = pack_command(Action.Action.FreeObject, "", 0, (), ())
    assert command is not None
    return command


_FREE_NOTHING = _free_nothing()


def pack_free(ids: list[int], answers_sent: int = 0) -> bytes:
    """A FreeObject command that releases every object `ids` names."""
    return with_frees(_FREE_NOTHING, ids, answers_sent)


_u16 = struct.Struct("<H").unpack_from
_i32 = struct.Struct("<i").unpack_from
_u32_at = _u32.unpack_from
_i64_at = _i64.unpack_from
_f64 = struct.Struct("<d").unpack_from
_response_slots = struct.Struct("<5H").unpack_from

NOT_PLAIN: Any = object()

# A Response whose last field present is return_val has a vtable of this size:
# is_error, error_msg, error_type, and the two positions of return_val. An
# error, a callback, a batch or an Arrow answer has more, or lacks the value.
_PLAIN_VTABLE_SIZE = 14


def read_plain(buf: bytes, client: Any, ref_class: Any) -> Any:
    """The value a response holds, or NOT_PLAIN if it is anything but one value.

    The values read here are an integer, a string, a boolean, a double, null and
    an object, which becomes `ref_class(client, object_id)`. Everything else,
    errors and callbacks among it, is for the generated reader.
    """
    table = _u32_at(buf, 0)[0]
    vtable = table - _i32(buf, table)[0]
    if _u16(buf, vtable)[0] != _PLAIN_VTABLE_SIZE:
        return NOT_PLAIN
    is_error, _, _, kind_at, value_at = _response_slots(buf, vtable + 4)
    if is_error or not kind_at or not value_at:
        return NOT_PLAIN
    kind = buf[table + kind_at]
    value = table + value_at
    value += _u32_at(buf, value)[0]
    vtable = value - _i32(buf, value)[0]
    # A field that has its default value is left out: no slot, or a slot of 0
    at = _u16(buf, vtable + 4)[0] if _u16(buf, vtable)[0] > 4 else 0
    if kind == _INT:
        return _i64_at(buf, value + at)[0] if at else 0
    if kind == _STRING:
        if not at:
            return NOT_PLAIN
        start = value + at
        start += _u32_at(buf, start)[0]
        return str(buf[start + 4 : start + 4 + _u32_at(buf, start)[0]], "utf-8")
    if kind == _BOOL:
        return bool(buf[value + at]) if at else False
    if kind == _NULL:
        return None
    if kind == _DOUBLE:
        return _f64(buf, value + at)[0] if at else 0.0
    if kind == _OBJECT:
        return ref_class(client, _i64_at(buf, value + at)[0] if at else 0)
    return NOT_PLAIN
