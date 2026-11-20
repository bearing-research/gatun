"""Arrow transfer by size, for every way gatun has of moving a table.

Python to Java:

- `send_arrow_table`: an IPC stream that Java reads and counts
- `send_arrow_buffers`: the table's buffers copied into the payload arena and
  loaded in place by the server
- `arrow_to_java`: an IPC stream that becomes a `VectorSchemaRoot` Java code can
  use, copied into the JVM
- `arrow_to_java(copy=False)`: the same, lent in place for the length of a block
- `arrow_to_java_batches`: the record batches as `byte[]`

Java to Python: `arrow_from_java`, `arrow_from_java(copy=False)` and
`get_arrow_data`.

These time the handoff: the call returning with the table on the other side.
`benchmark_transport.py` also times the receiver reading every value, and has a
Unix socket and a file as baselines; it needs a Java helper compiled, which this
script does not.

The last two sections put Arrow next to the other ways of getting numbers into
Java: a Python list, a batch of `add()` calls, a `pyarrow.Array` argument.

    uv run python benchmarks/benchmark_arrow.py [--quick] [--json FILE]
"""

from __future__ import annotations

import argparse

import pyarrow as pa
from _harness import Report

from gatun import connect

ROW_COUNTS = (1_000, 10_000, 100_000, 1_000_000)


def numbers(rows: int) -> pa.Table:
    """16 bytes a row: an int64 and a float64 column."""
    ids = pa.array(range(rows), type=pa.int64())
    return pa.table({"id": ids, "value": ids.cast(pa.float64())})


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--quick", action="store_true", help="a smoke run")
    parser.add_argument("--json", help="write results to this file")
    args = parser.parse_args()

    report = Report("Arrow transfer", quick=args.quick)
    client = connect(memory="256MB")
    arena = client.get_payload_arena()
    schema_cache: dict[int, bool] = {}

    def reset_arena() -> None:
        arena.reset()
        client.reset_payload_arena()

    try:
        for rows in ROW_COUNTS:
            table = numbers(rows)
            size = table.nbytes
            # A run of a megabyte and more is bounded by time, not by count
            budget = 3.0 if size >= 1 << 20 else None
            label = f"{rows:,} rows"

            def case(name, func, setup=None, rows=rows, size=size, budget=budget):
                return report.case(
                    name, func, setup=setup, budget=budget, rows=rows, bytes=size
                )

            report.section(f"Python to Java, {label} ({size / 1e6:.2f} MB)")

            def lend(table=table):
                with client.arrow_to_java(table, copy=False) as root:
                    return root

            case(
                f"send_arrow_table, {label}", lambda t=table: client.send_arrow_table(t)
            )
            case(
                f"send_arrow_buffers, {label}",
                lambda _, t=table: client.send_arrow_buffers(t, arena, schema_cache),
                setup=reset_arena,
            )
            reset_arena()
            # The handle is dropped as the call returns; its release goes out
            # with the next request, which is how a caller would use it
            case(f"arrow_to_java, {label}", lambda t=table: client.arrow_to_java(t))
            case(f"arrow_to_java(copy=False), {label}", lend)
            case(
                f"arrow_to_java_batches, {label}",
                lambda t=table: client.arrow_to_java_batches(t),
            )

            report.section(f"Java to Python, {label} ({size / 1e6:.2f} MB)")
            in_java = client.arrow_to_java(table)
            case(
                f"arrow_from_java, {label}",
                lambda root=in_java: client.arrow_from_java(root),
            )
            case(
                f"arrow_from_java(copy=False), {label}",
                lambda root=in_java: client.arrow_from_java(root, copy=False),
            )
            in_java.detach()
            client.free_object(in_java.object_id)
            # get_arrow_data returns the batch that send_arrow_buffers loaded
            reset_arena()
            client.send_arrow_buffers(table, arena, schema_cache)
            case(f"get_arrow_data, {label}", client.get_arrow_data)
            reset_arena()

        report.section("500 integers into Java, five ways")
        values = list(range(500))
        column = pa.table({"values": pa.array(values, type=pa.int64())})
        ints = pa.array(values, type=pa.int32())
        arrays = client.jvm.java.util.Arrays

        def one_by_one():
            target = client.create_object("java.util.ArrayList")
            for value in values:
                target.add(value)

        def batched():
            target = client.create_object("java.util.ArrayList")
            with client.batch() as b:
                for value in values:
                    b.call(target, "add", value)

        single = report.case("500 add() calls", one_by_one, n=500)
        for name, func in (
            ("500 add() calls in a batch()", batched),
            (
                "ArrayList(python list)",
                lambda: client.create_object("java.util.ArrayList", values),
            ),
            ("pyarrow.Array argument -> int[]", lambda: arrays.hashCode(ints)),
            ("arrow_to_java(table)", lambda: client.arrow_to_java(column)),
        ):
            took = report.case(name, func, n=500)
            report.note(f"{single / took:.0f}x faster than 500 add() calls")

        report.section("Collections and arrays as arguments")
        for count in (100, 500):
            items = list(range(count))
            words = [f"word_{i}" for i in range(count)]
            # ints and strings in one list: built one element at a time
            mixed = [i if i % 2 else f"word_{i}" for i in range(count)]
            mapping = {f"key_{i}": i for i in range(count)}
            int_array = pa.array(items, type=pa.int32())
            double_array = pa.array(items, type=pa.float64())
            for name, value in (
                (f"list of {count} ints -> ArrayList", items),
                (f"list of {count} strings -> ArrayList", words),
                (f"list of {count} ints and strings -> ArrayList", mixed),
            ):
                report.case(
                    name,
                    lambda v=value: client.create_object("java.util.ArrayList", v),
                    n=count,
                )
            report.case(
                f"dict of {count} entries -> HashMap",
                lambda v=mapping: client.create_object("java.util.HashMap", v),
                n=count,
            )
            report.case(
                f"pyarrow int32 array of {count} -> int[]",
                lambda v=int_array: arrays.hashCode(v),
                n=count,
            )
            report.case(
                f"pyarrow float64 array of {count} -> double[]",
                lambda v=double_array: arrays.hashCode(v),
                n=count,
            )

        report.section("Collections as return values")
        for count in (100, 500):
            for name, java_class, value in (
                (
                    f"ArrayList of {count} ints -> list",
                    "java.util.ArrayList",
                    list(range(count)),
                ),
                (
                    f"ArrayList of {count} strings -> list",
                    "java.util.ArrayList",
                    [f"word_{i}" for i in range(count)],
                ),
                (
                    f"HashMap of {count} entries -> dict",
                    "java.util.HashMap",
                    {f"key_{i}": i for i in range(count)},
                ),
            ):
                held = client.create_object(java_class, value)
                # clone() returns the collection itself, which arrives as its elements
                report.case(
                    name,
                    lambda h=held: client.invoke_method(h.object_id, "clone"),
                    n=count,
                )
    finally:
        arena.close()
        client.close()
    report.write(args.json)


if __name__ == "__main__":
    main()
