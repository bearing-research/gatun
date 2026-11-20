"""Several operations in one round trip against one round trip each.

Compares, for the same work:

- one call at a time
- `client.batch()`, which sends any mix of commands together
- the vectorized calls (`invoke_methods`, `create_objects`, `get_fields`), which
  send several operations on one target or of one kind

The method calls used do not change the list they are made on, so no case needs
cleaning up between runs, and each one makes exactly the round trips its name
says.

    uv run python benchmarks/benchmark_vectorized.py [--quick] [--json FILE]
"""

from __future__ import annotations

import argparse

from _harness import Report

from gatun import connect

# StringBuilder's fields are not public; get_field reaches them all the same
FIELDS = ["count", "coder"]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--quick", action="store_true", help="a smoke run")
    parser.add_argument("--json", help="write results to this file")
    args = parser.parse_args()

    report = Report("Vectorized calls and batches", quick=args.quick)
    client = connect()
    try:
        items = client.create_object("java.util.ArrayList", list(range(100)))

        report.section("Method calls on one object")
        for count in (3, 10, 100):
            calls = [("contains", (i,)) for i in range(count)]

            def one_by_one(calls=calls):
                for name, call_args in calls:
                    client.invoke_method(items.object_id, name, *call_args)

            def batched(calls=calls):
                with client.batch() as b:
                    for name, call_args in calls:
                        b.call(items, name, *call_args)

            def vectorized(calls=calls):
                client.invoke_methods(items, calls)

            single = report.case(f"{count} calls, one at a time", one_by_one, n=count)
            batch = report.case(f"{count} calls, batch()", batched, n=count)
            vector = report.case(
                f"{count} calls, invoke_methods()", vectorized, n=count
            )
            report.note(
                f"batch {single / batch:.1f}x, invoke_methods {single / vector:.1f}x "
                "faster than one at a time"
            )

        report.section("Object creation")
        for count in (3, 10, 100):
            specs = [("java.util.ArrayList", ()) for _ in range(count)]

            def one_by_one(specs=specs):
                return [client.create_object(name, *a) for name, a in specs]

            def batched(specs=specs):
                with client.batch() as b:
                    return [b.create(name, *a) for name, a in specs]

            def vectorized(specs=specs):
                return client.create_objects(specs)

            single = report.case(f"{count} objects, one at a time", one_by_one, n=count)
            batch = report.case(f"{count} objects, batch()", batched, n=count)
            vector = report.case(
                f"{count} objects, create_objects()", vectorized, n=count
            )
            report.note(
                f"batch {single / batch:.1f}x, create_objects {single / vector:.1f}x "
                "faster than one at a time"
            )

        report.section("Field reads on one object")
        text = client.create_object("java.lang.StringBuilder", "hello world")

        def fields_one_by_one():
            return [client.get_field(text, name) for name in FIELDS]

        single = report.case(
            f"{len(FIELDS)} fields, one at a time", fields_one_by_one, n=len(FIELDS)
        )
        vector = report.case(
            f"{len(FIELDS)} fields, get_fields()",
            lambda: client.get_fields(text, FIELDS),
            n=len(FIELDS),
        )
        report.note(f"get_fields {single / vector:.1f}x faster than one at a time")
        report.case(
            "1 field, get_field()", lambda: client.get_field(text, "count"), n=1
        )
        report.case(
            "1 field, get_fields()", lambda: client.get_fields(text, ["count"]), n=1
        )
    finally:
        client.close()
    report.write(args.json)


if __name__ == "__main__":
    main()
