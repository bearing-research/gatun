"""What it costs Java to call a Python function.

Two routes, because Gatun has two:

- A callback made by the thread that is running the client's request (a
  comparator passed to `Collections.sort`) travels on the request's own
  channel.
- A callback made by any other Java thread travels on the client's callback
  connection, and may itself call Java.

Each case's set-up (building the list to sort, for one) happens outside the timed
part, and the cost of one callback is what a Python comparator adds to the same
sort with a Java comparator, divided by the number of comparisons.

    uv run python benchmarks/benchmark_callbacks.py [--quick] [--json FILE]
"""

from __future__ import annotations

import argparse
import random

from _harness import Report

from gatun import connect

FUTURE = "java.util.concurrent.CompletableFuture"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--quick", action="store_true", help="a smoke run")
    parser.add_argument("--json", help="write results to this file")
    args = parser.parse_args()

    report = Report("Python callbacks", quick=args.quick)
    # CompletableFuture is how a callback gets called from another thread here
    client = connect(allowed_classes=[FUTURE])
    try:
        collections = client.jvm.java.util.Collections
        calls = [0]

        def compare(a, b):
            calls[0] += 1
            return -1 if a < b else (1 if a > b else 0)

        comparator = client.register_callback(compare, "java.util.Comparator")

        report.section("On the request's thread: sorting with a Python comparator")
        for size in (10, 100, 1000):
            values = list(range(size))
            random.Random(7).shuffle(values)

            def fresh_list(values=values):
                return client.create_object("java.util.ArrayList", values)

            in_java = report.case(
                f"sort {size}, Java comparator",
                lambda items: collections.sort(items),
                setup=fresh_list,
                n=size,
            )
            calls[0] = 0
            in_python = report.case(
                f"sort {size}, Python comparator",
                lambda items: collections.sort(items, comparator),
                setup=fresh_list,
                # A thousand elements take thousands of callbacks a sort
                budget=3.0 if size >= 1000 else None,
                n=size,
            )
            stats = report.cases[-1]
            per_sort = calls[0] / (stats["runs"] + stats["warmup_runs"])
            stats["callbacks"] = per_sort
            stats["per_callback_us"] = (in_python - in_java) / per_sort
            report.note(
                f"{per_sort:.0f} callbacks a sort, "
                f"{stats['per_callback_us']:.1f} us each"
            )

        # With a callback timeout, the default, the client runs each callback
        # on a thread of its own so that it can stop waiting for it
        report.section("The same sort with callback_timeout=None")
        untimed = connect(callback_timeout=None)
        try:
            untimed_comparator = untimed.register_callback(
                compare, "java.util.Comparator"
            )
            untimed_collections = untimed.jvm.java.util.Collections
            values = list(range(100))
            random.Random(7).shuffle(values)
            calls[0] = 0
            in_python = report.case(
                "sort 100, Python comparator, no timeout",
                lambda items: untimed_collections.sort(items, untimed_comparator),
                setup=lambda: untimed.create_object("java.util.ArrayList", values),
                n=100,
            )
            stats = report.cases[-1]
            per_sort = calls[0] / (stats["runs"] + stats["warmup_runs"])
            java_sort = next(
                c["median_us"]
                for c in report.cases
                if c["name"] == "sort 100, Java comparator"
            )
            stats["callbacks"] = per_sort
            stats["per_callback_us"] = (in_python - java_sort) / per_sort
            report.note(
                f"{per_sort:.0f} callbacks a sort, "
                f"{stats['per_callback_us']:.1f} us each"
            )
        finally:
            untimed.close()

        report.section("On the request's thread: one callback")
        merge = client.register_callback(
            lambda old, new: old + new, "java.util.function.BiFunction"
        )

        def fresh_map():
            return client.create_object("java.util.HashMap", {"key": 1})

        plain = report.case(
            "HashMap.put (no callback)",
            lambda counts: counts.put("key", 2),
            setup=fresh_map,
        )
        with_callback = report.case(
            "HashMap.merge with a Python BiFunction",
            lambda counts: counts.merge("key", 2, merge),
            setup=fresh_map,
        )
        report.note(f"the callback adds {with_callback - plain:.1f} us")

        report.section("From another Java thread (the callback connection)")
        future = client.jvm.java.util.concurrent.CompletableFuture
        items = client.create_object("java.util.ArrayList", list(range(10)))
        supplier = client.register_callback(lambda: 7, "java.util.function.Supplier")
        calling_java = client.register_callback(
            lambda: items.size(), "java.util.function.Supplier"
        )

        plain = report.case(
            "completedFuture(7).join() (no callback)",
            lambda: future.completedFuture(7).join(),
        )
        on_thread = report.case(
            "supplyAsync(python).join()",
            lambda: future.supplyAsync(supplier).join(),
        )
        report.note(f"the callback adds {on_thread - plain:.1f} us")
        nested = report.case(
            "supplyAsync(python that calls Java).join()",
            lambda: future.supplyAsync(calling_java).join(),
        )
        report.note(f"the Java call inside adds {nested - on_thread:.1f} us")
    finally:
        client.close()
    report.write(args.json)


if __name__ == "__main__":
    main()
