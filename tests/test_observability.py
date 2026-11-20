"""Tests for server observability features.

These tests verify that metrics are being tracked correctly by the server.
"""

import time

import pytest

from gatun import JavaException, connect
from gatun.client import GatunClient


class TestGetMetrics:
    """Test the get_metrics() API."""

    def test_get_metrics_returns_string(self, client):
        """Test that get_metrics returns a string report."""
        metrics = client.get_metrics()
        assert isinstance(metrics, str)
        assert "=== Gatun Server Metrics ===" in metrics

    def test_metrics_contains_global_stats(self, client):
        """Test that metrics report contains global statistics."""
        metrics = client.get_metrics()
        assert "Global:" in metrics
        assert "total_requests:" in metrics
        assert "total_errors:" in metrics
        assert "requests_per_sec:" in metrics
        assert "current_sessions:" in metrics
        assert "current_objects:" in metrics
        assert "peak_objects:" in metrics

    def test_metrics_contains_arrow_stats(self, client):
        """Test that metrics report contains Arrow statistics."""
        metrics = client.get_metrics()
        assert "Arrow:" in metrics
        assert "total_rows:" in metrics
        assert "total_bytes_copied:" in metrics

    def test_metrics_contains_callback_stats(self, client):
        """Test that metrics report contains callback statistics."""
        metrics = client.get_metrics()
        assert "Callbacks:" in metrics
        assert "total_invocations:" in metrics

    def test_metrics_contains_latency_section(self, client):
        """Test that metrics report contains per-action latency."""
        metrics = client.get_metrics()
        assert "Per-Action Latency" in metrics


class TestMetricsTracking:
    """Test that operations are correctly tracked in metrics."""

    def test_request_count_increases(self, client):
        """Test that request count increases with operations."""
        # Get initial metrics
        metrics1 = client.get_metrics()

        # Do some operations
        arr = client.create_object("java.util.ArrayList")
        arr.add("test")
        arr.size()

        # Get metrics again
        metrics2 = client.get_metrics()

        # Extract total_requests from both
        def extract_total_requests(metrics):
            for line in metrics.split("\n"):
                if "total_requests:" in line:
                    return int(line.split(":")[1].strip())
            return 0

        count1 = extract_total_requests(metrics1)
        count2 = extract_total_requests(metrics2)

        # Should have increased (at least 3: create + add + size)
        assert count2 > count1

    def test_object_count_field_present(self, client):
        """Test that object count fields are present in metrics."""
        # Create some objects
        objs = [client.create_object("java.util.ArrayList") for _ in range(5)]

        metrics = client.get_metrics()

        # The counts themselves are checked in TestMetricsAreRecorded, on a
        # server of its own; this one is shared with every other test
        assert "current_objects:" in metrics
        assert "peak_objects:" in metrics

        # Keep references to prevent GC
        _ = objs

    def test_error_count_tracked(self):
        """Test that errors are tracked in metrics."""
        client = connect()
        try:
            # Trigger some errors
            for _ in range(3):
                try:
                    client.invoke_method(999999, "nonexistent")
                except JavaException:
                    pass

            metrics = client.get_metrics()

            def extract_total_errors(metrics):
                for line in metrics.split("\n"):
                    if "total_errors:" in line:
                        return int(line.split(":")[1].strip())
                return 0

            errors = extract_total_errors(metrics)
            assert errors >= 3
        finally:
            client.close()


class TestMetricsPerAction:
    """Test per-action metrics tracking."""

    def test_create_object_tracked(self, client):
        """Test that CreateObject actions are tracked."""
        # Create several objects
        for _ in range(5):
            client.create_object("java.util.ArrayList")

        metrics = client.get_metrics()

        # Should see CreateObject in the per-action stats
        assert "CreateObject:" in metrics

    def test_invoke_method_tracked(self, client):
        """Test that InvokeMethod actions are tracked."""
        arr = client.create_object("java.util.ArrayList")
        for i in range(5):
            arr.add(i)

        metrics = client.get_metrics()

        # Should see InvokeMethod in the per-action stats
        assert "InvokeMethod:" in metrics

    def test_every_action_is_reported_by_name(self, client):
        """No action shows up as a number: Ping, action 0, was `Unknown(0)`."""
        client.ping()
        client.get_static_field("java.lang.Integer", "MAX_VALUE")

        metrics = client.get_metrics()

        assert "  Ping: count=" in metrics
        assert "  GetStaticField: count=" in metrics
        assert "Unknown(" not in metrics


class TestMetricsWithTracing:
    """Test metrics with tracing enabled."""

    def test_metrics_with_trace_mode(self):
        """Test that metrics work when trace mode is enabled."""
        client = connect(trace=True)
        try:
            arr = client.create_object("java.util.ArrayList")
            arr.add("test")

            metrics = client.get_metrics()
            assert "=== Gatun Server Metrics ===" in metrics
            assert "total_requests:" in metrics
        finally:
            client.close()


class TestAsyncMetrics:
    """Test async client get_metrics."""

    @pytest.mark.asyncio
    async def test_async_get_metrics(self):
        """Test that async get_metrics returns a string report."""
        from gatun import aconnect

        client = await aconnect()
        try:
            metrics = await client.get_metrics()
            assert isinstance(metrics, str)
            assert "=== Gatun Server Metrics ===" in metrics
            assert "Global:" in metrics
        finally:
            await client.close()


class TestMetricsIsolation:
    """Test that metrics aggregate across sessions."""

    def test_metrics_across_sessions(self):
        """Test that metrics accumulate across multiple sessions."""
        # First session
        client1 = connect()
        try:
            for _ in range(5):
                client1.create_object("java.util.ArrayList")

            # Get metrics from first session (verifies API works)
            _ = client1.get_metrics()
        finally:
            client1.close()

        # Second session - metrics should include both sessions' activity
        client2 = connect()
        try:
            for _ in range(5):
                client2.create_object("java.util.ArrayList")

            metrics2 = client2.get_metrics()

            # Total requests should be higher in second metrics
            def extract_total_requests(metrics):
                for line in metrics.split("\n"):
                    if "total_requests:" in line:
                        return int(line.split(":")[1].strip())
                return 0

            # Note: Different server instances won't share metrics
            # but within the same server process, metrics accumulate
            # This test just verifies the API works across sessions
            assert "total_requests:" in metrics2
        finally:
            client2.close()


def _metric(report: str, name: str) -> int:
    """Read one integer metric out of a metrics report."""
    for line in report.splitlines():
        if line.strip().startswith(f"{name}:"):
            return int(line.split(":", 1)[1])
    raise AssertionError(f"{name} not in metrics report")


def _action_count(report: str, action: str) -> int:
    for line in report.splitlines():
        if line.strip().startswith(f"{action}:"):
            return int(line.split("count=")[1].split()[0])
    return 0


class TestMetricsAreRecorded:
    """Regression: the object, Arrow and callback counters were never updated,
    so these sections of the report always read 0."""

    def test_object_counts(self):
        client = connect()
        try:
            held = [client.create_object("java.util.ArrayList") for _ in range(10)]
            report = client.get_metrics()
            assert _metric(report, "current_objects") == 10
            assert _metric(report, "peak_objects") == 10

            for obj in held[:4]:
                obj.detach()
                client.free_object(obj.object_id)
            report = client.get_metrics()
            assert _metric(report, "current_objects") == 6
            assert _metric(report, "peak_objects") == 10
        finally:
            client.close()

    def test_objects_of_a_closed_session_are_no_longer_counted(self):
        client = connect()
        try:
            other = GatunClient(client.socket_path)
            assert other.connect()
            keep = [other.create_object("java.util.ArrayList") for _ in range(5)]
            assert _metric(client.get_metrics(), "current_objects") == 5

            other.close()
            del keep
            for _ in range(50):
                if _metric(client.get_metrics(), "current_objects") == 0:
                    break
                time.sleep(0.05)
            assert _metric(client.get_metrics(), "current_objects") == 0
        finally:
            client.close()

    def test_callback_counts(self):
        client = connect()
        try:
            outcomes = []

            def compare(a, b):
                if a == "boom" or b == "boom":
                    outcomes.append("error")
                    raise ValueError("no")
                outcomes.append("ok")
                return (a > b) - (a < b)

            comparator = client.register_callback(compare, "java.util.Comparator")
            items = client.jvm.java.util.ArrayList()
            for text in ("b", "a", "c"):
                items.add(text)
            client.jvm.java.util.Collections.sort(items, comparator)
            assert outcomes.count("ok") >= 2

            items.add("boom")
            with pytest.raises(JavaException):
                client.jvm.java.util.Collections.sort(items, comparator)
            assert outcomes.count("error") == 1

            callbacks = client.get_metrics().split("Callbacks:")[1]
            assert _metric(callbacks, "total_invocations") == len(outcomes)
            assert _metric(callbacks, "total_errors") == 1
        finally:
            client.close()

    def test_arrow_counts(self):
        import pyarrow as pa

        client = connect()
        try:
            table = pa.table({"x": [1, 2, 3], "y": ["a", "b", "c"]})
            client.send_arrow_table(table)
            report = client.get_metrics()
            assert _metric(report, "total_rows") == 3
            ipc_bytes = _metric(report, "total_bytes_copied")
            assert ipc_bytes > 0

            arena = client.get_payload_arena()
            client.send_arrow_buffers(table, arena, {})
            received = client.get_arrow_data()
            assert received.num_rows == 3
            del received
            arena.close()

            report = client.get_metrics()
            assert _metric(report, "total_rows") == 9
            assert _metric(report, "total_bytes_copied") > ipc_bytes
        finally:
            client.close()

    def test_batch_and_arrow_requests_are_counted(self):
        """Regression: these two actions answered on their own path, which
        skipped the request bookkeeping."""
        import pyarrow as pa

        client = connect()
        try:
            items = client.create_object("java.util.ArrayList")
            for _ in range(3):
                with client.batch() as b:
                    b.call(items, "add", 1)

            arena = client.get_payload_arena()
            client.send_arrow_buffers(pa.table({"x": [1]}), arena, {})
            received = client.get_arrow_data()
            del received
            arena.close()

            report = client.get_metrics()
            assert _action_count(report, "Batch") == 3
            assert _action_count(report, "GetArrowData") == 1
        finally:
            client.close()

    def test_response_too_large_is_counted_as_an_error(self):
        """Regression: the request was recorded before the size check turned
        its response into an error, so it counted as a success."""
        from gatun import PayloadTooLargeError

        client = connect()
        try:
            sb = client.create_object("java.lang.StringBuilder", "x")
            items = client.create_object("java.util.ArrayList")
            before = _metric(client.get_metrics(), "total_errors")

            with pytest.raises(PayloadTooLargeError):
                client.jvm.java.util.Collections.nCopies(5000, sb)
            with pytest.raises(PayloadTooLargeError), client.batch() as b:
                for _ in range(40):
                    b.call_static("java.util.Collections", "nCopies", 200, sb)
                b.call(items, "size")

            assert _metric(client.get_metrics(), "total_errors") == before + 2
        finally:
            client.close()
