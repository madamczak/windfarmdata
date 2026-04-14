"""
Tests for backend/telemetry.py.

Covers: TraceContextFilter, _attach_loki_to_logger, setup_loki_logging,
        setup_tracing (OTLP path), setup_telemetry wrapper, metrics_response,
        and all custom Prometheus metric objects.
"""

import logging
import os
from unittest.mock import MagicMock, patch, call

import pytest
from fastapi import FastAPI

from backend.telemetry import (
    TraceContextFilter,
    _attach_loki_to_logger,
    metrics_response,
    setup_loki_logging,
    setup_tracing,
    setup_telemetry,
    REQUESTS_TOTAL,
    REQUEST_DURATION,
    ERRORS_TOTAL,
    DATA_QUERIES_TOTAL,
    DATA_QUERY_DURATION,
    DATA_ROWS_RETURNED,
    DATE_RANGE_QUERIES_TOTAL,
    COLUMN_QUERIES_TOTAL,
    EMPTY_RESULTS_TOTAL,
    INVALID_DATE_REQUESTS_TOTAL,
    UNKNOWN_FARM_REQUESTS_TOTAL,
    REQUEST_TIME,
)


# ---------------------------------------------------------------------------
# TraceContextFilter
# ---------------------------------------------------------------------------

class TestTraceContextFilter:
    """Tests for TraceContextFilter."""

    def test_adds_trace_id_zeros_when_no_span(self):
        """When there is no active span the trace_id should be all zeros."""
        f = TraceContextFilter()
        record = logging.LogRecord("test", logging.INFO, "", 0, "msg", (), None)
        f.filter(record)
        assert record.trace_id == "0" * 32

    def test_adds_valid_trace_id_when_span_active(self):
        """When an active valid span exists the trace_id should be non-zero."""
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry import trace

        provider = TracerProvider()
        tracer = provider.get_tracer("test")

        f = TraceContextFilter()
        record = logging.LogRecord("test", logging.INFO, "", 0, "msg", (), None)

        with tracer.start_as_current_span("test-span"):
            result = f.filter(record)

        assert result is True
        assert len(record.trace_id) == 32
        assert record.trace_id != "0" * 32


# ---------------------------------------------------------------------------
# metrics_response
# ---------------------------------------------------------------------------

class TestMetricsResponse:
    """Tests for metrics_response()."""

    def test_returns_response_object(self):
        """Should return a Response with Prometheus text format."""
        resp = metrics_response()
        assert resp.status_code == 200

    def test_content_type_is_prometheus(self):
        """Content-Type must be the Prometheus exposition format."""
        resp = metrics_response()
        assert "text/plain" in resp.headers["content-type"]


# ---------------------------------------------------------------------------
# setup_loki_logging — no-op in test env
# ---------------------------------------------------------------------------

class TestSetupLokiLogging:
    """Tests for setup_loki_logging() in test environment (no-op path)."""

    def test_skips_when_testing_env_set(self):
        """When TESTING=1 no Loki handler should be attached."""
        setup_loki_logging()   # should be a no-op
        root = logging.getLogger("windfarm")
        loki_handlers = [h for h in root.handlers if "Loki" in type(h).__name__]
        assert loki_handlers == []



# ---------------------------------------------------------------------------
# setup_tracing — OTLP path (TESTING env var temporarily unset)
# ---------------------------------------------------------------------------

class TestSetupTracing:
    """Tests for setup_tracing()."""

    def test_no_otlp_exporter_in_test_env(self):
        """In TESTING mode no OTLP span processor should be attached."""
        app = FastAPI()
        setup_tracing(app)   # no-op path — should not raise or block

    def test_otlp_exporter_configured_outside_test_env(self):
        """Outside test env the OTLP exporter should be added to the provider."""
        app = FastAPI()
        # Patch os.environ.get so TESTING / PYTEST_CURRENT_TEST both return None
        original_get = os.environ.get

        def fake_get(key, default=None):
            if key in ("TESTING", "PYTEST_CURRENT_TEST"):
                return None
            return original_get(key, default)

        with patch("backend.telemetry.os.environ.get", side_effect=fake_get), \
             patch("backend.telemetry.OTLPSpanExporter") as mock_exp, \
             patch("backend.telemetry.SimpleSpanProcessor") as mock_proc, \
             patch("backend.telemetry.FastAPIInstrumentor") as mock_inst:
            mock_inst.instrument_app = MagicMock()
            setup_tracing(app)

        mock_exp.assert_called_once()
        mock_proc.assert_called_once()


# ---------------------------------------------------------------------------
# _attach_loki_to_logger
# ---------------------------------------------------------------------------

class TestAttachLokiToLogger:
    """Tests for _attach_loki_to_logger()."""

    def test_attaches_handler_to_logger(self):
        """A LokiHandler should be added to the named logger."""
        mock_handler = MagicMock()
        mock_handler.__class__.__name__ = "LokiHandler"
        mock_handler.level = logging.NOTSET   # must be an int for callHandlers
        mock_loki_module = MagicMock()
        mock_loki_module.LokiHandler.return_value = mock_handler

        logger = logging.getLogger("windfarm.test_attach")
        try:
            with patch.dict("sys.modules", {"logging_loki": mock_loki_module}):
                _attach_loki_to_logger("windfarm.test_attach", "http://loki:3100/loki/api/v1/push")
            assert mock_handler in logger.handlers
        finally:
            logger.handlers = [h for h in logger.handlers if h is not mock_handler]

    def test_does_not_duplicate_handler(self):
        """Calling twice should not add the handler a second time."""
        mock_handler = MagicMock()
        mock_handler.__class__.__name__ = "LokiHandler"
        mock_handler.level = logging.NOTSET
        mock_loki_module = MagicMock()
        mock_loki_module.LokiHandler.return_value = mock_handler

        logger = logging.getLogger("windfarm.test_dedup")
        try:
            with patch.dict("sys.modules", {"logging_loki": mock_loki_module}):
                _attach_loki_to_logger("windfarm.test_dedup", "http://loki:3100/loki/api/v1/push")
                _attach_loki_to_logger("windfarm.test_dedup", "http://loki:3100/loki/api/v1/push")
            count = sum(1 for h in logger.handlers if h is mock_handler)
            assert count == 1
        finally:
            logger.handlers = [h for h in logger.handlers if h is not mock_handler]


# ---------------------------------------------------------------------------
# setup_loki_logging — full Loki path (TESTING unset via patching)
# ---------------------------------------------------------------------------

class TestSetupLokiLoggingFull:
    """Tests for the non-test path of setup_loki_logging()."""

    def test_attaches_loki_handlers_when_not_testing(self):
        """When TESTING is not set, loki handlers should be attached."""
        mock_handler = MagicMock()
        mock_handler.__class__.__name__ = "LokiHandler"
        mock_handler.level = logging.NOTSET
        mock_loki_module = MagicMock()
        mock_loki_module.LokiHandler.return_value = mock_handler

        original_get = os.environ.get

        def fake_get(key, default=None):
            if key in ("TESTING", "PYTEST_CURRENT_TEST"):
                return None
            return original_get(key, default)

        try:
            with patch("backend.telemetry.os.environ.get", side_effect=fake_get), \
                 patch.dict("sys.modules", {"logging_loki": mock_loki_module}):
                setup_loki_logging()
            assert mock_loki_module.LokiHandler.call_count >= 1
        finally:
            # Remove any mock handlers leaked onto real loggers
            for name in ("windfarm", "uvicorn", "uvicorn.access", "uvicorn.error"):
                lg = logging.getLogger(name)
                lg.handlers = [h for h in lg.handlers if h is not mock_handler]

    def test_loki_failure_does_not_raise(self):
        """If logging_loki raises, setup_loki_logging should log a warning, not raise."""
        original_get = os.environ.get

        def fake_get(key, default=None):
            if key in ("TESTING", "PYTEST_CURRENT_TEST"):
                return None
            return original_get(key, default)

        with patch("backend.telemetry.os.environ.get", side_effect=fake_get), \
             patch.dict("sys.modules", {"logging_loki": None}):
            try:
                setup_loki_logging()
            except Exception as exc:
                pytest.fail(f"setup_loki_logging raised unexpectedly: {exc}")


# ---------------------------------------------------------------------------
# setup_telemetry (backwards-compat wrapper)
# ---------------------------------------------------------------------------

class TestSetupTelemetry:
    """Tests for the setup_telemetry() backwards-compat wrapper."""

    def test_does_not_raise(self):
        """setup_telemetry should call setup_loki_logging without raising."""
        app = FastAPI()
        try:
            setup_telemetry(app)
        except Exception as exc:
            pytest.fail(f"setup_telemetry raised unexpectedly: {exc}")


# ---------------------------------------------------------------------------
# Custom Prometheus metrics — smoke tests (instantiation + increment)
# ---------------------------------------------------------------------------

class TestCustomMetrics:
    """Smoke tests confirming all custom metrics are importable and usable."""

    def test_requests_total_increments(self):
        before = REQUESTS_TOTAL.labels(method="GET", route="/test", status_code="200")._value.get()
        REQUESTS_TOTAL.labels(method="GET", route="/test", status_code="200").inc()
        after = REQUESTS_TOTAL.labels(method="GET", route="/test", status_code="200")._value.get()
        assert after == before + 1

    def test_errors_total_increments(self):
        before = ERRORS_TOTAL.labels(route="/test", status_code="404")._value.get()
        ERRORS_TOTAL.labels(route="/test", status_code="404").inc()
        after = ERRORS_TOTAL.labels(route="/test", status_code="404")._value.get()
        assert after == before + 1

    def test_data_queries_total_increments(self):
        before = DATA_QUERIES_TOTAL.labels(farm="kelmarsh", file_type="data")._value.get()
        DATA_QUERIES_TOTAL.labels(farm="kelmarsh", file_type="data").inc()
        after = DATA_QUERIES_TOTAL.labels(farm="kelmarsh", file_type="data")._value.get()
        assert after == before + 1

    def test_empty_results_total_increments(self):
        before = EMPTY_RESULTS_TOTAL.labels(farm="kelmarsh", file_type="data")._value.get()
        EMPTY_RESULTS_TOTAL.labels(farm="kelmarsh", file_type="data").inc()
        after = EMPTY_RESULTS_TOTAL.labels(farm="kelmarsh", file_type="data")._value.get()
        assert after == before + 1

    def test_invalid_date_requests_total_increments(self):
        before = INVALID_DATE_REQUESTS_TOTAL.labels(farm="kelmarsh")._value.get()
        INVALID_DATE_REQUESTS_TOTAL.labels(farm="kelmarsh").inc()
        after = INVALID_DATE_REQUESTS_TOTAL.labels(farm="kelmarsh")._value.get()
        assert after == before + 1

    def test_unknown_farm_requests_total_increments(self):
        before = UNKNOWN_FARM_REQUESTS_TOTAL._value.get()
        UNKNOWN_FARM_REQUESTS_TOTAL.inc()
        after = UNKNOWN_FARM_REQUESTS_TOTAL._value.get()
        assert after == before + 1

    def test_date_range_queries_total_increments(self):
        before = DATE_RANGE_QUERIES_TOTAL._value.get()
        DATE_RANGE_QUERIES_TOTAL.inc()
        after = DATE_RANGE_QUERIES_TOTAL._value.get()
        assert after == before + 1

    def test_column_queries_total_increments(self):
        before = COLUMN_QUERIES_TOTAL._value.get()
        COLUMN_QUERIES_TOTAL.inc()
        after = COLUMN_QUERIES_TOTAL._value.get()
        assert after == before + 1

    def test_request_duration_histogram_observes(self):
        """Histogram observe() should not raise."""
        REQUEST_DURATION.labels(route="/test").observe(0.5)

    def test_data_query_duration_histogram_observes(self):
        DATA_QUERY_DURATION.labels(farm="kelmarsh", file_type="data").observe(1.2)

    def test_data_rows_returned_histogram_observes(self):
        DATA_ROWS_RETURNED.labels(farm="kelmarsh", file_type="data").observe(500)

