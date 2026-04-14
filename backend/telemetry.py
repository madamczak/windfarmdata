"""
backend/telemetry.py — LGTM observability setup.

Two-phase initialisation:

  Phase 1 — setup_tracing(app)
    Called at MODULE LEVEL in main.py, before the app receives any requests.
    Configures the OpenTelemetry TracerProvider and wraps FastAPI with
    FastAPIInstrumentor so every endpoint automatically gets a span.
    Must happen before app.add_middleware() and app.include_router().

  Phase 2 — setup_loki_logging()
    Called inside the FastAPI lifespan (startup hook).
    Attaches a LokiHandler to the root "windfarm" and "uvicorn" loggers.

  metrics_response()
    Returns a Prometheus /metrics payload.
"""

import logging
import os

from fastapi import FastAPI, Response
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from prometheus_client import (
    CONTENT_TYPE_LATEST,
    Counter,
    Histogram,
    Summary,
    generate_latest,
)

# ---------------------------------------------------------------------------
# Constants / defaults
# ---------------------------------------------------------------------------
_DEFAULT_OTLP_ENDPOINT = "http://localhost:4318"
_DEFAULT_LOKI_ENDPOINT = "http://localhost:3100/loki/api/v1/push"

SERVICE_NAME = "windfarm-api"

# ---------------------------------------------------------------------------
# Custom Prometheus metrics — windfarm API
# ---------------------------------------------------------------------------

# ── HTTP layer ──────────────────────────────────────────────────────────────

REQUEST_TIME = Summary(
    "windfarm_request_processing_seconds",
    "Time spent processing a request, labelled by endpoint.",
    ["endpoint"],
)

REQUESTS_TOTAL = Counter(
    "windfarm_requests_total",
    "Total HTTP requests handled, by method, route and status code.",
    ["method", "route", "status_code"],
)

REQUEST_DURATION = Histogram(
    "windfarm_request_duration_seconds",
    "HTTP request latency in seconds, by route.",
    ["route"],
    buckets=[0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0],
)

ERRORS_TOTAL = Counter(
    "windfarm_errors_total",
    "Total HTTP error responses (4xx/5xx), by route and status code.",
    ["route", "status_code"],
)

# ── Data query layer ─────────────────────────────────────────────────────────

DATA_QUERIES_TOTAL = Counter(
    "windfarm_data_queries_total",
    "Total day-data queries, labelled by farm and file_type.",
    ["farm", "file_type"],
)

DATA_QUERY_DURATION = Histogram(
    "windfarm_data_query_duration_seconds",
    "Time to fetch parquet data for a single day, by farm and file_type.",
    ["farm", "file_type"],
    buckets=[0.1, 0.5, 1.0, 2.5, 5.0, 10.0, 20.0, 40.0, 60.0, 120.0],
)

DATA_ROWS_RETURNED = Histogram(
    "windfarm_data_rows_returned",
    "Number of rows returned per day-data query, by farm.",
    ["farm", "file_type"],
    buckets=[0, 10, 50, 100, 500, 1000, 5000, 10000, 50000, 100000],
)

DATE_RANGE_QUERIES_TOTAL = Counter(
    "windfarm_date_range_queries_total",
    "Total /time-ranges endpoint calls.",
    [],
)

COLUMN_QUERIES_TOTAL = Counter(
    "windfarm_column_queries_total",
    "Total /columns endpoint calls.",
    [],
)

# ── Data-quality / bad-request counters ─────────────────────────────────────

EMPTY_RESULTS_TOTAL = Counter(
    "windfarm_empty_results_total",
    "Queries that returned zero rows, by farm and file_type.",
    ["farm", "file_type"],
)

INVALID_DATE_REQUESTS_TOTAL = Counter(
    "windfarm_invalid_date_requests_total",
    "Requests with a date outside the farm's available range, by farm.",
    ["farm"],
)

UNKNOWN_FARM_REQUESTS_TOTAL = Counter(
    "windfarm_unknown_farm_requests_total",
    "Requests for a farm name that does not exist.",
    [],
)

# ---------------------------------------------------------------------------
# Logging helpers
# ---------------------------------------------------------------------------

class TraceContextFilter(logging.Filter):
    """Inject the current OpenTelemetry trace_id into every log record."""

    def filter(self, record: logging.LogRecord) -> bool:
        span = trace.get_current_span()
        ctx = span.get_span_context() if span else None
        if ctx and ctx.is_valid:
            record.trace_id = format(ctx.trace_id, "032x")
        else:
            record.trace_id = "0" * 32
        return True


def _attach_loki_to_logger(logger_name: str, loki_url: str) -> None:
    """Attach a LokiHandler to the named logger."""
    import logging_loki  # lazy import

    loki_handler = logging_loki.LokiHandler(
        url=loki_url,
        tags={"app": SERVICE_NAME, "logger": logger_name},
        version="1",
    )
    formatter = logging.Formatter(
        "%(asctime)s  %(levelname)-8s  [%(name)s]  %(message)s"
        "  trace_id=%(trace_id)s"
    )
    loki_handler.setFormatter(formatter)
    loki_handler.addFilter(TraceContextFilter())

    logger = logging.getLogger(logger_name)
    if not any(isinstance(h, type(loki_handler)) for h in logger.handlers):
        logger.addHandler(loki_handler)
        logger.propagate = True


# ---------------------------------------------------------------------------
# Phase 1 — tracing (must be called at module level, before app is used)
# ---------------------------------------------------------------------------

def setup_tracing(app: FastAPI) -> None:
    """Configure the OTLP TracerProvider and instrument FastAPI.

    MUST be called at module level in main.py — before add_middleware and
    include_router — so that FastAPIInstrumentor can wrap the ASGI app
    before any requests arrive.

    During pytest runs (PYTEST_CURRENT_TEST env var is set by pytest) the
    OTLP exporter is skipped entirely so no network calls are made to the
    collector. A plain TracerProvider with no exporters is used instead.
    """
    _log = logging.getLogger("windfarm.telemetry")

    resource = Resource.create({"service.name": SERVICE_NAME})
    provider = TracerProvider(resource=resource)

    # Skip OTLP export when running under pytest — avoids connection-timeout
    # delays against localhost:4318 which is not running during tests.
    if not os.environ.get("PYTEST_CURRENT_TEST") and not os.environ.get("TESTING"):
        otlp_base = os.environ.get(
            "OTEL_EXPORTER_OTLP_ENDPOINT", _DEFAULT_OTLP_ENDPOINT
        ).rstrip("/")
        exporter = OTLPSpanExporter(endpoint=f"{otlp_base}/v1/traces")
        provider.add_span_processor(SimpleSpanProcessor(exporter))
        _log.info(
            "OpenTelemetry tracing configured -> %s/v1/traces  (service=%s)",
            otlp_base, SERVICE_NAME,
        )
    else:
        _log.debug("OpenTelemetry tracing: no-op mode (test environment, OTLP export disabled)")

    trace.set_tracer_provider(provider)
    FastAPIInstrumentor.instrument_app(app, tracer_provider=provider)


# ---------------------------------------------------------------------------
# Phase 2 — Loki logging (called inside lifespan startup)
# ---------------------------------------------------------------------------

def setup_loki_logging() -> None:
    """Attach Loki handlers to the windfarm and uvicorn logger hierarchies.

    Skipped during pytest runs to avoid network calls to the Loki endpoint.
    """
    if os.environ.get("PYTEST_CURRENT_TEST") or os.environ.get("TESTING"):
        logging.getLogger("windfarm.telemetry").debug(
            "Loki logging: disabled in test environment"
        )
        return

    loki_url = os.environ.get("LOKI_ENDPOINT", _DEFAULT_LOKI_ENDPOINT)
    setup_log = logging.getLogger("windfarm.telemetry")

    loki_targets = [
        "windfarm",        # all windfarm.* child loggers
        "uvicorn",
        "uvicorn.access",
        "uvicorn.error",
    ]
    try:
        for target in loki_targets:
            _attach_loki_to_logger(target, loki_url)
        setup_log.info(
            "Loki logging enabled -> %s  (loggers: %s)",
            loki_url, ", ".join(loki_targets),
        )
    except Exception as exc:  # noqa: BLE001
        setup_log.warning("Loki logging NOT enabled: %s", exc)


# ---------------------------------------------------------------------------
# Backwards-compatible wrapper kept so existing imports don't break
# ---------------------------------------------------------------------------

def setup_telemetry(app: FastAPI) -> None:
    """Deprecated wrapper — prefer calling setup_tracing() + setup_loki_logging() separately."""
    setup_loki_logging()


# ---------------------------------------------------------------------------
# Prometheus helpers
# ---------------------------------------------------------------------------

def metrics_response() -> Response:
    """Return a Prometheus-format metrics response for the /metrics endpoint."""
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

