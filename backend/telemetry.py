"""
backend/telemetry.py — LGTM observability setup.

Configures three pillars of observability:
  Logs    -> Loki  (via python-logging-loki handler attached to root windfarm logger)
  Traces  -> Tempo (via OpenTelemetry OTLP HTTP exporter)
  Metrics -> Mimir (via prometheus_client; scraped by Prometheus at /metrics)

Call ``setup_telemetry(app)`` once during FastAPI application startup.

Environment variables (all optional):
  OTEL_EXPORTER_OTLP_ENDPOINT   e.g. http://lgtm:4318  (default: http://localhost:4318)
  LOKI_ENDPOINT                  e.g. http://lgtm:3100/loki/api/v1/push
"""

import logging
import os

from fastapi import FastAPI, Response
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from prometheus_client import (
    CONTENT_TYPE_LATEST,
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
# Prometheus metric
# ---------------------------------------------------------------------------
REQUEST_TIME = Summary(
    "windfarm_request_processing_seconds",
    "Time spent processing a request, labelled by endpoint.",
    ["endpoint"],
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
    """Attach a LokiHandler to the named logger.

    Uses logger_name as a Loki label so records from different loggers
    are filterable in Grafana Explore.
    """
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
    # Avoid duplicate handlers if setup_telemetry is called more than once
    if not any(isinstance(h, type(loki_handler)) for h in logger.handlers):
        logger.addHandler(loki_handler)
        logger.propagate = True   # still goes to console via root handler


def setup_telemetry(app: FastAPI) -> None:
    """Wire up OpenTelemetry tracing, Loki logging, and Prometheus metrics."""
    otlp_base = os.environ.get(
        "OTEL_EXPORTER_OTLP_ENDPOINT", _DEFAULT_OTLP_ENDPOINT
    ).rstrip("/")
    loki_url = os.environ.get("LOKI_ENDPOINT", _DEFAULT_LOKI_ENDPOINT)

    setup_log = logging.getLogger("windfarm.telemetry")

    # ------------------------------------------------------------------ #
    # 1. Tracing — OTLP -> Tempo                                          #
    # ------------------------------------------------------------------ #
    resource = Resource.create({"service.name": SERVICE_NAME})
    tracer_provider = TracerProvider(resource=resource)
    otlp_exporter = OTLPSpanExporter(endpoint=f"{otlp_base}/v1/traces")
    tracer_provider.add_span_processor(BatchSpanProcessor(otlp_exporter))
    trace.set_tracer_provider(tracer_provider)

    # Auto-instrument FastAPI — every endpoint gets a span automatically
    FastAPIInstrumentor.instrument_app(app, tracer_provider=tracer_provider)
    setup_log.info("OpenTelemetry tracing enabled -> %s/v1/traces", otlp_base)

    # ------------------------------------------------------------------ #
    # 2. Logs — Loki                                                       #
    #                                                                      #
    # Attach the handler to the TOP-LEVEL "windfarm" logger so that ALL   #
    # child loggers (windfarm.main, windfarm.routers.*, windfarm.services  #
    # etc.) automatically ship every record to Loki.                       #
    # Also attach to uvicorn loggers so HTTP access logs appear in Grafana.#
    # ------------------------------------------------------------------ #
    loki_targets = [
        "windfarm",       # catches windfarm.main, windfarm.routers.*, etc.
        "uvicorn",        # uvicorn startup messages
        "uvicorn.access", # HTTP access log lines
        "uvicorn.error",  # uvicorn error messages
    ]
    try:
        for target in loki_targets:
            _attach_loki_to_logger(target, loki_url)
        setup_log.info(
            "Loki logging enabled -> %s  (loggers: %s)",
            loki_url,
            ", ".join(loki_targets),
        )
    except Exception as exc:  # noqa: BLE001
        setup_log.warning(
            "Loki logging NOT enabled: %s", exc
        )


def metrics_response() -> Response:
    """Return a Prometheus-format metrics response for the /metrics endpoint."""
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

