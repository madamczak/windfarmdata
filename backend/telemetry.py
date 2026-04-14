"""
backend/telemetry.py — LGTM observability setup.

Configures three pillars of observability as described in the PDF guide:
  • Logs   → Loki  (via python-logging-loki handler)
  • Traces → Tempo  (via OpenTelemetry OTLP HTTP exporter)
  • Metrics→ Mimir  (via prometheus_client; scraped by Prometheus at /metrics)

Call ``setup_telemetry(app)`` once during FastAPI application startup.
The service also instruments FastAPI automatically with
``FastAPIInstrumentor`` so every endpoint gets spans + request metrics
without manual decoration.

Environment variables (all optional — fall back to localhost defaults):
  OTEL_EXPORTER_OTLP_ENDPOINT   Base URL of the OTel Collector, e.g.
                                  http://lgtm:4318  (default: http://localhost:4318)
  LOKI_ENDPOINT                  Full Loki push URL, e.g.
                                  http://lgtm:3100/loki/api/v1/push
                                  (default: http://localhost:3100/loki/api/v1/push)
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
# Prometheus metric — request processing time labelled by endpoint.
# Using a module-level singleton so it is shared across all imports.
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

    def filter(self, record: logging.LogRecord) -> bool:  # noqa: D102
        span = trace.get_current_span()
        ctx = span.get_span_context() if span else None
        if ctx and ctx.is_valid:
            record.trace_id = format(ctx.trace_id, "032x")
        else:
            record.trace_id = "0" * 32
        return True


def get_loki_logger(name: str, loki_url: str) -> logging.Logger:
    """Return a logger that ships records to Loki via *loki_url*.

    The logger also annotates every record with the current trace_id so that
    logs and traces can be correlated in Grafana.
    """
    import logging_loki  # imported lazily so missing dep only affects Loki path

    loki_handler = logging_loki.LokiHandler(
        url=loki_url,
        tags={"app": SERVICE_NAME, "logger": name},
        version="1",
    )
    formatter = logging.Formatter(
        "[%(name)s] %(asctime)s - %(levelname)s - %(message)s - trace_id=%(trace_id)s"
    )
    loki_handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.addHandler(loki_handler)
    logger.addFilter(TraceContextFilter())
    return logger


# ---------------------------------------------------------------------------
# Public setup function
# ---------------------------------------------------------------------------

def setup_telemetry(app: FastAPI) -> None:
    """Wire up OpenTelemetry tracing, Loki logging, and Prometheus metrics.

    Must be called *after* all routes have been added to *app* but *before*
    the server starts accepting requests (i.e. inside the lifespan context or
    at module level).
    """
    otlp_base = os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT", _DEFAULT_OTLP_ENDPOINT).rstrip("/")
    loki_url = os.environ.get("LOKI_ENDPOINT", _DEFAULT_LOKI_ENDPOINT)

    # -- Tracing setup -------------------------------------------------------
    resource = Resource.create({"service.name": SERVICE_NAME})
    tracer_provider = TracerProvider(resource=resource)
    otlp_exporter = OTLPSpanExporter(endpoint=f"{otlp_base}/v1/traces")
    tracer_provider.add_span_processor(BatchSpanProcessor(otlp_exporter))
    trace.set_tracer_provider(tracer_provider)

    # -- FastAPI auto-instrumentation ----------------------------------------
    FastAPIInstrumentor.instrument_app(app, tracer_provider=tracer_provider)

    # -- Loki logging --------------------------------------------------------
    try:
        get_loki_logger("windfarm.api", loki_url)
        logging.getLogger("windfarm.telemetry").info(
            "Loki logging enabled → %s", loki_url
        )
    except Exception as exc:  # noqa: BLE001
        logging.getLogger("windfarm.telemetry").warning(
            "Loki logging NOT enabled (%s). Install python-logging-loki to enable it.",
            exc,
        )

    logging.getLogger("windfarm.telemetry").info(
        "OpenTelemetry tracing enabled → %s/v1/traces", otlp_base
    )


def metrics_response() -> Response:
    """Return a Prometheus-format metrics response for the /metrics endpoint."""
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

