"""
scripts/test_otlp_trace.py - Sends a single test trace span directly to the
OTLP collector (port 4318) running inside Docker, then checks Tempo for it.

Run from outside Docker (on host) — targets localhost:4318.

Usage:
    python scripts/test_otlp_trace.py
"""

import sys
import time

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor, ConsoleSpanExporter

OTLP_ENDPOINT = "http://localhost:4318/v1/traces"
TEMPO_API     = "http://localhost:3200"   # Tempo HTTP API (inside otel-lgtm on port 3200)

# ── 1. Set up a provider with BOTH a console exporter (instant feedback)
#       and the real OTLP exporter pointing at localhost:4318
resource = Resource.create({"service.name": "windfarm-trace-test"})
provider = TracerProvider(resource=resource)

console_exporter = ConsoleSpanExporter()
provider.add_span_processor(SimpleSpanProcessor(console_exporter))

otlp_exporter = OTLPSpanExporter(endpoint=OTLP_ENDPOINT)
provider.add_span_processor(SimpleSpanProcessor(otlp_exporter))

trace.set_tracer_provider(provider)

# ── 2. Emit one span
tracer = trace.get_tracer("test-tracer")
trace_id_hex = None

with tracer.start_as_current_span("test-span-otlp") as span:
    span.set_attribute("test.source", "scripts/test_otlp_trace.py")
    span.set_attribute("test.target", OTLP_ENDPOINT)
    ctx = span.get_span_context()
    trace_id_hex = format(ctx.trace_id, "032x")
    print(f"\n[OK] Span created — trace_id: {trace_id_hex}")

# Flush (SimpleSpanProcessor is sync, so it's already exported by here)
provider.shutdown()
print(f"[OK] Span exported to {OTLP_ENDPOINT}")

# ── 3. Wait a moment then query Tempo to verify the trace arrived
time.sleep(3)
print(f"\n[CHECK] Querying Tempo for trace_id={trace_id_hex} ...")

import urllib.request
import urllib.error

tempo_url = f"{TEMPO_API}/api/traces/{trace_id_hex}"
try:
    req = urllib.request.Request(tempo_url)
    with urllib.request.urlopen(req, timeout=5) as resp:
        body = resp.read(500)
        print(f"[OK] Tempo returned HTTP {resp.status} — trace IS in Tempo!")
        print(f"     (first 500 bytes): {body[:200]}")
except urllib.error.HTTPError as e:
    if e.code == 404:
        print(f"[FAIL] Tempo returned 404 — trace NOT found in Tempo.")
        print("       Possible causes:")
        print("         - OTel collector did not forward the span to Tempo")
        print("         - Tempo has not yet ingested (try again in a few seconds)")
        print("         - Port 3200 not exposed — check docker-compose ports")
    else:
        print(f"[FAIL] Tempo returned HTTP {e.code}: {e.reason}")
except Exception as exc:
    print(f"[FAIL] Could not reach Tempo at {TEMPO_API}: {exc}")
    print("       Port 3200 may not be exposed in docker-compose.yml")
    print("       The span was still sent to the OTel collector — check Grafana manually.")
    print(f"       Grafana Explore -> Tempo -> TraceID: {trace_id_hex}")

print(f"\n[INFO] To view in Grafana:")
print(f"       http://localhost:3000 -> Explore -> Tempo -> TraceID: {trace_id_hex}")

