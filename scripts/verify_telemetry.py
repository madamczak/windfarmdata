"""
scripts/verify_telemetry.py — Verify the LGTM observability stack is working.

Checks:
  1. Backend /metrics endpoint (Prometheus metrics exposed by FastAPI)
  2. Backend /wind-farms endpoint (general API health)
  3. Prometheus / Mimir UI (port 9090)
  4. Loki push endpoint (port 3100)
  5. OTel Collector HTTP (port 4318)
  6. Grafana UI (port 3000)

Run this script AFTER `docker compose up` to confirm everything is wired up.

Usage:
    python scripts/verify_telemetry.py
"""

import sys
import time
import urllib.request
import urllib.error
import json

# ---------------------------------------------------------------------------
# Endpoints to check
# ---------------------------------------------------------------------------
CHECKS = [
    {
        "name": "Backend — /wind-farms",
        "url": "http://localhost:8000/wind-farms",
        "expected_status": 200,
        "check_json_key": "wind_farms",
    },
    {
        "name": "Backend — /metrics (Prometheus scrape target)",
        "url": "http://localhost:8000/metrics",
        "expected_status": 200,
        "expect_text": "windfarm_request_processing_seconds",
    },
    {
        "name": "Prometheus / Mimir UI",
        "url": "http://localhost:9090/-/healthy",
        "expected_status": 200,
    },
    {
        "name": "Loki push endpoint (should accept POST)",
        "url": "http://localhost:3100/loki/api/v1/push",
        "method": "POST",
        "post_data": json.dumps({
            "streams": [
                {
                    "stream": {"app": "verify_telemetry"},
                    "values": [[str(int(time.time() * 1e9)), "verify_telemetry: LGTM stack check"]]
                }
            ]
        }).encode(),
        "content_type": "application/json",
        "expected_status": 204,
    },
    {
        "name": "OTel Collector HTTP (port 4318) — health check",
        "url": "http://localhost:4318/",
        "expected_status": [200, 404, 405],   # collector may return 404/405 on GET /
    },
    {
        "name": "Grafana UI",
        "url": "http://localhost:3000/api/health",
        "expected_status": 200,
        "check_json_key": "database",
    },
]

# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

def check_endpoint(cfg: dict) -> bool:
    """Return True if the endpoint responds as expected."""
    url = cfg["url"]
    method = cfg.get("method", "GET")
    post_data = cfg.get("post_data")
    content_type = cfg.get("content_type", "application/json")
    expected = cfg.get("expected_status", 200)
    if isinstance(expected, int):
        expected = [expected]

    try:
        req = urllib.request.Request(url, method=method)
        if post_data:
            req.add_header("Content-Type", content_type)
            req.data = post_data

        with urllib.request.urlopen(req, timeout=5) as resp:
            status = resp.status
            body = resp.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as exc:
        status = exc.code
        body = exc.read().decode("utf-8", errors="replace")
    except urllib.error.URLError as exc:
        print(f"  ✗  FAIL — connection error: {exc.reason}")
        return False
    except Exception as exc:  # noqa: BLE001
        print(f"  ✗  FAIL — unexpected error: {exc}")
        return False

    if status not in expected:
        print(f"  ✗  FAIL — HTTP {status} (expected one of {expected})")
        return False

    # Optional body checks
    if "check_json_key" in cfg:
        try:
            data = json.loads(body)
            if cfg["check_json_key"] not in data:
                print(f"  ✗  FAIL — response JSON missing key '{cfg['check_json_key']}'")
                return False
        except json.JSONDecodeError:
            print(f"  ✗  FAIL — response is not valid JSON")
            return False

    if "expect_text" in cfg:
        if cfg["expect_text"] not in body:
            print(f"  ✗  FAIL — response body missing expected text: '{cfg['expect_text']}'")
            return False

    print(f"  ✓  OK   — HTTP {status}")
    return True


def main() -> int:
    print("=" * 60)
    print("  LGTM Observability Stack — Verification")
    print("=" * 60)

    results = []
    for cfg in CHECKS:
        print(f"\n[{cfg['name']}]")
        print(f"  URL: {cfg['url']}")
        ok = check_endpoint(cfg)
        results.append((cfg["name"], ok))

    print("\n" + "=" * 60)
    print("  Summary")
    print("=" * 60)
    all_ok = True
    for name, ok in results:
        icon = "✓" if ok else "✗"
        print(f"  {icon}  {name}")
        if not ok:
            all_ok = False

    print()
    if all_ok:
        print("  All checks passed — LGTM stack is fully operational!")
        print()
        print("  Grafana:    http://localhost:3000  (admin / admin)")
        print("  Prometheus: http://localhost:9090")
        print("  API Docs:   http://localhost:8000/docs")
        print("  Frontend:   http://localhost:80")
    else:
        print("  Some checks FAILED — see above for details.")
        print("  Make sure `docker compose up` has finished starting all services.")

    print("=" * 60)
    return 0 if all_ok else 1


if __name__ == "__main__":
    sys.exit(main())

