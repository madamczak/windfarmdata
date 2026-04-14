"""
scripts/traffic.py - Traffic generator for the Wind Farm API.

Sends realistic HTTP traffic to the running backend (http://localhost:8000)
so that traces, logs, and metrics are visible in Grafana.

Scenario
--------
1.  GET /wind-farms                  — list farms (repeated N times)
2.  GET /wind-farms/time-ranges      — discover available date range
3.  GET /wind-farms/columns          — discover column names
4.  GET /wind-farms/{farm}/data/{date}  — data requests for sampled dates
      - with specific columns  (Wind speed, Power, Rotor speed, etc.)
      - with no columns selected (full row)
      - with file_type=status
5.  GET /metrics                     — Prometheus scrape (simulates Prometheus)
6.  Error probing: 404, 422, 400 to verify error paths produce spans

Usage
-----
    python scripts/traffic.py [--base-url URL] [--rounds N] [--delay S]

Arguments
---------
  --base-url  Base URL of the running backend (default: http://localhost:8000)
  --rounds    Number of full traffic rounds to execute (default: 5)
  --delay     Seconds to pause between individual requests (default: 0.3)
"""

import argparse
import logging
import random
import sys
import time
from datetime import date, timedelta

import requests

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("traffic")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
DEFAULT_BASE_URL = "http://localhost:8000"
DEFAULT_ROUNDS = 5
DEFAULT_DELAY = 0.3   # seconds between requests

FAST_TIMEOUT = 10     # seconds
SLOW_TIMEOUT = 120    # seconds (R2 metadata scan: time-ranges / columns)
DATA_TIMEOUT = 60     # seconds (per-day data requests — overridable via --data-timeout)

# Interesting column sets to request  (kept small to minimise payload / R2 scan time)
COLUMN_SETS = [
    ["Wind speed (m/s)", "Power (kW)"],
    ["Wind speed (m/s)", "Wind direction (°)", "Power (kW)", "Rotor speed (RPM)"],
    ["Nacelle position (°)", "Generator RPM (RPM)"],
    ["Wind speed (m/s)", "Power (kW)", "Nacelle position (°)"],
]


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

def _get(session: requests.Session, base_url: str, path: str,
         params: dict | None = None, timeout: int = FAST_TIMEOUT,
         label: str = "") -> requests.Response | None:
    """Perform a GET request and log the outcome. Returns response or None on error."""
    url = f"{base_url}{path}"
    try:
        r = session.get(url, params=params, timeout=timeout)
        status_label = "OK" if r.status_code < 400 else "ERR"
        log.info("[%s] GET %-55s  %s %d  (%.0f ms)",
                 status_label, path[:55], label, r.status_code,
                 r.elapsed.total_seconds() * 1000)
        return r
    except requests.exceptions.Timeout:
        log.error("[TMO] GET %s timed out after %ss", path, timeout)
        return None
    except requests.exceptions.ConnectionError as exc:
        log.error("[CON] GET %s connection error: %s", path, exc)
        return None


def _wait(delay: float) -> None:
    """Sleep *delay* seconds between requests."""
    if delay > 0:
        time.sleep(delay)


# ---------------------------------------------------------------------------
# Traffic scenarios
# ---------------------------------------------------------------------------

def scenario_list_farms(session: requests.Session, base_url: str,
                         delay: float, repeat: int = 3) -> list[dict]:
    """Hit /wind-farms *repeat* times and return the farm list."""
    farms = []
    for i in range(repeat):
        r = _get(session, base_url, "/wind-farms", label=f"(call {i + 1}/{repeat})")
        _wait(delay)
        if r and r.status_code == 200:
            farms = r.json().get("wind_farms", [])
    return farms


def scenario_time_ranges(session: requests.Session, base_url: str,
                          delay: float) -> dict[str, dict]:
    """Hit /wind-farms/time-ranges and return a mapping of farm → time-range entry."""
    r = _get(session, base_url, "/wind-farms/time-ranges",
             timeout=SLOW_TIMEOUT, label="(metadata scan)")
    _wait(delay)
    if not r or r.status_code != 200:
        return {}
    entries = r.json().get("time_ranges", [])
    return {e["farm"]: e for e in entries if e.get("earliest")}


def scenario_columns(session: requests.Session, base_url: str,
                      delay: float) -> dict[str, dict]:
    """Hit /wind-farms/columns and return a mapping of farm → columns_by_type."""
    r = _get(session, base_url, "/wind-farms/columns",
             timeout=SLOW_TIMEOUT, label="(schema scan)")
    _wait(delay)
    if not r or r.status_code != 200:
        return {}
    entries = r.json().get("farms", [])
    return {e["farm"]: e["columns_by_type"] for e in entries}


def scenario_day_data(session: requests.Session, base_url: str, delay: float,
                       farm: str, time_range: dict,
                       columns_by_type: dict,
                       data_timeout: int = DATA_TIMEOUT) -> None:
    """Generate data requests for a farm across several sampled dates."""
    earliest_str = time_range.get("earliest")
    latest_str = time_range.get("latest")
    if not earliest_str or not latest_str:
        log.warning("Skipping day-data for '%s' — no time range available", farm)
        return

    earliest = date.fromisoformat(earliest_str[:10])
    latest = date.fromisoformat(latest_str[:10])
    total_days = (latest - earliest).days

    if total_days < 0:
        log.warning("Skipping day-data for '%s' — invalid time range", farm)
        return

    # Sample up to 4 dates spread across the range
    sample_offsets = [0, total_days // 4, total_days // 2, max(0, total_days - 1)]
    sampled_dates = list(dict.fromkeys(
        earliest + timedelta(days=offset) for offset in sample_offsets
    ))

    # Available file types for this farm
    file_types = list(columns_by_type.keys()) if columns_by_type else ["data"]

    for file_type in file_types:
        for query_date in sampled_dates:
            # Cycle through interesting column sets
            col_set = random.choice(COLUMN_SETS)

            # Only request columns that actually exist for this file_type
            known_cols = columns_by_type.get(file_type, [])
            requested_cols = [c for c in col_set if c in known_cols] if known_cols else col_set

            params: dict = {"file_type": file_type}
            if requested_cols:
                params["columns"] = requested_cols

            path = f"/wind-farms/{farm}/data/{query_date.isoformat()}"
            _get(session, base_url, path, params=params,
                 timeout=data_timeout,
                 label=f"({file_type}, {len(requested_cols) or 'all'} cols)")
            _wait(delay)


def scenario_error_paths(session: requests.Session, base_url: str,
                          delay: float) -> None:
    """Exercise error paths so error spans and metrics are visible in Grafana."""
    # 404 — unknown farm
    _get(session, base_url, "/wind-farms/no_such_farm/data/2021-06-01",
         label="(expect 404)")
    _wait(delay)

    # 422 — bad date format
    _get(session, base_url, "/wind-farms/kelmarsh/data/not-a-date",
         label="(expect 422)")
    _wait(delay)

    # 400 — bad file_type
    _get(session, base_url, "/wind-farms/kelmarsh/data/2021-06-01",
         params={"file_type": "bogus_type"},
         label="(expect 400)")
    _wait(delay)


def scenario_metrics(session: requests.Session, base_url: str,
                      delay: float, repeat: int = 2) -> None:
    """Simulate Prometheus scraping /metrics."""
    for i in range(repeat):
        _get(session, base_url, "/metrics",
             label=f"(scrape {i + 1}/{repeat})")
        _wait(delay)


def scenario_docs(session: requests.Session, base_url: str, delay: float) -> None:
    """Check OpenAPI docs endpoint."""
    _get(session, base_url, "/docs", label="(openapi ui)")
    _wait(delay)
    _get(session, base_url, "/openapi.json", label="(openapi json)")
    _wait(delay)


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def run_traffic(base_url: str, rounds: int, delay: float,
                data_timeout: int = DATA_TIMEOUT) -> None:
    """Execute *rounds* full traffic rounds against *base_url*."""
    log.info("=" * 65)
    log.info("Wind Farm API Traffic Generator")
    log.info("  Target       : %s", base_url)
    log.info("  Rounds       : %d", rounds)
    log.info("  Delay        : %.1f s between requests", delay)
    log.info("  Data timeout : %d s per data request", data_timeout)
    log.info("=" * 65)

    # Verify the backend is up before starting
    try:
        probe = requests.get(f"{base_url}/wind-farms", timeout=FAST_TIMEOUT)
        if probe.status_code != 200:
            log.error("Backend returned %d on /wind-farms. Aborting.", probe.status_code)
            sys.exit(1)
    except Exception as exc:
        log.error("Cannot reach backend at %s: %s", base_url, exc)
        sys.exit(1)

    session = requests.Session()
    session.headers.update({"User-Agent": "WindFarmTrafficGenerator/1.0"})

    # -----------------------------------------------------------------
    # Warm-up: fetch metadata once (may be slow on R2)
    # -----------------------------------------------------------------
    log.info("--- Warm-up: fetching metadata ---")
    farms = scenario_list_farms(session, base_url, delay, repeat=1)
    farm_names = [f["directory"] for f in farms]
    log.info("Farms discovered: %s", farm_names)

    time_ranges = scenario_time_ranges(session, base_url, delay)
    columns_map = scenario_columns(session, base_url, delay)

    # -----------------------------------------------------------------
    # Traffic rounds
    # -----------------------------------------------------------------
    for round_num in range(1, rounds + 1):
        log.info("--- Round %d / %d ---", round_num, rounds)

        # List farms
        scenario_list_farms(session, base_url, delay, repeat=2)

        # Data requests for each discovered farm
        for farm in farm_names:
            tr = time_ranges.get(farm)
            cols = columns_map.get(farm, {})
            if tr:
                scenario_day_data(session, base_url, delay, farm, tr, cols,
                                  data_timeout=data_timeout)
            else:
                log.warning("No time range for farm='%s' — skipping day-data", farm)

        # Error paths (once per round)
        scenario_error_paths(session, base_url, delay)

        # Metrics scrape
        scenario_metrics(session, base_url, delay)

        # Docs (every other round)
        if round_num % 2 == 0:
            scenario_docs(session, base_url, delay)

        log.info("Round %d complete.", round_num)

    session.close()
    log.info("=" * 65)
    log.info("Traffic generation complete.  Check Grafana for traces, logs & metrics.")
    log.info("  Grafana : http://localhost:3000")
    log.info("=" * 65)


# ---------------------------------------------------------------------------
# CLI entry-point
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Send realistic traffic to the Wind Farm API for LGTM observability.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--base-url", default=DEFAULT_BASE_URL,
        help="Base URL of the running backend",
    )
    parser.add_argument(
        "--rounds", type=int, default=DEFAULT_ROUNDS,
        help="Number of full traffic rounds to execute",
    )
    parser.add_argument(
        "--delay", type=float, default=DEFAULT_DELAY,
        help="Seconds to pause between individual requests",
    )
    parser.add_argument(
        "--data-timeout", type=int, default=DATA_TIMEOUT,
        help="Timeout in seconds for per-day data requests (R2 reads can be slow)",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    run_traffic(
        base_url=args.base_url,
        rounds=args.rounds,
        delay=args.delay,
        data_timeout=args.data_timeout,
    )

