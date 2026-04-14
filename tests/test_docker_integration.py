"""
tests/test_docker_integration.py - Live integration tests against the
Dockerised Wind Farm API backend.

These tests send real HTTP requests to http://localhost:8000 (the running
Docker backend) so that every call produces:
- A trace span visible in Grafana -> Explore -> Tempo
- A log line visible in Grafana -> Explore -> Loki  (app=windfarm-api)
- Prometheus counter updates visible in Grafana -> Explore -> Metrics

Prerequisites
-------------
  docker compose up           (lgtm + backend + frontend containers running)

Run with:
  pytest tests/test_docker_integration.py -v --no-cov

Skipping behaviour
------------------
If the backend is unreachable the entire module is skipped automatically
rather than failing -- so CI (which has no Docker) is unaffected.

Timeouts
--------
Fast endpoints (/wind-farms, /metrics, /docs): 10 s
Slow endpoints that scan R2 parquet metadata (/time-ranges, /columns): 120 s
"""

import pytest
import requests
import time

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
BASE_URL = "http://localhost:8000"
FAST_TIMEOUT = 10    # seconds -- for cheap endpoints
SLOW_TIMEOUT = 120   # seconds -- for R2 metadata scan endpoints


# ---------------------------------------------------------------------------
# Module-level skip if Docker backend is not reachable
# ---------------------------------------------------------------------------

def _backend_is_up() -> bool:
    """Return True if the live Docker backend responds on BASE_URL."""
    try:
        r = requests.get(f"{BASE_URL}/wind-farms", timeout=FAST_TIMEOUT)
        return r.status_code == 200
    except Exception:
        return False


pytestmark = pytest.mark.skipif(
    not _backend_is_up(),
    reason="Live Docker backend not reachable at http://localhost:8000 -- skipping integration tests",
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def get(path: str, params: dict | None = None, timeout: int = FAST_TIMEOUT) -> requests.Response:
    """GET *path* against the live backend and return the response."""
    return requests.get(f"{BASE_URL}{path}", params=params, timeout=timeout)


# ---------------------------------------------------------------------------
# Module-scoped fixtures -- fetch slow data once per test module
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def live_time_ranges() -> dict:
    """Fetch /wind-farms/time-ranges once for the whole test module."""
    r = get("/wind-farms/time-ranges", timeout=SLOW_TIMEOUT)
    assert r.status_code == 200, f"/wind-farms/time-ranges returned {r.status_code}: {r.text}"
    return r.json()


@pytest.fixture(scope="module")
def live_columns() -> dict:
    """Fetch /wind-farms/columns once for the whole test module."""
    r = get("/wind-farms/columns", timeout=SLOW_TIMEOUT)
    assert r.status_code == 200, f"/wind-farms/columns returned {r.status_code}: {r.text}"
    return r.json()


# ---------------------------------------------------------------------------
# /wind-farms
# ---------------------------------------------------------------------------

class TestDockerWindFarms:
    """Live tests for GET /wind-farms -- generates traces + logs + metrics."""

    def test_wind_farms_200(self):
        """Live backend must return 200 for /wind-farms."""
        r = get("/wind-farms")
        assert r.status_code == 200, f"Expected 200, got {r.status_code}: {r.text}"

    def test_wind_farms_json_structure(self):
        """Response must contain 'wind_farms' list and matching 'total'."""
        data = get("/wind-farms").json()
        assert "wind_farms" in data
        assert "total" in data
        assert isinstance(data["wind_farms"], list)
        assert data["total"] == len(data["wind_farms"])

    def test_wind_farms_known_farms_present(self):
        """Kelmarsh and Penmanshiel must appear in the live response."""
        data = get("/wind-farms").json()
        dirs = {f["directory"] for f in data["wind_farms"]}
        assert "kelmarsh" in dirs, f"kelmarsh missing from live farms: {dirs}"
        assert "penmanshiel" in dirs, f"penmanshiel missing from live farms: {dirs}"

    def test_wind_farms_turbine_counts_positive(self):
        """Each farm returned by the live backend must have turbine_count > 0."""
        data = get("/wind-farms").json()
        for farm in data["wind_farms"]:
            assert farm["turbine_count"] > 0, (
                f"Farm '{farm['name']}' has turbine_count={farm['turbine_count']}"
            )

    def test_wind_farms_field_types(self):
        """Field types must be correct in each farm entry."""
        data = get("/wind-farms").json()
        for farm in data["wind_farms"]:
            assert isinstance(farm["name"], str) and farm["name"]
            assert isinstance(farm["directory"], str) and farm["directory"]
            assert isinstance(farm["turbine_count"], int)


# ---------------------------------------------------------------------------
# /wind-farms/time-ranges
# ---------------------------------------------------------------------------

class TestDockerTimeRanges:
    """Live tests for GET /wind-farms/time-ranges."""

    def test_time_ranges_200(self, live_time_ranges):
        """Live backend must return 200 for /wind-farms/time-ranges."""
        assert "time_ranges" in live_time_ranges

    def test_time_ranges_structure(self, live_time_ranges):
        """Response must contain a non-empty 'time_ranges' list."""
        assert isinstance(live_time_ranges["time_ranges"], list)
        assert len(live_time_ranges["time_ranges"]) > 0

    def test_time_ranges_required_fields(self, live_time_ranges):
        """Every entry must have farm, earliest, latest, timestamp_column."""
        for entry in live_time_ranges["time_ranges"]:
            for key in ("farm", "earliest", "latest", "timestamp_column"):
                assert key in entry, f"Key '{key}' missing in entry: {entry}"

    def test_time_ranges_earliest_before_latest(self, live_time_ranges):
        """Earliest must be before latest for all farms with data."""
        from datetime import datetime
        for entry in live_time_ranges["time_ranges"]:
            if entry["earliest"] and entry["latest"]:
                e = datetime.fromisoformat(entry["earliest"].replace("Z", "+00:00"))
                l = datetime.fromisoformat(entry["latest"].replace("Z", "+00:00"))
                assert e <= l, (
                    f"Farm '{entry['farm']}': earliest {e} is after latest {l}"
                )

    def test_time_ranges_timestamp_column_set(self, live_time_ranges):
        """Farms with data must have a non-null timestamp_column."""
        for entry in live_time_ranges["time_ranges"]:
            if entry["earliest"]:
                assert entry["timestamp_column"] is not None, (
                    f"Farm '{entry['farm']}' has earliest but null timestamp_column"
                )

    def test_time_ranges_kelmarsh_has_data(self, live_time_ranges):
        """Kelmarsh must have a valid time range in the live response."""
        farm_map = {e["farm"]: e for e in live_time_ranges["time_ranges"]}
        assert "kelmarsh" in farm_map, "kelmarsh missing from time-ranges"
        entry = farm_map["kelmarsh"]
        assert entry["earliest"] is not None, "kelmarsh earliest is null"
        assert entry["latest"] is not None, "kelmarsh latest is null"

    def test_time_ranges_penmanshiel_has_data(self, live_time_ranges):
        """Penmanshiel must have a valid time range in the live response."""
        farm_map = {e["farm"]: e for e in live_time_ranges["time_ranges"]}
        assert "penmanshiel" in farm_map, "penmanshiel missing from time-ranges"
        entry = farm_map["penmanshiel"]
        assert entry["earliest"] is not None, "penmanshiel earliest is null"
        assert entry["latest"] is not None, "penmanshiel latest is null"


# ---------------------------------------------------------------------------
# /wind-farms/columns
# ---------------------------------------------------------------------------

class TestDockerColumns:
    """Live tests for GET /wind-farms/columns."""

    def test_columns_200(self, live_columns):
        """Live backend must return 200 for /wind-farms/columns."""
        assert "farms" in live_columns

    def test_columns_has_farms(self, live_columns):
        """Response must contain a non-empty 'farms' list."""
        assert isinstance(live_columns["farms"], list)
        assert len(live_columns["farms"]) > 0

    def test_columns_kelmarsh_has_data_and_status(self, live_columns):
        """Kelmarsh must expose both 'data' and 'status' file types."""
        farm_map = {e["farm"]: e["columns_by_type"] for e in live_columns["farms"]}
        assert "kelmarsh" in farm_map, "kelmarsh missing from /columns response"
        types = set(farm_map["kelmarsh"].keys())
        assert "data" in types, f"kelmarsh missing 'data' type; got: {types}"
        assert "status" in types, f"kelmarsh missing 'status' type; got: {types}"

    def test_columns_penmanshiel_has_data_and_status(self, live_columns):
        """Penmanshiel must expose both 'data' and 'status' file types."""
        farm_map = {e["farm"]: e["columns_by_type"] for e in live_columns["farms"]}
        assert "penmanshiel" in farm_map, "penmanshiel missing from /columns response"
        types = set(farm_map["penmanshiel"].keys())
        assert "data" in types
        assert "status" in types

    def test_columns_wind_speed_present_in_kelmarsh(self, live_columns):
        """Kelmarsh data columns must include 'Wind speed (m/s)'."""
        farm_map = {e["farm"]: e["columns_by_type"] for e in live_columns["farms"]}
        cols = farm_map.get("kelmarsh", {}).get("data", [])
        assert "Wind speed (m/s)" in cols, (
            f"'Wind speed (m/s)' not found in kelmarsh data columns: {cols[:10]}..."
        )

    def test_columns_power_present_in_kelmarsh(self, live_columns):
        """Kelmarsh data columns must include 'Power (kW)'."""
        farm_map = {e["farm"]: e["columns_by_type"] for e in live_columns["farms"]}
        cols = farm_map.get("kelmarsh", {}).get("data", [])
        assert "Power (kW)" in cols, (
            f"'Power (kW)' not found in kelmarsh data columns: {cols[:10]}..."
        )


# ---------------------------------------------------------------------------
# /wind-farms/{farm}/data/{date}
# ---------------------------------------------------------------------------

class TestDockerDayData:
    """Live tests for GET /wind-farms/{farm}/data/{date}."""

    def test_day_data_invalid_farm_404(self):
        """Unknown farm must return 404 from the live backend."""
        r = get("/wind-farms/no_such_farm/data/2021-06-01")
        assert r.status_code == 404, f"Expected 404, got {r.status_code}"

    def test_day_data_bad_date_422(self):
        """Malformed date must return 422 from the live backend."""
        r = get("/wind-farms/kelmarsh/data/not-a-date")
        assert r.status_code == 422, f"Expected 422, got {r.status_code}"

    def test_day_data_invalid_file_type_400(self):
        """Unknown file_type for a valid farm must return 400."""
        r = get(
            "/wind-farms/kelmarsh/data/2021-06-01",
            params={"file_type": "bogus_type"},
        )
        assert r.status_code == 400, f"Expected 400, got {r.status_code}: {r.text}"

    def test_day_data_kelmarsh_first_date(self, live_time_ranges):
        """Data request for kelmarsh on its first available date must return 200 or 404."""
        farm_map = {e["farm"]: e for e in live_time_ranges["time_ranges"]}
        entry = farm_map.get("kelmarsh")
        if not entry or not entry["earliest"]:
            pytest.skip("No time range data available for kelmarsh")

        first_date = entry["earliest"][:10]
        r = get(
            f"/wind-farms/kelmarsh/data/{first_date}",
            params={"file_type": "data"},
            timeout=SLOW_TIMEOUT,
        )
        assert r.status_code in (200, 404), (
            f"Unexpected status {r.status_code} for kelmarsh/data on {first_date}"
        )

    def test_day_data_response_shape_on_200(self, live_time_ranges):
        """When data exists, response must have farm/date/columns/row_count/rows."""
        farm_map = {e["farm"]: e for e in live_time_ranges["time_ranges"]}
        entry = farm_map.get("kelmarsh")
        if not entry or not entry["earliest"]:
            pytest.skip("No time range data for kelmarsh")

        first_date = entry["earliest"][:10]
        r = get(
            f"/wind-farms/kelmarsh/data/{first_date}",
            params={"file_type": "data"},
            timeout=SLOW_TIMEOUT,
        )
        if r.status_code == 404:
            pytest.skip(f"No data for kelmarsh on {first_date}")

        assert r.status_code == 200
        data = r.json()
        for key in ("farm", "file_type", "date", "columns", "row_count", "rows"):
            assert key in data, f"Missing key '{key}' in response"
        assert data["row_count"] == len(data["rows"])
        assert data["farm"] == "kelmarsh"

    def test_day_data_column_selection(self, live_time_ranges):
        """When columns are specified only those columns should be in response."""
        farm_map = {e["farm"]: e for e in live_time_ranges["time_ranges"]}
        entry = farm_map.get("kelmarsh")
        if not entry or not entry["earliest"]:
            pytest.skip("No time range data for kelmarsh")

        first_date = entry["earliest"][:10]
        wanted = ["Wind speed (m/s)", "Power (kW)"]
        r = get(
            f"/wind-farms/kelmarsh/data/{first_date}",
            params={"file_type": "data", "columns": wanted},
            timeout=SLOW_TIMEOUT,
        )
        if r.status_code == 404:
            pytest.skip(f"No data for kelmarsh on {first_date}")

        assert r.status_code == 200
        data = r.json()
        returned_cols = set(data["columns"])
        for col in wanted:
            assert col in returned_cols, (
                f"Requested column '{col}' missing in response columns: {returned_cols}"
            )


# ---------------------------------------------------------------------------
# /metrics  (Prometheus scrape endpoint)
# ---------------------------------------------------------------------------

class TestDockerMetrics:
    """Live tests for GET /metrics -- verifies Prometheus metrics are populated."""

    def test_metrics_200(self):
        """Prometheus /metrics endpoint must return 200."""
        r = get("/metrics")
        assert r.status_code == 200

    def test_metrics_content_type(self):
        """Content-Type must be text/plain (Prometheus format)."""
        r = get("/metrics")
        assert "text/plain" in r.headers.get("content-type", ""), (
            f"Unexpected content-type: {r.headers.get('content-type')}"
        )

    def test_metrics_contains_request_counter(self):
        """Custom windfarm metric must be present after hitting other endpoints."""
        # Hit a few fast endpoints to increment counters
        get("/wind-farms")
        get("/wind-farms")
        time.sleep(0.5)

        r = get("/metrics")
        assert "windfarm_request_processing_seconds" in r.text, (
            "Custom metric 'windfarm_request_processing_seconds' not found in /metrics output"
        )

    def test_metrics_contains_python_process_metrics(self):
        """Standard Python process metrics must be present."""
        r = get("/metrics")
        assert "python_gc_objects_collected_total" in r.text or \
               "process_virtual_memory_bytes" in r.text, (
            "No standard Python/process metrics found in /metrics output"
        )


# ---------------------------------------------------------------------------
# /docs  (OpenAPI -- sanity check the app is fully wired)
# ---------------------------------------------------------------------------

class TestDockerDocs:
    """Sanity check -- OpenAPI docs must be served."""

    def test_docs_200(self):
        """GET /docs must return 200."""
        r = get("/docs")
        assert r.status_code == 200

    def test_openapi_json_200(self):
        """GET /openapi.json must return valid JSON with 'paths' key."""
        r = get("/openapi.json")
        assert r.status_code == 200
        data = r.json()
        assert "paths" in data
        assert "/wind-farms" in data["paths"]

