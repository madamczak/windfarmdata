# INSTRUCTIONS: Wind Farm Time-Series API — Implementation Specification

## Context

You are implementing a Python REST API that serves time-series sensor data from
wind farms. The data originates from three wind farms stored as SQLite databases
(Kelmarsh ~8 GB, Penmanshiel ~8 GB, Hill of Towie ~30 GB). The target architecture
migrates this data to Apache Parquet files stored in cloud object storage.

---

## Technology Stack

- **Backend / API:** Python 3.11+, FastAPI
- **Data layer:** DuckDB (querying Parquet), pyarrow (Parquet I/O)
- **Storage:** Cloud object storage bucket (S3 / Azure Blob / GCS), Hive-partitioned Parquet
- **Events catalogue:** Small SQLite or PostgreSQL database (event metadata only)
- **Frontend:** Vue 3 (Composition API) — separate from backend
- **No README files** unless explicitly requested

---

## Step 1 — Migration: SQLite → Parquet

### Task
Create `scripts/migrate_sqlite_to_parquet.py`.

### Behaviour
- Accept CLI arguments: `--db-path`, `--output-dir`, `--farm-name`
- Read from the SQLite database in chunks (e.g., 100,000 rows at a time) to avoid
  memory exhaustion on large files (Hill of Towie is ~30 GB).
- Write output as Parquet files partitioned by `year`, `month`, `day` using
  Hive-style directory structure:
  ```
  <output-dir>/windfarm=<farm_name>/year=YYYY/month=MM/day=DD/data.parquet
  ```
- Use `pyarrow` for writing. Apply `zstd` compression (better ratio than snappy,
  still fast to decompress).
- Preserve all original columns. Ensure the datetime column is cast to
  `pyarrow.timestamp('us', tz='UTC')`.
- Print progress (chunk number, rows written, partition path) to stdout.

### Libraries
```
pip install pyarrow fastapi uvicorn duckdb
```

---

## Step 2 — Events Catalogue

### Schema (SQLite)
```sql
CREATE TABLE events (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    farm        TEXT NOT NULL,          -- 'kelmarsh' | 'penmanshiel' | 'hill_of_towie'
    event_type  TEXT NOT NULL,
    timestamp   TEXT NOT NULL,          -- ISO 8601 UTC, e.g. '2019-06-15T14:30:00Z'
    description TEXT
);
```

### Task
Create `scripts/seed_events.py` that populates sample events for local development.

---

## Step 3 — API Implementation

### File layout
```
backend/
  main.py              # FastAPI app entry point
  routers/
    events.py          # /events endpoints
    data.py            # /events/{id}/data endpoint
  services/
    query_service.py   # DuckDB query logic against Parquet files
    event_service.py   # Event catalogue lookups
  models/
    schemas.py         # Pydantic request/response models
  config.py            # Settings (storage path, bucket name, env vars)
```

### Endpoint: GET /events/{event_id}/data

**Query parameters:**
| Parameter | Type    | Default | Description                          |
|-----------|---------|---------|--------------------------------------|
| `before`  | integer | 60      | Minutes of data before the event     |
| `after`   | integer | 60      | Minutes of data after the event      |
| `format`  | string  | `json`  | Response format: `json`, `csv`, `parquet` |

**Logic (query_service.py):**
1. Resolve event → get `farm` and `event_timestamp`.
2. Compute `start_dt = event_timestamp - timedelta(minutes=before)`.
3. Compute `end_dt   = event_timestamp + timedelta(minutes=after)`.
4. Identify which partitions (year/month/day combinations) are touched by
   `[start_dt, end_dt]`.
5. Build a DuckDB SQL query against only those partition paths:
   ```python
   import duckdb
   conn = duckdb.connect()
   result = conn.execute("""
       SELECT *
       FROM read_parquet(?)
       WHERE timestamp >= ? AND timestamp <= ?
       ORDER BY timestamp
   """, [partition_glob, start_dt.isoformat(), end_dt.isoformat()]).fetchdf()
   ```
6. Return result serialised to the format requested.

**Response formats:**
- `json` → `application/json` (list of records)
- `csv`  → `text/csv` with `Content-Disposition: attachment`
- `parquet` → `application/octet-stream` (raw Parquet bytes via `pyarrow`)

### Error handling
- `404` if `event_id` does not exist.
- `400` if `before` or `after` are negative or exceed 10,080 (7 days).
- `500` with a structured error body `{"detail": "...", "farm": "...", "event_id": ...}`.

---

## Step 4 — Local vs Cloud Storage

### Config (`config.py`)
Use `pydantic-settings` with environment variables:

```python
class Settings(BaseSettings):
    storage_backend: str = "local"   # "local" | "s3" | "azure" | "gcs"
    parquet_base_path: str = "./data/parquet"   # local path or bucket URI
    events_db_path: str = "./data/events.db"
    aws_region: str = "eu-west-1"
    # ... other cloud credentials via env vars, never hardcoded
```

When `storage_backend == "s3"`:
- Use `s3://bucket-name/windfarm=.../...` paths in DuckDB.
- Set DuckDB S3 credentials via `duckdb.execute("SET s3_region=...")`  before queries.

---

## Step 5 — Automated Scraping Support

The API must support high-throughput automated queries:

- All endpoints must be **stateless** — no server-side session required.
- Support **bulk event queries**: `POST /events/batch/data` accepting a JSON body:
  ```json
  {
    "event_ids": [1, 2, 3],
    "before": 30,
    "after": 30,
    "format": "parquet"
  }
  ```
  Returns a ZIP archive of individual Parquet files named `event_{id}.parquet`.
- Add rate-limiting headers (`X-RateLimit-Limit`, `X-RateLimit-Remaining`) for
  scraping clients.
- Document all endpoints with OpenAPI descriptions (FastAPI auto-generates Swagger UI
  at `/docs`).

---

## Step 6 — Frontend (Vue 3)

### File layout
```
frontend/
  src/
    components/
      EventSelector.vue     # Dropdown/search to pick a wind farm and event
      TimeWindowPicker.vue  # Numeric inputs for before/after minutes
      DataChart.vue         # Time-series chart (use Chart.js or uPlot)
    services/
      api.js                # Axios wrapper for backend calls
    App.vue
    main.js
```

### Key behaviour
- User selects farm → events for that farm are fetched from `GET /events?farm=...`.
- User selects event + sets time window → fetches data from `GET /events/{id}/data`.
- Chart renders the returned time-series (at minimum: timestamp on X axis, one
  configurable sensor column on Y axis).
- Export button triggers download of `csv` or `parquet` format from the same endpoint.

---

## Coding Rules

- All Python code follows **PEP 8**.
- All Vue code uses **Vue 3 Composition API** (`<script setup>` syntax preferred).
- No secrets or credentials hardcoded — use environment variables.
- Every Python function has a docstring.
- Every script in `scripts/` is self-contained and runnable with `python scripts/<name>.py --help`.
- Do **not** create README files unless explicitly asked.
- Do **not** run code in the terminal — write executable logic as scripts in `scripts/`.
- **Never use `python -c "..."`** or any form of inline terminal code execution — every piece of code must be its own file in `scripts/`.

