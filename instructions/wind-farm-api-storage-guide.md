# Wind Farm API — Data Storage Strategy Guide

## Overview

This guide compares two storage strategies for serving time-series wind farm data
via a REST API:

1. **SQLite files** (current format) with datetime indexing
2. **Apache Parquet files** stored in cloud object storage (e.g., AWS S3, Azure Blob, GCS)

The three datasets in scope are:

| Wind Farm        | Approx. Size (SQLite) |
|------------------|-----------------------|
| Kelmarsh         | ~8 GB                 |
| Penmanshiel      | ~8 GB                 |
| Hill of Towie    | ~30 GB                |
| **Total**        | **~46 GB**            |

---

## The Core Use Case

A user (human or automated scraper) selects a **pre-defined event** from a wind farm
time series and requests data for a configurable **time window** around that event:

```
[event_timestamp - T_before]  →  event_timestamp  →  [event_timestamp + T_after]
```

`T_before` and `T_after` are user-supplied parameters (e.g., in minutes or hours).

---

## Option 1 — SQLite with Datetime Index

### How It Works
- Keep the existing `.db` files on disk (local server or a cloud VM).
- Add a `CREATE INDEX idx_timestamp ON <table>(timestamp)` on the datetime column.
- The API queries SQLite directly using a `WHERE timestamp BETWEEN ? AND ?` clause.

### Pros
- Zero migration effort — data is already in SQLite.
- B-tree index on datetime makes range queries fast on a local machine.
- Simple Python stack: `sqlite3` or `SQLAlchemy` + `FastAPI`.

### Cons
- **Cloud cost:** Storing 46 GB on a VM disk (e.g., AWS EBS, Azure Managed Disk)
  costs significantly more than object storage (~$4–5/GB/month on EBS vs ~$0.02/GB/month on S3).
- **Concurrent access:** SQLite is file-locked; multiple simultaneous API requests
  can cause contention or errors.
- **Scalability:** The entire file must be resident on one machine; horizontal scaling
  is not straightforward.
- **No partial reads:** Even for a small time window, SQLite must open and scan the
  full database file header and index pages.

---

## Option 2 — Parquet Files in Cloud Object Storage ✅ Recommended

### How It Works
- Convert each SQLite database to **Parquet** format, partitioned by time
  (e.g., year/month or year/month/day folders).
- Store Parquet files in an object storage bucket (AWS S3, Azure Blob Storage, or GCS).
- The API uses **DuckDB** or **pandas + pyarrow** to query only the relevant
  partition(s) for the requested time window.

### Typical Size Reduction
Parquet with Snappy or Zstd compression typically achieves **60–80% size reduction**
over raw SQLite for numerical time-series data:

| Wind Farm        | SQLite  | Parquet (est.) |
|------------------|---------|----------------|
| Kelmarsh         | ~8 GB   | ~1.6–3.2 GB    |
| Penmanshiel      | ~8 GB   | ~1.6–3.2 GB    |
| Hill of Towie    | ~30 GB  | ~6–12 GB       |
| **Total**        | ~46 GB  | **~9–19 GB**   |

### Pros
- **Dramatically lower cloud storage cost** — object storage is ~200× cheaper than
  block storage per GB.
- **Columnar format** — only the columns needed by the query are read from disk/network.
- **Partition pruning** — a query for a 2-hour window touches only the relevant
  day/hour partition file(s), not the entire dataset.
- **DuckDB** can query Parquet files directly on S3 with minimal boilerplate and
  very fast execution — no server-side database process needed.
- **Stateless API** — no persistent DB connection required; scales horizontally.
- Works well with **automated scraping** because responses can be serialised back
  to Parquet or CSV efficiently.

### Cons
- One-time migration effort to convert SQLite → Parquet.
- Requires designing a sensible partition scheme upfront.
- Cold-start latency on very first query to a new partition (object storage I/O).

---

## Recommended Partition Scheme

```
s3://your-bucket/
  windfarm=kelmarsh/
    year=2019/month=01/day=01/data.parquet
    year=2019/month=01/day=02/data.parquet
    ...
  windfarm=penmanshiel/
    year=2019/month=01/day=01/data.parquet
    ...
  windfarm=hill_of_towie/
    year=2019/month=01/day=01/data.parquet
    ...
```

Hive-style partitioning is recognised natively by DuckDB, pandas, Polars, and
Apache Spark — no custom parsing required.

---

## API Design Sketch

```
GET /events/{event_id}/data
    ?farm=kelmarsh
    &before=60       # minutes before event
    &after=60        # minutes after event
    &format=json     # or csv / parquet
```

The API backend:
1. Looks up `event_id` → retrieves `event_timestamp` and `farm` from an events
   catalogue (lightweight SQLite or PostgreSQL — this stays small).
2. Computes `start = event_timestamp - before` and `end = event_timestamp + after`.
3. Queries only the relevant Parquet partition(s) via DuckDB or pyarrow.
4. Returns the result in the requested format.

---

## Tooling Recommendations

| Purpose                   | Library / Service                        |
|---------------------------|------------------------------------------|
| Parquet read/write        | `pyarrow`, `pandas`, `polars`            |
| In-process SQL on Parquet | `duckdb`                                 |
| API framework             | `FastAPI` (Python)                       |
| Cloud object storage      | AWS S3 / Azure Blob / GCS                |
| Local development         | DuckDB querying local Parquet files      |
| Migration script          | `scripts/migrate_sqlite_to_parquet.py`   |

---

## Cost Comparison Summary (AWS example, eu-west-1)

| Storage Type          | Size    | Est. Monthly Cost |
|-----------------------|---------|-------------------|
| EBS gp3 (SQLite)      | 46 GB   | ~$3.70            |
| S3 Standard (Parquet) | ~14 GB  | ~$0.32            |
| **Saving**            |         | **~$3.40/month**  |

> At scale or with larger retention windows the savings multiply significantly.

---

## Decision

**Use Parquet on cloud object storage.** The combination of smaller file sizes,
partition pruning, columnar reads, and dramatically lower object-storage costs
makes it the right choice for a time-window event query API — especially when
automated scraping means high query volumes.

