"""
Write parquet schema info to a text file for inspection.
Output: scripts/diagnose_output.txt
"""

import os
import glob
import traceback
import duckdb
import pyarrow.parquet as pq

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DATA_ROOT = os.path.join(PROJECT_ROOT, "data")
OUTPUT_FILE = os.path.join(PROJECT_ROOT, "scripts", "diagnose_output.txt")

FARMS = ["kelmarsh", "penmanshiel", "hill_of_towie"]

lines = []

for farm in FARMS:
    farm_dir = os.path.join(DATA_ROOT, farm)
    if not os.path.isdir(farm_dir):
        lines.append(f"\n[SKIP] {farm} — directory not found")
        continue

    parquet_files = sorted(glob.glob(os.path.join(farm_dir, "*.parquet")))
    if not parquet_files:
        lines.append(f"\n[SKIP] {farm} — no parquet files")
        continue

    lines.append(f"\n{'='*60}")
    lines.append(f"FARM: {farm}  ({len(parquet_files)} files)")
    lines.append(f"First file: {os.path.basename(parquet_files[0])}")

    # PyArrow schema
    try:
        schema = pq.read_schema(parquet_files[0])
        lines.append("PyArrow schema:")
        for field in schema:
            lines.append(f"  {field.name!r:50s} {field.type}")
    except Exception:
        lines.append("PyArrow ERROR: " + traceback.format_exc())

    # DuckDB describe
    parquet_glob = os.path.join(farm_dir, "*.parquet").replace("\\", "/")
    conn = duckdb.connect()
    try:
        rows = conn.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{parquet_glob}') LIMIT 0"
        ).fetchall()
        lines.append("DuckDB DESCRIBE:")
        for row in rows:
            lines.append(f"  {row}")
    except Exception:
        lines.append("DuckDB DESCRIBE ERROR: " + traceback.format_exc())

    # Try min/max on all columns
    lines.append("MIN/MAX attempts:")
    try:
        col_rows = conn.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{parquet_glob}') LIMIT 0"
        ).fetchall()
        for row in col_rows:
            col = row[0]
            try:
                r = conn.execute(
                    f'SELECT MIN("{col}"), MAX("{col}") FROM read_parquet(?)',
                    [parquet_glob],
                ).fetchone()
                lines.append(f"  {col!r}: min={r[0]}  max={r[1]}")
            except Exception as e:
                lines.append(f"  {col!r}: ERROR — {e}")
    except Exception:
        lines.append("MIN/MAX ERROR: " + traceback.format_exc())

    conn.close()

lines.append("\nDone.")

with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
    f.write("\n".join(lines))

print(f"Output written to {OUTPUT_FILE}")

