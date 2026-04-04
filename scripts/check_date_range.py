"""
Quick script to print the min/max date in kelmarsh data_turbine_1.parquet
so we know a valid date to test the endpoint with.
"""

import pyarrow.parquet as pq

path = "data/kelmarsh/data_turbine_1.parquet"
schema = pq.read_schema(path)

# Find the timestamp column
ts_col = None
for field in schema:
    if "date and time" in field.name.lower() or "timestamp" in field.name.lower():
        ts_col = field.name
        break

print(f"Timestamp column: {ts_col}")

# Read just that column
table = pq.read_table(path, columns=[ts_col])
col = table.column(ts_col)

import pyarrow.compute as pc
min_val = pc.min(col).as_py()
max_val = pc.max(col).as_py()

print(f"Min: {min_val}")
print(f"Max: {max_val}")
print(f"\nSuggested test date: {min_val.date() if min_val else 'N/A'}")

