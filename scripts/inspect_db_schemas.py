"""
Inspect schemas and sample rows from all wind farm SQLite databases.
Run this script to understand table structure before migration.
"""

import sqlite3
import os

DB_FILES = {
    "kelmarsh_status_by_turbine": r"C:\Users\adamc\PycharmProjects\data_by_turbine — backup\kelmarsh_status_by_turbine.db",
    "kelmarsh_status_by_turbine_duration": r"C:\Users\adamc\PycharmProjects\data_by_turbine — backup\kelmarsh_status_by_turbine_duration.db",
    "penmanshiel_data_by_turbine": r"C:\Users\adamc\PycharmProjects\data_by_turbine — backup\penmanshiel_data_by_turbine.db",
    "penmanshiel_status_by_turbine": r"C:\Users\adamc\PycharmProjects\data_by_turbine — backup\penmanshiel_status_by_turbine.db",
}


def inspect_db(label, path):
    print(f"\n{'='*60}")
    print(f"DB: {label}")
    print(f"Path: {path}")
    print(f"{'='*60}")

    conn = sqlite3.connect(path)
    cursor = conn.cursor()

    # List all tables
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;")
    tables = [row[0] for row in cursor.fetchall()]
    print(f"Tables: {tables}")

    for table in tables:
        print(f"\n  --- Table: {table} ---")

        # Column info
        cursor.execute(f"PRAGMA table_info('{table}');")
        cols = cursor.fetchall()
        print(f"  Columns:")
        for col in cols:
            print(f"    {col[1]:40s} {col[2]}")

        # Row count
        cursor.execute(f"SELECT COUNT(*) FROM '{table}';")
        count = cursor.fetchone()[0]
        print(f"  Row count: {count:,}")

        # Sample rows (first 3)
        cursor.execute(f"SELECT * FROM '{table}' LIMIT 3;")
        rows = cursor.fetchall()
        col_names = [c[1] for c in cols]
        print(f"  Sample rows:")
        for row in rows:
            for name, val in zip(col_names, row):
                print(f"    {name}: {val}")
            print()

    conn.close()


if __name__ == "__main__":
    for label, path in DB_FILES.items():
        inspect_db(label, path)

