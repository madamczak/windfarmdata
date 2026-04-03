"""
Quick schema check - just table names, columns, row counts, and sample duration values
for the status databases.
"""

import sqlite3

DBS = {
    "kelmarsh_status_by_turbine": r"C:\Users\adamc\PycharmProjects\data_by_turbine — backup\kelmarsh_status_by_turbine.db",
    "kelmarsh_status_by_turbine_duration": r"C:\Users\adamc\PycharmProjects\data_by_turbine — backup\kelmarsh_status_by_turbine_duration.db",
    "penmanshiel_status_by_turbine": r"C:\Users\adamc\PycharmProjects\data_by_turbine — backup\penmanshiel_status_by_turbine.db",
    "penmanshiel_data_by_turbine": r"C:\Users\adamc\PycharmProjects\data_by_turbine — backup\penmanshiel_data_by_turbine.db",
}

for label, path in DBS.items():
    print(f"\n{'='*60}")
    print(f"DB: {label}")
    conn = sqlite3.connect(path)
    cur = conn.cursor()

    cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;")
    tables = [r[0] for r in cur.fetchall()]
    print(f"Tables ({len(tables)}): {tables[:5]} {'...' if len(tables) > 5 else ''}")

    # Inspect first table only for schema
    if tables:
        t = tables[0]
        cur.execute(f"PRAGMA table_info('{t}');")
        cols = cur.fetchall()
        print(f"\nFirst table '{t}' columns:")
        for c in cols:
            print(f"  [{c[0]}] {c[1]:45s} {c[2]}")

        cur.execute(f"SELECT COUNT(*) FROM '{t}';")
        print(f"  Row count: {cur.fetchone()[0]:,}")

        # Show first 3 rows
        cur.execute(f"SELECT * FROM '{t}' LIMIT 3;")
        rows = cur.fetchall()
        col_names = [c[1] for c in cols]
        print(f"  First 3 rows:")
        for row in rows:
            print("   ", dict(zip(col_names, row)))

    conn.close()

