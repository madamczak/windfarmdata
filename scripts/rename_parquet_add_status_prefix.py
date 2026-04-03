"""
Rename all parquet files in data/kelmarsh and data/penmanshiel
by adding a 'status_' prefix to each filename.

e.g. turbine_1.parquet  →  status_turbine_1.parquet

Safe to run multiple times — files already prefixed are skipped.
"""

import os
import glob

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DATA_ROOT = os.path.join(PROJECT_ROOT, "data")

DIRS = [
    os.path.join(DATA_ROOT, "kelmarsh"),
    os.path.join(DATA_ROOT, "penmanshiel"),
]

PREFIX = "status_"

for directory in DIRS:
    files = sorted(glob.glob(os.path.join(directory, "*.parquet")))
    print(f"\n{directory}  ({len(files)} files)")

    for old_path in files:
        filename = os.path.basename(old_path)

        # Skip files already prefixed
        if filename.startswith(PREFIX):
            print(f"  SKIP (already prefixed): {filename}")
            continue

        new_filename = PREFIX + filename
        new_path = os.path.join(directory, new_filename)
        os.rename(old_path, new_path)
        print(f"  {filename}  →  {new_filename}")

print("\nDone.")

