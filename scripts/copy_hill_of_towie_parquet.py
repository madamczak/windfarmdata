import os
import shutil
import glob

SOURCE_DIR = r"C:\Users\adamc\PycharmProjects\windhilloftowiefarm\data\parquet"

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DEST_DIR = os.path.join(PROJECT_ROOT, "data", "hill_of_towie")

os.makedirs(DEST_DIR, exist_ok=True)

files = sorted(glob.glob(os.path.join(SOURCE_DIR, "**", "*.parquet"), recursive=True))

if not files:
    print(f"No parquet files found in: {SOURCE_DIR}")
else:
    print(f"Found {len(files)} parquet file(s) — copying to {DEST_DIR}\n")
    for src in files:
        filename = os.path.basename(src)
        dest = os.path.join(DEST_DIR, filename)
        shutil.copy2(src, dest)
        size_mb = os.path.getsize(dest) / 1_048_576
        print(f"  Copied: {filename}  ({size_mb:.1f} MB)")

print("\nDone.")

