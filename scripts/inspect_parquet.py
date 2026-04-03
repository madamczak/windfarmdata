"""
Inspect and visualise the parquet files in data/kelmarsh and data/penmanshiel.

For each wind farm this script will:
  1. Print schema (column names + dtypes) for the first file (all files share the same schema).
  2. Print row counts per turbine file.
  3. Show a sample of 5 rows.
  4. If a numeric column is detected (e.g. power, wind_speed, duration) plot
     a time-series for turbine_1 of each wind farm.

Output plots are saved to data/plots/ as PNG files so they can be reviewed
without an interactive display.
"""

import os
import glob
import pandas as pd
import matplotlib
matplotlib.use("Agg")  # non-interactive backend — saves to file instead of popping a window
import matplotlib.pyplot as plt

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DATA_ROOT = os.path.join(PROJECT_ROOT, "data")
PLOTS_DIR = os.path.join(DATA_ROOT, "plots")
os.makedirs(PLOTS_DIR, exist_ok=True)

WIND_FARMS = {
    "kelmarsh": os.path.join(DATA_ROOT, "kelmarsh"),
    "penmanshiel": os.path.join(DATA_ROOT, "penmanshiel"),
}

# Columns to try plotting (first match wins)
TIMESTAMP_CANDIDATES = ["timestamp", "time", "datetime", "date"]
NUMERIC_PLOT_CANDIDATES = [
    "power_kw", "power", "wind_speed", "wind_speed_ms",
    "duration", "rotor_speed", "nacelle_direction",
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def find_column(df: pd.DataFrame, candidates: list[str]) -> str | None:
    """Return the first column name from candidates that exists in df (case-insensitive)."""
    col_map = {c.lower(): c for c in df.columns}
    for name in candidates:
        found = col_map.get(name.lower())
        if found:
            return found
    return None


def inspect_farm(farm_name: str, farm_dir: str) -> None:
    files = sorted(glob.glob(os.path.join(farm_dir, "status_*.parquet")))

    if not files:
        print(f"  No parquet files found in {farm_dir}")
        return

    print(f"\n{'=' * 60}")
    print(f"Wind farm : {farm_name.upper()}")
    print(f"Directory : {farm_dir}")
    print(f"Files     : {len(files)}")
    print("=" * 60)

    # --- Schema from first file ---
    first_df = pd.read_parquet(files[0])
    print(f"\nSchema (from {os.path.basename(files[0])}):")
    for col, dtype in first_df.dtypes.items():
        print(f"  {col:<45s} {dtype}")

    # --- Row counts ---
    print(f"\nRow counts per file:")
    total_rows = 0
    for f in files:
        df = pd.read_parquet(f)
        print(f"  {os.path.basename(f):<35s} {len(df):>10,} rows")
        total_rows += len(df)
    print(f"  {'TOTAL':<35s} {total_rows:>10,} rows")

    # --- Sample rows from first file ---
    print(f"\nSample rows (first 5) from {os.path.basename(files[0])}:")
    print(first_df.head(5).to_string(index=False))

    # --- Plot time-series for first turbine ---
    plot_farm(farm_name, files[0])


def plot_farm(farm_name: str, parquet_path: str) -> None:
    """
    Plot up to 4 numeric columns over time for a single turbine file.
    Saves the PNG to data/plots/.
    """
    df = pd.read_parquet(parquet_path)
    turbine_label = os.path.splitext(os.path.basename(parquet_path))[0]

    ts_col = find_column(df, TIMESTAMP_CANDIDATES)
    if ts_col is None:
        print(f"  [WARN] No timestamp column found in {parquet_path} — skipping plot.")
        return

    # Parse timestamp
    df[ts_col] = pd.to_datetime(df[ts_col], utc=True, errors="coerce")
    df = df.dropna(subset=[ts_col]).sort_values(ts_col)

    # Find numeric columns to plot
    numeric_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c]) and c != ts_col]

    # Prefer known interesting columns, fall back to all numeric
    preferred = [c for c in NUMERIC_PLOT_CANDIDATES if find_column(df, [c]) in numeric_cols]
    plot_cols = preferred[:4] if preferred else numeric_cols[:4]

    if not plot_cols:
        print(f"  [WARN] No numeric columns to plot in {parquet_path}.")
        return

    fig, axes = plt.subplots(len(plot_cols), 1, figsize=(14, 3 * len(plot_cols)), sharex=True)
    if len(plot_cols) == 1:
        axes = [axes]

    fig.suptitle(f"{farm_name.capitalize()} — {turbine_label}", fontsize=14, fontweight="bold")

    for ax, col in zip(axes, plot_cols):
        actual_col = find_column(df, [col]) or col
        ax.plot(df[ts_col], df[actual_col], linewidth=0.5, color="steelblue")
        ax.set_ylabel(actual_col, fontsize=9)
        ax.grid(True, alpha=0.3)

    axes[-1].set_xlabel("Time")
    plt.tight_layout()

    out_path = os.path.join(PLOTS_DIR, f"{farm_name}_{turbine_label}.png")
    plt.savefig(out_path, dpi=120)
    plt.close()
    print(f"\n  Plot saved: {out_path}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    # Check matplotlib is available
    try:
        import matplotlib
    except ImportError:
        print("matplotlib not installed. Run: pip install matplotlib")
        raise SystemExit(1)

    for farm_name, farm_dir in WIND_FARMS.items():
        inspect_farm(farm_name, farm_dir)

    print(f"\nAll plots saved to: {PLOTS_DIR}")
    print("Done.")

