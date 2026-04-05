"""
scripts/check_benchmark_regression.py

Compares two pytest-benchmark JSON result files and fails with exit code 1
if any benchmark in the CURRENT run is more than THRESHOLD % slower than
the same benchmark in the BASELINE run.

Usage:
    python scripts/check_benchmark_regression.py \\
        --baseline  .benchmarks/baseline.json \\
        --current   .benchmarks/current.json \\
        --threshold 5.0

The script is called by the CI workflow after benchmark runs.
The first run (no baseline artefact) saves the results as the baseline
and exits 0 without comparison.
"""

import argparse
import json
import sys


# Maximum allowed slowdown expressed as a percentage.
DEFAULT_THRESHOLD_PCT = 5.0


def load_benchmarks(path: str) -> dict[str, float]:
    """
    Load a pytest-benchmark JSON file and return a mapping of
    benchmark name → mean time in seconds.
    """
    with open(path, encoding="utf-8") as fh:
        data = json.load(fh)

    results: dict[str, float] = {}
    for bench in data.get("benchmarks", []):
        name = bench["fullname"]          # e.g. tests/test_performance.py::TestFoo::test_bar
        mean = bench["stats"]["mean"]     # seconds
        results[name] = mean
    return results


def compare(baseline: dict[str, float],
            current: dict[str, float],
            threshold_pct: float) -> list[str]:
    """
    Return a list of failure messages for benchmarks that regressed by more
    than threshold_pct percent.  An empty list means no regressions.
    """
    failures: list[str] = []

    for name, current_mean in current.items():
        if name not in baseline:
            # New benchmark — no baseline to compare against, skip.
            print(f"  [NEW]  {name}  mean={current_mean * 1000:.3f} ms  (no baseline)")
            continue

        baseline_mean = baseline[name]
        if baseline_mean <= 0:
            continue

        pct_change = ((current_mean - baseline_mean) / baseline_mean) * 100

        if pct_change > threshold_pct:
            failures.append(
                f"  [FAIL] {name}\n"
                f"         baseline={baseline_mean * 1000:.3f} ms  "
                f"current={current_mean * 1000:.3f} ms  "
                f"change=+{pct_change:.1f}%  (threshold={threshold_pct:.1f}%)"
            )
        elif pct_change >= 0:
            print(
                f"  [OK]   {name}  "
                f"+{pct_change:.1f}%  ({current_mean * 1000:.3f} ms)"
            )
        else:
            print(
                f"  [OK]   {name}  "
                f"{pct_change:.1f}%  ({current_mean * 1000:.3f} ms)  ← faster"
            )

    return failures


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Fail CI if benchmarks regressed by more than a threshold."
    )
    parser.add_argument(
        "--baseline",
        required=True,
        help="Path to the baseline benchmark JSON file.",
    )
    parser.add_argument(
        "--current",
        required=True,
        help="Path to the current benchmark JSON file.",
    )
    parser.add_argument(
        "--threshold",
        type=float,
        default=DEFAULT_THRESHOLD_PCT,
        help=(
            f"Maximum allowed slowdown in percent "
            f"(default: {DEFAULT_THRESHOLD_PCT})."
        ),
    )
    args = parser.parse_args()

    print(f"\n{'=' * 70}")
    print(f"Performance regression check  (threshold: {args.threshold:.1f} %)")
    print(f"  baseline : {args.baseline}")
    print(f"  current  : {args.current}")
    print(f"{'=' * 70}\n")

    try:
        baseline = load_benchmarks(args.baseline)
    except FileNotFoundError:
        print(f"Baseline file not found: {args.baseline}")
        print("This is the first run — no comparison possible.  Exiting 0.")
        return 0

    current = load_benchmarks(args.current)

    failures = compare(baseline, current, args.threshold)

    print()
    if failures:
        print(f"{'=' * 70}")
        print(f"PERFORMANCE REGRESSION DETECTED — {len(failures)} benchmark(s) failed:")
        for msg in failures:
            print(msg)
        print(f"{'=' * 70}\n")
        return 1

    print(f"{'=' * 70}")
    print("All benchmarks within threshold.  No regression detected.")
    print(f"{'=' * 70}\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())

