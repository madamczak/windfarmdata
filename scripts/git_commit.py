"""
Stage all changes (excluding data/ which is in .gitignore) and commit.

Run from the project root:
    python scripts/git_commit.py
"""

import subprocess
import sys
import os

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

COMMIT_MESSAGE = (
    "Add backend API structure, parquet conversion scripts, and prompt log\n\n"
    "- backend/: FastAPI app with GET /wind-farms endpoint\n"
    "- backend/config.py: pydantic-settings configuration\n"
    "- backend/models/schemas.py: WindFarm, WindFarmsResponse Pydantic models\n"
    "- backend/routers/wind_farms.py: /wind-farms router\n"
    "- scripts/convert_to_parquet.py: SQLite -> Parquet conversion (status + data DBs)\n"
    "- scripts/rename_parquet_add_status_prefix.py: rename existing parquet files\n"
    "- scripts/copy_hill_of_towie_parquet.py: copy Hill of Towie parquet files\n"
    "- scripts/inspect_parquet.py: inspect and visualise parquet files\n"
    "- requirements.txt: fastapi, uvicorn, pydantic-settings, duckdb, pandas, pyarrow, matplotlib\n"
    "- .gitignore: exclude data/ directory\n"
    "- promptlogger/prompts.md: updated prompt log"
)


def run(cmd: list[str]) -> int:
    """Run a shell command, printing output in real time. Returns exit code."""
    print(f"$ {' '.join(cmd)}")
    result = subprocess.run(cmd, cwd=PROJECT_ROOT)
    return result.returncode


if __name__ == "__main__":
    steps = [
        (["git", "add", "-A"], "Staging all changes..."),
        (["git", "status"], "Staged files:"),
        (["git", "commit", "-m", COMMIT_MESSAGE], "Committing..."),
    ]

    for cmd, label in steps:
        print(f"\n{label}")
        code = run(cmd)
        if code != 0 and "commit" in cmd:
            print("Commit failed or nothing to commit.")
            sys.exit(code)

    print("\nDone.")

