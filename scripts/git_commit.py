"""
Stage all changes (excluding data/ which is in .gitignore) and commit.

Usage:
    # Manual commit with a specific message:
    python scripts/git_commit.py -m "your commit message"

    # Pre-change snapshot (auto-generates a timestamped message):
    python scripts/git_commit.py

The data/ directory is excluded via .gitignore and is never committed.
"""

import subprocess
import sys
import os
import argparse
from datetime import datetime, timezone

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


def run(cmd: list[str]) -> int:
    """Run a git command in the project root, printing output. Returns exit code."""
    print(f"$ {' '.join(cmd)}")
    result = subprocess.run(cmd, cwd=PROJECT_ROOT)
    return result.returncode


def build_default_message() -> str:
    """Generate a timestamped snapshot commit message."""
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    return f"snapshot: pre-change state at {ts}"


def truncate(text: str, max_len: int = 72) -> str:
    """Truncate a string to max_len characters, appending '...' if needed."""
    return text if len(text) <= max_len else text[:max_len - 3] + "..."


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Stage and commit all changes.")
    parser.add_argument(
        "-m", "--message",
        type=str,
        default=None,
        help="Commit message. If omitted, a timestamped snapshot message is used.",
    )
    args = parser.parse_args()

    message = truncate(args.message) if args.message else build_default_message()

    steps = [
        (["git", "add", "-A"], "Staging all changes..."),
        (["git", "status", "--short"], "Staged files:"),
        (["git", "commit", "-m", message], f'Committing: "{message}"'),
    ]

    for cmd, label in steps:
        print(f"\n{label}")
        code = run(cmd)
        if code != 0 and "commit" in cmd:
            print("Commit failed or nothing new to commit.")
            sys.exit(code)

    print("\nDone.")

