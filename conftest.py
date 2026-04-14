"""
Root-level conftest.py — loaded by pytest before anything else.

Sets TESTING=1 before any backend module is imported so that:
  - setup_tracing()     skips the OTLP exporter (no blocking calls to localhost:4318)
  - setup_loki_logging() skips the Loki handler  (no blocking calls to localhost:3100)
"""

import os

os.environ["TESTING"] = "1"

