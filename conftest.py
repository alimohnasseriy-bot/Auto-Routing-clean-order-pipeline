"""
conftest.py
===========
Pytest configuration file — runs from the project root.

Adds the project root to sys.path so that `from src.X import Y` and
`from config.settings import Z` work correctly during test collection
and execution without requiring installation.
"""

import sys
from pathlib import Path

# Project root is the directory containing this conftest.py
PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
