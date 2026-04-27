"""Velora producer package.

Adds the repo root to sys.path so `from shared.X import Y` works when
running modules from inside the `producer/` directory.
"""
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))
