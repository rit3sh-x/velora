"""FastAPI dependency providers."""
from __future__ import annotations

import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

import asyncpg

from consumer.db import get_pool


async def db() -> asyncpg.Pool:
    """Return the global asyncpg pool. Pool is initialized by the supervisor."""
    return get_pool()
