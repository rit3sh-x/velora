import logging

import asyncpg

from consumer.config import settings

log = logging.getLogger("velora.db")


_pool: asyncpg.Pool | None = None


async def init_pool(min_size: int = 2, max_size: int = 16) -> asyncpg.Pool:
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(
            dsn=settings.dsn,
            min_size=min_size,
            max_size=max_size,
            command_timeout=30,
        )
    return _pool


def get_pool() -> asyncpg.Pool:
    if _pool is None:
        raise RuntimeError("db pool not initialized; call init_pool() first")
    return _pool


async def close_pool() -> None:
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None
