import logging
from pathlib import Path

import asyncpg

from consumer.config import settings

log = logging.getLogger("velora.db")

SCHEMA_PATH = Path(__file__).parent / "schema.sql"


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


def _split_statements(sql: str) -> list[str]:
    """Naive splitter: cut on `;` at end of line, drop blanks/comments."""
    cleaned = []
    for line in sql.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("--"):
            continue
        cleaned.append(line)
    body = "\n".join(cleaned)
    return [s.strip() for s in body.split(";") if s.strip()]


async def apply_schema() -> None:
    """Apply schema.sql idempotently. Each statement runs in autocommit so
    continuous aggregate DDL doesn't trip the 'cannot run in transaction' check.
    """
    sql = SCHEMA_PATH.read_text(encoding="utf-8")
    statements = _split_statements(sql)

    conn = await asyncpg.connect(dsn=settings.dsn)
    try:
        for stmt in statements:
            try:
                await conn.execute(stmt)
            except asyncpg.exceptions.DuplicateObjectError as exc:
                log.debug("skip duplicate: %s", exc)
            except asyncpg.exceptions.PostgresError as exc:
                log.warning("schema stmt failed: %s\n%s", exc, stmt[:120])
                raise
    finally:
        await conn.close()

    log.info("schema applied (%d statements)", len(statements))
