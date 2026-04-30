from __future__ import annotations

import asyncio
import logging
import sys
from pathlib import Path

import asyncpg

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from consumer.config import settings

log = logging.getLogger("velora.worker.maintenance")

_HYPERTABLES = (
    "prices_1m",
    "sentiment_scored",
    "coin_live_sentiment",
    "predictions",
    "recommendations",
)

_RETENTION_STATUS_SQL = """
SELECT job_id, application_name, schedule_interval,
       COALESCE(last_run_started_at, last_successful_execution) AS last_run,
       COALESCE(last_run_status, status) AS run_status,
       total_successes, total_failures
FROM timescaledb_information.jobs
ORDER BY job_id
"""


async def _vacuum_analyze(pool: asyncpg.Pool) -> None:
    """VACUUM ANALYZE each hypertable.

    Runs outside any transaction (asyncpg autocommit by default). Each
    table is attempted independently; one failure does not abort the
    others.
    """
    async with pool.acquire() as conn:
        for table in _HYPERTABLES:
            try:
                await conn.execute(f"VACUUM ANALYZE {table};")
                log.info("VACUUM ANALYZE %s ok", table)
            except Exception:
                log.exception("VACUUM ANALYZE %s failed", table)


async def _log_retention_status(pool: asyncpg.Pool) -> None:
    """Log a single summary line per TimescaleDB background job."""
    async with pool.acquire() as conn:
        try:
            rows = await conn.fetch(_RETENTION_STATUS_SQL)
            for r in rows:
                log.info(
                    "tsdb job %s [%s] every=%s last_run=%s status=%s ok=%d fail=%d",
                    r["job_id"],
                    r["application_name"],
                    r["schedule_interval"],
                    r["last_run"],
                    r["run_status"],
                    r["total_successes"],
                    r["total_failures"],
                )
        except Exception:
            log.exception("retention status query failed")


async def _tick(pool: asyncpg.Pool) -> None:
    await _vacuum_analyze(pool)
    await _log_retention_status(pool)


async def run(pool: asyncpg.Pool) -> None:
    """Long-running maintenance loop. Cancel-safe."""
    interval = settings.maintenance_interval_seconds
    log.info("maintenance worker started (interval=%ss)", interval)
    try:
        await asyncio.sleep(30)
        while True:
            try:
                await _tick(pool)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("maintenance tick failed")
            await asyncio.sleep(interval)
    except asyncio.CancelledError:
        log.info("maintenance worker cancelled")
        raise
