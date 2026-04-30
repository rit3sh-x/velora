"""One-shot historical backfill of prices_1m from Binance REST.

Run via the `seed` package entrypoint AFTER the consumer DB is up.
Pulls ~24h of 1m klines per coin and bulk-inserts with ON CONFLICT DO NOTHING.
Idempotent — safe to re-run; cheap when data is fresh.
"""
from __future__ import annotations

import asyncio
import json
import logging
import sys
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import asyncpg

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from shared.coins import COINS

log = logging.getLogger("velora.seed.prices")

_BINANCE_KLINES = "https://api.binance.com/api/v3/klines"
_INTERVAL = "1m"
_LIMIT = 1000
_TIMEOUT = 15.0
_BACKFILL_PAGES = 5

_INSERT_SQL = """
INSERT INTO prices_1m (coin, ts, open, high, low, close, volume)
VALUES ($1, $2, $3, $4, $5, $6, $7)
ON CONFLICT (coin, ts) DO NOTHING
"""


async def needs_backfill(pool: asyncpg.Pool, coin: str, min_hours_history: int = 23) -> bool:
    """True if we should backfill — i.e. less than `min_hours_history` of data exists."""
    row = await pool.fetchrow(
        "SELECT MIN(ts) AS oldest, COUNT(*) AS n FROM prices_1m WHERE coin = $1",
        coin,
    )
    if not row or (row["n"] or 0) == 0:
        return True
    age_hours = (datetime.now(timezone.utc) - row["oldest"]).total_seconds() / 3600
    return age_hours < min_hours_history


def _fetch_klines(symbol: str, end_time_ms: int | None = None) -> list[list[Any]]:
    url = f"{_BINANCE_KLINES}?symbol={symbol}&interval={_INTERVAL}&limit={_LIMIT}"
    if end_time_ms is not None:
        url += f"&endTime={end_time_ms}"
    req = urllib.request.Request(url, headers={"User-Agent": "velora-seed/1.0"})
    with urllib.request.urlopen(req, timeout=_TIMEOUT) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _kline_to_row(coin: str, kline: list[Any]) -> tuple[str, datetime, float, float, float, float, float]:
    open_ms = int(kline[0])
    ts = datetime.fromtimestamp(open_ms / 1000.0, tz=timezone.utc)
    return (
        coin,
        ts,
        float(kline[1]),
        float(kline[2]),
        float(kline[3]),
        float(kline[4]),
        float(kline[5]),
    )


async def _backfill_coin(pool: asyncpg.Pool, coin: str, symbol: str) -> int:
    """Walk klines backwards in pages of 1000 bars."""
    end_time_ms: int | None = None
    all_rows: list[tuple] = []

    for page in range(_BACKFILL_PAGES):
        try:
            klines = await asyncio.to_thread(_fetch_klines, symbol, end_time_ms)
        except (urllib.error.URLError, TimeoutError) as exc:
            log.warning("[%s] binance page %d fetch failed: %s", coin, page, exc)
            break
        except Exception:
            log.exception("[%s] unexpected error fetching klines page %d", coin, page)
            break

        if not klines:
            log.info("[%s] page %d empty, stop", coin, page)
            break

        page_rows = [_kline_to_row(coin, k) for k in klines]
        all_rows.extend(page_rows)
        log.info(
            "[%s] page %d: %d bars (cum %d), oldest=%s",
            coin,
            page + 1,
            len(klines),
            len(all_rows),
            page_rows[0][1].isoformat(),
        )

        oldest_ms = int(klines[0][0])
        end_time_ms = oldest_ms - 1
        if len(klines) < _LIMIT:
            break

    if not all_rows:
        log.info("[%s] no klines returned", coin)
        return 0

    async with pool.acquire() as conn:
        await conn.executemany(_INSERT_SQL, all_rows)
    log.info("[%s] backfilled %d 1m bars", coin, len(all_rows))
    return len(all_rows)


async def backfill_prices(pool: asyncpg.Pool) -> None:
    """Backfill ~24h of 1m klines for every configured coin (skip-if-fresh)."""
    log.info("price backfill starting (%d coins)", len(COINS))
    total = 0
    for spec in COINS:
        if not await needs_backfill(pool, spec.coin):
            row = await pool.fetchrow(
                "SELECT MIN(ts) AS oldest, COUNT(*) AS n FROM prices_1m WHERE coin = $1",
                spec.coin,
            )
            n = row["n"] if row else 0
            oldest = row["oldest"] if row else None
            log.info("[%s] skipping backfill: have %d rows since %s", spec.coin, n, oldest)
            continue
        n = await _backfill_coin(pool, spec.coin, spec.binance_pair)
        total += n
    log.info("price backfill complete: %d total bars inserted", total)
