from __future__ import annotations

import asyncio
import json
import logging
import sys
from pathlib import Path

import asyncpg

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from consumer.config import settings
from shared.coins import COIN_NAMES

log = logging.getLogger("velora.worker.recommender")

_PREDICT_HORIZON_FOR_SIGNAL = 60
_BUY_THRESHOLD = 0.4
_SELL_THRESHOLD = -0.4


_AGG_SQL = """
SELECT avg_sentiment_24h, avg_sentiment_bert_24h,
       positive_pct_24h, negative_pct_24h,
       change_1h_pct, lag1_corr, lag2_corr
FROM aggregates_summary
WHERE coin = $1
"""

_PRED_SQL = """
SELECT predicted_return_pct, confidence
FROM predictions
WHERE coin = $1 AND horizon_minutes = $2
ORDER BY ts DESC LIMIT 1
"""

_INSERT_SQL = """
INSERT INTO recommendations (coin, ts, signal, score, reasons)
VALUES ($1, NOW(), $2, $3, $4)
ON CONFLICT (coin, ts) DO UPDATE SET
    signal  = EXCLUDED.signal,
    score   = EXCLUDED.score,
    reasons = EXCLUDED.reasons
"""


def _sign(x: float | None) -> int:
    if x is None:
        return 0
    if x > 0.05:
        return 1
    if x < -0.05:
        return -1
    return 0


def _reason(feature: str, value: float | None, sign: int, contribution: float, note: str = "") -> dict:
    """V51 reason entry."""
    return {
        "feature": feature,
        "value": float(value) if value is not None else None,
        "sign": sign,
        "contribution": float(contribution),
        "note": note,
    }


def _build_signal(agg: asyncpg.Record | None, pred: asyncpg.Record | None) -> tuple[str, float, list[dict]]:
    """Return (signal, score ∈ [-1,1], reasons[])."""
    reasons: list[dict] = []
    components: list[float] = []

    if agg is not None:
        lag1 = agg["lag1_corr"]
        sent_mom = agg["avg_sentiment_24h"]
        if lag1 is not None and sent_mom is not None:
            f = max(-1.0, min(1.0, float(lag1) * float(sent_mom) * 4.0))
            components.append(f)
            reasons.append(_reason(
                "lag1_corr_x_sent",
                float(lag1) * float(sent_mom),
                _sign(f),
                f,
                f"lag1_corr={lag1:.2f} × sent24h={sent_mom:.2f}",
            ))

        ch1 = agg["change_1h_pct"]
        if ch1 is not None:
            f = max(-1.0, min(1.0, float(ch1) / 2.0))
            components.append(f)
            reasons.append(_reason(
                "price_trend_1h",
                float(ch1),
                _sign(f),
                f,
                f"1h Δ={ch1:.2f}%",
            ))

        pos = agg["positive_pct_24h"]
        neg = agg["negative_pct_24h"]
        if pos is not None and neg is not None:
            net = (float(pos) - float(neg)) / 100.0
            components.append(net)
            reasons.append(_reason(
                "net_pos_neg_24h",
                net,
                _sign(net),
                net,
                f"pos={pos:.0f}% neg={neg:.0f}%",
            ))

    if pred is not None:
        ret = pred["predicted_return_pct"]
        conf = pred["confidence"] or 0.0
        if ret is not None:
            f = max(-1.0, min(1.0, float(ret) / 2.0)) * float(conf)
            components.append(f)
            reasons.append(_reason(
                "predict_60min",
                float(ret),
                _sign(f),
                f,
                f"return={ret:.2f}% conf={conf:.2f}",
            ))

    if not components:
        return "hold", 0.0, reasons

    score = float(sum(components) / len(components))

    pos_count = sum(1 for c in components if _sign(c) > 0)
    neg_count = sum(1 for c in components if _sign(c) < 0)

    if score >= _BUY_THRESHOLD and pos_count >= 2:
        signal = "buy"
    elif score <= _SELL_THRESHOLD and neg_count >= 2:
        signal = "sell"
    else:
        signal = "hold"

    return signal, max(-1.0, min(1.0, score)), reasons


async def _recommend_coin(pool: asyncpg.Pool, coin: str) -> str | None:
    async with pool.acquire() as conn:
        agg = await conn.fetchrow(_AGG_SQL, coin)
        pred = await conn.fetchrow(_PRED_SQL, coin, _PREDICT_HORIZON_FOR_SIGNAL)

        if agg is None and pred is None:
            log.debug("recommender[%s]: no input data; skip", coin)
            return None

        signal, score, reasons = _build_signal(agg, pred)
        await conn.execute(_INSERT_SQL, coin, signal, score, json.dumps(reasons))
    return signal


async def _tick(pool: asyncpg.Pool) -> None:
    results = await asyncio.gather(
        *(_recommend_coin(pool, coin) for coin in COIN_NAMES),
        return_exceptions=True,
    )
    signals: dict[str, int] = {"buy": 0, "sell": 0, "hold": 0}
    for coin, r in zip(COIN_NAMES, results):
        if isinstance(r, asyncio.CancelledError):
            raise r
        if isinstance(r, BaseException):
            log.exception("recommender: failed coin=%s", coin, exc_info=r)
        elif r is not None:
            signals[r] = signals.get(r, 0) + 1
    log.info("recommender: %s", signals)


async def run(pool: asyncpg.Pool) -> None:
    """Long-running recommender loop. Cancel-safe (V35)."""
    interval = settings.recommend_interval_seconds
    log.info("recommender worker started (interval=%ss thresh=±%.2f)",
             interval, _BUY_THRESHOLD)
    try:
        await asyncio.sleep(30)
        while True:
            try:
                await _tick(pool)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("recommender tick failed")
            await asyncio.sleep(interval)
    except asyncio.CancelledError:
        log.info("recommender worker cancelled")
        raise
