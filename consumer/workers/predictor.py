from __future__ import annotations

import asyncio
import logging
import sys
from pathlib import Path

import asyncpg
import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from consumer.config import settings
from shared.coins import COIN_NAMES

log = logging.getLogger("velora.worker.predictor")

_MODEL_VERSION = "ridge-v1"
_RIDGE_LAMBDA = 1.0
_MIN_SAMPLES = 24


_FEATURE_SQL = """
WITH hourly AS (
    SELECT bucket,
           source,
           avg_compound_vader AS v,
           avg_compound_bert  AS b,
           post_count
    FROM sentiment_hourly
    WHERE coin = $1 AND bucket > NOW() - INTERVAL '7 days'
),
piv AS (
    SELECT bucket,
           MAX(CASE WHEN source='twitter' THEN v END) AS v_tw,
           MAX(CASE WHEN source='bluesky' THEN v END) AS v_bs,
           MAX(CASE WHEN source='twitter' THEN b END) AS b_tw,
           MAX(CASE WHEN source='bluesky' THEN b END) AS b_bs
    FROM hourly
    GROUP BY bucket
),
prices AS (
    SELECT bucket, close FROM prices_hourly
    WHERE coin = $1 AND bucket > NOW() - INTERVAL '7 days'
)
SELECT COALESCE(p.bucket, piv.bucket) AS hour,
       piv.v_tw, piv.v_bs, piv.b_tw, piv.b_bs,
       p.close AS price
FROM prices p
FULL OUTER JOIN piv ON piv.bucket = p.bucket
ORDER BY hour
"""

_INSERT_SQL = """
INSERT INTO predictions (coin, ts, horizon_minutes, predicted_return_pct, confidence, model_version)
VALUES ($1, NOW(), $2, $3, $4, $5)
ON CONFLICT (coin, ts, horizon_minutes) DO UPDATE SET
    predicted_return_pct = EXCLUDED.predicted_return_pct,
    confidence           = EXCLUDED.confidence,
    model_version        = EXCLUDED.model_version
"""


def _ridge_fit(X: np.ndarray, y: np.ndarray, lam: float) -> np.ndarray:
    """Closed-form ridge regression. X: (n, k), y: (n,). Returns weights (k,)."""
    XtX = X.T @ X
    reg = lam * np.eye(X.shape[1])
    return np.linalg.solve(XtX + reg, X.T @ y)


def _build_features(rows: list[asyncpg.Record]) -> pd.DataFrame | None:
    """Per-hour feature DataFrame. None when insufficient samples."""
    if len(rows) < _MIN_SAMPLES:
        return None

    df = pd.DataFrame([{
        "hour": r["hour"],
        "v_tw": r["v_tw"], "v_bs": r["v_bs"],
        "b_tw": r["b_tw"], "b_bs": r["b_bs"],
        "price": r["price"],
    } for r in rows])
    df = df.sort_values("hour").reset_index(drop=True)

    df["return_1h"] = df["price"].pct_change()
    df["v_tw_ff"] = df["v_tw"].ffill(limit=3)
    df["v_bs_ff"] = df["v_bs"].ffill(limit=3)
    df["b_tw_ff"] = df["b_tw"].ffill(limit=3)
    df["b_bs_ff"] = df["b_bs"].ffill(limit=3)

    df["divergence"] = (df["v_tw_ff"] - df["v_bs_ff"]).abs()

    feat_cols = ["v_tw_ff", "v_bs_ff", "b_tw_ff", "b_bs_ff", "divergence", "return_1h"]
    for col in feat_cols:
        df[f"{col}_lag1"] = df[col].shift(1)
        df[f"{col}_lag2"] = df[col].shift(2)
    return df


def _train_predict(df: pd.DataFrame, horizon_minutes: int) -> tuple[float | None, float | None]:
    """Fit ridge on past hourly data, predict next-horizon return %.

    Target = future return over `horizon_minutes / 60` hours (rolled forward).
    Forecast horizon ≤ 4h per V32. Returns (predicted_return_pct, confidence).
    """
    horizon_hours = max(1, round(horizon_minutes / 60))
    df = df.copy()
    df["target"] = df["return_1h"].shift(-horizon_hours).rolling(horizon_hours).sum().shift(-(horizon_hours - 1))

    feature_cols = [
        "v_tw_ff_lag1", "v_tw_ff_lag2",
        "v_bs_ff_lag1", "v_bs_ff_lag2",
        "b_tw_ff_lag1", "b_tw_ff_lag2",
        "b_bs_ff_lag1", "b_bs_ff_lag2",
        "divergence_lag1",
        "return_1h_lag1", "return_1h_lag2",
    ]

    train = df[feature_cols + ["target"]].dropna()
    if len(train) < _MIN_SAMPLES:
        return None, None

    X_train = train[feature_cols].to_numpy(dtype=float)
    y_train = train["target"].to_numpy(dtype=float)

    X_with_bias = np.hstack([np.ones((X_train.shape[0], 1)), X_train])
    try:
        w = _ridge_fit(X_with_bias, y_train, _RIDGE_LAMBDA)
    except np.linalg.LinAlgError:
        return None, None

    y_pred = X_with_bias @ w
    residuals = y_train - y_pred
    rss = float(np.sum(residuals ** 2))
    tss = float(np.sum((y_train - y_train.mean()) ** 2))
    r2 = 0.0 if tss == 0 else max(0.0, 1.0 - rss / tss)
    confidence = float(np.clip(r2, 0.0, 1.0))

    last = df[feature_cols].iloc[-1]
    if last.isna().any():
        return None, None
    x_last = np.concatenate([[1.0], last.to_numpy(dtype=float)])
    pred = float(x_last @ w) * 100.0
    pred = float(np.clip(pred, -50.0, 50.0))
    return pred, confidence


async def _predict_coin(conn: asyncpg.Connection, coin: str) -> int:
    rows = await conn.fetch(_FEATURE_SQL, coin)
    df = _build_features(list(rows))
    if df is None:
        log.debug("predictor[%s]: insufficient samples", coin)
        return 0

    horizons = settings.predict_horizons_list
    written = 0
    for h in horizons:
        pred, conf = _train_predict(df, h)
        if pred is None:
            continue
        await conn.execute(_INSERT_SQL, coin, h, pred, conf, _MODEL_VERSION)
        written += 1
    return written


async def _tick(pool: asyncpg.Pool) -> None:
    async with pool.acquire() as conn:
        total = 0
        for coin in COIN_NAMES:
            try:
                total += await _predict_coin(conn, coin)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("predictor: failed coin=%s", coin)
        log.info("predictor: wrote %d prediction rows across %d coins",
                 total, len(COIN_NAMES))


async def run(pool: asyncpg.Pool) -> None:
    """Long-running predictor loop. Cancel-safe (V35)."""
    interval = settings.predict_interval_seconds
    log.info("predictor worker started (interval=%ss horizons=%s model=%s)",
             interval, settings.predict_horizons_list, _MODEL_VERSION)
    try:
        await asyncio.sleep(15)
        while True:
            try:
                await _tick(pool)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("predictor tick failed")
            await asyncio.sleep(interval)
    except asyncio.CancelledError:
        log.info("predictor worker cancelled")
        raise
