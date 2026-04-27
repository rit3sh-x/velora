"""DistilBERT sentiment worker.

Re-scores tweets that already have a VADER row but no BERT compound.
Wakes every ``settings.bert_interval_seconds`` seconds. The model is
loaded lazily on the first iteration so process startup is cheap and the
worker can be disabled at runtime via ``settings.bert_enabled``.
"""

from __future__ import annotations

import asyncio
import logging
import sys
from pathlib import Path
from typing import Any

import asyncpg

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from consumer.config import settings
from consumer.workers.preprocess import preprocess

log = logging.getLogger("velora.worker.bert")

_FETCH_SQL = """
SELECT s.tweet_id, s.coin, s.ts, t.text
FROM sentiment_scored s
JOIN raw_tweets t
  ON t.tweet_id = s.tweet_id AND t.ts = s.ts
WHERE s.compound_bert IS NULL
ORDER BY s.ts DESC
LIMIT 500
"""

_UPDATE_SQL = """
UPDATE sentiment_scored
SET compound_bert  = $1,
    label_bert     = $2,
    scored_at_bert = NOW()
WHERE tweet_id = $3 AND ts = $4
"""


_pipeline: Any = None
_device_label: str | None = None


def _load_pipeline() -> tuple[Any, str]:
    """Build the HF sentiment pipeline. Heavy import + model download."""
    import torch
    from transformers import pipeline

    device = 0 if torch.cuda.is_available() else -1
    label = "cuda" if device == 0 else "cpu"
    log.info("loading bert model %r on %s", settings.bert_model, label)
    pipe = pipeline(
        "sentiment-analysis",
        model=settings.bert_model,
        device=device,
    )
    return pipe, label


def _ensure_pipeline() -> tuple[Any, str]:
    global _pipeline, _device_label
    if _pipeline is None:
        _pipeline, _device_label = _load_pipeline()
    assert _device_label is not None
    return _pipeline, _device_label


def _score_batch(
    rows: list[asyncpg.Record],
) -> list[tuple[float, str, str, object]]:
    """CPU/GPU-bound: preprocess + DistilBERT inference. Run in a thread."""
    pipe, _ = _ensure_pipeline()

    cleaned: list[str] = []
    keys: list[tuple[str, object]] = []
    for row in rows:
        text = preprocess(row["text"])
        if not text:
            continue
        cleaned.append(text)
        keys.append((row["tweet_id"], row["ts"]))

    if not cleaned:
        return []

    batch_size = max(1, int(settings.bert_batch_size))
    updates: list[tuple[float, str, str, object]] = []
    for start in range(0, len(cleaned), batch_size):
        chunk = cleaned[start : start + batch_size]
        chunk_keys = keys[start : start + batch_size]
        results = pipe(chunk, truncation=True, max_length=512)
        for (tweet_id, ts), result in zip(chunk_keys, results, strict=True):
            raw_label = str(result["label"]).upper()
            score = float(result["score"])
            if raw_label == "POSITIVE":
                compound = score
                label = "positive"
            else:
                compound = -score
                label = "negative"
            updates.append((compound, label, tweet_id, ts))
    return updates


async def _tick(pool: asyncpg.Pool) -> int:
    async with pool.acquire() as conn:
        rows = await conn.fetch(_FETCH_SQL)

    if not rows:
        log.debug("bert: no tweets pending bert scoring")
        return 0

    updates = await asyncio.to_thread(_score_batch, list(rows))
    if not updates:
        log.debug("bert: %d rows fetched but none had usable text", len(rows))
        return 0

    async with pool.acquire() as conn:
        await conn.executemany(_UPDATE_SQL, updates)

    log.info("bert: scored %d tweets", len(updates))
    return len(updates)


async def run(pool: asyncpg.Pool) -> None:
    """Long-running BERT scoring loop. Cancel-safe."""
    interval = settings.bert_interval_seconds
    if not settings.bert_enabled:
        log.info("bert worker disabled by config; idling at %ss", interval)
        try:
            while True:
                await asyncio.sleep(interval)
        except asyncio.CancelledError:
            log.info("bert worker cancelled (disabled mode)")
            raise

    log.info("bert worker started (interval=%ss)", interval)
    try:
        while True:
            try:
                await _tick(pool)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("bert tick failed")
            await asyncio.sleep(interval)
    except asyncio.CancelledError:
        log.info("bert worker cancelled")
        raise
