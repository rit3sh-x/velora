"""Binance kline_1m WebSocket producer.

Opens one combined-stream connection for all 6 coins, filters closed
bars (`kline.x == true`), and publishes a `PriceEvent` to the
`velora.prices` Kafka topic with the coin name as key.
"""
from __future__ import annotations

import producer

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Any

import websockets

from producer.kafka_client import publish
from shared.coins import BY_BINANCE_PAIR, COINS, SYMBOL_TO_COIN
from shared.schemas import PriceEvent
from shared.topics import PRICES

log = logging.getLogger("velora.producer.binance")

_WS_TEMPLATE = "wss://stream.binance.com:9443/stream?streams={streams}"
_INTERVAL = "1m"


def build_stream_url() -> str:
    streams = "/".join(
        f"{c.binance_pair.lower()}@kline_{_INTERVAL}" for c in COINS
    )
    return _WS_TEMPLATE.format(streams=streams)


def parse_kline_message(raw_message: str | bytes) -> PriceEvent | None:
    """Parse a single combined-stream message into a PriceEvent.

    Returns None for non-closed bars or unknown symbols.
    """
    msg: dict[str, Any] = json.loads(raw_message)
    data = msg.get("data") or {}
    kline = data.get("k") or {}

    if not kline.get("x"):
        return None

    symbol = kline.get("s")
    if not symbol:
        return None

    coin = SYMBOL_TO_COIN.get(symbol)
    if not coin:
        return None

    spec = BY_BINANCE_PAIR.get(symbol)
    if spec is None:
        return None

    open_ms = int(kline["t"])
    ts = datetime.fromtimestamp(open_ms / 1000, tz=timezone.utc)

    return PriceEvent(
        coin=coin,
        symbol=spec.symbol,
        ts=ts,
        open=float(kline["o"]),
        high=float(kline["h"]),
        low=float(kline["l"]),
        close=float(kline["c"]),
        volume=float(kline["v"]),
    )


async def run() -> None:
    """Long-running WS consumer with reconnect + exponential backoff."""
    url = build_stream_url()
    backoff = 1

    while True:
        try:
            log.info("connecting to binance ws: %s", url)
            async with websockets.connect(
                url, ping_interval=20, ping_timeout=20
            ) as ws:
                backoff = 1
                async for raw in ws:
                    event = parse_kline_message(raw)
                    if event is None:
                        continue
                    publish(PRICES, key=event.coin, event=event)
                    log.info(
                        "price %s %s close=%.6f vol=%.4f",
                        event.coin,
                        event.ts.isoformat(),
                        event.close,
                        event.volume,
                    )
        except asyncio.CancelledError:
            log.info("binance producer cancelled")
            raise
        except Exception as exc:
            log.warning(
                "binance ws error: %s; reconnecting in %ss", exc, backoff
            )
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)
