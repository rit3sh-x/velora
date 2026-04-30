import re
from typing import Iterable

from .coins import COIN_NAMES, COINS, BY_COIN


_ALWAYS_MATCH_SYMBOLS: dict[str, list[str]] = {
    "bitcoin":  ["bitcoin", "btc", "₿"],
    "ethereum": ["ethereum", "eth", "ether"],
    "solana":   ["solana", "sol"],
    "ripple":   ["ripple", "xrp"],
    "binance":  ["binance coin", "bnb"],
    "dogecoin": ["dogecoin", "doge"],
}


def _build_pattern(keywords: Iterable[str]) -> re.Pattern[str]:
    """Word-boundary OR pattern. Case-insensitive."""
    parts = [re.escape(k) for k in keywords]
    return re.compile(r"(?<![a-z0-9])(?:" + "|".join(parts) + r")(?![a-z0-9])", re.IGNORECASE)


KEYWORDS: dict[str, list[str]] = _ALWAYS_MATCH_SYMBOLS
_PATTERNS: dict[str, re.Pattern[str]] = {coin: _build_pattern(kws) for coin, kws in KEYWORDS.items()}


def match_coins(text: str) -> list[str]:
    """Return list of coin keys whose keywords appear in `text`. Empty if none.

    A single tweet may match multiple coins → fan-out one Kafka msg per match.
    """
    if not text:
        return []
    matched: list[str] = []
    for coin, pat in _PATTERNS.items():
        if pat.search(text):
            matched.append(coin)
    return matched


def search_query(coin: str) -> str:
    """Single search query string for HTTP search APIs (Bluesky searchPosts, Nitter).

    Picks the most specific keyword (full coin name) to keep noise low.
    """
    spec = BY_COIN[coin]
    return spec.coin


__all__ = ["KEYWORDS", "match_coins", "search_query"]
