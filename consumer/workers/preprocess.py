"""Text preprocessing for sentiment scoring.

Ported from `logic/sentiment-pipeline.py::_preprocess`.
Replaces emojis with bull/bear words, normalises cashtags, strips urls,
and produces a clean lowercase token stream suitable for VADER and
DistilBERT.
"""

from __future__ import annotations

import re

COIN_MAP: dict[str, str] = {
    "btc": "bitcoin",
    "eth": "ethereum",
    "sol": "solana",
    "xrp": "ripple",
    "bnb": "binance",
    "doge": "dogecoin",
}

EMOJI_MAP: dict[str, str] = {
    "🚀": "bullish",
    "🌙": "bullish",
    "💎": "bullish",
    "🙌": "bullish",
    "📈": "bullish",
    "🔥": "bullish",
    "✅": "bullish",
    "📉": "bearish",
    "💀": "bearish",
    "🩸": "bearish",
    "⚠️": "bearish",
}

_URL_RE = re.compile(r"http\S+|www\S+")
_CASHTAG_RE = re.compile(r"\$([a-zA-Z]+)")
_MENTION_HASH_RE = re.compile(r"[@#]")
_NON_ALNUM_RE = re.compile(r"[^\w\s']")


def _replace_cashtag(match: re.Match[str]) -> str:
    tag = match.group(1).lower()
    return COIN_MAP.get(tag, tag)


def preprocess(text: str) -> str:
    """Clean a tweet for sentiment scoring.

    Steps:
      1. Replace bullish/bearish emojis with words.
      2. Lowercase.
      3. Strip urls.
      4. Map `$tag` cashtags through ``COIN_MAP`` (or keep the bare tag).
      5. Strip `@` and `#` characters but keep the trailing word.
      6. Drop any character that is not alphanumeric, whitespace, or apostrophe.
      7. Collapse whitespace.
    """
    if not text:
        return ""
    for emoji, word in EMOJI_MAP.items():
        if emoji in text:
            text = text.replace(emoji, f" {word} ")
    text = text.lower()
    text = _URL_RE.sub("", text)
    text = _CASHTAG_RE.sub(_replace_cashtag, text)
    text = _MENTION_HASH_RE.sub("", text)
    text = _NON_ALNUM_RE.sub(" ", text)
    return " ".join(text.split()).strip()
