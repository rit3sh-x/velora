from __future__ import annotations

import html
import re

import emoji

_RE_RT_PREFIX = re.compile(r"^RT\s+@\w+\s*:\s*", re.IGNORECASE)
_RE_URL = re.compile(r"https?://\S+")
_RE_MENTION = re.compile(r"@\w+")
_RE_HASHTAG = re.compile(r"#(\w+)")
_RE_REPEAT = re.compile(r"(.)\1{2,}")
_RE_WHITESPACE = re.compile(r"\s+")

_LANG_PASSTHROUGH_LEN = 30


def clean_text(raw: str | None) -> str:
    """Apply V44 pipeline. Returns "" on None / empty."""
    if not raw:
        return ""

    s = html.unescape(raw)
    s = _RE_RT_PREFIX.sub("", s)
    s = _RE_URL.sub("", s)
    s = _RE_MENTION.sub("", s)
    s = _RE_HASHTAG.sub(r"\1", s)
    s = emoji.demojize(s, delimiters=(" :", ": "))
    s = _RE_REPEAT.sub(r"\1\1", s)
    s = _RE_WHITESPACE.sub(" ", s).strip()
    return s


def is_english(text: str | None) -> bool:
    """V53 — passthrough when len(text) < 30 (langdetect unreliable on short).

    Else use langdetect. ⊥ false-drop short EN tweets like "BTC moon!".
    """
    if not text:
        return False
    if len(text) < _LANG_PASSTHROUGH_LEN:
        return True
    try:
        from langdetect import detect, DetectorFactory
        DetectorFactory.seed = 0
        return detect(text) == "en"
    except Exception:
        return True


__all__ = ["clean_text", "is_english"]
