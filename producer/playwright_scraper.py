"""Async Playwright scraper for a single Nitter instance.

Ports `test/index.js` to async Python. Returns a list of plain dicts
ready to be turned into `TweetEvent`s upstream. Does not raise on
empty pages — returns [] instead.
"""
from __future__ import annotations

import producer

import logging
from typing import Any
from urllib.parse import quote_plus, urlsplit

from playwright.async_api import async_playwright

log = logging.getLogger("velora.producer.scraper")

_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

_EXTRACT_JS = """
() => {
    const out = [];
    document.querySelectorAll('.timeline-item').forEach((el) => {
        if (!el.querySelector('.tweet-content')) return;
        const text = el.querySelector('.tweet-content')?.innerText.trim() ?? null;
        const username = el.querySelector('.username')?.innerText.trim() ?? null;
        const fullname = el.querySelector('.fullname')?.innerText.trim() ?? null;
        const dateEl = el.querySelector('.tweet-date a');
        const date = dateEl?.title ?? null;
        const url = dateEl?.href ?? null;
        const stats = { replies: '0', retweets: '0', likes: '0' };
        el.querySelectorAll('.tweet-stat').forEach((s) => {
            const cls = s.querySelector('.icon')?.className ?? '';
            const val = s.innerText.trim();
            if (cls.includes('comment')) stats.replies = val;
            else if (cls.includes('retweet')) stats.retweets = val;
            else if (cls.includes('heart')) stats.likes = val;
        });
        out.push({ username, fullname, text, date, url, stats });
    });
    return out;
}
"""

_NEXT_HREF_JS = """
() => {
    const a = document.querySelector('.show-more a');
    return a ? a.getAttribute('href') : null;
}
"""


def tweet_id_from_url(url: str) -> str:
    """Derive a stable tweet id from a Nitter status url.

    Example: `https://nitter.tiekoetter.com/user/status/123?foo=1#x`
             → `user/status/123`
    """
    parts = urlsplit(url)
    path = parts.path or url
    return path.lstrip("/")


def _parse_int(raw: str | None) -> int:
    if raw is None:
        return 0
    cleaned = raw.replace(",", "").replace(" ", "").strip()
    if not cleaned:
        return 0
    try:
        return int(float(cleaned))
    except (ValueError, TypeError):
        return 0


def _normalize_tweet(raw: dict[str, Any]) -> dict[str, Any] | None:
    """Trim/coerce a raw scraped tweet dict; drop ones without a url."""
    url = raw.get("url")
    if not url:
        return None
    stats = raw.get("stats") or {}
    return {
        "username": raw.get("username"),
        "fullname": raw.get("fullname"),
        "text": raw.get("text") or "",
        "date": raw.get("date"),
        "url": url,
        "tweet_id": tweet_id_from_url(url),
        "likes": _parse_int(stats.get("likes")),
        "retweets": _parse_int(stats.get("retweets")),
        "replies": _parse_int(stats.get("replies")),
    }


async def scrape_coin(
    coin: str,
    query: str,
    instance: str,
    limit: int,
    timeout_ms: int,
) -> list[dict[str, Any]]:
    """Scrape up to `limit` tweets from a Nitter search for `query`.

    Returns a list of normalized dicts. Empty list on empty/error page.
    """
    instance = instance.rstrip("/")
    initial_qs = f"?f=tweets&q={quote_plus(query)}"
    seen_urls: set[str] = set()
    tweets: list[dict[str, Any]] = []

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        try:
            context = await browser.new_context(user_agent=_USER_AGENT)
            page = await context.new_page()

            cursor: str | None = None
            while len(tweets) < limit:
                qs = cursor if cursor else initial_qs
                url = f"{instance}/search{qs}"
                log.info("[%s] fetching %s", coin, url)

                try:
                    await page.goto(
                        url, wait_until="networkidle", timeout=timeout_ms
                    )
                except Exception as exc:
                    log.warning(
                        "[%s] navigation failed for %s: %s", coin, url, exc
                    )
                    break

                try:
                    await page.wait_for_selector(
                        ".timeline-item, .error-panel",
                        timeout=min(timeout_ms, 15000),
                    )
                except Exception as exc:
                    log.warning(
                        "[%s] timeline selector timeout: %s", coin, exc
                    )

                raw_items: list[dict[str, Any]] = await page.evaluate(
                    _EXTRACT_JS
                )
                if not raw_items:
                    break

                added_this_page = 0
                for raw in raw_items:
                    normalized = _normalize_tweet(raw)
                    if normalized is None:
                        continue
                    if normalized["url"] in seen_urls:
                        continue
                    seen_urls.add(normalized["url"])
                    tweets.append(normalized)
                    added_this_page += 1
                    if len(tweets) >= limit:
                        break

                if added_this_page == 0:
                    break

                if len(tweets) >= limit:
                    break

                next_href: str | None = await page.evaluate(_NEXT_HREF_JS)
                if not next_href:
                    break
                cursor = next_href
        finally:
            await browser.close()

    return tweets[:limit]
