# seed/

Optional one-shot historical backfill for Velora. Schemas live in `schema/` (Timescale + Mongo init scripts), NOT here.

- `schema/schema.sql` — TimescaleDB schema. Mounted at Timescale `/docker-entrypoint-initdb.d/01_schema.sql:ro`. **Mandatory.**
- `schema/01_indexes.js` — Mongo bronze indexes. Mounted at Mongo `/docker-entrypoint-initdb.d/01_indexes.js:ro`. **Mandatory.**
- `seed/prices.py` + `seed/tweets.py` + `seed/bluesky.py` — historical backfills. Run via `python -m seed`. **Optional.**

## Boot order

```
1. docker compose -f docker-compose.consumer.yml up -d   # timescale + mongo + grafana (schemas auto-applied)
2. docker compose -f docker-compose.producer.yml up -d   # zookeeper + kafka
3. docker compose -f docker-compose.spark.yml up -d --build   # spark master + worker + sentiment_stream
4. cd seed && uv run python -m seed                       # OPTIONAL. Backfills prices + twitter + bluesky.
5. cd producer && uv run python run.py                    # Live Binance WS + Nitter + Bluesky Jetstream.
6. cd consumer && uv run python run.py                    # Live ingest + analytics + API.
```

Seed publishes tweets to Kafka (consumer dedupes via Mongo unique idx). Prices write direct to Timescale. Kafka buffers tweets if consumer not yet running.

## Run

```bash
cd seed
uv sync                                # asyncpg, kafka-python, playwright, httpx, pydantic
uv run playwright install chromium     # ~2 min, ~200 MB, ONE-TIME
uv run python -m seed
```

Idempotent — skips coins with sufficient silver history. Cheap when fresh.

## What it does

- **Prices** (`seed/prices.py`): ~24h of 1m klines from Binance REST per coin → `prices_1m` direct insert (`ON CONFLICT DO NOTHING`). Skips coins with ≥23h of existing history.
- **Twitter** (`seed/tweets.py`): ~`TWEET_BACKFILL_DAYS` (default 2) of tweets per coin via Nitter date-bounded search → Kafka `velora.tweets` w/ `source: "twitter"`. Skips coins with ≥200 silver tweets in last 24h.
- **Bluesky** (`seed/bluesky.py`): ~7d of posts per coin via public unauthenticated XRPC `searchPosts` (`https://public.api.bsky.app/xrpc/app.bsky.feed.searchPosts`) → Kafka `velora.tweets` w/ `source: "bluesky"`. **No app password needed** — public read endpoint.

## Config (`.env`)

| Var | Default | Effect |
|-----|---------|--------|
| `TWEET_BACKFILL_ENABLED` | true | Set false to skip Twitter seed (prices + bluesky still run) |
| `TWEET_BACKFILL_DAYS` | 2 | Days of Twitter scrape per coin |
| `TWEET_BACKFILL_MIN_FAVES` | 0 | Nitter min_faves filter |
| `TWEET_BACKFILL_PAGES_PER_DAY` | 30 | Nitter pagination cap per day |
| `TWEET_BACKFILL_MAX_PER_DAY` | 600 | Twitter cap per coin per day |
| `BLUESKY_PUBLIC_API_URL` | public.api.bsky.app | Bluesky search base |
| `BLUESKY_SEED_POSTS_PER_QUERY` | 100 | Bluesky XRPC page size |

## Skip seed entirely

Don't run it. Producer + consumer cold-start fine — first hourly bucket in ~60 min, lag correlations meaningful in ~3h. Useful for cold-start demos.
