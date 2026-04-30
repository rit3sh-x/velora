# seed/

Owns DB schema + optional historical data backfill for Velora.

- `seed/schema.sql` — TimescaleDB schema. Mounted into the Timescale container at `/docker-entrypoint-initdb.d/01_schema.sql:ro`. Applied automatically on first container init. **Mandatory.**
- `seed/prices.py` + `seed/tweets.py` — one-shot historical backfill. Run via `python -m seed`. **Optional.**

## Boot order

```
1. docker compose up -d         # consumer.yml + producer.yml. Schema auto-applied at first init.
2. cd seed && uv run python -m seed   # OPTIONAL. Backfill prices + tweets.
3. cd producer && uv run python run.py     # Live Binance WS + Nitter scraper.
4. cd consumer && uv run python run.py     # Live ingest + sentiment + API.
```

Seed can run before or after producer/consumer come up — it publishes tweets to Kafka and writes prices direct to DB. Kafka buffers tweets if consumer not yet running.

## Run

```bash
cd seed
uv sync
uv run playwright install chromium     # ~2 min, ~200MB. ONE-TIME install.
uv run python -m seed
```

Idempotent: skips coins that already have sufficient data. Safe to re-run; cheap when fresh.

## What it does

- **Prices**: `seed/prices.py` pulls ~24h of 1m klines from Binance REST per coin, bulk-inserts direct into `prices_1m` (`ON CONFLICT DO NOTHING`). Skips coins with ≥23h of existing history.
- **Tweets**: `seed/tweets.py` scrapes `TWEET_BACKFILL_DAYS` (default 2) of tweets per coin via Nitter date-bounded search and **publishes to Kafka** as `TweetEvent`s. The consumer ingests them via the normal path; `raw_tweets` PK + `ON CONFLICT DO NOTHING` dedupes. Skips coins with ≥200 tweets in the last 24h.

## Config

Reads `.env` at repo root (same as producer + consumer). Relevant keys:

| Var | Default | Effect |
|-----|---------|--------|
| `TWEET_BACKFILL_ENABLED` | true | Set false to skip tweet seed entirely (prices still seeded) |
| `TWEET_BACKFILL_DAYS` | 2 | Days of historical scrape per coin |
| `TWEET_BACKFILL_MIN_FAVES` | 0 | Filter floor on Nitter `min_faves` query param |
| `TWEET_BACKFILL_PAGES_PER_DAY` | 30 | "Load more" pagination cap per day |
| `TWEET_BACKFILL_MAX_PER_DAY` | 600 | Tweet cap per coin per day |

## Skip seed entirely

Just don't run it. Producer + consumer cold-start fine — first hourly bucket appears in ~60 minutes, lag correlations meaningful in ~3h. Useful for true cold-start demos.
