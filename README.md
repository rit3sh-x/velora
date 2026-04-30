# Velora

Crypto sentiment dashboard. Live Binance price feed + Nitter tweet scrape, dual-backend sentiment (VADER + DistilBERT), Pearson lag-correlation between sentiment and 1h returns. Grafana dashboards on top.

> **For boot, install, and troubleshooting → see [SETUP.md](SETUP.md).**
> **For API contract → see [docs/api.md](docs/api.md).**

---

## Architecture

```
                                  consumer infra (docker)
                                  TimescaleDB ← schema applied @ container init
                                       ↑
producer (machine A)              consumer (machine B)
─────────────────────             ────────────────────
binance_producer ──┐               ┌─→ consumers/prices ─┐
tweet_producer  ──┴─→ Kafka :9092 ─┤                     ├─→ TimescaleDB ─→ aggregator ─→ FastAPI ─→ Grafana
                                   └─→ consumers/tweets ─┘                       ↑
                                                                                 │
                                                                          vader + bert workers
seed/ (optional, one-shot) ── prices REST → DB direct
                            └─ tweets Nitter → Kafka → consumer dedupes
```

Two-machine pipeline, Kafka-bridged. Producer stateless (no DB). Consumer owns storage + analytics + serving. Schema lives in `seed/schema.sql` and is applied automatically by Postgres init scripts on first volume init (V1, V2). Single-machine dev = everything on localhost.

---

## What it does

| Step | Component | Cadence |
|------|-----------|---------|
| Apply schema | `seed/schema.sql` mounted into Timescale `/docker-entrypoint-initdb.d/` | once @ first container init |
| Optional historical backfill | `seed/` (Binance REST → DB; Nitter → Kafka) | one-shot, idempotent, skip-if-fresh |
| Pull live OHLCV from Binance | `producer/binance_producer.py` (kline_1m WS) | per closed bar (~60s) |
| Scrape tweets from Nitter | `producer/tweet_producer.py` (Playwright) | concurrent all coins per 120s |
| Persist to TimescaleDB | `consumer/consumers/{prices,tweets}.py` | live (tweets dedupe `ON CONFLICT DO NOTHING`) |
| Score sentiment | `consumer/workers/vader.py` (live) + `bert.py` (batch) | 30s / 5min |
| Compute lag corr + summary | `consumer/workers/aggregator.py` | 60s |
| Live sentiment rolling buckets | `consumer/workers/live_sentiment.py` | 60s |
| DB maintenance | `consumer/workers/maintenance.py` | 6h |
| Serve via REST | `consumer/api/` (FastAPI, 13 endpoints) | per request |
| Visualize | Grafana 11 + Infinity datasource | 10–30s refresh |

---

## Why these design choices

**Kafka over direct DB writes** — producer + consumer can run on different machines. Kafka buffers if consumer crashes. Topics are 6-partition for parallelism. Demos cleanly as "distributed".

**TimescaleDB over Cassandra/Mongo** — time-series specialty (hypertables auto-partition by time, native retention policies, continuous aggregates handle hourly/daily rollups for free). SQL > CQL for ad-hoc analytics. ~1M writes/day fits this scale; Cassandra would be overkill.

**Asyncio over Spark Streaming** — volume is ~30 msg/min sustained. Spark = 4GB JVM overhead for 1 msg/sec. Pure-Python asyncio handles this on a single core. Spark reserved for batch backtest jobs (not implemented yet).

**VADER + DistilBERT, not just one** —
- VADER: lexicon-based, ~5ms/tweet, no model load. Live hot path.
- DistilBERT: transformer, ~50ms/tweet on CPU (~5ms GPU). Batch enricher, runs every 5min.
- Both compound scores stored side-by-side. API can expose either.

**Public Nitter (`tiekoetter.com`), not self-hosted** — saves a docker service + Redis. Single instance, no fallback. On failure, scraper logs and skips the cycle. Sentiment degrades gracefully.

**Optional seed step** (`seed/`) — one-shot historical backfill: ~24h of 1m klines from Binance REST (direct DB insert) + ~N days of tweets via Nitter date-bounded search (publish to Kafka, consumer dedupes via PK). Idempotent + skip-if-fresh per coin. Run once after boot to skip the cold-start fill window; skip entirely for a true cold-start demo.

**Schema applied automatically** — `seed/schema.sql` mounted into Timescale `/docker-entrypoint-initdb.d/`. Runs on first container init only. Subsequent restarts keep existing data. Schema changes require `down -v` (wipe + re-init) in dev.

---

## Storage layout (TimescaleDB)

| Table | Retention | Purpose |
|-------|-----------|---------|
| `prices_1m` | 30d | OHLCV bars (hypertable) |
| `raw_tweets` | 24h | unprocessed scraped tweets |
| `sentiment_scored` | 7d | per-tweet dual-backend scores |
| `sentiment_hourly` | 7d (cont. aggregate) | hourly sentiment rollup |
| `prices_hourly` | 30d (cont. aggregate) | hourly OHLCV rollup |
| `aggregates_summary` | – | one row per coin, refreshed by aggregator |

Retention enforced by Timescale automatically (`add_retention_policy`). Continuous aggregates auto-refresh every 1–5 min in background. No cron needed.

---

## Layout

```
velora/
├── docker-compose.producer.yml     zookeeper + kafka
├── docker-compose.consumer.yml     timescaledb (schema auto-applied) + grafana
├── .env / .env.example              shared per-machine config
├── producer/                        Binance WS + Nitter scraper → Kafka
├── consumer/                        Kafka → TimescaleDB + sentiment + FastAPI
├── seed/                            schema + optional historical backfill
│   └── schema.sql                  mounted into Timescale init dir
├── shared/                          coins registry, kafka topics, pydantic schemas
├── monitoring/grafana/              dashboards + datasource provisioning
├── docs/api.md                      API response contract (source of truth)
├── SETUP.md                         install + boot + ops + troubleshoot
├── SPEC.md                          machine-readable spec (cavekit format)
├── logic/                           reference (old Spark sentiment pipeline)
└── test/                            reference (Node prototypes for binance ws + nitter)
```

---

## Tracked coins

Hard-coded in `shared/coins.py`. Currently 6: bitcoin, ethereum, solana, ripple, binance (BNB), dogecoin. Add more by editing the `COINS` tuple — producer + consumer + Grafana variable pick up automatically.

```python
CoinSpec("cardano", "ADA", "Cardano", "ADAUSDT", "cardano"),
```

---

## Tunables (`.env`)

| Var | Default | Effect |
|-----|---------|--------|
| `TWEET_POLL_INTERVAL_SECONDS` | 120 | Rotation cadence (one coin per tick) |
| `TWEETS_PER_SCRAPE` | 100 | Tweets fetched per coin per cycle |
| `TWEET_MAX_PAGES` | 12 | Nitter "Load more" pagination cap |
| `PLAYWRIGHT_TIMEOUT_MS` | 45000 | Per-page nav timeout |
| `TWEET_BACKFILL_DAYS` | 2 | Seed: days of historical scrape per coin |
| `TWEET_BACKFILL_MAX_PER_DAY` | 600 | Seed: tweet cap per coin per day |
| `VADER_INTERVAL_SECONDS` | 30 | VADER scoring loop |
| `BERT_INTERVAL_SECONDS` | 300 | DistilBERT enrichment loop |
| `BERT_ENABLED` | true | Set false to skip BERT entirely |
| `AGGREGATOR_INTERVAL_SECONDS` | 60 | Lag corr + summary refresh |
| `HOST_LAN_IP` | localhost | Producer machine's LAN IP (cross-machine) |
| `KAFKA_BROKER` | localhost:9092 | Consumer's broker target (cross-machine) |

Boot, verify, ops, and troubleshooting → **[SETUP.md](SETUP.md)**.
