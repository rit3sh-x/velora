# Velora

Crypto sentiment dashboard. Live Binance prices + dual-source social ingest (Twitter via Nitter, Bluesky via Jetstream firehose), Spark Structured Streaming sentiment compute (VADER + DistilBERT), 4-tier analytics (Descriptive / Diagnostic / Predictive / Prescriptive). Medallion lake: Mongo bronze → Timescale silver+gold. Grafana on top.

> **For boot, install, and troubleshooting → see [SETUP.md](SETUP.md).**
> **For Spark job → see [spark/README.md](spark/README.md).**
> **For API contract → see [docs/api.md](docs/api.md).**

---

## Architecture (medallion)

```
┌─────────────────── PRODUCER (machine A) ───────────────────┐
│  binance_producer ─────────┐                                │
│  tweet_producer (Nitter) ──┤                                │
│  bluesky_producer (Jetstream WS + keyword filter) ──┐       │
│                            │                        │       │
│                       Kafka :9092                            │
└────────────────────────────┬─────────────────────────────────┘
                             │
       ┌─────────────────────┼─────────────────────┐
       │                     │                     │
       ▼                     ▼                     ▼
   prices    velora.tweets (3 consumer groups, V41)        velora.sentiment
       │           │                                          ▲
       │           ├──→ consumer/tweets ──→ Mongo bronze      │
       │           └──→ Spark sentiment_stream ─── VADER + BERT
       ▼                                                       │
   Timescale prices_1m                          consumer/sentiment ─┘
       │                                              │
       └────────────────────┬─────────────────────────┘
                            ▼
              Timescale silver (sentiment_scored)
                            │
                            ▼
       Timescale gold ── aggregator + predictor + recommender
       (aggregates_summary, aggregates_by_source, predictions, recommendations,
        sentiment_hourly cagg, prices_hourly cagg, coin_live_sentiment)
                            │
                            ▼
                    FastAPI (consumer/api/) ──→ Grafana
```

**Bronze** = Mongo `raw_tweets` (every raw msg, schemaless, 30d TTL). **Silver** = Timescale `sentiment_scored` (per-tweet score). **Gold** = Timescale aggregates + CAGGs + predictions + recommendations. ⊥ cross-store join @ runtime per V39.

---

## What it does

| Step | Component | Cadence |
|------|-----------|---------|
| Apply schema | `schema/schema.sql` mounted into Timescale init | once @ first volume init |
| Mongo init | `schema/01_indexes.js` mounted into Mongo init | once @ first volume init |
| Optional seed | `seed/` (Binance REST direct, Nitter via Kafka, Bluesky XRPC searchPosts via Kafka) | one-shot, idempotent |
| Live OHLCV | `producer/binance_producer.py` (kline_1m WS) | per closed bar |
| Twitter live | `producer/tweet_producer.py` (Nitter Playwright) | concurrent all coins, 120s |
| Bluesky live | `producer/bluesky_producer.py` (Jetstream WS firehose + keyword filter) | continuous |
| Bronze ingest | `consumer/consumers/tweets.py` → Mongo upsert | live (V38) |
| Spark sentiment | `spark/jobs/sentiment_stream.py` — Q1 VADER 30s, Q2 BERT 5min | streaming |
| Silver ingest | `consumer/consumers/sentiment.py` → `sentiment_scored` UPSERT | live (V41) |
| Gold aggregator | `consumer/workers/aggregator.py` → `aggregates_summary` + `aggregates_by_source` | 60s |
| Predictor | `consumer/workers/predictor.py` (numpy ridge, multi-horizon 15/60/240) | 5min |
| Recommender | `consumer/workers/recommender.py` (rule combiner, V51 reasons) | 5min |
| Live sentiment | `consumer/workers/live_sentiment.py` (last 100 per coin × source) | 60s |
| CAGG refresh | `consumer/workers/cagg_refresh.py` | 60s |
| Maintenance | `consumer/workers/maintenance.py` (VACUUM) | 6h |
| API | `consumer/api/` (FastAPI, ~21 endpoints incl. predict/recommend/divergence) | per request |
| Visualize | Grafana 11 + Infinity (4-tier dashboard rows) | 10–30s |

---

## 4-Tier analytics (V31, V47, V48)

| Tier | Question | Panels |
|---|---|---|
| 🟦 **Descriptive** | What happened | price, 24h Δ%, 1h Δ%, posts 24h, hourly sentiment, candle, live tweet feed, per-source post counts |
| 🟨 **Diagnostic** | Why | lag1/lag2 corr (V+B), correlation scatter, **cross-source divergence**, sentiment-price overlay |
| 🟧 **Predictive** | What's next | **predicted return % @ 15/60/240min**, confidence, 7d forecast history |
| 🟥 **Prescriptive** | What to do | **BUY / SELL / HOLD signal** (color-mapped), score gauge, reasons table, 7d signal timeline |

Per-coin signal heatmap on main dashboard.

---

## Sentiment backend toggle (V27)

`SENTIMENT_BACKEND=spark` (default) — Spark Structured Streaming computes sentiment.
`SENTIMENT_BACKEND=python` — legacy in-process VADER/BERT workers (broken vs. V37; rewrite pending).

Mutually exclusive. Spark image bakes Kafka connector + BERT deps. HF cache volume-mounted (no re-download on restart, V52).

---

## Why Spark for sentiment

VADER alone is fast (~5ms/tweet) and handles ~30 msg/min trivially in Python. Spark added when scale + 2-source ingest + heavy BERT batches require:

- **Backpressure-tolerant streaming** (checkpointed Kafka offsets, exactly-once semantics)
- **Batched DistilBERT** via pandas-UDF (50ms/tweet on CPU; 64-tweet batches mortar amortize tokenizer cost)
- **Independent VADER + BERT cadence** (V54 — two writeStream queries, separate checkpoint subdirs)
- **Compute decoupled from compute** — Mongo bronze ⊥ Spark dependency (V40)

Pure-asyncio path retained as fallback (`SENTIMENT_BACKEND=python`).

---

## Why Mongo for bronze (V36, V37)

Raw tweets are schemaless (Twitter shape ≠ Bluesky shape, future sources will differ again). Mongo is the right tool:

- Schemaless writes — additive fields don't need migrations (V42)
- Unique idx `(tweet_id, source)` for cheap upsert dedupe (V38)
- TTL idx for automatic 30d retention
- Future re-score reads raw text from bronze (no re-scrape)

Timescale stays for time-series compute (silver + gold). Cross-store join ⊥ runtime per V39.

---

## Storage layout

**Mongo bronze** (`velora.raw_tweets`):
| Idx | Purpose |
|---|---|
| `(tweet_id, source)` unique | Upsert dedupe (V38) |
| `(coin, ts DESC)` | API tweet feed |
| `ts` TTL 30d | Auto-prune |

**Timescale silver + gold:**
| Table | Retention | Purpose |
|-------|-----------|---------|
| `prices_1m` | 30d | OHLCV bars (hypertable) |
| `sentiment_scored` | 7d | per-tweet dual-backend scores (PK incl. source) |
| `sentiment_hourly` | cagg | hourly rollup, grouped by (coin, source, bucket) |
| `prices_hourly` | cagg | hourly OHLCV rollup |
| `aggregates_summary` | – | per-coin combined headline (PK coin) |
| `aggregates_by_source` | – | per-coin per-source diagnostic (PK coin, source) |
| `coin_live_sentiment` | 7d | rolling last-100 per (coin, source) |
| `predictions` | 7d | per-(coin, ts, horizon) predicted returns |
| `recommendations` | 7d | per-(coin, ts) signal + reasons[] |

---

## Layout

```
velora/
├── docker-compose.producer.yml     zookeeper + kafka
├── docker-compose.consumer.yml     timescale + mongo + grafana (schema auto-applied)
├── docker-compose.spark.yml        spark master + worker + sentiment_stream submit
├── schema/                          schema.sql (Timescale) + 01_indexes.js (Mongo)
├── .env / .env.example
├── producer/                        Binance WS + Nitter + Bluesky → Kafka
├── consumer/                        Kafka → Mongo bronze + Timescale silver/gold + FastAPI
│   ├── consumers/{prices,tweets,sentiment}.py
│   ├── workers/{aggregator,live_sentiment,cagg_refresh,maintenance,predictor,recommender,vader,bert}.py
│   ├── api/                         FastAPI routers
│   ├── db.py                        asyncpg pool
│   └── db_mongo.py                  motor client
├── spark/                           Structured Streaming sentiment compute
│   ├── jobs/sentiment_stream.py
│   ├── preprocess.py
│   ├── Dockerfile.spark
│   └── README.md
├── seed/                            optional one-shot backfill (Binance REST + Nitter + Bluesky XRPC)
├── shared/                          coins, kafka topics, pydantic schemas, queries
├── monitoring/grafana/              dashboards (4-tier) + datasource provisioning
├── docs/api.md                      API response contract
├── SETUP.md                         install + boot + ops
└── SPEC.md                          machine-readable spec (cavekit)
```

---

## Tracked coins

Hard-coded in `shared/coins.py`. Currently 6: bitcoin, ethereum, solana, ripple, binance, dogecoin. Keywords for matching live in `shared/queries.py`. Add coins by editing both — producer (Bluesky filter, Nitter search), seed (Bluesky search query), aggregator/predictor/recommender (loop over `COIN_NAMES`), and Grafana variable all pick up automatically.

---

## Tunables (`.env`) — major

| Var | Default | Effect |
|-----|---------|--------|
| `SENTIMENT_BACKEND` | spark | spark|python |
| `BLUESKY_JETSTREAM_URL` | jetstream1.us-east | WS firehose endpoint |
| `BLUESKY_PUBLIC_API_URL` | public.api.bsky.app | XRPC searchPosts (seed only) |
| `SPARK_VADER_TRIGGER_SECONDS` | 30 | VADER stream cadence |
| `SPARK_BERT_TRIGGER_SECONDS` | 300 | BERT stream cadence |
| `BERT_BATCH_SIZE` | 64 | BERT pandas-UDF batch |
| `PREDICT_HORIZONS` | 15,60,240 | Predict horizons (minutes) |
| `PREDICT_INTERVAL_SECONDS` | 300 | Predictor tick |
| `RECOMMEND_INTERVAL_SECONDS` | 300 | Recommender tick |

Full list in `.env.example`. Boot, verify, ops, troubleshooting → **[SETUP.md](SETUP.md)**.
