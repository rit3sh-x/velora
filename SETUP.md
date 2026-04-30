# Velora — Setup Guide

Crypto sentiment dashboard. Producer + Spark + Consumer + Grafana, Kafka-bridged, medallion lake (Mongo bronze → Timescale silver+gold).

> **⚠ Schema migration**: this revision changed silver PKs + dropped `raw_tweets` (now in Mongo) + new `predictions`/`recommendations`/`aggregates_by_source` tables (V50). **`down -v` mandatory** — no in-place upgrade. Re-seed required after.
>
> **🧠 Memory**: full stack ~6–8 GB RAM. Spark worker w/ BERT loaded ≈ 3 GB. Borderline on 8 GB dev laptops; close other apps.

```
                                       MEDALLION
                                       ─────────
PRODUCER (machine A)                                         CONSUMER (machine B)
──────────────────                                           ────────────────────
binance_producer ─┐                                             ┌─→ prices ─→ Timescale prices_1m
tweet_producer  ──┤                                             │
bluesky_producer ─┴─→ Kafka :9092 (velora.tweets) ──┬──→ tweets ─→ Mongo BRONZE
                                                    │
                                                    └──→ Spark sentiment_stream ─→ Kafka velora.sentiment
                                                                                          │
                                                                                          ▼
                                                                          consumer/sentiment ─→ Timescale SILVER
                                                                                          │
                                                                                          ▼
                                                                                   GOLD: aggregator,
                                                                                   predictor, recommender
                                                                                          │
                                                                                          ▼
                                                                                   FastAPI → Grafana
                                                                                   (4-tier dashboard)
```

**4-tier dashboard rows**: 🟦 DESCRIPTIVE | 🟨 DIAGNOSTIC | 🟧 PREDICTIVE | 🟥 PRESCRIPTIVE.

---

## 1. Prerequisites

Install on every machine that will run any component.

| Tool | Purpose | Install |
|------|---------|---------|
| Docker Desktop | Kafka, TimescaleDB, Grafana | https://www.docker.com/products/docker-desktop |
| Python 3.11+ | Producer + consumer + seed runtime | uv pulls automatically |
| uv | Python package manager | `curl -LsSf https://astral.sh/uv/install.sh \| sh` (mac/linux) <br> `powershell -c "irm https://astral.sh/uv/install.ps1 \| iex"` (windows) |
| Git | Clone repo | preinstalled on most systems |

Verify:

```bash
docker --version          # Docker version 24+ recommended
uv --version              # 0.5.0+
python --version          # not strictly needed, uv manages
```

---

## 2. Single-machine dev (everything localhost)

Easiest path. Producer + consumer + infra all on one box.

### A. Clone

```bash
git clone <repo-url> velora
cd velora
```

### B. Configure .env

```bash
cp .env.example .env
```

Defaults are correct for single-machine. No edits needed.

### C. Boot infrastructure

```bash
docker compose -f docker-compose.consumer.yml up -d
docker compose -f docker-compose.producer.yml up -d
docker compose -f docker-compose.spark.yml    up -d --build
```

Producer compose includes a one-shot `kafka-init` service that depends on `kafka: service_healthy` and creates the 3 velora topics (`velora.prices`, `velora.tweets`, `velora.sentiment`) w/ 6 partitions each, then exits. Auto-runs every `up -d`. ⊥ manual step needed.

The first Spark build downloads Kafka connector JARs + bakes Python deps incl. torch/transformers/pyarrow (~6 min cold). First Spark run downloads BERT model into `spark-hf-cache` volume (~250 MB). Subsequent restarts skip both.

Wait ~60 seconds. Verify all healthy:

```bash
docker ps --format 'table {{.Names}}\t{{.Status}}'
```

Expected:

```
NAMES                       STATUS
velora-kafka                Up 60 seconds (healthy)
velora-zookeeper            Up 60 seconds (healthy)
velora-grafana              Up 60 seconds (healthy)
velora-timescaledb          Up 60 seconds (healthy)
velora-mongodb              Up 60 seconds (healthy)
velora-spark-master         Up 60 seconds (healthy)
velora-spark-worker         Up 60 seconds
velora-spark-submit         Up 60 seconds
```

**Schemas are applied automatically:**
- Timescale: `schema/schema.sql` → `/docker-entrypoint-initdb.d/01_schema.sql`
- Mongo:     `schema/01_indexes.js` → `/docker-entrypoint-initdb.d/01_indexes.js`

Init scripts run **only on empty data dir**. Subsequent `up -d` skips them.

> **Schema changes (any silver/gold PK, CAGG group-by, new table) → `down -v` mandatory** (V50). No in-place migration path. Re-seed required after.

Quick checks tables/collections exist:

```bash
docker exec velora-timescaledb psql -U velora -d velora -c "\dt"
# expect: prices_1m, sentiment_scored, aggregates_summary, aggregates_by_source,
#         coin_live_sentiment, predictions, recommendations
docker exec velora-mongodb mongosh --quiet --eval "db.getSiblingDB('velora').raw_tweets.getIndexes().map(i=>i.name)"
# expect: _id_, uniq_tweet_source, coin_ts_desc, coin_source_ts_desc, ts_ttl
```

### D. Seed (optional, one-shot historical backfill)

Skip for cold-start demo. Run to skip the first ~12 minute fill window: ~24h of prices + ~7d of tweets from both Twitter and Bluesky.

```bash
cd seed
uv sync                                # asyncpg, kafka-python, playwright, httpx, pydantic
uv run playwright install chromium     # ~2 min, ~200 MB, ONE-TIME
uv run python -m seed
```

What it does:
- **Prices**: ~24h of 1m klines from Binance REST per coin → `prices_1m` direct insert (`ON CONFLICT DO NOTHING`). Skips coins with ≥23h existing history.
- **Twitter**: ~7d of tweets per coin via Nitter date-bounded search → Kafka. Consumer dedupes via Mongo unique idx `(tweet_id, source)`.
- **Bluesky**: ~7d of posts per coin via public unauthenticated `searchPosts` (`https://public.api.bsky.app/xrpc/app.bsky.feed.searchPosts`) → Kafka. Same dedupe path.

Idempotent — safe to re-run; cheap when fresh. All three publish to Kafka (except prices) so works whether consumer is up or buffering.

### E. Producer (terminal 1)

```bash
cd producer
uv sync                                # kafka-python, websockets, playwright, pydantic
uv run playwright install chromium     # ~2 min, ~200 MB, ONE-TIME (skip if done in D)
uv run python run.py                   # Binance WS + Twitter (Nitter) + Bluesky (Jetstream WS) → Kafka
```

Expected log:
```
INFO velora.producer.binance: connecting to wss://stream.binance.com...
INFO velora.producer.tweets: tweet producer running: 6 coins concurrent, interval=120s, limit=100
INFO velora.producer.bluesky: connecting bluesky jetstream wss://jetstream1.us-east...
INFO velora.producer.bluesky: bluesky jetstream connected
```

Leave running.

### F. Consumer (terminal 2)

```bash
cd consumer
uv sync                                # ~3 min cold (torch + transformers heavy)
uv run python run.py                   # asyncpg + motor + 8 async tasks + uvicorn
```

Expected log:
```
INFO velora: initializing pg pool + mongo client...
INFO velora.db_mongo: mongo connected
INFO velora: spawning workers + uvicorn on 0.0.0.0:8000
INFO velora: sentiment_backend=spark: ingesting velora.sentiment from Kafka
INFO velora.consumer.tweets: tweets consumer connected
INFO velora.consumer.sentiment: sentiment consumer connected
INFO velora.worker.aggregator: aggregator worker started (interval=60s)
INFO velora.worker.predictor: predictor worker started (interval=300s horizons=[15, 60, 240])
INFO velora.worker.recommender: recommender worker started (interval=300s thresh=±0.40)
```

> No more `vader/bert worker started` lines — Spark owns sentiment compute. Set `SENTIMENT_BACKEND=python` to revert (legacy, broken vs. V37 — needs rewrite).

Leave running.

### G. Spark logs (terminal 3, optional)

```bash
docker compose -f docker-compose.spark.yml logs -f spark-submit
```

Expected:
```
INFO velora.spark.sentiment: starting velora sentiment stream | kafka=... checkpoint=/opt/spark/checkpoints
INFO velora.spark.sentiment: Q1 VADER stream started: trigger=30s
INFO velora.spark.sentiment: Q2 BERT stream started: model=distilbert-... batch=64 trigger=300s
```

Spark UI: http://localhost:8080 (master), http://localhost:4040 (driver, only while job running).

---

## 3. Verify

### API

```bash
curl http://localhost:8000/health        # {"status":"ok"}
curl http://localhost:8000/coins         # [...] with 6 coins
```

### Database

```bash
docker exec -it velora-timescaledb psql -U velora -d velora -c \
  "SELECT coin, COUNT(*) FROM prices_1m GROUP BY coin;"
```

After ~2 min you should see rows starting to populate per coin (or immediately, if you ran the seed step).

```bash
docker exec velora-mongodb mongosh --quiet --eval \
  "db.getSiblingDB('velora').raw_tweets.aggregate([{\$group:{_id:'\$coin',n:{\$sum:1}}}]).toArray()"
```

After one full rotation (~2 min) Mongo bronze should have ~100+ tweets per coin (Twitter via Nitter every 120 s + Bluesky Jetstream firehose continuous). Several thousand if seed ran.

### Grafana

Browser → http://localhost:3000 → Dashboards → Velora — Overview.

Anonymous Admin auth: no login needed.

### Expected fill timeline (cold start, no seed)

| Time after boot | What appears |
|----------------|--------------|
| ~60s | First candle |
| ~2 min | First tweets, first VADER scores |
| ~5–7 min | First BERT scores |
| ~10 min | All 6 coins covered |
| ~15 min | Dashboard "looks alive" |
| ~60 min | First hourly sentiment bucket |
| ~3 hours | Lag1/Lag2 correlations meaningful |
| ~24 hours | Full 24h candles, full /global stats |
| ~7 days | Full 7d sentiment + price-sentiment graphs |

### Expected fill timeline (with seed)

| Time after seed completes | What appears |
|----------------|--------------|
| immediate | ~24h prices populated, ~thousands of tweets queued in Kafka |
| ~2 min | Consumer ingest catches up; sentiment workers score backlog |
| ~5–10 min | Hourly buckets backfilled, lag correlations meaningful |
| ~15 min | Full dashboard |

---

## 4. Cross-machine setup

Two machines on the same LAN. Producer on machine A. Consumer on machine B.

### Machine A (Producer)

```bash
# 1. Find LAN IP
ipconfig                # windows: look for IPv4 Address
ip addr                 # linux/mac
# e.g. 192.168.1.50

# 2. Edit .env
HOST_LAN_IP=192.168.1.50

# 3. Open firewall on port 9092
# Windows: New-NetFirewallRule -DisplayName "Kafka" -Direction Inbound -LocalPort 9092 -Protocol TCP -Action Allow
# Linux:   sudo ufw allow 9092/tcp

# 4. Boot infra + producer
docker compose -f docker-compose.producer.yml up -d
cd producer && uv sync && uv run playwright install chromium && uv run python run.py
```

### Machine B (Consumer)

```bash
# 1. Edit .env
KAFKA_BROKER=192.168.1.50:9092
POSTGRES_HOST=localhost          # TimescaleDB still local on machine B

# 2. Boot infra + consumer
docker compose -f docker-compose.consumer.yml up -d
cd consumer && uv sync && uv run python run.py
```

Verify cross-machine connection from machine B:

```bash
# Test kafka reachability
nc -zv 192.168.1.50 9092         # should say "succeeded"
```

Optional seed from machine B (writes prices direct to local DB; publishes tweets to remote Kafka):

```bash
cd seed && uv sync && uv run playwright install chromium && uv run python -m seed
```

---

## 5. Daily operations

### Stop everything (full)

```bash
# 1. Ctrl-C in producer + consumer terminals (graceful Python shutdown)

# 2. Stop docker stacks (preserves volumes/data)
docker compose -f docker-compose.spark.yml    stop
docker compose -f docker-compose.producer.yml stop
docker compose -f docker-compose.consumer.yml stop
```

### Start everything (after first setup, data preserved)

```bash
# 1. Boot stacks (kafka-init auto-creates topics on healthy)
docker compose -f docker-compose.consumer.yml up -d
docker compose -f docker-compose.producer.yml up -d
docker compose -f docker-compose.spark.yml    up -d

# 2. Verify all healthy (kafka-init shows status=Exited 0 when done — that's correct)
docker ps -a --format "table {{.Names}}\t{{.Status}}"

# 3. Producer (terminal 1)
cd producer ; uv run python run.py

# 4. Consumer (terminal 2)
cd consumer ; uv run python run.py

# 5. (optional) Spark stream logs (terminal 3)
docker compose -f docker-compose.spark.yml logs -f spark-submit
```

### Apply changes after `.env` edit (re-read env)

`docker compose restart` does NOT re-read `.env`. Must `up -d --force-recreate`:

```bash
docker compose -f docker-compose.consumer.yml up -d --force-recreate
docker compose -f docker-compose.producer.yml up -d --force-recreate
docker compose -f docker-compose.spark.yml    up -d --force-recreate

# Restart Python processes (Ctrl-C then re-run) so they pick up new .env
cd producer ; uv run python run.py
cd consumer ; uv run python run.py
```

### Restart only one container (no env change)

```bash
docker compose -f docker-compose.consumer.yml restart grafana
docker compose -f docker-compose.spark.yml    restart spark-submit
docker compose -f docker-compose.producer.yml restart kafka
```

### Tail container logs

```bash
docker logs -f velora-kafka
docker logs -f velora-timescaledb
docker logs -f velora-mongodb
docker logs -f velora-grafana
docker logs -f velora-spark-master
docker logs -f velora-spark-submit
```

---

## 6. Reset / start over

### Soft reset (keep data, restart processes)

```bash
docker compose -f docker-compose.consumer.yml restart
docker compose -f docker-compose.producer.yml restart
```

### Hard reset (wipe all data + re-apply schema)

```bash
# 1. Kill app processes (Ctrl-C in producer + consumer terminals)

# 2. Tear down all 3 stacks WITH volumes
docker compose -f docker-compose.spark.yml    down -v
docker compose -f docker-compose.producer.yml down -v
docker compose -f docker-compose.consumer.yml down -v

# 3. (optional) Wipe Python venvs to force-resync deps
rm -rf producer/.venv consumer/.venv seed/.venv spark/.venv

# 4. Boot fresh — kafka-init auto-creates topics
docker compose -f docker-compose.consumer.yml up -d
docker compose -f docker-compose.producer.yml up -d
docker compose -f docker-compose.spark.yml    up -d --build
```

`-v` deletes volumes (Timescale rows, Mongo bronze, Kafka offsets, Grafana state, Spark checkpoints, HF model cache). Drop `-v` to keep data across container recreation.

### Schema evolution (changed `schema/schema.sql` or `schema/01_indexes.js`)

Postgres + Mongo init scripts run **only on empty data volumes**. To pick up schema changes in dev:

```bash
docker compose -f docker-compose.consumer.yml down -v
docker compose -f docker-compose.consumer.yml up -d
```

This wipes Timescale + Mongo (+ Grafana state) and re-applies the new schemas. For production, use a real migration tool (Alembic, sqitch). Out of scope for this project.

---

## 7. Common boot issues

| Symptom | Cause | Fix |
|---------|-------|-----|
| `kafka unhealthy` | Still booting (slow first start) | Wait 30s, retry. `docker logs velora-kafka` |
| `InconsistentClusterIdException: ... doesn't match stored clusterId` | `kafka-data` volume has old cluster ID; zookeeper got wiped (or vice versa) | `docker compose -f docker-compose.producer.yml down -v && up -d` — wipes both volumes, fresh cluster |
| `NodeExistsException` when kafka starts | Stale zookeeper broker registration | `docker compose -f docker-compose.producer.yml down -v && up -d` |
| `NoBrokersAvailable` from python producer/consumer | `.env` `KAFKA_BROKER` ≠ reachable LOCAL listener | Must be `localhost:9092` on same machine; restart processes |
| `pull access denied for bitnami/spark` or `velora/spark` | Bitnami removed free tier Aug 28 2025 / pre-build pull race | Image now `apache/spark:3.5.4`; `pull_policy: never` on worker+submit |
| `ModuleNotFoundError: No module named 'spark'` in spark-submit | preprocess.py outside mounted dir | Already fixed: file at `spark/jobs/preprocess.py` w/ sibling import |
| `PyArrow >= 4.0.0 must be installed` | pandas-UDF needs Arrow | Already in Dockerfile.spark; rebuild w/ `--no-cache` if missing |
| `UnknownTopicOrPartitionException` in Spark | Topics not pre-created | Run topic-create command (§2.C above or §5 Start) |
| Spark `No resolvable bootstrap urls` | Stale env in container | `docker compose -f docker-compose.spark.yml up -d --force-recreate spark-submit` |
| Bluesky seed 6× 403 Forbidden | Bluesky public XRPC ⊥ supports cursor/since unauth | Already handled — graceful skip; live Jetstream firehose covers ongoing data |
| Dashboard rows show `â€"` mojibake | Em-dash encoding mismatch | Already fixed (replaced w/ ASCII `\|`); hard-refresh browser |
| `connection refused :5432` | TimescaleDB not ready | Wait 10s |
| `relation "prices_1m" does not exist` | Schema not applied (volume pre-existed without schema mount) | `docker compose -f docker-compose.consumer.yml down -v && up -d` |
| `connection refused :9092` cross-machine | Firewall blocking | Open port 9092 inbound on producer machine |
| `ModuleNotFoundError: consumer` | Wrong CWD | Run `uv run python run.py` from inside `consumer/` dir |
| API returns `[]` for `/coins` | First minute, no data | Wait 60s for first kline close |
| `503 no data` from `/ticker` | First minute, no data | Wait 60s, or run seed |
| Grafana panels empty | Check API is up: `curl localhost:8000/coins` | If empty, check consumer logs for errors |
| Producer crash on chromium launch | Playwright browser missing | `uv run playwright install chromium` in `producer/` |
| Seed crash on chromium launch | Playwright browser missing in seed venv | `uv run playwright install chromium` in `seed/` |
| BERT loading hangs | First-run model download (~250MB) | Wait, or set `BERT_ENABLED=false` in `.env` |
| `.env not loading` | Env file in wrong dir | Must be at repo root (next to `docker-compose.*.yml`) |
| Tweet table empty for some coins | Nitter instance flaky | Try alternate instance in `NITTER_URL`. Producer logs cycle errors |
| Tweets missing in Grafana but Mongo has them | Spark didn't score yet (first 30s after producer start) | Wait one VADER trigger cycle; check `docker logs velora-spark-submit` |

---

## 8. Useful commands reference

### Inspect Kafka

```bash
# List topics
docker exec velora-kafka kafka-topics --bootstrap-server localhost:9092 --list

# Tail price stream
docker exec velora-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic velora.prices \
  --from-beginning --max-messages 5

# Tail tweet stream
docker exec velora-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic velora.tweets \
  --from-beginning --max-messages 5
```

### Inspect TimescaleDB

```bash
# Open psql shell
docker exec -it velora-timescaledb psql -U velora -d velora

# Common queries
SELECT coin, COUNT(*) FROM prices_1m GROUP BY coin;
SELECT coin, source, COUNT(*) FROM sentiment_scored WHERE compound_vader IS NOT NULL GROUP BY coin, source;
SELECT * FROM aggregates_summary;
SELECT * FROM aggregates_by_source;
SELECT * FROM predictions ORDER BY ts DESC LIMIT 12;
SELECT * FROM recommendations ORDER BY ts DESC LIMIT 6;
SELECT view_name, materialization_hypertable_name FROM timescaledb_information.continuous_aggregates;
```

### Inspect MongoDB (bronze)

```bash
docker exec -it velora-mongodb mongosh velora

# Common queries
db.raw_tweets.countDocuments()
db.raw_tweets.aggregate([{$group: {_id: {coin:"$coin", source:"$source"}, n:{$sum:1}}}])
db.raw_tweets.find({coin:"bitcoin"}).sort({ts:-1}).limit(5)
db.raw_tweets.getIndexes().map(i => i.name)
```

### Test scrape one coin manually

```bash
cd producer
uv run python -c "
import asyncio
from producer.playwright_scraper import scrape_coin
print(asyncio.run(scrape_coin('bitcoin', 'bitcoin', 'https://nitter.tiekoetter.com', 5, 30000)))
"
```

### Re-run seed only

```bash
cd seed && uv run python -m seed
```

(Idempotent. Skips coins already fresh.)

---

## 9. File structure

```
velora/
├── docker-compose.producer.yml     zookeeper + kafka
├── docker-compose.consumer.yml     timescaledb (schema auto-applied) + grafana
├── .env / .env.example              shared per-machine config
├── producer/                        Binance WS + Nitter scraper → Kafka
├── consumer/                        Kafka → TimescaleDB + sentiment + FastAPI
├── seed/                            schema + optional historical backfill
│   └── schema.sql                  mounted into Timescale init dir
├── shared/                          coins, kafka topics, pydantic schemas
├── monitoring/grafana/              dashboards + datasource provisioning
├── docs/api.md                      API response contract
├── SPEC.md                          machine-readable spec (cavekit format)
├── logic/                           reference (old sentiment pipeline)
└── test/                            reference (Node prototypes)
```

---

## 10. Endpoints quick reference

| URL | Purpose |
|-----|---------|
| http://localhost:8000/health | API liveness |
| http://localhost:8000/docs | Swagger UI |
| http://localhost:8000/coins | All coin snapshots |
| http://localhost:3000 | Grafana (anonymous Admin) |
| http://localhost:3000/d/velora-main | Overview dashboard |
| http://localhost:3000/d/velora-coin | Per-coin detail |
| postgresql://velora:velora@localhost:5432/velora | TimescaleDB |
| localhost:9092 | Kafka broker |

Full API contract: [`docs/api.md`](docs/api.md)
