# Velora — Setup Guide

Crypto sentiment dashboard. Two-machine pipeline (or single machine for dev).

```
producer (machine A)                consumer (machine B)
─────────────────────               ────────────────────
binance_producer ──┐                 ┌─→ consumers/prices ─┐
tweet_producer  ──┴─→ Kafka :9092 ──→┤                     ├─→ TimescaleDB ─→ aggregator ─→ FastAPI ─→ Grafana
                                     └─→ consumers/tweets ─┘                       ↑
                                                                                   │
                                                                            vader + bert workers
```

---

## 1. Prerequisites

Install on every machine that will run any component.

| Tool | Purpose | Install |
|------|---------|---------|
| Docker Desktop | Kafka, TimescaleDB, Grafana | https://www.docker.com/products/docker-desktop |
| Python 3.11+ | Producer + consumer runtime | uv pulls automatically |
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
docker compose -f docker-compose.consumer.yml up -d   # timescaledb + grafana
docker compose -f docker-compose.producer.yml up -d   # zookeeper + kafka
```

Wait ~30 seconds. Verify all four healthy:

```bash
docker ps --format 'table {{.Names}}\t{{.Status}}'
```

Expected:

```
NAMES                STATUS
velora-kafka         Up 30 seconds (healthy)
velora-zookeeper     Up 30 seconds (healthy)
velora-grafana       Up 30 seconds
velora-timescaledb   Up 30 seconds (healthy)
```

### D. Producer (terminal 1)

```bash
cd producer
uv sync                                # ~30s, installs kafka-python, websockets, playwright, etc.
uv run playwright install chromium     # ~2 min, ~200MB. ONE-TIME install.
uv run python run.py                   # Binance WS + Nitter scraper → Kafka
```

Expected log:

```
INFO velora.producer.binance: connecting to wss://stream.binance.com...
INFO velora.producer.tweets: starting tweet producer (poll=120s, limit=30)
INFO velora.producer.binance: published bitcoin 67065.20 ...
```

Leave running.

### E. Consumer (terminal 2)

```bash
cd consumer
uv sync                                # ~3 min on first run (torch + transformers heavy)
uv run python run.py                   # schema apply + 6 async tasks + uvicorn
```

Expected log:

```
INFO velora: applying schema...
INFO velora.db: schema applied (15 statements)
INFO velora: initializing db pool...
INFO velora: spawning workers + uvicorn...
INFO uvicorn.error: Uvicorn running on http://0.0.0.0:8000
INFO velora.consumer.prices: kafka consumer ready
INFO velora.worker.vader: vader loop started (interval=30s)
INFO velora.worker.aggregator: aggregator loop started (interval=60s)
```

Leave running.

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

After ~2 min you should see rows starting to populate per coin.

```bash
docker exec -it velora-timescaledb psql -U velora -d velora -c \
  "SELECT coin, COUNT(*) FROM raw_tweets GROUP BY coin;"
```

After one full rotation (~12 min), all 6 coins should have ~30 tweets each.

### Grafana

Browser → http://localhost:3000 → Dashboards → Velora — Overview.

Anonymous Admin auth: no login needed.

### Expected fill timeline (cold start)

| Time after boot | What appears |
|----------------|--------------|
| ~60s | First candle |
| ~2 min | First tweets, first VADER scores |
| ~5–7 min | First BERT scores |
| ~12 min | All 6 coins covered (full rotation) |
| ~15 min | Dashboard "looks alive" |
| ~60 min | First hourly sentiment bucket |
| ~3 hours | Lag1/Lag2 correlations meaningful |
| ~24 hours | Full 24h candles, full /global stats |
| ~7 days | Full 7d sentiment + price-sentiment graphs |

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

---

## 5. Daily operations

### Start (after first setup)

```bash
docker compose -f docker-compose.consumer.yml up -d
docker compose -f docker-compose.producer.yml up -d
cd producer && uv run python run.py    # terminal 1
cd consumer && uv run python run.py    # terminal 2
```

### Stop

- Ctrl-C in both terminals (graceful shutdown)
- Optional: `docker compose -f docker-compose.consumer.yml stop` (preserves data)

### Restart Grafana only (after dashboard JSON change)

```bash
docker compose -f docker-compose.consumer.yml restart grafana
```

### Tail container logs

```bash
docker logs -f velora-kafka
docker logs -f velora-timescaledb
docker logs -f velora-grafana
```

---

## 6. Reset / start over

### Soft reset (keep data, restart processes)

```bash
docker compose -f docker-compose.consumer.yml restart
docker compose -f docker-compose.producer.yml restart
```

### Hard reset (wipe all data)

```bash
# Kill app processes (Ctrl-C in both terminals)

docker compose -f docker-compose.consumer.yml down -v
docker compose -f docker-compose.producer.yml down -v

# Optional: wipe Python venvs
rm -rf producer/.venv consumer/.venv

# Re-run section 2 from step C
```

`-v` deletes volumes (TimescaleDB rows, Kafka offsets, Grafana state). Drop `-v` to keep data across container recreation.

---

## 7. Common boot issues

| Symptom | Cause | Fix |
|---------|-------|-----|
| `kafka unhealthy` | Still booting (slow first start) | Wait 30s, retry. `docker logs velora-kafka` |
| `connection refused :5432` | TimescaleDB not ready | Wait 10s |
| `connection refused :9092` cross-machine | Firewall blocking | Open port 9092 inbound on producer machine |
| `ModuleNotFoundError: consumer` | Wrong CWD | Run `uv run python run.py` from inside `consumer/` dir |
| API returns `[]` for `/coins` | First minute, no data | Wait 60s for first kline close |
| `503 no data` from `/ticker` | First minute, no data | Wait 60s |
| Grafana panels empty | Check API is up: `curl localhost:8000/coins` | If empty, check consumer logs for errors |
| Producer crash on chromium launch | Playwright browser missing | `uv run playwright install chromium` in `producer/` |
| BERT loading hangs | First-run model download (~250MB) | Wait, or set `BERT_ENABLED=false` in `.env` |
| `.env not loading` | Env file in wrong dir | Must be at repo root (next to `docker-compose.*.yml`) |
| Tweet table empty for some coins | Partial rotation cycle | Wait 12 min for full cycle. If still empty, check `nitter_query` in `shared/coins.py` |

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
SELECT coin, COUNT(*) FROM raw_tweets GROUP BY coin;
SELECT coin, COUNT(*) FROM sentiment_scored WHERE compound_vader IS NOT NULL GROUP BY coin;
SELECT * FROM aggregates_summary;
SELECT view_name, materialization_hypertable_name FROM timescaledb_information.continuous_aggregates;
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

### Force schema reapply

```bash
cd consumer
uv run python -c "
import asyncio
from consumer.db import apply_schema
asyncio.run(apply_schema())
"
```

---

## 9. File structure

```
velora/
├── docker-compose.producer.yml     zookeeper + kafka
├── docker-compose.consumer.yml     timescaledb + grafana
├── .env / .env.example              shared per-machine config
├── producer/                        Binance WS + Nitter scraper → Kafka
├── consumer/                        Kafka → TimescaleDB + sentiment + FastAPI
├── shared/                          coins, kafka topics, pydantic schemas
├── monitoring/grafana/              dashboards + datasource provisioning
├── docs/api.md                      API response contract
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
