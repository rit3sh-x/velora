# Velora Producer

Ingests two streams and publishes both to Kafka. No DB writes here.

- **Binance** WebSocket → `velora.prices` (one closed 1m bar per coin per minute)
- **Nitter** Playwright scraper → `velora.tweets` (~30 tweets, one coin per cycle, round-robin every 120s)

## Install

```bash
cd producer
uv sync
uv run playwright install chromium
```

## Run

```bash
uv run python run.py
```

Both pipelines run concurrently under one asyncio event loop. Ctrl-C to stop.

## Modules

| File | Purpose |
| ---- | ------- |
| `kafka_client.py` | Singleton `KafkaProducer`, JSON value serializer, utf-8 key serializer, `publish(topic, key, event)`. |
| `binance_producer.py` | One combined-stream WS connection for all 6 coins; publishes `PriceEvent` on closed bars. |
| `playwright_scraper.py` | `scrape_coin(...)` async chromium scrape with pagination via `.show-more`; `tweet_id_from_url(...)`. |
| `tweet_producer.py` | Round-robin coin rotator; builds `TweetEvent`s and publishes them. |
| `run.py` | Entry point; `asyncio.gather(...)` of both runners. |

## Env vars

Read by `producer/config.py` from the repo-root `.env`:

| Var | Default | Notes |
| --- | ------- | ----- |
| `KAFKA_BROKER` | `localhost:9092` | Bootstrap broker. Cross-machine: producer host LAN IP + 9092. |
| `NITTER_URL` | `https://nitter.tiekoetter.com` | Single instance; no fallbacks. |
| `TWEET_POLL_INTERVAL_SECONDS` | `120` | Seconds between coin rotations. |
| `TWEETS_PER_SCRAPE` | `30` | Max tweets per cycle. |
| `PLAYWRIGHT_TIMEOUT_MS` | `30000` | Per-page navigation/wait timeout. |

## Cross-machine Kafka

The producer publishes to `KAFKA_BROKER`. When producer and consumer live on different LAN hosts:

1. On the producer host, set `HOST_LAN_IP` in `.env` to the producer's LAN IP. The bundled `docker-compose.producer.yml` uses this to set Kafka's `advertised.listeners` so external clients can connect.
2. On the consumer host, set `KAFKA_BROKER=<producer_lan_ip>:9092`.
3. On the producer host itself, `KAFKA_BROKER=localhost:9092` is fine — Kafka still advertises the LAN address to external clients.

## Failure behavior

- Binance WS: reconnect with exponential backoff (1s → 60s).
- Nitter scrape: log warning on any failure, skip the cycle, retry next interval. No fallback URLs.
- Empty Nitter results: returned as `[]`, not an exception.
