# Velora Consumer

FastAPI app + supervisor that:

1. Applies the TimescaleDB schema (idempotent).
2. Initializes an asyncpg pool.
3. Spawns 5 long-running async tasks: `prices`, `tweets` (consumers) and
   `vader`, `bert`, `aggregator` (workers).
4. Hosts uvicorn for the FastAPI app exposing the 13 endpoints in
   `docs/api.md`.

There is no preseed: the app starts cold and fills in as workers run.

## Prerequisites

- Python 3.11+
- [uv](https://docs.astral.sh/uv/) for dependency management.
- A running TimescaleDB instance reachable on `localhost:5432` (or override
  via env vars below). `docker-compose.consumer.yml` at the repo root spins
  one up.
- A running Kafka broker on `localhost:9092` for the `prices`/`tweets`
  consumers to read from.

## Install

```bash
cd consumer
uv sync
```

## Run

```bash
cd consumer
uv run python run.py
```

The supervisor blocks until `Ctrl+C`. The API will be available at
`http://localhost:8000` (see `docs/api.md` for the contract).

## Environment variables

All read by `consumer/config.py` via `pydantic-settings`. Override by
exporting them or by creating `.env` at the repo root.

| Variable                          | Default                                          | Notes                          |
| --------------------------------- | ------------------------------------------------ | ------------------------------ |
| `KAFKA_BROKER`                    | `localhost:9092`                                 | Kafka bootstrap server         |
| `POSTGRES_HOST`                   | `localhost`                                      |                                |
| `POSTGRES_PORT`                   | `5432`                                           |                                |
| `POSTGRES_DB`                     | `velora`                                         |                                |
| `POSTGRES_USER`                   | `velora`                                         |                                |
| `POSTGRES_PASSWORD`               | `velora`                                         |                                |
| `VADER_INTERVAL_SECONDS`          | `30`                                             | VADER worker tick              |
| `BERT_INTERVAL_SECONDS`           | `300`                                            | BERT worker tick               |
| `AGGREGATOR_INTERVAL_SECONDS`     | `60`                                             | Aggregator worker tick         |
| `BERT_MODEL`                      | `distilbert-base-uncased-finetuned-sst-2-english`| Hugging Face model id          |
| `BERT_BATCH_SIZE`                 | `64`                                             |                                |
| `BERT_ENABLED`                    | `true`                                           | Set to `false` to skip BERT    |
| `API_HOST`                        | `0.0.0.0`                                        |                                |
| `API_PORT`                        | `8000`                                           |                                |

## Layout

```
consumer/
├── api/
│   ├── deps.py        # FastAPI dependency providers
│   ├── main.py        # FastAPI app + CORS + /health
│   └── routers.py     # all 13 endpoints from docs/api.md
├── consumers/         # Kafka -> Timescale ingestion (prices, tweets)
├── workers/           # background workers (vader, bert, aggregator)
├── config.py          # pydantic-settings
├── db.py              # asyncpg pool + schema apply
├── schema.sql         # TimescaleDB schema (idempotent)
└── run.py             # supervisor entrypoint
```
