# Spark — Structured Streaming sentiment

Velora's silver-layer sentiment compute. Reads `velora.tweets` from Kafka,
applies preprocessing + VADER + DistilBERT, emits `velora.sentiment`. The
Python consumer ingests `velora.sentiment` → Timescale `sentiment_scored`.

Architecture:

```
producer (twitter + bluesky) ─┐
                              ├─► Kafka velora.tweets ─► Spark sentiment_stream ─► Kafka velora.sentiment ─► consumer ─► Timescale silver
seed (twitter + bluesky)     ─┘                              │
                                                             └─► HF cache vol (BERT model)
```

## Boot order

1. Producer-side stack up (Kafka): `docker compose -f docker-compose.producer.yml up -d`
2. Consumer-side stack up (Timescale + Mongo + Grafana): `docker compose -f docker-compose.consumer.yml up -d`
3. Spark up: `docker compose -f docker-compose.spark.yml up -d --build`
   - First build downloads Kafka connector JARs + bakes Python deps. ~5 min cold.
   - First run downloads BERT model into `spark-hf-cache` volume. ~250 MB.
4. Consumer python process: `cd consumer && uv run python run.py`
5. Producer python process: `cd producer && uv run python run.py`

## Submit job

`spark-submit` runs as a service inside compose (`spark-submit` container).
Restarts on failure. To force restart:

```
docker compose -f docker-compose.spark.yml restart spark-submit
```

To watch driver logs:

```
docker compose -f docker-compose.spark.yml logs -f spark-submit
```

## Spark UIs

- Master:  http://localhost:8080
- Driver:  http://localhost:4040  (only while job running)

## Checkpoint management

Two streaming queries (V54), each with its own checkpoint subdir:

- `/opt/spark/checkpoints/vader` — VADER query state
- `/opt/spark/checkpoints/bert`  — BERT query state

Backed by Docker volume `spark-checkpoints`. To reset (drops Kafka offsets,
re-scores from `latest` per startingOffsets):

```
docker compose -f docker-compose.spark.yml down
docker volume rm velora-spark_spark-checkpoints
docker compose -f docker-compose.spark.yml up -d
```

## BERT model cache

HuggingFace cache at `/opt/spark/hf-cache` (volume `spark-hf-cache`). Persistent
across restarts. Downloaded on first run; subsequent restarts skip download.

To force re-download (e.g. model upgrade):

```
docker volume rm velora-spark_spark-hf-cache
```

## Disable BERT (debug)

Set `BERT_ENABLED=false` in `.env`. Q1 (VADER) keeps running. Q2 (BERT) skipped.
Useful when iterating on preprocessing.

## Troubleshooting

| symptom | likely cause | fix |
|---|---|---|
| `Failed to find Kafka data source` | Connector JAR missing | rebuild image; check `/opt/spark/jars/` has `spark-sql-kafka-0-10*.jar` |
| `pull access denied for bitnami/spark` | Bitnami removed free tag (Aug 28 2025) | image now FROM `apache/spark:3.5.4`; `docker compose -f docker-compose.spark.yml build --no-cache` |
| BERT cold-start hang ~3 min | First-time model download | watch `spark-submit` logs; subsequent runs cached |
| `host.docker.internal` not resolving on Linux | bridge network | already mapped via `extra_hosts: host-gateway` |
| Q2 OOM | BERT batch too big | lower `BERT_BATCH_SIZE` in `.env` |
| Multiple checkpoint conflicts after rebase | Stale state | rm `spark-checkpoints` volume |
