CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS prices_1m (
    coin       TEXT NOT NULL,
    ts         TIMESTAMPTZ NOT NULL,
    open       DOUBLE PRECISION,
    high       DOUBLE PRECISION,
    low        DOUBLE PRECISION,
    close      DOUBLE PRECISION,
    volume     DOUBLE PRECISION,
    PRIMARY KEY (coin, ts)
);

SELECT create_hypertable('prices_1m', 'ts', if_not_exists => TRUE);
SELECT add_retention_policy('prices_1m', INTERVAL '30 days', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_prices_coin_ts ON prices_1m (coin, ts DESC);

CREATE TABLE IF NOT EXISTS sentiment_scored (
    tweet_id            TEXT NOT NULL,
    coin                TEXT NOT NULL,
    ts                  TIMESTAMPTZ NOT NULL,
    source              TEXT NOT NULL DEFAULT 'twitter',
    compound_vader      DOUBLE PRECISION,
    label_vader         TEXT,
    compound_bert       DOUBLE PRECISION,
    label_bert          TEXT,
    scored_at_vader     TIMESTAMPTZ,
    scored_at_bert      TIMESTAMPTZ,
    PRIMARY KEY (tweet_id, ts, source)
);

SELECT create_hypertable('sentiment_scored', 'ts', if_not_exists => TRUE);
SELECT add_retention_policy('sentiment_scored', INTERVAL '7 days', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_sentiment_coin_ts ON sentiment_scored (coin, ts DESC);
CREATE INDEX IF NOT EXISTS idx_sentiment_coin_source_ts ON sentiment_scored (coin, source, ts DESC);

CREATE MATERIALIZED VIEW IF NOT EXISTS sentiment_hourly
WITH (timescaledb.continuous) AS
SELECT
    coin,
    source,
    time_bucket('1 hour', ts) AS bucket,
    AVG(compound_vader) FILTER (WHERE compound_vader IS NOT NULL) AS avg_compound_vader,
    AVG(compound_bert)  FILTER (WHERE compound_bert  IS NOT NULL) AS avg_compound_bert,
    COUNT(*) FILTER (WHERE compound_vader IS NOT NULL) AS post_count,
    AVG(CASE WHEN compound_vader >  0.05 THEN 1.0 ELSE 0.0 END) FILTER (WHERE compound_vader IS NOT NULL) AS pos_ratio,
    AVG(CASE WHEN compound_vader < -0.05 THEN 1.0 ELSE 0.0 END) FILTER (WHERE compound_vader IS NOT NULL) AS neg_ratio
FROM sentiment_scored
GROUP BY coin, source, bucket
WITH NO DATA;

SELECT add_continuous_aggregate_policy('sentiment_hourly',
    start_offset      => INTERVAL '7 days',
    end_offset        => INTERVAL '5 minutes',
    schedule_interval => INTERVAL '1 minute',
    if_not_exists     => TRUE);

CREATE MATERIALIZED VIEW IF NOT EXISTS prices_hourly
WITH (timescaledb.continuous) AS
SELECT
    coin,
    time_bucket('1 hour', ts) AS bucket,
    first(open, ts)   AS open,
    max(high)         AS high,
    min(low)          AS low,
    last(close, ts)   AS close,
    sum(volume)       AS volume
FROM prices_1m
GROUP BY coin, bucket
WITH NO DATA;

SELECT add_continuous_aggregate_policy('prices_hourly',
    start_offset      => INTERVAL '30 days',
    end_offset        => INTERVAL '5 minutes',
    schedule_interval => INTERVAL '1 minute',
    if_not_exists     => TRUE);

CREATE TABLE IF NOT EXISTS aggregates_summary (
    coin                   TEXT PRIMARY KEY,
    total_posts_24h        INT DEFAULT 0,
    avg_sentiment_24h      DOUBLE PRECISION DEFAULT 0,
    avg_sentiment_bert_24h DOUBLE PRECISION DEFAULT 0,
    positive_pct_24h       DOUBLE PRECISION DEFAULT 0,
    negative_pct_24h       DOUBLE PRECISION DEFAULT 0,
    last_price             DOUBLE PRECISION,
    change_24h_pct         DOUBLE PRECISION,
    change_1h_pct          DOUBLE PRECISION,
    volume_24h             DOUBLE PRECISION DEFAULT 0,
    volume_1h              DOUBLE PRECISION DEFAULT 0,
    lag1_corr              DOUBLE PRECISION,
    lag2_corr              DOUBLE PRECISION,
    lag1_corr_bert         DOUBLE PRECISION,
    lag2_corr_bert         DOUBLE PRECISION,
    lag1_points            INT DEFAULT 0,
    lag2_points            INT DEFAULT 0,
    computed_at            TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS aggregates_by_source (
    coin                   TEXT NOT NULL,
    source                 TEXT NOT NULL,
    total_posts_24h        INT DEFAULT 0,
    avg_sentiment_24h      DOUBLE PRECISION DEFAULT 0,
    avg_sentiment_bert_24h DOUBLE PRECISION DEFAULT 0,
    positive_pct_24h       DOUBLE PRECISION DEFAULT 0,
    negative_pct_24h       DOUBLE PRECISION DEFAULT 0,
    lag1_corr              DOUBLE PRECISION,
    lag2_corr              DOUBLE PRECISION,
    lag1_corr_bert         DOUBLE PRECISION,
    lag2_corr_bert         DOUBLE PRECISION,
    lag1_points            INT DEFAULT 0,
    lag2_points            INT DEFAULT 0,
    computed_at            TIMESTAMPTZ,
    PRIMARY KEY (coin, source)
);

CREATE TABLE IF NOT EXISTS coin_live_sentiment (
    coin           TEXT NOT NULL,
    source         TEXT NOT NULL DEFAULT 'twitter',
    bucket_ts      TIMESTAMPTZ NOT NULL,
    n_tweets       INT NOT NULL DEFAULT 0,
    avg_vader      DOUBLE PRECISION,
    avg_bert       DOUBLE PRECISION,
    oldest_age_sec INT,
    PRIMARY KEY (coin, source, bucket_ts)
);

SELECT create_hypertable('coin_live_sentiment', 'bucket_ts', if_not_exists => TRUE);
SELECT add_retention_policy('coin_live_sentiment', INTERVAL '7 days', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_live_sent_coin_source_ts
    ON coin_live_sentiment (coin, source, bucket_ts DESC);

CREATE TABLE IF NOT EXISTS predictions (
    coin                 TEXT NOT NULL,
    ts                   TIMESTAMPTZ NOT NULL,
    horizon_minutes      INT NOT NULL,
    predicted_return_pct DOUBLE PRECISION,
    confidence           DOUBLE PRECISION,
    model_version        TEXT,
    PRIMARY KEY (coin, ts, horizon_minutes)
);

SELECT create_hypertable('predictions', 'ts', if_not_exists => TRUE);
SELECT add_retention_policy('predictions', INTERVAL '7 days', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_predictions_coin_horizon_ts
    ON predictions (coin, horizon_minutes, ts DESC);

CREATE TABLE IF NOT EXISTS recommendations (
    coin     TEXT NOT NULL,
    ts       TIMESTAMPTZ NOT NULL,
    signal   TEXT NOT NULL,
    score    DOUBLE PRECISION,
    reasons  JSONB,
    PRIMARY KEY (coin, ts)
);

SELECT create_hypertable('recommendations', 'ts', if_not_exists => TRUE);
SELECT add_retention_policy('recommendations', INTERVAL '7 days', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_recommendations_coin_ts
    ON recommendations (coin, ts DESC);
