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

CREATE TABLE IF NOT EXISTS raw_tweets (
    tweet_id    TEXT NOT NULL,
    coin        TEXT NOT NULL,
    ts          TIMESTAMPTZ NOT NULL,
    scraped_at  TIMESTAMPTZ NOT NULL,
    text        TEXT NOT NULL,
    username    TEXT,
    url         TEXT,
    likes       INT DEFAULT 0,
    retweets    INT DEFAULT 0,
    replies     INT DEFAULT 0,
    PRIMARY KEY (tweet_id, ts)
);

SELECT create_hypertable('raw_tweets', 'ts', if_not_exists => TRUE);

SELECT remove_retention_policy('raw_tweets', if_exists => TRUE);
SELECT add_retention_policy('raw_tweets', INTERVAL '7 days', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_tweets_coin_ts ON raw_tweets (coin, ts DESC);

CREATE TABLE IF NOT EXISTS sentiment_scored (
    tweet_id            TEXT NOT NULL,
    coin                TEXT NOT NULL,
    ts                  TIMESTAMPTZ NOT NULL,
    compound_vader      DOUBLE PRECISION,
    label_vader         TEXT,
    compound_bert       DOUBLE PRECISION,
    label_bert          TEXT,
    scored_at_vader     TIMESTAMPTZ,
    scored_at_bert      TIMESTAMPTZ,
    PRIMARY KEY (tweet_id, ts)
);

SELECT create_hypertable('sentiment_scored', 'ts', if_not_exists => TRUE);

SELECT add_retention_policy('sentiment_scored', INTERVAL '7 days', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_sentiment_coin_ts ON sentiment_scored (coin, ts DESC);

-- Continuous aggregates for hourly sentiment + price rollups.
-- end_offset = 5min so the in-progress hour materializes within 5min of
-- a tick arriving. Refresh cadence is owned by the cagg_refresh worker
-- (consumer/workers/cagg_refresh.py, default 60s) plus the policy below
-- as a safety net (1min schedule).
CREATE MATERIALIZED VIEW IF NOT EXISTS sentiment_hourly
WITH (timescaledb.continuous) AS
SELECT
    coin,
    time_bucket('1 hour', ts) AS bucket,
    AVG(compound_vader) FILTER (WHERE compound_vader IS NOT NULL) AS avg_compound_vader,
    AVG(compound_bert)  FILTER (WHERE compound_bert  IS NOT NULL) AS avg_compound_bert,
    COUNT(*) FILTER (WHERE compound_vader IS NOT NULL) AS post_count,
    AVG(CASE WHEN compound_vader >  0.05 THEN 1.0 ELSE 0.0 END) FILTER (WHERE compound_vader IS NOT NULL) AS pos_ratio,
    AVG(CASE WHEN compound_vader < -0.05 THEN 1.0 ELSE 0.0 END) FILTER (WHERE compound_vader IS NOT NULL) AS neg_ratio
FROM sentiment_scored
GROUP BY coin, bucket
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
    coin                TEXT PRIMARY KEY,
    total_posts_24h     INT DEFAULT 0,
    avg_sentiment_24h   DOUBLE PRECISION DEFAULT 0,
    avg_sentiment_bert_24h DOUBLE PRECISION DEFAULT 0,
    positive_pct_24h    DOUBLE PRECISION DEFAULT 0,
    negative_pct_24h    DOUBLE PRECISION DEFAULT 0,
    last_price          DOUBLE PRECISION,
    change_24h_pct      DOUBLE PRECISION,
    change_1h_pct       DOUBLE PRECISION,
    volume_24h          DOUBLE PRECISION DEFAULT 0,
    volume_1h           DOUBLE PRECISION DEFAULT 0,
    lag1_corr           DOUBLE PRECISION,
    lag2_corr           DOUBLE PRECISION,
    lag1_corr_bert      DOUBLE PRECISION,
    lag2_corr_bert      DOUBLE PRECISION,
    lag1_points         INT DEFAULT 0,
    lag2_points         INT DEFAULT 0,
    computed_at         TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS coin_live_sentiment (
    coin           TEXT NOT NULL,
    bucket_ts      TIMESTAMPTZ NOT NULL,
    n_tweets       INT NOT NULL DEFAULT 0,
    avg_vader      DOUBLE PRECISION,
    avg_bert       DOUBLE PRECISION,
    oldest_age_sec INT,
    PRIMARY KEY (coin, bucket_ts)
);

SELECT create_hypertable('coin_live_sentiment', 'bucket_ts', if_not_exists => TRUE);
SELECT add_retention_policy('coin_live_sentiment', INTERVAL '7 days', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_live_sent_coin_ts ON coin_live_sentiment (coin, bucket_ts DESC);
