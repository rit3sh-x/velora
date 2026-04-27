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

DROP MATERIALIZED VIEW IF EXISTS sentiment_hourly CASCADE;
DROP MATERIALIZED VIEW IF EXISTS prices_hourly CASCADE;

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
    matched_hours       INT DEFAULT 0,
    lag1_points         INT DEFAULT 0,
    lag2_points         INT DEFAULT 0,
    computed_at         TIMESTAMPTZ
);
