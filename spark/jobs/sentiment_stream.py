from __future__ import annotations

import logging
import os
import sys
from datetime import datetime, timezone

import pandas as pd

sys.path.insert(0, "/opt/spark")
sys.path.insert(0, "/opt/spark/jobs")

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from preprocess import clean_text, is_english

log = logging.getLogger("velora.spark.sentiment")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")


_TWEET_SCHEMA = StructType([
    StructField("coin",       StringType(),    False),
    StructField("tweet_id",   StringType(),    False),
    StructField("ts",         TimestampType(), False),
    StructField("scraped_at", TimestampType(), True),
    StructField("text",       StringType(),    True),
    StructField("source",     StringType(),    True),
    StructField("username",   StringType(),    True),
    StructField("likes",      DoubleType(),    True),
    StructField("retweets",   DoubleType(),    True),
    StructField("replies",    DoubleType(),    True),
    StructField("url",        StringType(),    True),
])


_VADER_RESULT_SCHEMA = StructType([
    StructField("compound", DoubleType(), True),
    StructField("label",    StringType(), True),
])


def _vader_label(c: float) -> str:
    if c >= 0.05:
        return "positive"
    if c <= -0.05:
        return "negative"
    return "neutral"


def _make_vader_udf():
    """VADER pandas-UDF — batched, lazy-init analyzer per executor.

    ~5x faster than row-UDF: avoids per-row Python crossing JVM-Python boundary.
    """
    @F.pandas_udf(_VADER_RESULT_SCHEMA)
    def vader_udf(text_series: pd.Series) -> pd.DataFrame:
        from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
        global _vader_analyzer
        try:
            _vader_analyzer
        except NameError:
            _vader_analyzer = SentimentIntensityAnalyzer()

        compounds: list[float | None] = []
        labels: list[str | None] = []
        for t in text_series.tolist():
            if not isinstance(t, str) or not t:
                compounds.append(None)
                labels.append(None)
                continue
            c = float(_vader_analyzer.polarity_scores(t)["compound"])
            compounds.append(c)
            labels.append(_vader_label(c))
        return pd.DataFrame({"compound": compounds, "label": labels})

    return vader_udf


_CLEAN_UDF = F.udf(clean_text, StringType())
_LANG_UDF = F.udf(is_english, BooleanType())


def _bert_pandas_udf_factory(model_name: str, batch_size: int):
    """Build a pandas-UDF for batched DistilBERT scoring.

    Lazy-loads the model PER EXECUTOR (not per batch). HF cache @ /opt/spark/hf-cache.
    Returns compound ∈ [-1, 1] (positive prob - negative prob mapped) + label.
    """
    @F.pandas_udf(_VADER_RESULT_SCHEMA)
    def bert_udf(text_series: pd.Series) -> pd.DataFrame:
        global _bert_tokenizer, _bert_model, _bert_device
        try:
            _bert_model
        except NameError:
            from transformers import AutoModelForSequenceClassification, AutoTokenizer
            import torch
            _bert_tokenizer = AutoTokenizer.from_pretrained(model_name)
            _bert_model = AutoModelForSequenceClassification.from_pretrained(model_name)
            _bert_device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
            _bert_model.to(_bert_device)
            _bert_model.eval()

        import torch

        compounds: list[float | None] = []
        labels: list[str | None] = []

        texts = [t if isinstance(t, str) and t else "" for t in text_series.tolist()]
        for i in range(0, len(texts), batch_size):
            chunk = texts[i:i + batch_size]
            enc = _bert_tokenizer(chunk, padding=True, truncation=True,
                                   max_length=128, return_tensors="pt")
            enc = {k: v.to(_bert_device) for k, v in enc.items()}
            with torch.no_grad():
                logits = _bert_model(**enc).logits
                probs = torch.softmax(logits, dim=1).cpu().numpy()

            for j, t in enumerate(chunk):
                if not t:
                    compounds.append(None)
                    labels.append(None)
                    continue
                p_neg, p_pos = float(probs[j][0]), float(probs[j][1])
                c = p_pos - p_neg
                label = "positive" if c > 0.05 else ("negative" if c < -0.05 else "neutral")
                compounds.append(c)
                labels.append(label)

        return pd.DataFrame({"compound": compounds, "label": labels})

    return bert_udf


def build_session(app_name: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "6")
        .getOrCreate()
    )


def _read_tweets_stream(spark: SparkSession, kafka_bootstrap: str):
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("subscribe", "velora.tweets")
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )
    parsed = (
        raw.select(F.from_json(F.col("value").cast("string"), _TWEET_SCHEMA).alias("d"))
        .select("d.*")
        .where(F.col("text").isNotNull())
        .withColumn("cleaned_text", _CLEAN_UDF(F.col("text")))
        .where(F.length(F.col("cleaned_text")) > 0)
        .where(_LANG_UDF(F.col("cleaned_text")))
    )
    return parsed


def _to_kafka_payload(df, score_col: str, score_kind: str):
    """Project per-V46 sentiment event payload into Kafka value JSON.

    score_kind ∈ {vader, bert}: emits compound_<kind> + label_<kind> + scored_at_<kind>.
    Other compound/label fields = NULL so consumer UPSERTs partial.
    """
    now_col = F.lit(datetime.now(timezone.utc).isoformat()).cast(StringType())
    if score_kind == "vader":
        struct = F.struct(
            F.col("tweet_id").alias("tweet_id"),
            F.col("coin").alias("coin"),
            F.date_format(F.col("ts"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX").alias("ts"),
            F.col("source").alias("source"),
            F.col(f"{score_col}.compound").alias("compound_vader"),
            F.col(f"{score_col}.label").alias("label_vader"),
            F.lit(None).cast(DoubleType()).alias("compound_bert"),
            F.lit(None).cast(StringType()).alias("label_bert"),
            F.date_format(F.current_timestamp(), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX").alias("scored_at_vader"),
            F.lit(None).cast(StringType()).alias("scored_at_bert"),
            F.col("cleaned_text").alias("cleaned_text"),
        )
    else:
        struct = F.struct(
            F.col("tweet_id").alias("tweet_id"),
            F.col("coin").alias("coin"),
            F.date_format(F.col("ts"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX").alias("ts"),
            F.col("source").alias("source"),
            F.lit(None).cast(DoubleType()).alias("compound_vader"),
            F.lit(None).cast(StringType()).alias("label_vader"),
            F.col(f"{score_col}.compound").alias("compound_bert"),
            F.col(f"{score_col}.label").alias("label_bert"),
            F.lit(None).cast(StringType()).alias("scored_at_vader"),
            F.date_format(F.current_timestamp(), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX").alias("scored_at_bert"),
            F.col("cleaned_text").alias("cleaned_text"),
        )
    return df.select(
        F.col("coin").cast(StringType()).alias("key"),
        F.to_json(struct).alias("value"),
    )


def main() -> None:
    kafka_bootstrap = os.environ.get("SPARK_KAFKA_BOOTSTRAP", "kafka:9092")
    checkpoint_root = os.environ.get("SPARK_CHECKPOINT_DIR", "/opt/spark/checkpoints")
    vader_trigger = os.environ.get("SPARK_VADER_TRIGGER_SECONDS", "30")
    bert_trigger = os.environ.get("SPARK_BERT_TRIGGER_SECONDS", "300")
    bert_model = os.environ.get("BERT_MODEL", "distilbert-base-uncased-finetuned-sst-2-english")
    bert_batch = int(os.environ.get("BERT_BATCH_SIZE", "64"))
    bert_enabled = os.environ.get("BERT_ENABLED", "true").lower() == "true"

    log.info("starting velora sentiment stream | kafka=%s checkpoint=%s",
             kafka_bootstrap, checkpoint_root)

    spark = build_session("velora.sentiment.stream")
    spark.sparkContext.setLogLevel("WARN")

    parsed = _read_tweets_stream(spark, kafka_bootstrap)

    vader_udf = _make_vader_udf()
    q1_in = parsed.withColumn("vader", vader_udf(F.col("cleaned_text")))
    q1_out = _to_kafka_payload(q1_in, "vader", "vader")

    q1 = (
        q1_out.writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("topic", "velora.sentiment")
        .option("checkpointLocation", f"{checkpoint_root}/vader")
        .outputMode("append")
        .trigger(processingTime=f"{vader_trigger} seconds")
        .start()
    )
    log.info("Q1 VADER stream started: trigger=%ss", vader_trigger)

    if bert_enabled:
        bert_udf = _bert_pandas_udf_factory(bert_model, bert_batch)
        q2_in = parsed.withColumn("bert", bert_udf(F.col("cleaned_text")))
        q2_out = _to_kafka_payload(q2_in, "bert", "bert")

        q2 = (
            q2_out.writeStream
            .format("kafka")
            .option("kafka.bootstrap.servers", kafka_bootstrap)
            .option("topic", "velora.sentiment")
            .option("checkpointLocation", f"{checkpoint_root}/bert")
            .outputMode("append")
            .trigger(processingTime=f"{bert_trigger} seconds")
            .start()
        )
        log.info("Q2 BERT stream started: model=%s batch=%d trigger=%ss",
                 bert_model, bert_batch, bert_trigger)
    else:
        log.warning("BERT_ENABLED=false: skipping Q2")

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
