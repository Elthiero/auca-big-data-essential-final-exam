"""
streaming-job — Prediction Engine: PySpark Structured Streaming, local[*].

Two independent sinks read from the same Kafka topic:

  1. Raw events -> HDFS Parquet (data lake archive), micro-batched on a
     trigger interval to avoid the small-file problem.
  2. Per-session running feature aggregates -> scored by the current
     Champion model -> written to MySQL `predictions`.

Why aggregation works the same way as training:
  Structured Streaming's groupBy(...).agg(...) on a streaming DataFrame
  maintains incremental state per key (user_session) and emits an
  updated row every time a new event for that key arrives — this is
  the SAME aggregation logic as train.py's build_session_features,
  just incremental instead of computed over a finished session. Using
  identical feature expressions here is what makes the trained
  PipelineModel valid to apply to these rows.

Why foreachBatch:
  MLlib's PipelineModel.transform() only works on a static (batch)
  DataFrame. foreachBatch hands us exactly that for each micro-batch,
  which is also where we do the MySQL write and the Champion hot-reload
  check.

Session boundary heuristic:
  A watermark of 30 minutes on event_time bounds state size and defines
  when a session is considered "over" for state-cleanup purposes — a
  session with no new events for 30 minutes gets its state evicted.
"""

import os
import time

import pymysql
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
from pyspark.ml import PipelineModel
from pyspark.ml.functions import vector_to_array

# Config from environment (see docker-compose.yml)
HDFS_URI = os.environ["HDFS_URI"]
HDFS_STREAM_SINK_PATH = os.environ["HDFS_STREAM_SINK_PATH"]  # /data/streaming/2019-11
HDFS_CHECKPOINT_PATH = os.environ["HDFS_CHECKPOINT_PATH"]  # /checkpoints/streaming-job

KAFKA_BOOTSTRAP = os.environ["KAFKA_BOOTSTRAP"]
KAFKA_TOPIC = os.environ["KAFKA_TOPIC"]

MYSQL_HOST = os.environ["MYSQL_HOST"]
MYSQL_DB = os.environ["MYSQL_DB"]
MYSQL_USER = os.environ["MYSQL_USER"]
MYSQL_PASSWORD = os.environ["MYSQL_PASSWORD"]

MODEL_POLL_INTERVAL_SECONDS = int(os.environ.get("MODEL_POLL_INTERVAL_SECONDS", "30"))
PARQUET_TRIGGER_INTERVAL_SECONDS = int(
    os.environ.get("PARQUET_TRIGGER_INTERVAL_SECONDS", "60")
)
SCORING_TRIGGER_INTERVAL_SECONDS = int(
    os.environ.get("SCORING_TRIGGER_INTERVAL_SECONDS", "30")
)

# Must match the Spark version in requirements.txt (pyspark==3.5.1) —
# a mismatched Scala/Spark suffix here is a very common source of
# "class not found" errors at runtime.
KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"

# Same feature list as train.py — the loaded PipelineModel's
# VectorAssembler stage was fit with these exact column names.
FEATURE_COLS = [
    "view_count",
    "cart_count",
    "remove_count",
    "distinct_products",
    "distinct_categories",
    "distinct_brands",
    "avg_price",
    "max_price",
    "session_duration_sec",
    "has_cart",
]

EVENT_SCHEMA = StructType(
    [
        StructField("event_time", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("product_id", LongType(), True),
        StructField("category_id", LongType(), True),
        StructField("category_code", StringType(), True),
        StructField("brand", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("user_id", LongType(), True),
        StructField("user_session", StringType(), True),
    ]
)


# Champion model hot-reload state (module-level; foreachBatch calls
# back into this from the driver for every micro-batch)
_model_state = {
    "model": None,
    "version": None,
    "hdfs_path": None,
    "last_checked": 0.0,
}


def get_mysql_connection():
    return pymysql.connect(
        host=MYSQL_HOST,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DB,
        autocommit=True,
    )


def get_active_champion():
    conn = get_mysql_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT hdfs_path, version FROM model_registry "
                "WHERE is_active = TRUE ORDER BY created_at DESC LIMIT 1"
            )
            return cur.fetchone()
    finally:
        conn.close()


def refresh_champion_if_needed(spark):
    """
    Polls MySQL at most once per MODEL_POLL_INTERVAL_SECONDS. Only
    reloads the model from HDFS if the active hdfs_path actually
    changed — this is how a Champion promotion in training-job
    propagates to live scoring without restarting this container.
    """
    now = time.time()
    if now - _model_state["last_checked"] < MODEL_POLL_INTERVAL_SECONDS:
        return
    _model_state["last_checked"] = now

    champion = get_active_champion()
    if champion is None:
        if _model_state["model"] is None:
            print(
                "[consumer.py] No active Champion in registry yet — skipping scoring."
            )
        return

    hdfs_path, version = champion
    if hdfs_path == _model_state["hdfs_path"]:
        return  # unchanged, nothing to reload

    print(f"[consumer.py] Loading Champion model version={version} from {hdfs_path}")
    _model_state["model"] = PipelineModel.load(hdfs_path)
    _model_state["hdfs_path"] = hdfs_path
    _model_state["version"] = version
    print(f"[consumer.py] Champion model version={version} active.")


def write_predictions_partition(rows):
    rows = list(rows)
    if not rows:
        return

    batch_total = len(rows)
    batch_purchases = sum(1 for r in rows if bool(r.predicted_label))

    conn = pymysql.connect(
        host=MYSQL_HOST,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DB,
        autocommit=False,
    )
    try:
        with conn.cursor() as cur:
            # Chunk writes into batches of 5,000 to keep transaction logs light
            chunk_size = 5000
            for i in range(0, len(rows), chunk_size):
                chunk = rows[i : i + chunk_size]
                cur.executemany(
                    "INSERT INTO predictions "
                    "(user_session, event_time, purchase_probability, predicted_label, model_version) "
                    "VALUES (%s, %s, %s, %s, %s)",
                    [
                        (
                            r.user_session,
                            r.event_time,
                            float(r.purchase_probability),
                            bool(r.predicted_label),
                            r.model_version,
                        )
                        for r in chunk
                    ],
                )

            # Update running metrics atomically
            cur.execute(
                "UPDATE running_metrics "
                "SET metric_value = metric_value + %s "
                "WHERE metric_key = 'total_predictions'",
                (batch_total,),
            )
            cur.execute(
                "UPDATE running_metrics "
                "SET metric_value = metric_value + %s "
                "WHERE metric_key = 'purchase_predicted'",
                (batch_purchases,),
            )
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"[consumer.py] Failed transaction write to database: {e}")
        raise e
    finally:
        conn.close()

def build_session_aggregates(parsed_df):
    """
    Same aggregation logic as train.py's build_session_features —
    now using session_window to ensure state eviction works correctly.
    """
    return (
        parsed_df.withWatermark("event_time", "30 minutes")
        # Grouping by both user_session AND a session window on event_time
        # enables true state eviction after 30 minutes of inactivity.
        .groupBy(
            "user_session",
            F.session_window("event_time", "30 minutes").alias("session"),
        )
        .agg(
            F.min("event_time").alias("session_start"),
            F.max("event_time").alias("session_end"),
            F.sum(F.when(F.col("event_type") == "view", 1).otherwise(0)).alias(
                "view_count"
            ),
            F.sum(F.when(F.col("event_type") == "cart", 1).otherwise(0)).alias(
                "cart_count"
            ),
            F.sum(
                F.when(F.col("event_type") == "remove_from_cart", 1).otherwise(0)
            ).alias("remove_count"),
            F.approx_count_distinct("product_id").alias("distinct_products"),
            F.approx_count_distinct("category_id").alias("distinct_categories"),
            F.approx_count_distinct("brand").alias("distinct_brands"),
            F.avg("price").alias("avg_price"),
            F.max("price").alias("max_price"),
        )
        .withColumn(
            "session_duration_sec",
            F.col("session_end").cast("long") - F.col("session_start").cast("long"),
        )
        .withColumn("has_cart", (F.col("cart_count") > 0).cast("int"))
        .na.fill({"avg_price": 0.0, "max_price": 0.0, "distinct_brands": 0})
    )


def score_and_write_batch(batch_df, batch_id):
    spark = batch_df.sparkSession
    refresh_champion_if_needed(spark)

    model = _model_state["model"]
    if model is None:
        print(f"[consumer.py] Batch {batch_id}: no Champion loaded yet, skipping.")
        return

    if batch_df.rdd.isEmpty():
        return

    predictions = model.transform(batch_df)

    result_df = predictions.withColumn(
        "purchase_probability", vector_to_array(F.col("probability"))[1]
    ).select(
        F.col("user_session"),
        F.col("session_end").alias("event_time"),
        F.col("purchase_probability"),
        F.col("prediction").cast("boolean").alias("predicted_label"),
        F.lit(_model_state["version"]).alias("model_version"),
    )

    count = result_df.count()
    result_df.foreachPartition(write_predictions_partition)
    print(
        f"[consumer.py] Batch {batch_id}: scored and wrote {count} session updates "
        f"(model version={_model_state['version']})."
    )


def get_spark():
    return (
        SparkSession.builder.appName("streaming-job")
        .master("local[*]")
        .config("spark.hadoop.fs.defaultFS", HDFS_URI)
        .config("spark.jars.packages", KAFKA_PACKAGE)
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )


def main():
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    log4j = spark.sparkContext._jvm.org.apache.log4j
    log4j.LogManager.getLogger(
        "org.apache.spark.sql.catalyst.expressions.RowBasedKeyValueBatch"
    ).setLevel(log4j.Level.ERROR)

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        # Rate-limit: Only pull a maximum of 30,000 raw events per trigger cycle.
        # This keeps your streaming batches small, quick, and stable.
        .option("maxOffsetsPerTrigger", 30000)
        .load()
    )

    parsed = (
        raw.selectExpr("CAST(value AS STRING) AS json_str")
        .select(F.from_json(F.col("json_str"), EVENT_SCHEMA).alias("event"))
        .select("event.*")
        .withColumn(
            "event_time", F.to_timestamp("event_time", "yyyy-MM-dd HH:mm:ss 'UTC'")
        )
        .filter(F.col("event_time").isNotNull() & F.col("user_session").isNotNull())
    )

    # --- Sink 1: raw events archived to HDFS as Parquet -----------------
    raw_for_archive = parsed.withColumn("event_date", F.to_date("event_time"))

    archive_query = (
        raw_for_archive.writeStream.format("parquet")
        .option("path", HDFS_STREAM_SINK_PATH)
        .option("checkpointLocation", f"{HDFS_CHECKPOINT_PATH}/archive")
        .partitionBy("event_type", "event_date")
        .trigger(processingTime=f"{PARQUET_TRIGGER_INTERVAL_SECONDS} seconds")
        .outputMode("append")
        .start()
    )

    # --- Sink 2: incremental per-session features -> score -> MySQL -----
    session_aggregates = build_session_aggregates(parsed)

    scoring_query = (
        session_aggregates.writeStream.foreachBatch(score_and_write_batch)
        .option("checkpointLocation", f"{HDFS_CHECKPOINT_PATH}/scoring")
        .trigger(processingTime=f"{SCORING_TRIGGER_INTERVAL_SECONDS} seconds")
        .outputMode("append")
        .start()
    )

    print("[consumer.py] Both streaming queries started. Awaiting data...")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
