"""
training-job — Batch MLOps: Champion/Challenger training pipeline.
Reads raw eCommerce clickstream CSV from HDFS, derives session-level
"will this session end in a purchase" labels, engineers features,
trains a PySpark MLlib pipeline, evaluates it against the current
Champion (re-scored on the same held-out test set for a fair
comparison), saves the model to HDFS, and writes metadata-only rows
(no model blobs) to MySQL's model_registry table.
"""
import os
import sys
import time
import datetime
import pymysql

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import VectorAssembler

from pyspark.ml.classification import (
    LogisticRegression,
    RandomForestClassifier,
    GBTClassifier,
)

from pyspark.ml.evaluation import (
    BinaryClassificationEvaluator,
    MulticlassClassificationEvaluator,
)
from pyspark.ml.functions import vector_to_array

# Config from environment (see docker-compose.yml)
HDFS_URI = os.environ["HDFS_URI"]  # hdfs://namenode:9000
HDFS_TRAIN_SOURCE_PATH = os.environ["HDFS_TRAIN_SOURCE_PATH"]  # /data/raw/2019-10
HDFS_STREAM_SINK_PATH = os.environ.get("HDFS_STREAM_SINK_PATH", "/data/streaming/2019-11")
HDFS_MODEL_BASE_PATH = os.environ["HDFS_MODEL_BASE_PATH"]  # /models

MYSQL_HOST = os.environ["MYSQL_HOST"]
MYSQL_DB = os.environ["MYSQL_DB"]
MYSQL_USER = os.environ["MYSQL_USER"]
MYSQL_PASSWORD = os.environ["MYSQL_PASSWORD"]

TRAINING_INTERVAL_SECONDS = int(os.environ.get("TRAINING_INTERVAL_SECONDS", "300"))
CHAMPION_IMPROVEMENT_THRESHOLD = float(os.environ.get("CHAMPION_IMPROVEMENT_THRESHOLD", "0.02"))

# Set RUN_ONCE=true for the first manual training run (no infinite loop)
RUN_ONCE = os.environ.get("RUN_ONCE", "false").lower() == "true"
RAW_CSV_FILENAME = os.environ.get("RAW_CSV_FILENAME", "2019-Oct.csv")

# Which MLlib classifier to train. Set in .env / docker-compose rather than
# by editing this file, so the model actually in use is always visible in
# configuration (and in the run logs) instead of buried in a code comment.
MODEL_TYPE = os.environ.get("MODEL_TYPE", "logistic_regression").strip().lower()


def get_spark():
    return (
        SparkSession.builder.appName("training-job")
        .master("local[*]")
        .config("spark.hadoop.fs.defaultFS", HDFS_URI)
        .config("spark.sql.shuffle.partitions", "32")
        .config("spark.driver.memory", "3g")  # Reduced from 8g to fit in 4.5g container limit
        .config("spark.driver.maxResultSize", "512m")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.parquet.compression.codec", "zstd")
        .config("spark.hadoop.fs.hdfs.impl.disable.cache", "true")
        .getOrCreate()
    )


def hdfs_path_exists(spark, path):
    sc = spark.sparkContext
    hadoop_conf = sc._jsc.hadoopConfiguration()
    jvm_path = sc._jvm.org.apache.hadoop.fs.Path(path)
    fs = jvm_path.getFileSystem(hadoop_conf)
    return fs.exists(jvm_path)


def load_or_create_parquet(spark, raw_dir, csv_filename):
    parquet_path = raw_dir.replace("/data/raw/", "/data/processed/") + "_parquet"
    if hdfs_path_exists(spark, parquet_path):
        print(f"[train.py] Reusing existing Parquet at {parquet_path}")
        return spark.read.parquet(parquet_path)

    csv_path = f"{raw_dir}/{csv_filename}"
    print(f"[train.py] Converting {csv_path} -> {parquet_path}")

    from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

    schema = StructType([
        StructField("event_time", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("product_id", LongType(), True),
        StructField("category_id", LongType(), True),
        StructField("category_code", StringType(), True),
        StructField("brand", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("user_id", LongType(), True),
        StructField("user_session", StringType(), True),
    ])

    df = (
        spark.read.option("header", "true")
        .schema(schema)
        .csv(csv_path)
        .withColumn("event_time", F.to_timestamp("event_time", "yyyy-MM-dd HH:mm:ss 'UTC'"))
    )

    (
        df.coalesce(8)
        .write.mode("overwrite")
        .partitionBy("event_type")
        .parquet(parquet_path)
    )
    return spark.read.parquet(parquet_path)


def load_consolidated_datasets(spark):
    baseline_df = load_or_create_parquet(spark, HDFS_TRAIN_SOURCE_PATH, RAW_CSV_FILENAME)

    core_columns = [
        "event_time", "event_type", "product_id", "category_id",
        "category_code", "brand", "price", "user_id", "user_session",
    ]
    baseline_df = baseline_df.select(*core_columns)

    if hdfs_path_exists(spark, HDFS_STREAM_SINK_PATH):
        print(f"[train.py] Processing Ingestion Data found at: {HDFS_STREAM_SINK_PATH}")
        try:
            stream_df = spark.read.parquet(HDFS_STREAM_SINK_PATH)
            if stream_df.limit(1).count() > 0:  # Cheap emptiness check
                # Deduplicate: the Parquet archive is append-only, so replaying
                # the producer or resetting Kafka offsets writes the same events
                # a second time. Without this, each retrain cycle would train on
                # an inflated, artificially reweighted copy of the same sessions.
                # Only the streamed side needs it — the Oct baseline is read once
                # from a single immutable CSV.
                stream_df = stream_df.select(*core_columns).dropDuplicates(core_columns)
                combined_df = baseline_df.unionByName(stream_df)
                print("[train.py] Dataset consolidation successful! Training on combined records.")
                return combined_df
        except Exception as e:
            print(f"[train.py] Skipping streaming ingestion merge: {e}")

    print("[train.py] Training baseline configuration activated. Using static data lake records only.")
    return baseline_df


def build_session_features(df):
    """
    Builds one row per session window: pre-purchase behavioural features
    plus the binary outcome label.

    Two correctness constraints are enforced here, and both must hold
    identically in streaming-job/consumer.py or live scores silently drift.

    1. Leakage control. The label is derived from the full event list (did
       this window ever contain a purchase?), but every FEATURE is computed
       with purchase events excluded via conditional aggregation. Letting
       the purchase event into feature aggregation would make purchasing
       sessions mechanically longer, wider and differently priced — the
       model would be reading the target off its own input.

    2. Session boundaries. Grouping is by user_session AND a 30-minute
       session_window, not by user_session alone. In this dataset a
       user_session id can persist across long idle gaps, so grouping by id
       alone would train on one large merged session while the streaming
       job — which must use session_window for state eviction — scores two
       or three smaller ones. Matching the windowing here is what keeps the
       two feature distributions comparable.

    Note this costs a sort per key rather than a flat hash aggregate. If
    training memory becomes the binding constraint, the lever to pull is
    spark.sql.shuffle.partitions, not this grouping.
    """
    non_purchase = F.col("event_type") != "purchase"

    session_agg = (
        df.groupBy(
            "user_session",
            F.session_window("event_time", "30 minutes").alias("session"),
        )
        .agg(
            # Label: computed across ALL events in the window.
            F.max(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias("label"),
            # Features: purchase events excluded from every one of them.
            F.min(F.when(non_purchase, F.col("event_time"))).alias("session_start"),
            F.max(F.when(non_purchase, F.col("event_time"))).alias("session_end"),
            F.sum(F.when(F.col("event_type") == "view", 1).otherwise(0)).alias("view_count"),
            F.sum(F.when(F.col("event_type") == "cart", 1).otherwise(0)).alias("cart_count"),
            F.sum(F.when(F.col("event_type") == "remove_from_cart", 1).otherwise(0)).alias("remove_count"),
            F.approx_count_distinct(F.when(non_purchase, F.col("product_id"))).alias("distinct_products"),
            F.approx_count_distinct(F.when(non_purchase, F.col("category_id"))).alias("distinct_categories"),
            F.approx_count_distinct(F.when(non_purchase, F.col("brand"))).alias("distinct_brands"),
            F.avg(F.when(non_purchase, F.col("price"))).alias("avg_price"),
            F.max(F.when(non_purchase, F.col("price"))).alias("max_price"),
        )
        # A window containing only purchase events has no pre-purchase
        # behaviour to learn from. Dropping it is deliberate: imputing zeros
        # would teach the model that an empty session predicts a purchase.
        .filter(F.col("session_start").isNotNull())
        .withColumn(
            "session_duration_sec",
            F.col("session_end").cast("long") - F.col("session_start").cast("long"),
        )
        .withColumn("has_cart", (F.col("cart_count") > 0).cast("int"))
        .na.fill({"avg_price": 0.0, "max_price": 0.0, "distinct_brands": 0})
    )
    return session_agg


def time_based_split(session_df, train_frac=0.60, val_frac=0.15):
    time_bounds = session_df.select(
        F.min("session_start").cast("long"), F.max("session_start").cast("long")
    ).first()
    start_ts, end_ts = time_bounds[0], time_bounds[1]
    total_duration = end_ts - start_ts

    train_cutoff = start_ts + int(total_duration * train_frac)
    val_cutoff = start_ts + int(total_duration * (train_frac + val_frac))

    ts = F.col("session_start").cast("long")
    train_df = session_df.filter(ts <= train_cutoff)
    val_df = session_df.filter((ts > train_cutoff) & (ts <= val_cutoff))
    test_df = session_df.filter(ts > val_cutoff)
    return train_df, val_df, test_df


def add_class_weights(train_df):
    counts = {
        row["label"]: row["count"]
        for row in train_df.groupBy("label").count().collect()
    }
    neg, pos = counts.get(0, 0), counts.get(1, 0)
    ratio = (neg / pos) if pos > 0 else 1.0
    print(f"[train.py] Class weight ratio (neg/pos) = {ratio:.2f} (neg={neg}, pos={pos})")
    return train_df.withColumn(
        "class_weight", F.when(F.col("label") == 1, F.lit(ratio)).otherwise(F.lit(1.0))
    )


FEATURE_COLS = [
    "view_count", "cart_count", "remove_count", "distinct_products",
    "distinct_categories", "distinct_brands", "avg_price", "max_price",
    "session_duration_sec", "has_cart",
]


def build_classifier():
    """
    Selected by the MODEL_TYPE environment variable. All three support
    weightCol in Spark 3.5, so the class-imbalance correction applies
    identically whichever is chosen.
    """
    common = dict(featuresCol="features", labelCol="label", weightCol="class_weight")

    if MODEL_TYPE == "random_forest":
        return RandomForestClassifier(numTrees=100, maxDepth=10, seed=42, **common)
    if MODEL_TYPE == "gbt":
        return GBTClassifier(maxIter=50, maxDepth=5, seed=42, **common)
    if MODEL_TYPE != "logistic_regression":
        print(
            f"[train.py] WARNING: unknown MODEL_TYPE='{MODEL_TYPE}', "
            "falling back to logistic_regression."
        )
    return LogisticRegression(maxIter=50, **common)


def build_pipeline():
    assembler = VectorAssembler(inputCols=FEATURE_COLS, outputCol="features", handleInvalid="skip")
    classifier = build_classifier()
    print(f"[train.py] Classifier: {type(classifier).__name__} (MODEL_TYPE={MODEL_TYPE})")
    return Pipeline(stages=[assembler, classifier])


def find_best_threshold(model, val_df, thresholds=None):
    if thresholds is None:
        thresholds = [round(0.05 * i, 2) for i in range(1, 20)]

    scored = (
        model.transform(val_df)
        .withColumn("purchase_probability", vector_to_array(F.col("probability"))[1])
        .select("label", "purchase_probability")
        .cache()
    )
    
    best = {"threshold": 0.5, "f1": -1.0, "precision": 0.0, "recall": 0.0}
    print("[train.py] Threshold sweep on validation set:")
    
    for t in thresholds:
        row = scored.select(
            F.sum(F.when((F.col("purchase_probability") >= t) & (F.col("label") == 1), 1).otherwise(0)).alias("tp"),
            F.sum(F.when((F.col("purchase_probability") >= t) & (F.col("label") == 0), 1).otherwise(0)).alias("fp"),
            F.sum(F.when((F.col("purchase_probability") < t) & (F.col("label") == 1), 1).otherwise(0)).alias("fn"),
        ).first()
        
        tp, fp, fn = row["tp"] or 0, row["fp"] or 0, row["fn"] or 0
        precision = tp / (tp + fp) if (tp + fp) else 0.0
        recall = tp / (tp + fn) if (tp + fn) else 0.0
        f1 = 2 * precision * recall / (precision + recall) if (precision + recall) else 0.0
        
        print(f"[train.py]   threshold={t:.2f}  precision={precision:.3f}  recall={recall:.3f}  f1={f1:.3f}")
        if f1 > best["f1"]:
            best = {"threshold": t, "f1": f1, "precision": precision, "recall": recall}

    scored.unpersist()
    print(f"[train.py] Best threshold: {best['threshold']:.2f} (val F1={best['f1']:.3f})")
    return best["threshold"]


def evaluate(model, test_df):
    predictions = model.transform(test_df).cache()
    predictions.count()

    auc = BinaryClassificationEvaluator(labelCol="label", rawPredictionCol="rawPrediction", metricName="areaUnderROC").evaluate(predictions)
    accuracy = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy").evaluate(predictions)
    precision = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="weightedPrecision").evaluate(predictions)
    recall = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="weightedRecall").evaluate(predictions)
    
    precision_purchase = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="precisionByLabel", metricLabel=1.0).evaluate(predictions)
    recall_purchase = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="recallByLabel", metricLabel=1.0).evaluate(predictions)

    return {
        "auc": auc, "accuracy": accuracy, "precision": precision, "recall": recall,
        "precision_purchase": precision_purchase, "recall_purchase": recall_purchase,
    }


def get_mysql_connection():
    return pymysql.connect(
        host=MYSQL_HOST, user=MYSQL_USER, password=MYSQL_PASSWORD,
        database=MYSQL_DB, autocommit=False,
    )


def get_active_champion(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT hdfs_path, version, accuracy FROM model_registry WHERE is_active = TRUE ORDER BY created_at DESC LIMIT 1")
        return cur.fetchone()


def record_challenger(conn, hdfs_path, version, trained_on, metrics, promoted):
    with conn.cursor() as cur:
        if promoted:
            cur.execute("UPDATE model_registry SET is_active = FALSE WHERE is_active = TRUE")
        cur.execute(
            "INSERT INTO model_registry "
            "(hdfs_path, version, trained_on, auc_score, accuracy, precision_score, "
            "recall_score, precision_purchase, recall_purchase, decision_threshold, is_active) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            (
                hdfs_path, version, trained_on, metrics["auc"], metrics["accuracy"],
                metrics["precision"], metrics["recall"], metrics["precision_purchase"],
                metrics["recall_purchase"], metrics.get("decision_threshold", 0.5), promoted,
            ),
        )
    conn.commit()


def run_training_cycle(spark):
    version = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    trained_on = os.path.basename(HDFS_TRAIN_SOURCE_PATH.rstrip("/"))
    print(f"[train.py] === Training cycle {version} on {trained_on} ===")

    raw_df = load_consolidated_datasets(spark)
    session_df = build_session_features(raw_df).cache()
    
    n_sessions = session_df.count()
    n_purchased = session_df.filter(F.col("label") == 1).count()
    print(f"[train.py] Sessions: {n_sessions}, purchased: {n_purchased} ({100 * n_purchased / max(n_sessions, 1):.1f}%)")

    train_df, val_df, test_df = time_based_split(session_df)
    train_df = add_class_weights(train_df)

    pipeline = build_pipeline()
    challenger_model = pipeline.fit(train_df)
    best_threshold = find_best_threshold(challenger_model, val_df)

    classifier_model = challenger_model.stages[-1]
    if hasattr(classifier_model, "setThreshold"):
        classifier_model.setThreshold(best_threshold)
    elif hasattr(classifier_model, "setThresholds"):
        classifier_model.setThresholds([1.0 - best_threshold, best_threshold])

    challenger_metrics = evaluate(challenger_model, test_df)
    challenger_metrics["decision_threshold"] = best_threshold
    print(f"[train.py] Challenger metrics: {challenger_metrics}")

    candidate_path = f"{HDFS_MODEL_BASE_PATH}/candidates/{version}"
    challenger_model.write().overwrite().save(candidate_path)
    print(f"[train.py] Challenger model saved to {candidate_path}")

    conn = get_mysql_connection()
    try:
        champion_row = get_active_champion(conn)
        if champion_row is None:
            print("[train.py] No existing Champion — promoting as Champion v1.")
            record_challenger(conn, candidate_path, version, trained_on, challenger_metrics, promoted=True)
        else:
            champion_path, champion_version, _ = champion_row
            
            try:
                print(f"[train.py] Re-scoring existing Champion ({champion_version}) on the current test set...")
                
                # Strip whitespace to defend against DB artifacts
                champion_path = champion_path.strip() 
                
                champion_model = PipelineModel.load(champion_path)
                champion_metrics = evaluate(champion_model, test_df)
                print(f"[train.py] Champion (re-scored) metrics: {champion_metrics}")
            except Exception as e:
                print(f"[train.py] WARNING: Could not load champion model from {champion_path}: {e}")
                print("[train.py] Treating as no champion — challenger will be promoted.")
                champion_metrics = None

            # Ensure 'promote' is always defined to prevent NameError
            if champion_metrics is None:
                promote = True
                record_challenger(conn, candidate_path, version, trained_on, challenger_metrics, promoted=promote)
            else:
                improvement = challenger_metrics["auc"] - champion_metrics["auc"]
                promote = improvement > CHAMPION_IMPROVEMENT_THRESHOLD
                print(
                    f"[train.py] AUC improvement: {improvement:.4f} "
                    f"(threshold {CHAMPION_IMPROVEMENT_THRESHOLD}) -> "
                    f"{'PROMOTE' if promote else 'keep current Champion'}"
                )
                record_challenger(conn, candidate_path, version, trained_on, challenger_metrics, promoted=promote)
    finally:
        conn.close()

    session_df.unpersist()
    print(f"[train.py] === Cycle {version} complete ===\n")


def main():
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    log4j = spark.sparkContext._jvm.org.apache.log4j
    log4j.LogManager.getLogger("org.apache.spark.sql.catalyst.expressions.RowBasedKeyValueBatch").setLevel(log4j.Level.ERROR)

    try:
        run_training_cycle(spark)
        if RUN_ONCE:
            print("[train.py] RUN_ONCE=true, exiting after single cycle.")
            return
        while True:
            print(f"[train.py] Sleeping {TRAINING_INTERVAL_SECONDS}s until next training cycle...")
            time.sleep(TRAINING_INTERVAL_SECONDS)
            run_training_cycle(spark)
    except KeyboardInterrupt:
        print("[train.py] Interrupted, shutting down.")
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main() or 0)