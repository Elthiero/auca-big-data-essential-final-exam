# eCommerce Purchase Prediction — Real-Time Big Data Pipeline

**Big Data Essentials — Final Course Project**
Master's in Big Data Analytics, Adventist University of Central Africa (AUCA)
Instructor: Dr. Kundan Kumar

**Group members**

| ID | Name |
|---|---|
| 101201 | Ahourdet Donambi Thierry |
| XXXXXX | Emmanuel Manzi |
| XXXXXX | Gwiza Rodrigue |
| XXXXXX | Niyindora Emile |
| XXXXXX | SHEMA Joshua |

[View Report Document](report/BDE_final_exam.pdf)

---

## 1. What this project does

Given a live clickstream of browsing events from an online store, the system predicts **whether a shopping session will end in a purchase** — while the session is still in flight — and surfaces those predictions on a live web dashboard.

The pipeline is fully containerized and runs end to end with Docker Compose. It implements every component required by the course brief:

| Required component | Implementation |
|---|---|
| HDFS | NameNode + DataNode cluster; raw CSVs, Parquet archives, model binaries, streaming checkpoints |
| Kafka Producer (reading from HDFS) | `ingestion-job` streams rows out of HDFS over WebHDFS and publishes to Kafka |
| Kafka Topic | `clickstream-events`, 3 partitions, keyed by `user_session` |
| Kafka Consumer (PySpark Streaming) | `streaming-job`, Structured Streaming with session windows |
| MLlib predictions | `training-job` fits a `PipelineModel`; the streaming job loads and applies it |
| MySQL / AWS RDS | `model_registry`, `predictions`, `running_metrics` tables |
| Django dashboard | Read-only live monitor at `http://localhost:8000` |

**Case study:** customer behaviour prediction on the Kaggle *eCommerce Behavior Data from Multi-Category Store* dataset. October 2019 (~5.3 GB) is the historical training corpus loaded into HDFS; November 2019 (~9 GB) is replayed through Kafka to stand in for live traffic. Both files are well above the 1 GB minimum.

---

## 2. System architecture

![System Architecture](screenshots/architecture.png)

The system runs **two independent loops** over the same Kafka topic.

### Loop A — real-time scoring (seconds)

```
HDFS (2019-Nov.csv)
   │  WebHDFS, line-by-line
   ▼
ingestion-job  ──►  Kafka topic: clickstream-events (3 partitions, key = user_session)
                          │
                          ▼
                   streaming-job (PySpark Structured Streaming)
                          ├── Sink 1: raw events ──► HDFS Parquet (partitioned by event_type, event_date)
                          └── Sink 2: session aggregates ──► Champion model ──► MySQL predictions
                                                                                      │
                                                                                      ▼
                                                                              Django dashboard
```

### Loop B — batch retraining (scheduled)

```
HDFS: /data/raw/2019-10 (baseline)  +  /data/streaming/2019-11 (accumulated by Sink 1)
   │  unionByName + dropDuplicates
   ▼
training-job ──► session features ──► chronological split ──► MLlib pipeline
                                                                   │
                                    ┌──────────────────────────────┤
                                    ▼                              ▼
                       model artifact ──► HDFS /models/     metrics ──► MySQL model_registry
                                                                            │
                       streaming-job polls registry every 30 s ◄────────────┘
                       and hot-swaps the model if is_active changed
```

The registry is the coupling point between the two loops. It stores **pointers and metrics only** — never model blobs. Artifacts live in HDFS, which is what HDFS is for.

### Storage split

| Store | Contents |
|---|---|
| HDFS | Raw CSV, Parquet archives (historical + streamed), model artifacts, Spark checkpoints |
| MySQL | Model registry rows, prediction rows, pre-aggregated counters |

---

## 3. Design decisions worth defending

These are the four choices that most affect whether the numbers in Section 7 mean anything.

### 3.1 Label leakage is excluded from features

The label for a session is "did this window ever contain a `purchase` event?" — derived from the **full** event list. But every *feature* is aggregated with purchase events filtered out:

```python
non_purchase = F.col("event_type") != "purchase"
F.avg(F.when(non_purchase, F.col("price"))).alias("avg_price")
```

Without this, purchasing sessions are mechanically longer, wider, and differently priced than non-purchasing ones, and the model reads its own target off its input. Scores look excellent and mean nothing.

Windows containing *only* purchase events are dropped rather than zero-imputed — imputing zeros would teach the model that an empty session predicts a purchase.

### 3.2 Train/serve feature parity is enforced by a test

Both `train.py` and `consumer.py` group by `user_session` **and** a 30-minute `session_window`, and derive each feature with identical expressions. Grouping by session id alone would not be equivalent: a `user_session` in this dataset persists across long idle gaps, so training would learn from one merged session while the streaming job — which needs `session_window` for state eviction under Append mode — scores two or three smaller ones.

This is the pipeline's most dangerous failure mode because it is silent. A *missing* column raises at runtime; a **reordered** column list, a **changed grouping key**, and a **changed aggregation expression** all fail quietly, producing confident nonsense with no error anywhere.

`tests/test_feature_parity.py` parses both source files with `ast` and compares grouping keys and per-alias aggregation expressions structurally. It was mutation-checked: reordering `FEATURE_COLS`, swapping the session-window grouping for a plain `groupBy("user_session")`, and dropping the purchase exclusion from a single price aggregate each produce a failure.

### 3.3 The split is chronological, not random

60% train / 15% validation / 25% test by `session_start`. A random split would let the model learn from sessions occurring *after* the ones it is evaluated on, which corresponds to no real deployment.

### 3.4 Class imbalance is corrected at runtime, not hardcoded

`add_class_weights()` computes the negative-to-positive ratio from the training split each cycle and passes it as `weightCol`, so the correction tracks the data as the data grows. On the October baseline this came out at **13.57**.

---

## 4. Repository structure

```text
├── data/raw/                                # not in git — see setup step 2
│   ├── 2019-10/2019-Oct.csv                 # historical training corpus
│   └── 2019-11/2019-Nov.csv                 # replayed as the live stream
├── django-dashboard/
│   ├── dashboard/
│   │   ├── settings.py
│   │   ├── test_runner.py                   # creates unmanaged tables in the test DB
│   │   ├── urls.py
│   │   ├── asgi.py  wsgi.py
│   ├── monitor/
│   │   ├── templates/monitor/index.html     # Chart.js frontend, 8-second polling
│   │   ├── models.py                        # unmanaged mappings to the streaming tables
│   │   ├── views.py                         # index + /api/stats/ polling endpoint
│   │   ├── tests.py                         # view and aggregation tests
│   │   ├── admin.py  apps.py
│   ├── test_settings.py                     # SQLite overlay for offline test runs
│   ├── Dockerfile  manage.py  requirements.txt
├── ingestion-job/
│   ├── producer.py                          # WebHDFS → Kafka row relay
│   ├── Dockerfile  requirements.txt
├── mysql-init/init.sql                      # schema, auto-applied on first MySQL boot
├── screenshots/                             # evidence captures referenced below
├── report/
│   ├── BDE_final_exam                       # Report of the project
├── scripts/
│   ├── create_kafka_topic.sh                # creates the topic with 3 partitions
│   ├── load_to_hdfs.sh                      # pushes the raw CSVs into HDFS
│   └── clean_checkpoints.sh                 # clears Spark streaming checkpoints
├── streaming-job/
│   ├── consumer.py                          # Structured Streaming, two sinks
│   ├── Dockerfile  requirements.txt
├── tests/test_feature_parity.py             # cross-service feature contract test
├── training-job/
│   ├── train.py                             # Champion/Challenger trainer
│   ├── Dockerfile  requirements.txt
├── .env.example  .gitignore  .dockerignore
├── docker-compose.yml
├── hadoop.env
└── README.md
```

---

## 5. Deployment guide

Requires Docker, Docker Compose v2, and at least **8 GB of free RAM**. The training container alone is allocated 4.5 GB.

### Step 1 — Clone and configure

```bash
git clone https://github.com/Elthiero/auca-big-data-essential-final-exam.git
cd auca-big-data-essential-final-exam
cp .env.example .env
```

Edit `.env` if you want non-default credentials, a different classifier, or a different replay rate. Note that `.env` takes precedence over the defaults baked into `docker-compose.yml`.

### Step 2 — Place the dataset

Download `2019-Oct.csv` and `2019-Nov.csv` from the [Kaggle eCommerce Behavior Data dataset](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store), unzip, and arrange them as:

```text
data/raw/2019-10/2019-Oct.csv
data/raw/2019-11/2019-Nov.csv
```

### Step 3 — Start MySQL and verify the schema

```bash
docker compose up mysql -d

docker exec -it mysql mysql -u bigdata_user -p"$MYSQL_PASSWORD" \
  -e "USE model_registry; SHOW TABLES; SELECT * FROM running_metrics;"
```

You should see `model_registry`, `predictions`, and `running_metrics`, with both counters at 0. If you are using AWS RDS instead, run `mysql-init/init.sql` against your endpoint manually — the auto-init only fires for the local container.

![MySQL schema](screenshots/mysql.png)

### Step 4 — Bootstrap the HDFS data lake

```bash
docker compose up namenode datanode -d
docker compose ps          # wait until both report healthy
./scripts/load_to_hdfs.sh
```

The NameNode web UI is at `http://localhost:9870`.

![HDFS cluster detail](screenshots/hdfs_details.png)

The raw CSVs are now split into HDFS blocks across the DataNode:

![HDFS raw data](screenshots/hdfs_raw.png)
![HDFS block detail](screenshots/hdfs_raw2.png)

### Step 5 — Start Kafka and create the topic

```bash
docker compose up kafka -d
docker compose ps          # wait for healthy
./scripts/create_kafka_topic.sh
```

Kafka runs in **KRaft mode** — no ZooKeeper. The topic is created with 3 partitions so consumers can parallelize; messages are keyed by `user_session` so all events for one session land on one partition and stay ordered.

![Kafka topic](screenshots/kafka.png)

### Step 6 — Train the first model

Pick a classifier in `.env` if you want something other than the default:

```ini
MODEL_TYPE=logistic_regression    # or random_forest | gbt
```

```bash
docker compose up training-job -d
docker logs -f training-job
```

Expect roughly **20 minutes** for a full cycle on the October corpus inside the 4.5 GB container. You will see the Parquet conversion, session aggregation counts, the class weight ratio, the threshold sweep, and finally the Champion promotion.

![Training log](screenshots/training_log.png)

Confirm the registry row:

```bash
docker exec -it mysql mysql -u bigdata_user -p"$MYSQL_PASSWORD" \
  -e "SELECT version, auc_score, decision_threshold, is_active FROM model_registry.model_registry;"
```

The model artifact itself is written to HDFS at `/models/candidates/<version>`.

### Step 7 — Start ingestion, scoring, and the dashboard

Only do this **after** a Champion exists — the streaming job will start, but it skips scoring and logs `no Champion loaded yet` until one is registered.

```bash
docker compose up streaming-job ingestion-job django-dashboard -d
docker logs -f streaming-job
```

![Producer and consumer logs](screenshots/logs.png)

Sink 1 archives every incoming event to the Parquet data lake, partitioned by `event_type` and `event_date`. This is what the next retraining cycle consolidates with the October baseline:

![Streamed Parquet in HDFS](screenshots/hdfs_stream_data.png)

Open `http://localhost:8000`:

![Dashboard](screenshots/dashboard.png)

The dashboard is a read-only white/blue interface. Frontend JavaScript polls `/api/stats/` every 8 seconds and updates Chart.js visuals in place, without a full page reload.

### Timing note

At the default 500 events/sec, replaying the whole November file takes many hours. For a bounded demo, set `MAX_EVENTS=50000` in `.env`. Sessions are only emitted once their 30-minute watermark closes, so allow a few minutes of event time before predictions appear.

---

## 6. Technologies and versions

| Layer | Technology | Version | Notes |
|---|---|---|---|
| Distributed storage | Apache Hadoop / HDFS | 3.2.1 | `bde2020/hadoop-*:2.0.0-hadoop3.2.1-java8`, replication factor 1, WebHDFS enabled |
| Message broker | Apache Kafka | 3.7.0 | `bitnamilegacy/kafka:3.7.0`, KRaft mode, 3 partitions |
| Processing engine | Apache Spark (PySpark) | 3.5.1 | `local[*]` inside each job container |
| Scala runtime | Scala | 2.12 | Determines the Kafka connector artifact suffix |
| ML library | Spark MLlib | 3.5.1 | Ships with PySpark; no separate install |
| Database | MySQL | 8.0 | Local container, or AWS RDS MySQL |
| Web framework | Django | 5.0.6 | Development server, read-only dashboard |
| Charting | Chart.js | 4.4.1 | CDN, client-side |
| Language runtime | Python | 3.10 | `python:3.10-slim` base for all four job images |
| Java runtime | OpenJDK JRE headless | 17 | `default-jre-headless`, JVM side only |
| Orchestration | Docker Compose | v2 | Single host, six services |

### JAR dependency

Resolved at runtime via `spark.jars.packages` in `streaming-job/consumer.py`, downloaded on first run and cached in the `spark_ivy_cache` volume:

```text
org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1
```

The `_2.12` suffix must match the Scala version Spark was built against, and `3.5.1` must match the PySpark version in `requirements.txt`. A mismatch in either produces a `ClassNotFoundException` for the Kafka source at *query start* rather than at import time, which makes it easy to misdiagnose.

### Python dependencies by service

| Service | Packages |
|---|---|
| `ingestion-job` | `hdfs==2.7.3`, `confluent-kafka==2.5.0`, `requests==2.31.0` |
| `streaming-job` | `pyspark==3.5.1`, `pymysql==1.1.0`, `cryptography==42.0.8`, `numpy==1.26.4`, `pandas==2.1.1` |
| `training-job` | `pyspark==3.5.1`, `pymysql==1.1.0`, `cryptography==42.0.8`, `numpy==1.26.4`, `pandas==2.1.1` |
| `django-dashboard` | `Django==5.0.6`, `PyMySQL==1.1.0`, `cryptography==42.0.8` |

Two deliberate substitutions:

- **`confluent-kafka` over `kafka-python`** — delivery failures surface through its callback instead of being retried silently on a background thread. The original implementation hung at roughly 80,000 events with no error output; the callback plus periodic `flush()` fixed it.
- **`PyMySQL` over `mysqlclient`** — pure Python, avoiding `libmysqlclient-dev` and a C toolchain in every image.

---

## 7. Results

All figures below come from the training cycle `20260726_083630`, logistic regression on the October 2019 corpus. This was the cold-start run, so `training-job` reported *"Training baseline configuration activated. Using static data lake records only"* — no streamed November data had accumulated yet, and the model was promoted directly as Champion v1 with no Challenger comparison.

### 7.1 Dataset after session aggregation

| Quantity | Value |
|---|---|
| Sessions (October, after windowing) | 9,436,444 |
| Sessions ending in a purchase | 630,085 |
| Purchase rate | 6.7% |
| Training split — negative | 5,291,223 |
| Training split — positive | 389,875 |
| Class weight ratio (neg/pos) | 13.57 |

### 7.2 Threshold sweep on the validation set

| Threshold | Precision | Recall | F1 |
|---|---|---|---|
| 0.30 | 0.080 | 0.952 | 0.148 |
| 0.40 | 0.237 | 0.741 | 0.359 |
| 0.50 | 0.325 | 0.635 | 0.430 |
| 0.60 | 0.381 | 0.596 | 0.465 |
| 0.70 | 0.422 | 0.574 | 0.487 |
| 0.80 | 0.453 | 0.560 | 0.501 |
| **0.85** | **0.465** | **0.551** | **0.504** |
| 0.90 | 0.481 | 0.517 | 0.498 |
| 0.95 | 0.451 | 0.156 | 0.232 |

Selected threshold: **0.85**, validation F1 = 0.504.

### 7.3 Held-out test metrics

| Metric | Value |
|---|---|
| AUC (ROC) | 0.8280 |
| Accuracy | 0.9333 |
| Weighted precision | 0.9323 |
| Weighted recall | 0.9333 |
| Precision, purchase class | 0.4788 |
| Recall, purchase class | 0.4621 |
| Decision threshold | 0.85 |

Registered in MySQL as version `20260726_083630`, `auc_score = 0.828021`, `is_active = 1`.

---

## 8. Findings

**Accuracy is the wrong metric on this problem, and the numbers show exactly why.** The test accuracy of 93.33% is almost identical to what a model that predicts "no purchase" for every single session would score, given a ~6.7% purchase rate. A trivial constant predictor achieves essentially the same headline number. This is why `train.py` makes promotion decisions on AUC and tracks `precision_purchase` and `recall_purchase` separately from their weighted counterparts — the weighted figures (0.932, 0.933) are dominated by the majority class and carry almost no information about whether the model can find buyers.

**The model has real signal.** AUC of 0.828 means that given one purchasing and one non-purchasing session at random, the model ranks the purchaser higher about 83% of the time. On the purchase class it recovers 46.2% of actual buyers at 47.9% precision. Against a 6.7% base rate, flagging a session lifts the probability that it converts from roughly 1 in 15 to roughly 1 in 2 — a **7-fold lift**. That is the number that matters commercially, and it is the one the accuracy figure completely hides.

**The optimal threshold is 0.85, not 0.5, and that is a consequence of the class weighting.** Weighting positives by 13.57 inflates predicted probabilities across the board, so the natural 0.5 cut is far too permissive: at 0.5 the model achieves recall of 0.635 but precision of only 0.325. The sweep shows the trade-off is smooth and shallow between 0.70 and 0.90 — F1 moves only from 0.487 to 0.498 across that whole range — which means the operating point can be chosen on business grounds rather than statistical ones without much cost.

**That choice is genuinely open.** At threshold 0.90 precision rises to 0.481 while recall falls to 0.517; at 0.70 precision drops to 0.422 while recall rises to 0.574. If the intervention is cheap — a banner, a free-shipping nudge — the lower threshold is right, because a wasted impression costs nothing and missed buyers cost margin. If the intervention is expensive — a percentage discount applied to the cart — the higher threshold is right, because every false positive is money handed to someone who was going to buy anyway. Below 0.35 the model collapses into flagging nearly everything: at threshold 0.30 recall is 95.2% but precision is 8.0%, barely above the base rate, which is operationally worthless.

**The steep cliff between 0.30 and 0.35 is informative.** Precision jumps from 0.080 to 0.173 while recall only falls from 0.952 to 0.836. That discontinuity suggests a large, tightly clustered mass of low-intent sessions — almost certainly single-view bounces — that the model separates cleanly from everything else. The expensive discrimination problem is not "browser vs buyer," it is distinguishing serious browsers from buyers, which is where the remaining error lives.

**Retraining has not yet been demonstrated end to end.** This cycle ran before any November data had accumulated in the Parquet lake, so it promoted unopposed as Champion v1. The Challenger path — consolidating October with streamed November data via `unionByName`, re-scoring the existing Champion on the same held-out test set, and promoting only on an AUC gain above 0.02 — is implemented and exercised by the code, but the comparative result is not in this report. Whether the Challenger clears the bar is an open question, and a *failure* to clear it would be a legitimate finding rather than a defect: it would say the October model already generalizes to November traffic.

### Limitations

Stating these plainly is more defensible than claiming production readiness.

- **Single-node everything.** HDFS runs with replication factor 1 on one DataNode, Kafka is a single broker, and Spark runs `local[*]` rather than against a cluster manager. The architecture is distributed; the deployment is not. Nothing here demonstrates behaviour under node failure.
- **Replayed history is not live traffic.** November 2019 events pushed at a fixed rate have no diurnal pattern, no bursts, no late-arriving or out-of-order records beyond what the file already contains, and no schema drift. A real stream has all four.
- **Session scoring happens at close, not mid-session.** Append mode over a `session_window` emits one row per session once the 30-minute watermark expires. That is correct for evaluation but limits real-time intervention — by the time a session is scored, the user has been idle for half an hour. Update mode over a fixed tumbling window would invert this trade-off, at the cost of emitting repeated partial predictions per session.
- **Feature set is deliberately shallow.** Ten aggregate behavioural features, no product embeddings, no user history across sessions, no category-level or temporal features. Cross-session user history in particular is available in the data and is the obvious next lever.
- **Memory pressure during training.** The log shows Spark spilling cached RDD blocks to disk under the 4.5 GB container limit. Results are unaffected but the cycle is slower than it needs to be.
- **`approx_count_distinct`** is used for the three cardinality features, trading a small amount of accuracy for tractable memory. Consistent between train and serve, so it does not cause skew.

---

## 9. Testing

```bash
# Cross-service contract tests — no PySpark, JVM, HDFS or MySQL required
python -m pytest tests/ -v

# Dashboard tests — SQLite, no running MySQL required
cd django-dashboard && python manage.py test monitor --settings=test_settings
```

`tests/test_feature_parity.py` guards the silent-drift failure mode described in Section 3.2. The dashboard tests cover the empty-database path (the frontend polls `/api/stats/` from page load, before any prediction exists), the pre-aggregated counter reads, and the trend window bounds.

Both suites run offline. `dashboard/test_runner.py` creates the unmanaged tables in the test database, since Django does not manage them.

---

## 10. Performance engineering

A few choices that keep the system responsive at scale rather than only at demo size.

**Pre-aggregated counters.** The dashboard's headline figures would otherwise require `COUNT(*)` over the `predictions` table on every 8-second poll. Instead, `write_predictions_partition` increments the `running_metrics` counter table inside the same transaction as the insert, so a poll reads two integers rather than counting millions of rows.

**Bounded trend query.** The per-minute trend chart is a `GROUP BY` restricted to a 30-minute `scored_at` window, which uses the `idx_scored_at` index instead of scanning the full table. It also orders descending before slicing — slicing an ascending ordering returns the *oldest* buckets, which freezes the chart on the first minutes of the run.

**Intake rate limiting.** `maxOffsetsPerTrigger=30000` caps how much backlog a single trigger absorbs, keeping micro-batch memory bounded when the consumer starts from `earliest` against an already-full topic.

**Batched writes.** Prediction rows are inserted with `executemany` in chunks of 5,000 inside one transaction per partition.

**Parquet caching.** `load_or_create_parquet` converts CSV to Parquet once and reuses it on subsequent cycles, which is why the log shows *"Reusing existing Parquet"* rather than repeating a multi-minute conversion.

**Stream deduplication.** The Parquet archive is append-only, so replaying the producer or resetting Kafka offsets writes the same events twice. `dropDuplicates` on the streamed side prevents each retrain cycle from training on an inflated, artificially reweighted copy of the same sessions.

---

## 11. AWS RDS migration

To move the data tier to a managed cloud database:

1. **Provision.** Launch an RDS instance with the **MySQL Community Edition** engine under the Free Tier. Set the VPC security group to allow inbound TCP on port `3306` from your host's public IP.
2. **Apply the schema.** Connect via DBeaver, MySQL Workbench, or the `mysql` CLI and run `mysql-init/init.sql`. The container's auto-init does not apply to RDS.
3. **Repoint credentials.** In `.env`:

    ```ini
    MYSQL_HOST=your-instance.abcdefgh.us-east-1.rds.amazonaws.com
    MYSQL_DATABASE=model_registry
    MYSQL_USER=your_rds_user
    MYSQL_PASSWORD=your_rds_password
    ```

4. **Drop the local container.** Comment out or remove the `mysql` service block in `docker-compose.yml`, along with the `mysql` entries under each service's `depends_on`, then `docker compose up -d`. This also frees roughly 750 MB of RAM.

Note that `MYSQL_HOST` is currently hardcoded to `mysql` in the `docker-compose.yml` environment blocks for `streaming-job`, `training-job`, and `django-dashboard`. Change those to `${MYSQL_HOST:-mysql}` so the `.env` value takes effect.

---

## 12. Cluster overview

![Cluster overview](screenshots/system_architecture.png)