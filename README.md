# eCommerce Purchase Prediction Pipeline (Big Data & MLOps Infrastructure)

This repository contains a production-grade, containerized MLOps and Big Data analytics pipeline designed to process real-time clickstream events, predict customer purchasing behavior, and continuously retrain models on a distributed data lake.

The architecture seamlessly orchestrates distributed storage, high-throughput message streaming, stateful micro-batch aggregation, online machine learning inference, and an asynchronous operational web console.

---

## System Architecture & Data Lifecycle

The system operates using two distinct operational loops: a real-time predictive stream and an asynchronous batch-retraining lifecycle.

![System Architecture](screenshots/architecture.png)

### 1. Storage & Messaging Layer

* **HDFS (Data Lake Cluster):** Houses raw input datasets (exceeding 1GB), column-oriented Parquet historical archives, serialized model binaries, and stateful streaming checkpoint files.
* **Apache Kafka (KRaft Mode):** Operates a zero-ZooKeeper message broker. The core streaming topic utilizes 3 distinct partitions to allow highly parallelized consumer access. Sessions are distributed via `user_session` hashing keys to ensure sequential execution for individual customer journeys.

### 2. Compute & ML Ingestion Layer

* **PySpark Structured Streaming Engine:** Consumes real-time JSON packets from Kafka. It tracks user sessions using an event-time `session_window` bounded by a 30-minute watermark. To comply with Spark’s dynamic window constraints, it processes streams in **Append Mode**, gracefully evicting inactive sessions from the state store to completely eliminate memory leaks.
* **PySpark MLlib Batch Pipeline:** Features a high-performance **`RandomForestClassifier`** ensemble (100 parallel trees) optimized to handle severe data imbalance ($6.8\%$ positive baseline) via an automated instance weight calculation ($13.41$ negative-to-positive penalty ratio).
* **Dynamic MLOps Orchestration:** Features a data-lake consolidator that uses a distributed `unionByName()` merge to aggregate historical training logs with the streaming Parquet lake. It re-scores the existing Champion against incoming Challengers using an apples-to-apples validation test matrix, executing zero-downtime memory model hot-swaps every 30 seconds.

### 3. Storage Sink & Analytical Dashboard

* **High-Scale Pre-Aggregated Summary Pattern:** Completely eliminates expensive `COUNT(*)` table scans on massive datasets. The Spark engine commits raw rows and increments a localized `running_metrics` lookup table within a single transaction block.
* **Asynchronous Web Engine:** A responsive, dark-themed Django administrative interface. It utilizes a background JavaScript worker to poll a lightweight `/api/stats/` JSON view every 8 seconds, executing fluid in-place updates on hardware-accelerated **Chart.js** visuals.

---

## Repository Structure

```text
├── data/
│   └── raw/
│       ├── 2019-10/
│       │   └── 2019-Oct.csv                 # Raw historical dataset (>= 1GB training split)
│       └── 2019-11/
│           └── 2019-Nov.csv                 # Raw deployment dataset (>= 1GB streaming simulation)
├── django-dashboard/                        # Operational UI and Admin Console
│   ├── dashboard/                           # Django core settings configuration
│   │   ├── settings.py
│   │   └── urls.py
│   ├── monitor/                             # Analytical monitoring web application
│   │   ├── templates/monitor/index.html     # Real-time Chart.js dark-theme frontend
│   │   ├── models.py                        # Unmanaged mappings to streaming tables
│   │   └── views.py                         # Highly optimized O(1) polling endpoints
│   ├── Dockerfile
│   ├── manage.py
│   └── requirements.txt
├── ingestion-job/                           # High-speed data ingestion pipeline
│   ├── producer.py                          # WebHDFS stream-to-Kafka row relay script
│   ├── Dockerfile
│   └── requirements.txt
├── mysql-init/
│   └── init.sql                             # Database tables initialization schema
├── screenshots/
│   ├── architecture.png                     # Formatted system workflow visual artifact
│   └── system_architecture.png              # Multi-tier cluster overview screenshot
├── scripts/
│   ├── create_kafka_topic.sh                # Stream topic partition mapping initialization
│   ├── clean_checkpoints.sh                 # Cleaning local Spark streaming checkpoints
│   └── load_to_hdfs.sh                      # HDFS data lake bootstrapper utility
├── streaming-job/                           # Live stream scoring runtime
│   ├── consumer.py                          # Stateful PySpark Structured Streaming script
│   ├── Dockerfile
│   └── requirements.txt
├── training-job/                            # Automated batch model retraining module
│   ├── train.py                             # Polymorphic Champion/Challenger trainer script
│   ├── Dockerfile
│   └── requirements.txt
├── .env.example                             # Pipeline infrastructure environment setup template
├── .gitignore                               # Local compilation exclusions registry
├── README.md                                # Architectural overview document
├── docker-compose.yml                       # Distributed container runtime orchestrator
└── hadoop.env                               # Ecosystem cluster filesystem environmental variables

```

---

## Step-by-Step Deployment Guide

Ensure your development environment contains Docker, Docker Compose, and a minimum of 8GB of free system RAM.

### 1. Initialize Configuration and Database Schema

1. **Clone the Repository**: Download the source code to your local machine by cloning the project repository.

   ```bash
   git clone https://github.com/Elthiero/auca-big-data-essential-final-exam.git
   cd auca-big-data-essential-final-exam
   ```

   *(Alternatively, you can manually download the [Project Source Code ZIP File](https://github.com/Elthiero/auca-big-data-essential-final-exam.git) and extract it.)*

2. Download the dataset files (`2019-Oct.csv` and `2019-Nov.csv`) directly from the [Kaggle eCommerce Behavior Data Dataset](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store).
3. Unzip the downloaded archive.
4. Move and organize the unzipped `.csv` files into the project structure shown below:

    ```text
    ├── data/
    │   └── raw/
    │       ├── 2019-10/
    │       │   └── 2019-Oct.csv                 
    │       └── 2019-11/
    │           └── 2019-Nov.csv  
    ```

Generate your environment runtime profile and spin up your metadata storage block:

```bash
cp .env.example .env
docker compose up mysql -d
```

*you can modify the .env file.*

Verify that the unmanaged core tables (`model_registry`, `predictions`, and `running_metrics`) have initialized perfectly:

```bash
docker exec -it mysql mysql -u bigdata_user -pchangeme -e "USE model_registry; SHOW TABLES;"
```

*use the database user and password from your .env, if you are using AWS RDS or any other database you can check mysql-init/init.sql to get the sql queries.*

![Mysql Local](screenshots/mysql.png)

### 2. Bootstrap the HDFS Data Lake

Launch the distributed storage nodes:

```bash
docker compose up namenode datanode -d
```

Access the NameNode web UI at `http://localhost:9870` to verify node health. Execute the bootstrapper script to automatically push your raw datasets into HDFS blocks:

```bash
./scripts/load_to_hdfs.sh
```

![Hadoop Logs](screenshots/hdfs_details.png)

### 3. Spin Up Message Ingestion Network

Boot the self-managed Kafka communication node:

```bash
docker compose up kafka -d
```

Initialize your event topic with 3 partitions to guarantee parallel streaming channels:

```bash
./scripts/create_kafka_topic.sh
```

![Kafka logs](screenshots/kafka.png)

### 4. Execute the Initial Training Pass

Kick off the batch machine learning process to establish your initial base model:

```bash
docker compose up training-job -d
```

To observe features being calculated, threshold sweeps executing, and weights generating, watch the logs:

```bash
docker logs -f training-job
```

*Upon completion, the system saves your model directly inside HDFS at `/models/candidates/` and crowns it as `Champion v1` in the database.*

![Training Model](screenshots/training_log.png)

### 5. Launch Ingestion, Real-Time Inference, and the UI

Now that an active model is registered, ignite your live ingestion stream, PySpark consumer, and web dashboard simultaneously:

```bash
docker compose up streaming-job ingestion-job django-dashboard -d
```

Verify the real-time processing metrics using your container log tail:

```bash
docker logs -f streaming-job
```

Open your web browser and navigate to `http://localhost:8000` to interact with your live operations monitoring engine.

![Producer & Consumer](screenshots/logs.png)

![Dashboard](screenshots/dashboard.png)

---

## Core Performance Metrics & Optimizations

### 1. Batch Retraining Engine (`training-job`)

* **Scale Profile:** Aggregates and models user tracking patterns for **10,403,453 unique sessions**.
* **Imbalance Correction:** Counteracts severe data distribution skewness ($6.7\%$ true purchase rate) by dynamically enforcing an active instance weight modifier of **13.41**.
* **Validation Threshold Sweep:** Maximizes the $F_1$ score metric on a validation dataset split to secure high operational utility:
* *Threshold = 0.50:* Precision = 0.300, Recall = 0.853, $F_1$ = 0.444
* *Threshold = 0.80:* Precision = 0.556, Recall = 0.610, **$F_1$ = 0.582 (Optimal Balance)**
* **Evaluation Telemetry:** Outperforms the linear baseline by securing an area under the ROC curve (**AUC**) of **0.9222** and a total accuracy rate of **95.03%**.

### 2. Stream-Scoring Optimization Specs (`streaming-job`)

* **Intake Rate-Limiting:** Incorporates `maxOffsetsPerTrigger=30000`. This prevents memory allocation issues during large backlog replays by capping processing data per cycle.
* **Database I/O Lightening:** Replaces multi-million row processing table scans with localized $O(1)$ scalar reads. Writes records and modifies global stats in transactional chunks of **5,000 items** to eliminate database connection timeouts.

---

## Cloud Strategy: AWS RDS Migration

Follow this production blueprint to shift your data tier to an AWS cloud-managed system:

1. **Deploy Instance:** Launch an AWS RDS instance using the engine type **MySQL Community Edition** under the AWS Free Tier. Set the inbound VPC Security Group Rules to accept active TCP traffic on port `3306` from your host gateway IP.
2. **Import Schema:** Connect your database administration workspace (e.g., DBeaver or MySQL Workbench) to your AWS RDS Endpoint and run the initialization script located in `mysql-init/init.sql`.
3. **Re-route Credentials:** Open your project `.env` file and replace local variables with your cloud database instance location coordinates:

    ```ini
    MYSQL_HOST=your-instance-id.cluster-hash.us-east-1.rds.amazonaws.com
    MYSQL_DATABASE=model_registry
    MYSQL_USER=your_aws_rds_username
    MYSQL_PASSWORD=your_aws_rds_secure_password
    ```

4. **Optimize Resource Limits:** Open your `docker-compose.yml` file and completely remove or comment out the `mysql` service container block. This frees up valuable RAM and local CPU allocation cycles. Run `docker compose up -d` to resume system functions over your cloud data storage tier.
