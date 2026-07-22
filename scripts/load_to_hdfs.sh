#!/usr/bin/env bash
# Loads the two Kaggle CSV months from the host's ./data/raw folder into HDFS.
# Run this AFTER `docker compose up namenode datanode -d` and after both
# report healthy (`docker compose ps`).
#
# Expects, on the host, before running:
#   ./data/raw/2019-10/2019-Oct.csv   (unzipped from 2019-Oct.csv.gz)
#   ./data/raw/2019-11/2019-Nov.csv   (unzipped from 2019-Nov.csv.gz)
#
# The ./data/raw folder is bind-mounted into the namenode container at
# /data/raw (see docker-compose.yml), so we exec into namenode and use the
# hdfs CLI directly rather than copying the multi-GB files a second time.

set -euo pipefail

echo "Creating HDFS directory structure..."
docker exec namenode hdfs dfs -mkdir -p /data/raw/2019-10
docker exec namenode hdfs dfs -mkdir -p /data/raw/2019-11
docker exec namenode hdfs dfs -mkdir -p /data/streaming
docker exec namenode hdfs dfs -mkdir -p /models/champion
docker exec namenode hdfs dfs -mkdir -p /models/candidates
docker exec namenode hdfs dfs -mkdir -p /checkpoints

echo "Loading October (training month) into HDFS..."
docker exec namenode hdfs dfs -put -f /data/raw/2019-10/2019-Oct.csv /data/raw/2019-10/2019-Oct.csv

echo "Loading November (live-replay month) into HDFS..."
docker exec namenode hdfs dfs -put -f /data/raw/2019-11/2019-Nov.csv /data/raw/2019-11/2019-Nov.csv

echo "Verifying..."
docker exec namenode hdfs dfs -ls -h /data/raw/2019-10
docker exec namenode hdfs dfs -ls -h /data/raw/2019-11

echo "Done. Raw CSVs are in HDFS. Convert to Parquet as the first step of"
echo "training-job/train.py (partitioned by event_type) rather than reading"
echo "CSV directly on every run."