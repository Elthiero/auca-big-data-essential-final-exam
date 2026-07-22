#!/usr/bin/env bash
# Run once, after `docker compose up kafka -d` and kafka reports healthy.
# Explicit creation (rather than relying on Kafka's auto-create-on-produce
# default) lets us control partition count up front — 3 partitions gives
# the Streaming Job room to parallelize consumption later without a
# repartition migration.

set -euo pipefail

TOPIC=${KAFKA_TOPIC:-clickstream-events}
PARTITIONS=${KAFKA_PARTITIONS:-3}

echo "Creating Kafka topic '$TOPIC' with $PARTITIONS partitions..."
docker exec kafka kafka-topics.sh --create \
  --topic "$TOPIC" \
  --bootstrap-server localhost:9092 \
  --partitions "$PARTITIONS" \
  --replication-factor 1 \
  --if-not-exists

echo "Verifying..."
docker exec kafka kafka-topics.sh --describe \
  --topic "$TOPIC" \
  --bootstrap-server localhost:9092
