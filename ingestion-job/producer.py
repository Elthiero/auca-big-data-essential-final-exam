"""
ingestion-job — Data Producer: replays historical CSV rows from HDFS into
Kafka at a controlled rate, simulating a live clickstream.

Deliberately does NOT use PySpark. This job does no aggregation and no
ML — it reads rows sequentially and publishes them one at a time, which
a plain Python WebHDFS client + Kafka producer handles fine, without
the JRE/JVM overhead a Spark container would carry.

Design notes:
- Reads via HDFS WebHDFS (REST API), streaming line-by-line — the CSV
  is never fully loaded into memory, regardless of file size.
- Assumes the source CSV is already time-ordered (true for this
  dataset — verified against the Kaggle sample). If that assumption
  ever breaks for a different dataset, a full sort would be needed
  before replay, which is a genuinely expensive operation.
- Kafka message key = user_session, so all events for a given session
  land on the same partition — this matters once the Streaming Job
  needs to track state per session in order.
- MAX_EVENTS lets you bound a replay run for demo purposes. Replaying
  an entire month at any reasonable rate takes hours — see the sizing
  note in the README / chat for the math.
"""

import os
import csv
import json
import time
import socket

import requests
from requests.adapters import HTTPAdapter
from hdfs import InsecureClient
from confluent_kafka import Producer

# Global socket default as a backstop — covers any blocking network call
# (including inside kafka-python, which uses raw sockets) that wouldn't
# otherwise respect a per-library timeout setting.
socket.setdefaulttimeout(60)

# Environment variables (required for containerized deployment)
HDFS_WEBHDFS_URL = os.environ.get("HDFS_WEBHDFS_URL", "http://namenode:9870")
HDFS_USER = os.environ.get("HDFS_USER", "root")
HDFS_SOURCE_PATH = os.environ["HDFS_SOURCE_PATH"]  # e.g. /data/raw/2019-11
RAW_CSV_FILENAME = os.environ.get("RAW_CSV_FILENAME", "2019-Nov.csv")

KAFKA_BOOTSTRAP = os.environ["KAFKA_BOOTSTRAP"]
KAFKA_TOPIC = os.environ["KAFKA_TOPIC"]

REPLAY_RATE_EVENTS_PER_SEC = float(os.environ.get("REPLAY_RATE_EVENTS_PER_SEC", "500"))
# 0 = unlimited (replay the whole file). Set to a small number for a
# fast demo run rather than waiting hours for a full month to finish.
MAX_EVENTS = int(os.environ.get("MAX_EVENTS", "0"))

PROGRESS_EVERY = 500


class TimeoutHTTPAdapter(HTTPAdapter):
    """
    Without this, the hdfs WebHDFS client has NO timeout at all — a
    stalled connection (redirect to an unreachable DataNode, a hung
    NameNode response, etc.) hangs forever with zero error output,
    which is exactly the "stuck with no traceback" failure mode this
    project kept hitting. This forces every HTTP call through the
    client to fail loudly after a bounded wait instead.
    """

    def __init__(self, *args, timeout=30, **kwargs):
        self.timeout = timeout
        super().__init__(*args, **kwargs)

    def send(self, request, **kwargs):
        kwargs.setdefault("timeout", self.timeout)
        return super().send(request, **kwargs)


def build_hdfs_client():
    session = requests.Session()
    session.mount("http://", TimeoutHTTPAdapter(timeout=30))
    session.mount("https://", TimeoutHTTPAdapter(timeout=30))
    return InsecureClient(HDFS_WEBHDFS_URL, user=HDFS_USER, session=session)


# --- Edge Sanitization & Data Parsing Optimization ---

def safe_int(val):
    """
    Safely casts strings to integers. Strips whitespace, normalizes common
    database null representations, and uses a cascading float cast to
    safely resolve decimal strings like '101.0' into 101 without throwing a ValueError.
    """
    if val is None:
        return None
    clean_val = str(val).strip().lower()
    if not clean_val or clean_val in ("null", "none", "n/a"):
        return None
    try:
        return int(float(clean_val))
    except ValueError:
        print(f"[producer.py] WARNING: Failed to cast integer payload: '{val}'")
        return None


def safe_float(val):
    """
    Safely casts strings to floats. Strips whitespace and normalizes
    common database null representations.
    """
    if val is None:
        return None
    clean_val = str(val).strip().lower()
    if not clean_val or clean_val in ("null", "none", "n/a"):
        return None
    try:
        return float(clean_val)
    except ValueError:
        print(f"[producer.py] WARNING: Failed to cast float payload: '{val}'")
        return None


def row_to_event(row):
    """
    Maps raw CSV rows into an optimized, sanitized Python dictionary structure
    suitable for upstream JSON serialization and schema-strict Spark Streaming ingest.
    """
    (
        event_time,
        event_type,
        product_id,
        category_id,
        category_code,
        brand,
        price,
        user_id,
        user_session,
    ) = row
    return {
        "event_time": event_time,
        "event_type": event_type,
        "product_id": safe_int(product_id),
        "category_id": safe_int(category_id),
        "category_code": category_code or None,
        "brand": brand or None,
        "price": safe_float(price),
        "user_id": safe_int(user_id),
        "user_session": user_session,
    }


delivery_failures = {"count": 0}


def delivery_callback(err, msg):
    """
    confluent-kafka calls this per message once delivery is confirmed
    OR permanently failed. Unlikely kafka-python, failures actually
    surface here instead of retrying silently forever in the
    background — this is the core reliability difference that fixed
    the original hang.
    """
    if err is not None:
        delivery_failures["count"] += 1
        if delivery_failures["count"] <= 5:  # avoid flooding logs
            print(f"[producer.py] Delivery failed: {err}")


def build_producer():
    return Producer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "linger.ms": 50,
            "acks": "1",
            "queue.buffering.max.kbytes": 65536,  # 64MB, matches prior sizing
            "socket.timeout.ms": 30000,
            "message.timeout.ms": 60000,  # bounded — a message that can't be
            # delivered within 60s fails loudly
            # via the callback instead of
            # retrying forever.
        }
    )


def main():
    client = build_hdfs_client()
    csv_path = f"{HDFS_SOURCE_PATH}/{RAW_CSV_FILENAME}"

    delay = (
        (1.0 / REPLAY_RATE_EVENTS_PER_SEC) if REPLAY_RATE_EVENTS_PER_SEC > 0 else 0.0
    )

    print(f"[producer.py] Source: hdfs://{csv_path}")
    print(f"[producer.py] Target: kafka topic '{KAFKA_TOPIC}' @ {KAFKA_BOOTSTRAP}")
    print(
        f"[producer.py] Rate: {REPLAY_RATE_EVENTS_PER_SEC}/sec"
        + (
            f", capped at {MAX_EVENTS} events"
            if MAX_EVENTS > 0
            else ", no cap (full file)"
        )
    )

    producer_start = time.time()
    producer = build_producer()
    print(f"[producer.py] Kafka producer ready in {time.time() - producer_start:.1f}s")
    sent = 0
    skipped = 0
    start = time.time()

    with client.read(csv_path, encoding="utf-8") as reader:
        print("[producer.py] HDFS stream opened, reading header row...")
        csv_reader = csv.reader(reader)
        next(csv_reader)  # skip header row
        print(
            "[producer.py] Header skipped, beginning replay (first progress "
            f"line in ~{PROGRESS_EVERY} events)..."
        )

        for row in csv_reader:
            if len(row) != 9:
                skipped += 1
                continue

            event = row_to_event(row)
            key_bytes = (
                event["user_session"].encode("utf-8") if event["user_session"] else None
            )
            value_bytes = json.dumps(event).encode("utf-8")

            if sent == 0:
                first_send_start = time.time()
            producer.produce(
                KAFKA_TOPIC,
                key=key_bytes,
                value=value_bytes,
                callback=delivery_callback,
            )
            if sent == 0:
                print(
                    f"[producer.py] First produce() call returned in "
                    f"{time.time() - first_send_start:.1f}s"
                )

            # poll(0) is not optional — it's what actually services
            # delivery callbacks and drains the internal librdkafka
            # queue. Skipping this is the confluent-kafka equivalent of
            # the earlier missing-flush bug: the queue fills silently
            # until produce() itself starts blocking.
            producer.poll(0)

            sent += 1

            if delay:
                time.sleep(delay)

            if sent % PROGRESS_EVERY == 0:
                # Periodic flush — without this, send() only queues
                # messages into the producer's in-memory buffer, and a
                # slow/lagging background sender thread lets that buffer
                # fill up until it runs out of room (the KafkaTimeoutError
                # this project hit at ~80k events). Flushing regularly
                # bounds buffer growth AND ensures messages are actually
                # delivered promptly, not just "queued," so downstream
                # consumers see them without a long invisible lag.
                producer.flush(timeout=30)
                elapsed = time.time() - start
                print(
                    f"[producer.py] Sent {sent} events "
                    f"({sent / elapsed:.1f}/sec actual, {skipped} skipped, "
                    f"{delivery_failures['count']} delivery failures)"
                )

            if MAX_EVENTS and sent >= MAX_EVENTS:
                print(f"[producer.py] MAX_EVENTS={MAX_EVENTS} reached, stopping.")
                break

    remaining = producer.flush(timeout=60)
    if remaining > 0:
        print(
            f"[producer.py] WARNING: {remaining} messages still undelivered after final flush."
        )

    elapsed = time.time() - start
    print(
        f"[producer.py] Done. Sent {sent} events, skipped {skipped} malformed rows, "
        f"{delivery_failures['count']} delivery failures, in {elapsed:.1f}s "
        f"({sent / max(elapsed, 0.001):.1f}/sec average)."
    )


if __name__ == "__main__":
    main()
