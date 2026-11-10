"""Archive raw events from Kafka to S3-compatible storage as hourly JSONL.gz files (at-least-once).

Temporary: fills the gap until the Spark raw -> Parquet query (Step 7) takes over.

Usage:
    uv run scripts/archive_raw.py

Environment:
    KAFKA_BOOTSTRAP         default localhost:9092 (kafka:19092 inside docker compose)
    KAFKA_TOPIC             default wikimedia.recentchange.raw
    S3_ENDPOINT             default http://localhost:8333 (http://seaweedfs:8333 inside docker compose)
    S3_ACCESS_KEY           default wikiheatmap
    S3_SECRET_KEY           default wikiheatmap-secret
    S3_BUCKET               default wikiheatmap
    FLUSH_INTERVAL_SECONDS  default 300

Output:
    s3://<bucket>/raw/recentchange/date=YYYY-MM-DD/hour=HH/<first Kafka timestamp ms>-<random>.jsonl.gz
    One Kafka message value (raw recentchange JSON) per line. Partitioned by the
    Kafka message timestamp (time the producer sent it), not by event time, so a
    late event never reopens an old hour.

Delivery guarantee:
    Offsets are committed only after every file of a flush is written. After a
    crash the uncommitted messages are read and written again: duplicates are
    possible, losses are not. Duplicates keep the same `meta.id`.
"""

import gzip
import json
import logging
import os
import signal
import sys
import time
import uuid
from collections import defaultdict
from datetime import datetime, timezone

import boto3
from confluent_kafka import Consumer, KafkaError, KafkaException

PREFIX = "raw/recentchange"
GROUP_ID = "wikiheatmap-raw-archiver"

log = logging.getLogger("archive_raw")


class Archiver:
    def __init__(self, consumer: Consumer, s3, bucket: str):
        self.consumer = consumer
        self.s3 = s3
        self.bucket = bucket
        self.buffers: dict[str, list[bytes]] = defaultdict(list)  # "date=.../hour=..." -> lines
        self.first_ts: dict[str, int] = {}
        self.pending = 0
        self.archived = 0

    def add(self, msg) -> None:
        ts_ms = msg.timestamp()[1]
        hour = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).strftime("date=%Y-%m-%d/hour=%H")
        value = msg.value()
        if b"\n" in value:  # keep one event per line
            value = json.dumps(json.loads(value), ensure_ascii=False).encode("utf-8")
        self.buffers[hour].append(value)
        self.first_ts.setdefault(hour, ts_ms)
        self.pending += 1

    def flush(self) -> None:
        """Write one file per buffered hour, then commit the consumed offsets."""
        if not self.pending:
            return
        for hour, lines in self.buffers.items():
            key = f"{PREFIX}/{hour}/{self.first_ts[hour]}-{uuid.uuid4().hex[:8]}.jsonl.gz"
            body = gzip.compress(b"\n".join(lines) + b"\n")
            self.s3.put_object(Bucket=self.bucket, Key=key, Body=body)
            log.info("Wrote %d events (%d KB) to s3://%s/%s", len(lines), len(body) // 1024, self.bucket, key)
        self.consumer.commit(asynchronous=False)
        self.archived += self.pending
        self.buffers.clear()
        self.first_ts.clear()
        self.pending = 0

    def on_revoke(self, consumer, partitions) -> None:
        # Another consumer will own these partitions: write what we have first.
        log.info("Partitions revoked, flushing")
        self.flush()


stopping = False


def request_stop(signum, frame) -> None:
    # Set a flag instead of raising: an exception between poll() and add() would
    # drop a consumed message whose offset the final flush then commits.
    global stopping
    stopping = True


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
        stream=sys.stderr,
    )
    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)

    bootstrap = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
    topic = os.environ.get("KAFKA_TOPIC", "wikimedia.recentchange.raw")
    bucket = os.environ.get("S3_BUCKET", "wikiheatmap")
    flush_interval = float(os.environ.get("FLUSH_INTERVAL_SECONDS", "300"))

    s3 = boto3.client(
        "s3",
        endpoint_url=os.environ.get("S3_ENDPOINT", "http://localhost:8333"),
        aws_access_key_id=os.environ.get("S3_ACCESS_KEY", "wikiheatmap"),
        aws_secret_access_key=os.environ.get("S3_SECRET_KEY", "wikiheatmap-secret"),
        region_name="us-east-1",
    )
    consumer = Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": GROUP_ID,
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",  # first run: archive everything Kafka still holds
    })
    archiver = Archiver(consumer, s3, bucket)
    consumer.subscribe([topic], on_revoke=archiver.on_revoke)
    log.info("Archiving %s from %s to s3://%s/%s every %.0f s", topic, bootstrap, bucket, PREFIX, flush_interval)

    last_flush = time.monotonic()
    try:
        while not stopping:
            msg = consumer.poll(1.0)
            if msg is not None:
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    raise KafkaException(msg.error())
                archiver.add(msg)
            if time.monotonic() - last_flush >= flush_interval:
                archiver.flush()
                last_flush = time.monotonic()
    finally:
        try:
            archiver.flush()
        except Exception as error:  # offsets stay uncommitted, the events are archived again next run
            log.error("Final flush failed: %s", error)
        consumer.close()
        log.info("Stopped after archiving %d events", archiver.archived)


if __name__ == "__main__":
    main()
