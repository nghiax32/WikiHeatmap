"""Stream Wikimedia EventStreams `recentchange` into Kafka (raw events, at-least-once).

Usage:
    WIKI_CONTACT="you@example.com" uv run scripts/produce_recentchange.py

Environment:
    WIKI_CONTACT     required, contact information for the User-Agent
    KAFKA_BOOTSTRAP  default localhost:9092 (kafka:19092 inside docker compose)
    KAFKA_TOPIC      default wikimedia.recentchange.raw
    STATE_FILE       default state/last_event_id, where the resume position is kept

Delivery guarantee:
    The SSE `id` of the last event is written to STATE_FILE only after
    producer.flush() confirms that every event before it is stored in Kafka.
    After a crash the producer resumes from that checkpoint, so events since the
    checkpoint are sent again: duplicates are possible, losses are not.
    Duplicates keep the same `meta.id` and are dropped downstream.
"""

import http.client
import json
import logging
import os
import signal
import sys
import time
import urllib.error

from confluent_kafka import KafkaException, Producer

from read_recentchange import STREAM_URL, build_user_agent, read_events

CHECKPOINT_INTERVAL_SECONDS = 5
PROGRESS_INTERVAL_SECONDS = 30
MAX_RETRY_DELAY_SECONDS = 60

log = logging.getLogger("produce_recentchange")


def partition_key(event: dict) -> bytes | None:
    """`wiki:title`, or None for events without a page (canary)."""
    wiki, title = event.get("wiki"), event.get("title")
    if wiki is None or title is None:
        return None
    return f"{wiki}:{title}".encode("utf-8")


def load_last_event_id(path: str) -> str | None:
    try:
        with open(path, encoding="utf-8") as f:
            return f.read().strip() or None
    except FileNotFoundError:
        return None


def save_last_event_id(path: str, event_id: str) -> None:
    """Write atomically, so a crash mid-write never leaves a broken file."""
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(event_id)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


class Checkpointer:
    """Tracks the last read SSE id and persists it once Kafka has acknowledged everything before it."""

    def __init__(self, producer: Producer, path: str):
        self.producer = producer
        self.path = path
        self.pending_id: str | None = None
        self.saved_id: str | None = None
        self.failed = 0
        self.last_time = time.monotonic()

    def on_delivery(self, err, msg) -> None:
        if err is not None:
            self.failed += 1
            log.error("Delivery failed: %s", err)

    def checkpoint(self) -> None:
        self.last_time = time.monotonic()
        remaining = self.producer.flush(30)
        if remaining:
            raise KafkaException(f"{remaining} messages still not delivered after 30 s")
        if self.failed:
            raise KafkaException(f"{self.failed} messages failed delivery; not advancing the checkpoint")
        if self.pending_id and self.pending_id != self.saved_id:
            save_last_event_id(self.path, self.pending_id)
            self.saved_id = self.pending_id

    def due(self) -> bool:
        return time.monotonic() - self.last_time >= CHECKPOINT_INTERVAL_SECONDS


def handle_sigterm(signum, frame) -> None:
    # `docker stop` sends SIGTERM: stop like Ctrl+C so the final checkpoint runs.
    raise KeyboardInterrupt


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
        stream=sys.stderr,
    )
    signal.signal(signal.SIGTERM, handle_sigterm)

    user_agent = build_user_agent()
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
    topic = os.environ.get("KAFKA_TOPIC", "wikimedia.recentchange.raw")
    state_file = os.environ.get("STATE_FILE", "state/last_event_id")

    producer = Producer({
        "bootstrap.servers": bootstrap,
        "client.id": "wikiheatmap-producer",
        "enable.idempotence": True,  # implies acks=all, retries without duplicates or reordering per partition
        "acks": "all",
        "compression.type": "zstd",
        "linger.ms": 50,
    })
    checkpointer = Checkpointer(producer, state_file)
    last_event_id = load_last_event_id(state_file)
    checkpointer.saved_id = last_event_id
    log.info("Producing to %s on %s; %s", topic, bootstrap,
             f"resuming from {state_file}" if last_event_id else "no saved position, starting from now")

    count = 0
    start = last_progress = time.monotonic()
    last_progress_count = 0
    retry_delay = 1
    try:
        while True:
            try:
                for event_id, data in read_events(STREAM_URL, user_agent, last_event_id):
                    retry_delay = 1
                    try:
                        event = json.loads(data)
                    except json.JSONDecodeError:
                        log.warning("Skipping non-JSON data: %r", data[:200])
                        continue
                    while True:
                        try:
                            # Value is the raw JSON text as received, not re-serialized.
                            producer.produce(topic, value=data.encode("utf-8"), key=partition_key(event),
                                             on_delivery=checkpointer.on_delivery)
                            break
                        except BufferError:
                            producer.poll(1)  # local queue full: wait for deliveries
                    producer.poll(0)
                    count += 1
                    if event_id:
                        last_event_id = event_id
                        checkpointer.pending_id = event_id
                    if checkpointer.due():
                        checkpointer.checkpoint()
                    now = time.monotonic()
                    if now - last_progress >= PROGRESS_INTERVAL_SECONDS:
                        interval = count - last_progress_count
                        log.info("Produced %d events (+%d in last %.0f s, %.1f events/s)",
                                 count, interval, now - last_progress, interval / (now - last_progress))
                        last_progress, last_progress_count = now, count
                log.info("Connection closed by server")
            except urllib.error.HTTPError as error:
                if 400 <= error.code < 500 and error.code != 429:
                    log.error("HTTP %s %s, not retrying", error.code, error.reason)
                    break
                log.warning("HTTP %s %s", error.code, error.reason)
            except (OSError, http.client.HTTPException) as error:
                log.warning("Connection error: %s", error)
            checkpointer.checkpoint()
            log.info("Reconnecting in %d s", retry_delay)
            time.sleep(retry_delay)
            retry_delay = min(retry_delay * 2, MAX_RETRY_DELAY_SECONDS)
    except KeyboardInterrupt:
        pass
    finally:
        try:
            checkpointer.checkpoint()
        except KafkaException as error:
            log.error("Final checkpoint failed, last saved position kept: %s", error)
        log.info("Stopped after %d events in %.0f s", count, time.monotonic() - start)


if __name__ == "__main__":
    main()
