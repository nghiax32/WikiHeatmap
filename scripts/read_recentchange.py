"""Read the Wikimedia EventStreams `recentchange` stream and print each event as one JSON line.

Usage:
    WIKI_CONTACT="you@example.com" python scripts/read_recentchange.py > sample.jsonl

Wikimedia requires a User-Agent with contact information:
https://meta.wikimedia.org/wiki/User-Agent_policy
"""

import http.client
import json
import logging
import os
import sys
import time
import urllib.error
import urllib.request

STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"
PROGRESS_INTERVAL_SECONDS = 30
MAX_RETRY_DELAY_SECONDS = 60

log = logging.getLogger("read_recentchange")


def build_user_agent() -> str:
    contact = os.environ.get("WIKI_CONTACT")
    if not contact:
        sys.exit("Set WIKI_CONTACT (email or URL) so the User-Agent carries contact information.")
    return f"WikiHeatmap/0.1 ({contact}) python-urllib/{sys.version_info.major}.{sys.version_info.minor}"


def read_events(url: str, user_agent: str, last_event_id: str | None = None):
    """Open one connection to Wikimedia EventStreams and yield (event_id, data) for each SSE message.

    Request:
        GET https://stream.wikimedia.org/v2/stream/recentchange
        (the `url` argument; the optional `?since=<ISO 8601 or ms timestamp>` query
        parameter is supported by the server but not used here)

        Headers:
            User-Agent:    required by https://meta.wikimedia.org/wiki/User-Agent_policy,
                           must carry contact information.
            Accept:        text/event-stream
            Last-Event-ID: optional, the `id` of the last event received. The server
                           resumes right after it (at-least-once on reconnect).

    Response:
        HTTP 200, `Content-Type: text/event-stream; charset=utf-8`, an endless stream
        of Server-Sent Events. The server closes the connection after about 15 minutes;
        the caller is expected to reconnect with `Last-Event-ID`.

        SSE format: lines of `field: value`, a blank line ends one message,
        lines starting with `:` are comments (the stream starts with `:ok`):

            event: message
            id: [{"topic":"eqiad.mediawiki.recentchange","partition":0,"offset":-1},
                 {"topic":"codfw.mediawiki.recentchange","partition":0,"timestamp":1794211950845}]
            data: {"$schema":"/mediawiki/recentchange/1.0.0","meta":{...},"type":"edit",...}

        `id` is a JSON array with one Kafka position (offset or timestamp) per
        datacenter topic; it is opaque to the client and only sent back as Last-Event-ID.

        `data` is one JSON recentchange event (schema /mediawiki/recentchange/1.0.x).
        Fields used in this project:
            meta.id, meta.dt (event time, ISO 8601 UTC), meta.domain ("canary" for
            monitoring events), type (edit | new | log | categorize), wiki, server_name,
            namespace, title, user, bot, comment, timestamp (Unix seconds),
            length.old/new and revision.old/new (edit/new only), log_type (log only).

    Args:
        url: stream URL.
        user_agent: User-Agent header value with contact information.
        last_event_id: `id` of the last received event, or None to start from now.

    Yields:
        (event_id, data): `event_id` is the raw `id` string or None if the message
        has none; `data` is the raw JSON text (multi-line `data:` fields joined by "\\n").
        The generator ends when the server closes the connection.

    Raises:
        urllib.error.HTTPError: non-2xx response (e.g. 403 for a rejected User-Agent).
        OSError, http.client.HTTPException: network errors, including a read
            timeout of 60 s without data.
    """
    headers = {"User-Agent": user_agent, "Accept": "text/event-stream"}
    if last_event_id:
        headers["Last-Event-ID"] = last_event_id
    request = urllib.request.Request(url, headers=headers)
    log.info("Connecting to %s%s", url, " (resuming from Last-Event-ID)" if last_event_id else "")
    with urllib.request.urlopen(request, timeout=60) as response:
        log.info("Connected (HTTP %s)", response.status)
        event_id, data_lines = None, []
        for raw in response:
            line = raw.decode("utf-8").rstrip("\r\n")
            if not line:
                if data_lines:
                    yield event_id, "\n".join(data_lines)
                event_id, data_lines = None, []
                continue
            if line.startswith(":"):
                continue
            field, _, value = line.partition(":")
            value = value.removeprefix(" ")
            if field == "data":
                data_lines.append(value)
            elif field == "id":
                event_id = value


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
        stream=sys.stderr,
    )
    user_agent = build_user_agent()
    count = 0
    start = last_progress = time.monotonic()
    last_progress_count = 0
    last_event_id = None
    retry_delay = 1
    try:
        while True:
            try:
                for event_id, data in read_events(STREAM_URL, user_agent, last_event_id):
                    if event_id:
                        last_event_id = event_id
                    retry_delay = 1
                    try:
                        event = json.loads(data)
                    except json.JSONDecodeError:
                        log.warning("Skipping non-JSON data: %r", data[:200])
                        continue
                    print(json.dumps(event, ensure_ascii=False), flush=True)
                    count += 1
                    now = time.monotonic()
                    if now - last_progress >= PROGRESS_INTERVAL_SECONDS:
                        interval = count - last_progress_count
                        log.info("Read %d events (+%d in last %.0f s, %.1f events/s)",
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
            log.info("Reconnecting in %d s", retry_delay)
            time.sleep(retry_delay)
            retry_delay = min(retry_delay * 2, MAX_RETRY_DELAY_SECONDS)
    except KeyboardInterrupt:
        pass
    finally:
        log.info("Stopped after %d events in %.0f s", count, time.monotonic() - start)


if __name__ == "__main__":
    main()
