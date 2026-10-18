#!/usr/bin/env python3
"""
cmd/consumer.py  --  Phase-3 YAK Broker Consumer.

Flags / env vars
----------------
--consumer-id     ID string               (CONSUMER_ID,    default consumer-1)
--seeds           host:port,...           (SEEDS,          default localhost:8001,...:8005)
--topics          topic1,topic2,...       (TOPICS,         default orders,payments)
--partitions      0,1,...                 (PARTITIONS,     default 0,1)
--poll-interval   seconds between polls   (POLL_INTERVAL,  default 0.5)
--max-records     max records per fetch   (MAX_RECORDS,    default 10)
--start-position  earliest|latest         (START_POSITION, default earliest)
--data-dir        storage directory       (DATA_DIR,       default data/<consumer-id>)
--offset-file     path to offsets.json    (OFFSET_FILE,    default <data-dir>/offsets.json)
--processed-log   path to processed log   (PROCESSED_LOG,  default <data-dir>/consumer-<id>-processed.log)
--status-port     HTTP status port; 0=off (STATUS_PORT,    default 0)
--status-host     HTTP status host        (STATUS_HOST,    default 0.0.0.0)
--log-level       DEBUG/INFO/WARNING      (LOG_LEVEL,      default INFO)
--run-once        poll each part once     (RUN_ONCE,       default false)
--max-polls       exit after N poll loops (MAX_POLLS,      default 0 = infinite)
"""
from __future__ import annotations

import argparse
import http.server
import json
import logging
import os
import signal
import sys
import threading
import time
from typing import Any, Dict, List, Optional, Tuple

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from client.discovery import LeaderDiscovery

logger = logging.getLogger("consumer")


# ---------------------------------------------------------------------------
# OffsetStore — atomic persistence with crash safety & fail-fast
# ---------------------------------------------------------------------------

class OffsetStore:
    """
    Manages consumer offsets with atomic disk writes.
    Stored offset = NEXT offset to read (i.e. last_processed + 1).
    Fails fast if the file exists but contains invalid JSON.
    """

    def __init__(self, file_path: str):
        self.file_path = file_path
        self._lock = threading.Lock()
        self._offsets: Dict[str, int] = {}
        self._load()

    def _load(self) -> None:
        if not os.path.exists(self.file_path):
            self._offsets = {}
            return

        try:
            with open(self.file_path, "r", encoding="utf-8") as f:
                content = f.read().strip()
                if not content:
                    self._offsets = {}
                    return
                data = json.loads(content)
                if not isinstance(data, dict):
                    raise ValueError(f"Offset file content is not a dict: {type(data)}")
                self._offsets = {str(k): int(v) for k, v in data.items()}
                logger.info("Loaded %d offsets from %s", len(self._offsets), self.file_path)
        except Exception as exc:
            logger.critical("FATAL: Corrupt offset file at %s: %s", self.file_path, exc)
            raise RuntimeError(f"Corrupt offset file {self.file_path}: {exc}") from exc

    def get_offset(self, topic: str, partition: int) -> Optional[int]:
        key = f"{topic}/{partition}"
        with self._lock:
            return self._offsets.get(key)

    def commit(self, topic: str, partition: int, next_offset: int) -> None:
        key = f"{topic}/{partition}"
        with self._lock:
            self._offsets[key] = next_offset
            self._persist_atomic()

    def _persist_atomic(self) -> None:
        dir_name = os.path.dirname(os.path.abspath(self.file_path))
        os.makedirs(dir_name, exist_ok=True)
        tmp_path = f"{self.file_path}.tmp.{os.getpid()}_{threading.get_ident()}"
        try:
            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(self._offsets, f, indent=2)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp_path, self.file_path)
        except Exception as exc:
            if os.path.exists(tmp_path):
                try:
                    os.remove(tmp_path)
                except Exception:
                    pass
            logger.error("Failed to atomically persist offsets to %s: %s", self.file_path, exc)
            raise

    def get_all(self) -> Dict[str, int]:
        with self._lock:
            return dict(self._offsets)


# ---------------------------------------------------------------------------
# ProcessedLog — append-only record tracking
# ---------------------------------------------------------------------------

class ProcessedLog:
    """Append-only fsynced JSONL log of all records processed by this consumer."""

    def __init__(self, file_path: str):
        self.file_path = file_path
        self._lock = threading.Lock()
        dir_name = os.path.dirname(os.path.abspath(file_path))
        os.makedirs(dir_name, exist_ok=True)
        self._fh = open(file_path, "a", encoding="utf-8")
        logger.info("ProcessedLog opened at %s", file_path)

    def record(self, entry: Dict[str, Any]) -> None:
        with self._lock:
            self._fh.write(json.dumps(entry) + "\n")
            self._fh.flush()
            os.fsync(self._fh.fileno())

    def close(self) -> None:
        with self._lock:
            try:
                self._fh.close()
            except Exception:
                pass


# ---------------------------------------------------------------------------
# Status HTTP Server
# ---------------------------------------------------------------------------

class ConsumerStatusHandler(http.server.BaseHTTPRequestHandler):
    consumer_ref: Optional["Consumer"] = None

    def log_message(self, format: str, *args: Any) -> None:
        # Suppress noisy HTTP request logging
        pass

    def do_GET(self) -> None:
        if self.path in ("/status", "/"):
            status_data = self.consumer_ref.get_status() if self.consumer_ref else {}
            body = json.dumps(status_data, indent=2).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        elif self.path == "/healthz":
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(b"OK\n")
        else:
            self.send_response(404)
            self.end_headers()


# ---------------------------------------------------------------------------
# Consumer
# ---------------------------------------------------------------------------

class Consumer:
    def __init__(
        self,
        consumer_id: str,
        seeds: List[str],
        topics: List[str],
        partitions: List[int],
        poll_interval: float = 0.5,
        max_records: int = 10,
        start_position: str = "earliest",
        data_dir: str = "",
        offset_file: str = "",
        processed_log: str = "",
        status_port: int = 0,
        status_host: str = "0.0.0.0",
        run_once: bool = False,
        max_polls: int = 0,
    ):
        self.consumer_id = consumer_id
        self.seeds = seeds
        self.topics = [t.strip() for t in topics if t.strip()]
        self.partitions = sorted(partitions)
        self.poll_interval = poll_interval
        self.max_records = max_records
        self.start_position = start_position.lower()
        self.run_once = run_once
        self.max_polls = max_polls

        if not data_dir:
            data_dir = os.path.join("data", consumer_id)
        self.data_dir = data_dir
        os.makedirs(self.data_dir, exist_ok=True)

        if not offset_file:
            offset_file = os.path.join(self.data_dir, "offsets.json")
        self.offset_file = offset_file

        if not processed_log:
            processed_log = os.path.join(self.data_dir, f"{consumer_id}-processed.log")
        self.processed_log_path = processed_log

        self.offset_store = OffsetStore(self.offset_file)
        self.processed_log = ProcessedLog(self.processed_log_path)
        self.discovery = LeaderDiscovery(self.seeds, client_id=consumer_id)

        # In-memory status tracking: {"topic/partition": {"offset": o, "hwm": h, "lag": l}}
        self._part_status: Dict[str, Dict[str, Any]] = {}
        self._status_lock = threading.Lock()
        self.total_processed = 0

        self._running = False
        self._shutdown_event = threading.Event()

        # Status HTTP server
        self.status_port = status_port
        self.status_host = status_host
        self._status_server: Optional[http.server.HTTPServer] = None

    def start_status_server(self) -> None:
        if self.status_port <= 0:
            return

        class BoundHandler(ConsumerStatusHandler):
            consumer_ref = self

        try:
            self._status_server = http.server.HTTPServer((self.status_host, self.status_port), BoundHandler)
            thread = threading.Thread(target=self._status_server.serve_forever, daemon=True, name="status-http")
            thread.start()
            logger.info("[%s] Status HTTP server listening on %s:%d", self.consumer_id, self.status_host, self.status_port)
        except Exception as exc:
            logger.warning("[%s] Failed to start status HTTP server on %d: %s", self.consumer_id, self.status_port, exc)

    def stop_status_server(self) -> None:
        if self._status_server:
            try:
                self._status_server.shutdown()
            except Exception:
                pass

    def get_status(self) -> Dict[str, Any]:
        with self._status_lock:
            parts = dict(self._part_status)
        return {
            "consumer_id": self.consumer_id,
            "subscriptions": self.topics,
            "partitions": parts,
            "total_processed": self.total_processed,
            "offsets": self.offset_store.get_all(),
            "time": time.time(),
        }

    def _resolve_start_offset(self, topic: str, partition: int) -> int:
        """Resolve starting offset if partition is not yet in OffsetStore."""
        stored = self.offset_store.get_offset(topic, partition)
        if stored is not None:
            return stored

        if self.start_position == "earliest":
            logger.info("[%s] [%s/%d] Fresh partition: starting at offset 0 (earliest)", self.consumer_id, topic, partition)
            return 0

        # "latest": Query leader for current HWM
        try:
            status_code, body = self.discovery.send_request(
                f"/fetch?topic={topic}&partition={partition}&offset=0&max=1",
                method="GET",
                timeout=4.0,
            )
            if status_code == 200:
                hwm = body.get("hwm", 0)
                logger.info("[%s] [%s/%d] Fresh partition: starting at HWM %d (latest)", self.consumer_id, topic, partition, hwm)
                self.offset_store.commit(topic, partition, hwm)
                return hwm
        except Exception as exc:
            logger.warning("[%s] [%s/%d] Could not fetch HWM for latest start: %s; defaulting to 0", self.consumer_id, topic, partition, exc)
        return 0

    def poll_partition(self, topic: str, partition: int) -> int:
        """
        Poll single topic-partition once.
        Returns number of newly processed records.
        """
        key = f"{topic}/{partition}"
        current_offset = self._resolve_start_offset(topic, partition)

        endpoint = f"/fetch?topic={topic}&partition={partition}&offset={current_offset}&max={self.max_records}"
        try:
            status_code, body = self.discovery.send_request(endpoint, method="GET", timeout=5.0)
        except Exception as exc:
            logger.warning("[%s] [%s] Fetch failed: %s", self.consumer_id, key, exc)
            return 0

        if status_code == 404:
            logger.debug("[%s] Unknown topic %s (404)", self.consumer_id, topic)
            return 0
        if status_code != 200:
            logger.warning("[%s] [%s] Fetch returned status %d: %s", self.consumer_id, key, status_code, body)
            return 0

        records: List[Dict[str, Any]] = body.get("records", [])
        hwm = body.get("hwm", current_offset)
        lag = max(0, hwm - current_offset)

        with self._status_lock:
            self._part_status[key] = {
                "offset": current_offset,
                "hwm": hwm,
                "lag": lag,
            }

        if not records:
            return 0

        processed_count = 0
        next_offset = current_offset

        for rec in records:
            rec_offset = rec.get("offset")
            if rec_offset is None:
                continue

            # Redelivery check: ignore records below current stored offset
            if rec_offset < current_offset:
                logger.debug("[%s] [%s] Skipping duplicate/re-delivered offset %d (want >= %d)",
                             self.consumer_id, key, rec_offset, current_offset)
                continue

            # Log to processed JSONL
            entry = {
                "consumer_id": self.consumer_id,
                "topic": topic,
                "partition": partition,
                "offset": rec_offset,
                "messageID": rec.get("messageID"),
                "key": rec.get("key"),
                "payload": rec.get("payload"),
                "timestamp": rec.get("timestamp"),
                "raftIndex": rec.get("raftIndex"),
                "processed_at": time.time(),
            }
            self.processed_log.record(entry)

            next_offset = rec_offset + 1
            processed_count += 1
            self.total_processed += 1

            logger.info(
                "[%s] CONSUMED [%s/%d] off=%d msgId=%s key=%s (lag=%d)",
                self.consumer_id, topic, partition, rec_offset,
                rec.get("messageID"), rec.get("key"), max(0, hwm - next_offset)
            )

        if processed_count > 0:
            # Commit offset after successfully logging all records in batch
            self.offset_store.commit(topic, partition, next_offset)
            with self._status_lock:
                self._part_status[key] = {
                    "offset": next_offset,
                    "hwm": hwm,
                    "lag": max(0, hwm - next_offset),
                }

        return processed_count

    def run(self) -> None:
        """Main polling loop."""
        self._running = True
        self.start_status_server()

        # Build list of (topic, partition) targets
        targets: List[Tuple[str, int]] = []
        for topic in self.topics:
            for part in self.partitions:
                targets.append((topic, part))

        logger.info(
            "[%s] Started consumer subscriptions=%s partitions=%s targets=%d poll_interval=%.2fs",
            self.consumer_id, self.topics, self.partitions, len(targets), self.poll_interval
        )

        poll_loop_count = 0
        try:
            while self._running and not self._shutdown_event.is_set():
                poll_loop_count += 1
                total_in_round = 0

                for topic, part in targets:
                    if not self._running or self._shutdown_event.is_set():
                        break
                    fetched = self.poll_partition(topic, part)
                    total_in_round += fetched

                if self.run_once:
                    logger.info("[%s] --run-once requested; exiting after initial poll round.", self.consumer_id)
                    break

                if self.max_polls > 0 and poll_loop_count >= self.max_polls:
                    logger.info("[%s] Reached max polls (%d); exiting.", self.consumer_id, self.max_polls)
                    break

                # Sleep interval between poll rounds
                self._shutdown_event.wait(self.poll_interval)

        except KeyboardInterrupt:
            logger.info("[%s] Interrupted by user", self.consumer_id)
        finally:
            self.close()

    def close(self) -> None:
        self._running = False
        self._shutdown_event.set()
        self.stop_status_server()
        self.processed_log.close()
        logger.info("[%s] Consumer closed. Total records processed: %d", self.consumer_id, self.total_processed)


# ---------------------------------------------------------------------------
# CLI & Entrypoint
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Phase-3 YAK Broker Consumer")

    parser.add_argument(
        "--consumer-id",
        default=os.environ.get("CONSUMER_ID", "consumer-1"),
        help="Unique consumer identifier",
    )
    parser.add_argument(
        "--seeds",
        default=os.environ.get("SEEDS", "localhost:8001,localhost:8002,localhost:8003,localhost:8004,localhost:8005"),
        help="Comma-separated seed broker host:port list",
    )
    parser.add_argument(
        "--topics",
        default=os.environ.get("TOPICS", "orders,payments"),
        help="Comma-separated topics to subscribe to",
    )
    parser.add_argument(
        "--partitions",
        default=os.environ.get("PARTITIONS", "0,1"),
        help="Comma-separated partition numbers to consume",
    )
    parser.add_argument(
        "--poll-interval",
        type=float,
        default=float(os.environ.get("POLL_INTERVAL", "0.5")),
        help="Seconds between poll passes",
    )
    parser.add_argument(
        "--max-records",
        type=int,
        default=int(os.environ.get("MAX_RECORDS", "10")),
        help="Maximum records to request in a single /fetch call",
    )
    parser.add_argument(
        "--start-position",
        choices=["earliest", "latest"],
        default=os.environ.get("START_POSITION", "earliest"),
        help="Starting position for unseen partitions",
    )
    parser.add_argument(
        "--data-dir",
        default=os.environ.get("DATA_DIR", ""),
        help="Data directory for offsets and processed log",
    )
    parser.add_argument(
        "--offset-file",
        default=os.environ.get("OFFSET_FILE", ""),
        help="Path to offsets.json (defaults to <data-dir>/offsets.json)",
    )
    parser.add_argument(
        "--processed-log",
        default=os.environ.get("PROCESSED_LOG", ""),
        help="Path to processed JSONL log",
    )
    parser.add_argument(
        "--status-port",
        type=int,
        default=int(os.environ.get("STATUS_PORT", "0")),
        help="Port for local HTTP status server (0 = disabled)",
    )
    parser.add_argument(
        "--status-host",
        default=os.environ.get("STATUS_HOST", "0.0.0.0"),
        help="Host for HTTP status server",
    )
    parser.add_argument(
        "--log-level",
        default=os.environ.get("LOG_LEVEL", "INFO"),
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level",
    )
    parser.add_argument(
        "--run-once",
        action="store_true",
        default=os.environ.get("RUN_ONCE", "").lower() in ("1", "true", "yes"),
        help="Poll each partition once and exit",
    )
    parser.add_argument(
        "--max-polls",
        type=int,
        default=int(os.environ.get("MAX_POLLS", "0")),
        help="Exit after N poll loops (0 = run forever)",
    )

    return parser.parse_args()


def main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
    )

    seeds = [s.strip() for s in args.seeds.split(",") if s.strip()]
    topics = [t.strip() for t in args.topics.split(",") if t.strip()]
    partitions = [int(p.strip()) for p in args.partitions.split(",") if p.strip()]

    consumer = Consumer(
        consumer_id=args.consumer_id,
        seeds=seeds,
        topics=topics,
        partitions=partitions,
        poll_interval=args.poll_interval,
        max_records=args.max_records,
        start_position=args.start_position,
        data_dir=args.data_dir,
        offset_file=args.offset_file,
        processed_log=args.processed_log,
        status_port=args.status_port,
        status_host=args.status_host,
        run_once=args.run_once,
        max_polls=args.max_polls,
    )

    def _sig_handler(signum: int, frame: Any) -> None:
        logger.info("Signal %d received, shutting down...", signum)
        consumer.close()
        sys.exit(0)

    signal.signal(signal.SIGINT, _sig_handler)
    signal.signal(signal.SIGTERM, _sig_handler)

    consumer.run()


if __name__ == "__main__":
    main()
