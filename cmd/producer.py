#!/usr/bin/env python3
"""
cmd/producer.py  --  Phase-3 YAK Broker Producer.

Flags / env vars
----------------
--producer-id   ID string            (PRODUCER_ID,  default prod-1)
--seeds         host:port,...        (SEEDS,         default localhost:8001,...:8005)
--topics        topic1,topic2,...    (TOPICS,        default all 4)
--count         messages; 0=forever  (COUNT,         default 0)
--interval      secs between sends   (INTERVAL,      default 0.5)
--timeout       HTTP ack timeout s   (ACK_TIMEOUT,   default 8.0)
--max-retries   per-message retries  (MAX_RETRIES,   default 10)
--ledger        ledger JSONL path    (LEDGER_PATH)
--dedup-demo    re-send last N IDs   (DEDUP_DEMO,    default 0)
--key-count     distinct keys        (KEY_COUNT,     default 10)
--topic-mode    round-robin|random   (TOPIC_MODE,    default round-robin)
--log-level     DEBUG/INFO/WARNING   (LOG_LEVEL,     default INFO)
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import random
import signal
import sys
import time
import uuid

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from client.discovery import LeaderDiscovery

logger = logging.getLogger("producer")


# ---------------------------------------------------------------------------
# Ledger — fsync-backed append-only ground truth
# ---------------------------------------------------------------------------

class Ledger:
    """Append-only JSONL ledger; fsynced after every record."""

    def __init__(self, path: str) -> None:
        self.path = path
        self._fh = open(path, "a", encoding="utf-8")
        logger.info("Ledger opened at %s", path)

    def record(self, entry: dict) -> None:
        self._fh.write(json.dumps(entry) + "\n")
        self._fh.flush()
        os.fsync(self._fh.fileno())

    def close(self) -> None:
        try:
            self._fh.close()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Producer
# ---------------------------------------------------------------------------

class Producer:
    def __init__(
        self,
        producer_id: str = "prod-1",
        seeds: list = None,
        topics: list = None,
        count: int = 0,
        interval: float = 0.5,
        timeout: float = 8.0,
        max_retries: int = 10,
        ledger_path: str = "",
        dedup_demo: int = 0,
        key_count: int = 10,
        topic_mode: str = "round-robin",
    ) -> None:
        self.producer_id = producer_id
        self.topics      = topics
        self.count       = count          # 0 = run forever
        self.interval    = interval
        self.timeout     = timeout
        self.max_retries = max_retries
        self.dedup_demo  = dedup_demo
        self.key_count   = key_count
        self.topic_mode  = topic_mode

        # LeaderDiscovery manages the leader-address cache.
        # It re-discovers automatically on cache invalidation.
        self._disc       = LeaderDiscovery(seeds, client_id=producer_id)
        self._ledger     = Ledger(ledger_path)
        self._topic_idx  = 0
        self._recent_ids: list = []   # for dedup demo
        self._running    = True

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _pick_topic(self) -> str:
        if self.topic_mode == "random":
            return random.choice(self.topics)
        t = self.topics[self._topic_idx % len(self.topics)]
        self._topic_idx += 1
        return t

    def _pick_key(self) -> str:
        return "user{}".format(random.randint(1, self.key_count))

    # ------------------------------------------------------------------
    # Core send (idempotent via stable messageID)
    # ------------------------------------------------------------------

    def _send_once(self, topic: str, message_id: str, key: str,
                   payload: dict) -> tuple:
        """One HTTP attempt; returns (status, resp_dict)."""
        body = {"topic": topic, "messageID": message_id,
                "key": key, "payload": payload}
        try:
            return self._disc.send_request(
                "/message", method="POST", body=body,
                timeout=self.timeout, max_retries=1,
            )
        except RuntimeError as exc:
            logger.warning("[%s] discovery error: %s", self.producer_id, exc)
            return 0, {}

    def _send(self, topic: str, message_id: str, key: str, payload: dict) -> dict:
        """
        Send one logical message with exponential-backoff retries.
        Always reuses the same messageID on retry — broker dedup makes
        it idempotent, so duplicate=True on a retry counts as success.
        """
        for attempt in range(1, self.max_retries + 1):
            logger.debug("[%s] send attempt %d/%d topic=%s msgId=%s",
                         self.producer_id, attempt, self.max_retries,
                         topic, message_id)

            status, resp = self._send_once(topic, message_id, key, payload)

            if status == 200:
                if resp.get("duplicate"):
                    logger.info(
                        "[%s] DUPLICATE ack  topic=%s part=%s off=%s msgId=%s",
                        self.producer_id, resp.get("topic"),
                        resp.get("partition"), resp.get("offset"), message_id)
                else:
                    logger.info(
                        "[%s] ACK  topic=%s part=%s off=%s hwm=%s msgId=%s",
                        self.producer_id, resp.get("topic"),
                        resp.get("partition"), resp.get("offset"),
                        resp.get("hwm"), message_id)
                return resp

            # timeout (status 0) is AMBIGUOUS — the message may have
            # committed already.  Retry with the same messageID is safe.
            if status == 0:
                logger.warning(
                    "[%s] timeout/network error attempt %d msgId=%s "
                    "-- retrying same ID (dedup safe)",
                    self.producer_id, attempt, message_id)
            else:
                logger.warning(
                    "[%s] HTTP %d attempt %d msgId=%s: %s",
                    self.producer_id, status, attempt, message_id,
                    resp.get("error", ""))

            backoff = min(4.0, 0.3 * (2 ** (attempt - 1))) + random.uniform(0.05, 0.3)
            time.sleep(backoff)

        raise RuntimeError(
            "[{}] gave up after {} attempts for msgId={}".format(
                self.producer_id, self.max_retries, message_id))

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    def run(self) -> None:
        logger.info("[%s] starting  topics=%s  count=%s  interval=%.2fs",
                    self.producer_id, self.topics,
                    self.count if self.count else "inf", self.interval)
        seq = 0
        sent = 0

        while self._running and (self.count == 0 or sent < self.count):
            topic      = self._pick_topic()
            key        = self._pick_key()
            seq       += 1
            message_id = str(uuid.uuid4())   # stable ID for this logical msg
            payload    = {"seq": seq, "ts": time.time()}

            try:
                result = self._send(topic, message_id, key, payload)
            except RuntimeError as exc:
                logger.error("[%s] %s  -- skipping message", self.producer_id, exc)
                time.sleep(self.interval)
                continue

            # Persist to ledger (fsync)
            self._ledger.record({
                "producerId": self.producer_id,
                "messageID":  message_id,
                "topic":      result.get("topic", topic),
                "partition":  result.get("partition"),
                "offset":     result.get("offset"),
                "duplicate":  result.get("duplicate", False),
                "hwm":        result.get("hwm"),
                "seq":        seq,
                "ts":         time.time(),
            })
            sent += 1

            # Dedup demo: re-send last N message IDs and verify
            # that the broker returns duplicate=True for all of them.
            if self.dedup_demo > 0:
                self._recent_ids.append(message_id)
                if len(self._recent_ids) > self.dedup_demo:
                    self._recent_ids.pop(0)
                for old_id in list(self._recent_ids):
                    logger.info("[%s] DEDUP DEMO re-sending %s",
                                self.producer_id, old_id)
                    try:
                        r2 = self._send(topic, old_id, key, payload)
                        if not r2.get("duplicate"):
                            logger.error(
                                "[%s] DEDUP FAILED -- expected duplicate=True "
                                "for %s", self.producer_id, old_id)
                    except Exception as e:
                        logger.warning("[%s] dedup demo send error: %s",
                                       self.producer_id, e)

            time.sleep(self.interval)

        logger.info("[%s] done: sent %d messages", self.producer_id, sent)
        self._ledger.close()

    def stop(self) -> None:
        self._running = False


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    p = argparse.ArgumentParser(description="YAK Broker -- Phase-3 Producer")
    p.add_argument("--producer-id", default=os.environ.get("PRODUCER_ID", "prod-1"))
    p.add_argument("--seeds",       default=os.environ.get(
        "SEEDS", "localhost:8001,localhost:8002,localhost:8003,localhost:8004,localhost:8005"))
    p.add_argument("--topics",      default=os.environ.get(
        "TOPICS", "orders,payments,notifications,audit"))
    p.add_argument("--count",       type=int,   default=int(os.environ.get("COUNT",       "0")))
    p.add_argument("--interval",    type=float, default=float(os.environ.get("INTERVAL",  "0.5")))
    p.add_argument("--timeout",     type=float, default=float(os.environ.get("ACK_TIMEOUT","8.0")))
    p.add_argument("--max-retries", type=int,   default=int(os.environ.get("MAX_RETRIES", "10")))
    p.add_argument("--ledger",      default=os.environ.get("LEDGER_PATH", ""))
    p.add_argument("--dedup-demo",  type=int,   default=int(os.environ.get("DEDUP_DEMO",  "0")))
    p.add_argument("--key-count",   type=int,   default=int(os.environ.get("KEY_COUNT",   "10")))
    p.add_argument("--topic-mode",  default=os.environ.get("TOPIC_MODE", "round-robin"),
                   choices=["round-robin", "random"])
    p.add_argument("--log-level",   default=os.environ.get("LOG_LEVEL", "INFO"))
    args = p.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s.%(msecs)03d  %(levelname)-7s  %(message)s",
        datefmt="%H:%M:%S", stream=sys.stdout,
    )

    seeds  = [s.strip() for s in args.seeds.split(",")  if s.strip()]
    topics = [t.strip() for t in args.topics.split(",") if t.strip()]
    ledger_path = args.ledger or "producer-{}.ledger".format(args.producer_id)

    prod = Producer(
        producer_id=args.producer_id, seeds=seeds, topics=topics,
        count=args.count,       interval=args.interval,
        timeout=args.timeout,   max_retries=args.max_retries,
        ledger_path=ledger_path, dedup_demo=args.dedup_demo,
        key_count=args.key_count, topic_mode=args.topic_mode,
    )

    def _sig(sig, _frame):
        logger.info("stopping (signal %d)...", sig)
        prod.stop()

    signal.signal(signal.SIGINT,  _sig)
    signal.signal(signal.SIGTERM, _sig)
    prod.run()


if __name__ == "__main__":
    main()

