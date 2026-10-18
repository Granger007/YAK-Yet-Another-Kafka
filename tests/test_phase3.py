"""
tests/test_phase3.py  -  Integration tests for Phase 3:
Producer, Consumers, Offsets, At-Least-Once Delivery, Failover & Dedup.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
import unittest
import urllib.error
import urllib.request
import uuid
from typing import Dict, List, Optional

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

_PYTHON = sys.executable

from client.discovery import LeaderDiscovery
from cmd.consumer import Consumer, OffsetStore
from cmd.producer import Producer
from scripts.verify import verify


def _port(base: int, node_id: int) -> int:
    return base + node_id


def _peers_arg(base: int, all_ids: list, exclude: int) -> str:
    return ",".join(f"{i}:127.0.0.1:{_port(base, i)}" for i in all_ids if i != exclude)


def _get(url: str, timeout: float = 3.0) -> tuple[int, Optional[dict]]:
    req = urllib.request.Request(url, headers={"Connection": "close"})

    class NoRedirect(urllib.request.HTTPRedirectHandler):
        def redirect_request(self, req, fp, code, msg, headers, newurl):
            return None

    opener = urllib.request.build_opener(NoRedirect)
    try:
        with opener.open(req, timeout=timeout) as r:
            return r.status, json.loads(r.read())
    except urllib.error.HTTPError as e:
        try:
            return e.code, json.loads(e.read())
        except Exception:
            return e.code, None
    except Exception:
        return 0, None


def _post(url: str, body: dict, timeout: float = 12.0) -> tuple[int, Optional[dict]]:
    data = json.dumps(body).encode()
    req = urllib.request.Request(
        url, data=data,
        headers={"Content-Type": "application/json", "Connection": "close"},
        method="POST",
    )

    class NoRedirect(urllib.request.HTTPRedirectHandler):
        def redirect_request(self, req, fp, code, msg, headers, newurl):
            return None

    opener = urllib.request.build_opener(NoRedirect)
    try:
        with opener.open(req, timeout=timeout) as r:
            return r.status, json.loads(r.read())
    except urllib.error.HTTPError as e:
        try:
            return e.code, json.loads(e.read())
        except Exception:
            return e.code, None
    except Exception:
        return 0, None


class ClusterHelper:
    def __init__(self, n: int, base_port: int, data_dir: str, topics: str = "orders:2,payments:2,notifications:2,audit:2"):
        self.n = n
        self.base_port = base_port
        self.data_dir = data_dir
        self.topics = topics
        self.ids = list(range(1, n + 1))
        self._procs: Dict[int, subprocess.Popen] = {}

    def start(self, node_id: int):
        nd = os.path.join(self.data_dir, f"node-{node_id}")
        os.makedirs(nd, exist_ok=True)
        cmd = [
            _PYTHON, "-m", "broker.broker",
            "--id", str(node_id),
            "--port", str(_port(self.base_port, node_id)),
            "--peers", _peers_arg(self.base_port, self.ids, node_id),
            "--data-dir", nd,
            "--topics", self.topics,
            "--election-min-ms", "600",
            "--election-max-ms", "1200",
            "--heartbeat-ms", "80",
            "--log-level", "INFO",
        ]
        proc = subprocess.Popen(cmd, cwd=_ROOT)
        self._procs[node_id] = proc
        return proc

    def start_all(self):
        for nid in self.ids:
            self.start(nid)
        deadline = time.monotonic() + 8.0
        for nid in self.ids:
            while time.monotonic() < deadline:
                code, _ = _get(f"http://127.0.0.1:{_port(self.base_port, nid)}/status")
                if code == 200:
                    break
                time.sleep(0.1)

    def kill(self, node_id: int):
        proc = self._procs.pop(node_id, None)
        if proc:
            proc.terminate()
            try:
                proc.wait(timeout=3)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=2)

    def stop_all(self):
        for nid in list(self._procs):
            self.kill(nid)

    def wait_for_leader(self, timeout: float = 12.0) -> Optional[int]:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            leaders = []
            for nid in list(self._procs):
                code, s = _get(f"http://127.0.0.1:{_port(self.base_port, nid)}/status")
                if code == 200 and s and s.get("role") == "leader":
                    leaders.append(s["id"])
            if len(leaders) == 1:
                return leaders[0]
            time.sleep(0.15)
        return None


class TestPhase3(unittest.TestCase):
    _port_counter = 30000

    def setUp(self):
        TestPhase3._port_counter += 100
        self.base_port = TestPhase3._port_counter
        self.temp_dir = tempfile.mkdtemp(prefix="phase3-test-")
        self.cluster = ClusterHelper(5, self.base_port, self.temp_dir)
        self.seeds = [f"127.0.0.1:{_port(self.base_port, i)}" for i in range(1, 6)]

    def tearDown(self):
        self.cluster.stop_all()
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_01_producer_discovers_leader_once_and_reuses_cache(self):
        """Producer discovers the leader once and reuses the cache across many sends."""
        self.cluster.start_all()
        leader = self.cluster.wait_for_leader()
        self.assertIsNotNone(leader)

        discovery = LeaderDiscovery(self.seeds, client_id="test-prod-1")
        self.assertIsNone(discovery.cached_leader)

        # Send first message -> triggers leader discovery
        body0 = {
            "topic": "orders",
            "partition": 0,
            "messageID": str(uuid.uuid4()),
            "payload": {"n": 0},
        }
        code, resp = discovery.send_request("/message", method="POST", body=body0)
        self.assertEqual(code, 200)
        self.assertEqual(resp.get("offset"), 0)
        initial_calls = discovery.metadata_call_count
        self.assertGreater(initial_calls, 0)
        self.assertIsNotNone(discovery.cached_leader)

        # Send next 4 messages -> MUST reuse cached leader without any additional metadata calls
        for i in range(1, 5):
            body = {
                "topic": "orders",
                "partition": 0,
                "messageID": str(uuid.uuid4()),
                "payload": {"n": i},
            }
            code, resp = discovery.send_request("/message", method="POST", body=body)
            self.assertEqual(code, 200)
            self.assertEqual(resp.get("offset"), i)

        # Leader discovery query count must not increase across sends
        self.assertEqual(discovery.metadata_call_count, initial_calls)

    def test_02_producer_recovers_after_leader_killed(self):
        """Producer recovers after the leader is killed mid-stream: cache is invalidated, new leader found."""
        self.cluster.start_all()
        l1 = self.cluster.wait_for_leader()
        self.assertIsNotNone(l1)

        discovery = LeaderDiscovery(self.seeds, client_id="test-prod-failover")
        body1 = {
            "topic": "orders",
            "partition": 0,
            "messageID": str(uuid.uuid4()),
            "payload": {"step": "before_kill"},
        }
        code, resp1 = discovery.send_request("/message", method="POST", body=body1)
        self.assertEqual(code, 200)

        # Kill current leader
        self.cluster.kill(l1)

        # Wait for cluster to elect new leader among remaining 4 nodes
        l2 = self.cluster.wait_for_leader(timeout=10.0)
        self.assertIsNotNone(l2)
        self.assertNotEqual(l1, l2)

        # Producer sends new message
        body2 = {
            "topic": "orders",
            "partition": 0,
            "messageID": str(uuid.uuid4()),
            "payload": {"step": "after_kill"},
        }
        code, resp2 = discovery.send_request("/message", method="POST", body=body2, max_retries=10)
        self.assertEqual(code, 200)
        self.assertGreater(resp2.get("offset", -1), resp1.get("offset", -1))

    def test_03_producer_retry_after_timeout_does_not_duplicate(self):
        """A producer retry after an ack timeout does not create a duplicate record."""
        self.cluster.start_all()
        leader = self.cluster.wait_for_leader()
        self.assertIsNotNone(leader)

        discovery = LeaderDiscovery(self.seeds, client_id="test-prod-retry")
        msg_id = str(uuid.uuid4())
        body = {
            "topic": "payments",
            "partition": 0,
            "messageID": msg_id,
            "payload": {"amount": 100},
        }

        # First send
        code1, resp1 = discovery.send_request("/message", method="POST", body=body)
        self.assertEqual(code1, 200)
        self.assertFalse(resp1.get("duplicate"))
        first_offset = resp1.get("offset")

        # Retry with SAME messageID
        code2, resp2 = discovery.send_request("/message", method="POST", body=body)
        self.assertEqual(code2, 200)
        self.assertTrue(resp2.get("duplicate"))
        self.assertEqual(resp2.get("offset"), first_offset)

    def test_04_restarted_producer_new_messages_not_deduped(self):
        """A restarted producer's new messages are not deduplicated against its old ones."""
        self.cluster.start_all()
        leader = self.cluster.wait_for_leader()
        self.assertIsNotNone(leader)

        ledger_p1 = os.path.join(self.temp_dir, "prod1.ledger")
        p1 = Producer(
            producer_id="p1",
            seeds=self.seeds,
            topics=["orders"],
            count=2,
            interval=0.05,
            timeout=5.0,
            max_retries=5,
            ledger_path=ledger_p1,
            dedup_demo=0,
        )
        p1.run()

        # Restart: New producer instance with same ID
        ledger_p2 = os.path.join(self.temp_dir, "prod2.ledger")
        p2 = Producer(
            producer_id="p1",
            seeds=self.seeds,
            topics=["orders"],
            count=2,
            interval=0.05,
            timeout=5.0,
            max_retries=5,
            ledger_path=ledger_p2,
            dedup_demo=0,
        )
        p2.run()

        # Check records in ledger 2 were not deduplicated
        with open(ledger_p2, "r", encoding="utf-8") as f:
            lines = [json.loads(line) for line in f if line.strip()]
        self.assertEqual(len(lines), 2)
        for entry in lines:
            self.assertFalse(entry.get("duplicate"))

    def test_05_consumers_receive_all_subscribed_messages_in_order(self):
        """Consumers receive all messages of their subscribed topics, in order within each partition."""
        self.cluster.start_all()
        leader = self.cluster.wait_for_leader()
        self.assertIsNotNone(leader)

        discovery = LeaderDiscovery(self.seeds)
        # Produce 4 messages on orders partition 0 and 4 on orders partition 1
        for part in (0, 1):
            for i in range(4):
                body = {
                    "topic": "orders",
                    "partition": part,
                    "messageID": str(uuid.uuid4()),
                    "payload": {"i": i},
                }
                code, resp = discovery.send_request("/message", method="POST", body=body)
                self.assertEqual(code, 200)

        # Run consumer
        c_dir = os.path.join(self.temp_dir, "c-order-test")
        consumer = Consumer(
            consumer_id="c-order",
            seeds=self.seeds,
            topics=["orders"],
            partitions=[0, 1],
            data_dir=c_dir,
            run_once=True,
        )
        consumer.run()

        # Check processed log
        with open(consumer.processed_log_path, "r", encoding="utf-8") as f:
            entries = [json.loads(line) for line in f if line.strip()]

        self.assertEqual(len(entries), 8)
        p0_offsets = [e["offset"] for e in entries if e["partition"] == 0]
        p1_offsets = [e["offset"] for e in entries if e["partition"] == 1]
        self.assertEqual(p0_offsets, [0, 1, 2, 3])
        self.assertEqual(p1_offsets, [0, 1, 2, 3])

    def test_06_consumers_independent_offsets_on_shared_topic(self):
        """Consumer-1 and consumer-2 both receive every message on payments, with independent offsets."""
        self.cluster.start_all()
        leader = self.cluster.wait_for_leader()
        self.assertIsNotNone(leader)

        discovery = LeaderDiscovery(self.seeds)
        for i in range(5):
            body = {
                "topic": "payments",
                "partition": 0,
                "messageID": str(uuid.uuid4()),
                "payload": {"pay_seq": i},
            }
            code, _ = discovery.send_request("/message", method="POST", body=body)
            self.assertEqual(code, 200)

        c1_dir = os.path.join(self.temp_dir, "c1-shared")
        c2_dir = os.path.join(self.temp_dir, "c2-shared")

        c1 = Consumer(
            consumer_id="consumer-1",
            seeds=self.seeds,
            topics=["orders", "payments"],
            partitions=[0, 1],
            data_dir=c1_dir,
            run_once=True,
        )
        c2 = Consumer(
            consumer_id="consumer-2",
            seeds=self.seeds,
            topics=["payments", "notifications", "audit"],
            partitions=[0, 1],
            data_dir=c2_dir,
            run_once=True,
        )

        c1.run()
        c2.run()

        # Both consumers should have processed all 5 messages on payments/0
        self.assertEqual(c1.offset_store.get_offset("payments", 0), 5)
        self.assertEqual(c2.offset_store.get_offset("payments", 0), 5)

    def test_07_fetch_hwm_boundaries(self):
        """Fetch never returns records at or beyond hwm, and offset > hwm returns empty rather than an error."""
        self.cluster.start_all()
        leader = self.cluster.wait_for_leader()
        self.assertIsNotNone(leader)

        discovery = LeaderDiscovery(self.seeds)
        # Produce 2 records to audit/0
        for i in range(2):
            body = {
                "topic": "audit",
                "partition": 0,
                "messageID": str(uuid.uuid4()),
                "payload": {"i": i},
            }
            code, resp = discovery.send_request("/message", method="POST", body=body)
            self.assertEqual(code, 200)

        leader_addr = discovery.get_leader()

        # Fetch at HWM (offset=2) -> empty records list
        code, resp = _get(f"http://{leader_addr}/fetch?topic=audit&partition=0&offset=2&max=10")
        self.assertEqual(code, 200)
        self.assertEqual(resp.get("records"), [])
        self.assertEqual(resp.get("hwm"), 2)

        # Fetch beyond HWM (offset=100) -> empty records list, NOT an error
        code, resp = _get(f"http://{leader_addr}/fetch?topic=audit&partition=0&offset=100&max=10")
        self.assertEqual(code, 200)
        self.assertEqual(resp.get("records"), [])

        # Fetch from a follower node -> 307 Temporary Redirect
        follower_id = [nid for nid in self.cluster.ids if nid != leader][0]
        follower_addr = f"127.0.0.1:{_port(self.base_port, follower_id)}"
        code, resp = _get(f"http://{follower_addr}/fetch?topic=audit&partition=0&offset=0&max=10")
        self.assertEqual(code, 307)
        self.assertIn("leaderAddress", resp)

    def test_08_consumer_stop_and_resume(self):
        """Stop a consumer and restart it: resumes from saved offsets and does not re-read from 0."""
        self.cluster.start_all()
        leader = self.cluster.wait_for_leader()
        self.assertIsNotNone(leader)

        discovery = LeaderDiscovery(self.seeds)
        # Produce batch 1 (3 messages)
        for i in range(3):
            body = {"topic": "orders", "partition": 0, "messageID": str(uuid.uuid4()), "payload": {"b": 1, "i": i}}
            discovery.send_request("/message", method="POST", body=body)

        c_dir = os.path.join(self.temp_dir, "resume-consumer")
        c1 = Consumer(consumer_id="c-resume", seeds=self.seeds, topics=["orders"], partitions=[0], data_dir=c_dir, run_once=True)
        c1.run()
        self.assertEqual(c1.offset_store.get_offset("orders", 0), 3)

        # Produce batch 2 (3 messages)
        for i in range(3):
            body = {"topic": "orders", "partition": 0, "messageID": str(uuid.uuid4()), "payload": {"b": 2, "i": i}}
            discovery.send_request("/message", method="POST", body=body)

        # Restart consumer
        c2 = Consumer(consumer_id="c-resume", seeds=self.seeds, topics=["orders"], partitions=[0], data_dir=c_dir, run_once=True)
        c2.run()
        self.assertEqual(c2.offset_store.get_offset("orders", 0), 6)

        # Total processed records across runs = 6, no duplicate processing from 0
        with open(c2.processed_log_path, "r", encoding="utf-8") as f:
            lines = [json.loads(l) for l in f if l.strip()]
        self.assertEqual(len(lines), 6)
        offsets = [l["offset"] for l in lines]
        self.assertEqual(offsets, [0, 1, 2, 3, 4, 5])

    def test_09_consumer_crash_before_offset_persist(self):
        """Crash a consumer after processing but before persisting: on restart batch is redelivered and no record is skipped."""
        self.cluster.start_all()
        leader = self.cluster.wait_for_leader()
        self.assertIsNotNone(leader)

        discovery = LeaderDiscovery(self.seeds)
        for i in range(3):
            body = {"topic": "orders", "partition": 0, "messageID": str(uuid.uuid4()), "payload": {"i": i}}
            discovery.send_request("/message", method="POST", body=body)

        c_dir = os.path.join(self.temp_dir, "crash-consumer")
        c1 = Consumer(consumer_id="c-crash", seeds=self.seeds, topics=["orders"], partitions=[0], data_dir=c_dir)

        # Fetch records and log them, but DO NOT commit offset (simulate crash before commit)
        code, body = discovery.send_request("/fetch?topic=orders&partition=0&offset=0&max=10")
        self.assertEqual(code, 200)
        for rec in body["records"]:
            c1.processed_log.record({
                "consumer_id": "c-crash",
                "topic": "orders",
                "partition": 0,
                "offset": rec["offset"],
                "messageID": rec["messageID"],
            })
        c1.close()

        # Offset file should NOT have been created/updated yet
        self.assertIsNone(c1.offset_store.get_offset("orders", 0))

        # Restart consumer: resumes from 0 (redelivery) and commits properly
        c2 = Consumer(consumer_id="c-crash", seeds=self.seeds, topics=["orders"], partitions=[0], data_dir=c_dir, run_once=True)
        c2.run()
        self.assertEqual(c2.offset_store.get_offset("orders", 0), 3)

    def test_10_corrupt_offsets_fail_fast(self):
        """A corrupt offsets file causes a fail-fast error, not a silent reset."""
        c_dir = os.path.join(self.temp_dir, "corrupt-c")
        os.makedirs(c_dir, exist_ok=True)
        offset_file = os.path.join(c_dir, "offsets.json")
        with open(offset_file, "w", encoding="utf-8") as f:
            f.write("!!CORRUPT_NOT_JSON!!")

        with self.assertRaises(RuntimeError):
            OffsetStore(offset_file)

    def test_11_failover_during_active_produce_consume(self):
        """Kill the leader while producer and consumer run: both recover and verify passes."""
        self.cluster.start_all()
        l1 = self.cluster.wait_for_leader()
        self.assertIsNotNone(l1)

        ledger_path = os.path.join(self.temp_dir, "prod.ledger")
        c1_dir = os.path.join(self.temp_dir, "c1-failover")
        c2_dir = os.path.join(self.temp_dir, "c2-failover")

        # Produce first 10 messages
        p = Producer(
            producer_id="p-failover",
            seeds=self.seeds,
            topics=["orders", "payments"],
            count=10,
            interval=0.05,
            timeout=5.0,
            max_retries=10,
            ledger_path=ledger_path,
        )
        p.run()

        # Kill leader
        self.cluster.kill(l1)
        l2 = self.cluster.wait_for_leader(timeout=10.0)
        self.assertIsNotNone(l2)

        # Produce 10 more messages after failover
        p2 = Producer(
            producer_id="p-failover",
            seeds=self.seeds,
            topics=["orders", "payments"],
            count=10,
            interval=0.05,
            timeout=5.0,
            max_retries=10,
            ledger_path=ledger_path,
        )
        p2.run()

        # Run consumers
        c1 = Consumer(consumer_id="c1", seeds=self.seeds, topics=["orders", "payments"], partitions=[0, 1], data_dir=c1_dir, run_once=True)
        c2 = Consumer(consumer_id="c2", seeds=self.seeds, topics=["payments"], partitions=[0, 1], data_dir=c2_dir, run_once=True)
        c1.run()
        c2.run()

        # Run verification check
        ok = verify(
            ledger_path=ledger_path,
            c1_log_path=c1.processed_log_path,
            c2_log_path=c2.processed_log_path,
            c1_topics={"orders", "payments"},
            c2_topics={"payments"},
        )
        self.assertTrue(ok)

    def test_12_consumer_start_position_earliest_vs_latest(self):
        """A consumer started after messages already exist reads from offset 0 (earliest), or only new messages (latest)."""
        self.cluster.start_all()
        leader = self.cluster.wait_for_leader()
        self.assertIsNotNone(leader)

        discovery = LeaderDiscovery(self.seeds)
        # Pre-populate 4 messages on notifications/0
        for i in range(4):
            body = {"topic": "notifications", "partition": 0, "messageID": str(uuid.uuid4()), "payload": {"m": i}}
            code, _ = discovery.send_request("/message", method="POST", body=body)
            self.assertEqual(code, 200)

        # Consumer Earliest: reads all 4 existing records
        c_earliest_dir = os.path.join(self.temp_dir, "c-earliest")
        c_earliest = Consumer(
            consumer_id="c-earliest",
            seeds=self.seeds,
            topics=["notifications"],
            partitions=[0],
            start_position="earliest",
            data_dir=c_earliest_dir,
            run_once=True,
        )
        c_earliest.run()
        self.assertEqual(c_earliest.total_processed, 4)

        # Consumer Latest: starts at HWM (4), reads 0 existing records
        c_latest_dir = os.path.join(self.temp_dir, "c-latest")
        c_latest = Consumer(
            consumer_id="c-latest",
            seeds=self.seeds,
            topics=["notifications"],
            partitions=[0],
            start_position="latest",
            data_dir=c_latest_dir,
            run_once=True,
        )
        c_latest.run()
        self.assertEqual(c_latest.total_processed, 0)
        self.assertEqual(c_latest.offset_store.get_offset("notifications", 0), 4)

        # Now produce 2 new messages
        for i in range(2):
            body = {"topic": "notifications", "partition": 0, "messageID": str(uuid.uuid4()), "payload": {"new": i}}
            discovery.send_request("/message", method="POST", body=body)

        # Poll again with Consumer Latest -> reads only the 2 new messages
        c_latest2 = Consumer(
            consumer_id="c-latest",
            seeds=self.seeds,
            topics=["notifications"],
            partitions=[0],
            start_position="latest",
            data_dir=c_latest_dir,
            run_once=True,
        )
        c_latest2.run()
        self.assertEqual(c_latest2.total_processed, 2)
        self.assertEqual(c_latest2.offset_store.get_offset("notifications", 0), 6)


if __name__ == "__main__":
    unittest.main()
