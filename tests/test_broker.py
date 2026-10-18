"""
tests/test_broker.py  -  Integration tests for Phase 2: Topics, Partitions, Replicated State Machine.
"""

from __future__ import annotations

import concurrent.futures
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
from typing import Dict, List, Optional

_ROOT   = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_PYTHON = sys.executable

from broker.config import BrokerConfig
from broker.hash import select_partition, fnv1a_32


def _port(base: int, node_id: int) -> int:
    return base + node_id


def _peers_arg(base: int, all_ids: list, exclude: int) -> str:
    return ",".join(f"{i}:127.0.0.1:{_port(base, i)}" for i in all_ids if i != exclude)


def _get(url: str, timeout: float = 3.0) -> tuple[int, Optional[dict]]:
    req = urllib.request.Request(url, headers={"Connection": "close"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
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
    # Prevent auto-following redirects so 307 can be inspected
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


class BrokerCluster:
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
            "--election-min-ms", "800",
            "--election-max-ms", "1600",
            "--heartbeat-ms", "100",
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

    def produce(self, node_id: int, body: dict, timeout: float = 10.0) -> tuple[int, Optional[dict]]:
        return _post(f"http://127.0.0.1:{_port(self.base_port, node_id)}/debug/produce", body, timeout=timeout)

    def fetch(self, node_id: int, topic: str, partition: int, offset: int = 0, max_records: int = 100) -> tuple[int, Optional[dict]]:
        return _get(f"http://127.0.0.1:{_port(self.base_port, node_id)}/debug/fetch?topic={topic}&partition={partition}&offset={offset}&max={max_records}")

    def partition_node(self, node_id: int, blocked: list):
        _post(f"http://127.0.0.1:{_port(self.base_port, node_id)}/debug/partition", {"blocked": blocked}, timeout=3.0)


class TestBrokerPhase2(unittest.TestCase):
    _port_counter = 20000

    def setUp(self):
        TestBrokerPhase2._port_counter += 100
        self.base_port = TestBrokerPhase2._port_counter
        self.temp_dir = tempfile.mkdtemp(prefix="broker-test-")
        self.cluster = BrokerCluster(5, self.base_port, self.temp_dir)

    def tearDown(self):
        self.cluster.stop_all()
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_01_config_validation(self):
        """Config creates exactly 4 topics x 2 partitions; invalid config fails fast."""
        cfg = BrokerConfig.parse()
        self.assertEqual(len(cfg.topics), 4)
        self.assertEqual(cfg.topics["orders"], 2)
        self.assertEqual(cfg.topics["payments"], 2)
        self.assertEqual(cfg.topics["notifications"], 2)
        self.assertEqual(cfg.topics["audit"], 2)

        # Invalid partition count
        with self.assertRaises(ValueError):
            BrokerConfig.parse("orders:0")

        # Invalid topic name
        with self.assertRaises(ValueError):
            BrokerConfig.parse("invalid!topic:2")

        # Empty spec
        with self.assertRaises(ValueError):
            BrokerConfig.parse("")

    def test_02_deterministic_partition_selection(self):
        """Same key always maps to same partition; same messageID with no key maps to same partition."""
        p1, r1 = select_partition(2, key="user-42")
        p2, r2 = select_partition(2, key="user-42")
        self.assertEqual(p1, p2)
        self.assertEqual(r1, "key")

        # Key takes precedence over messageID
        p3, r3 = select_partition(2, key="user-42", message_id="msg-999")
        self.assertEqual(p3, p1)
        self.assertEqual(r3, "key")

        # Same messageID without key
        pm1, rm1 = select_partition(2, message_id="msg-alpha")
        pm2, rm2 = select_partition(2, message_id="msg-alpha")
        self.assertEqual(pm1, pm2)
        self.assertEqual(rm1, "messageID")

        # Explicit partition
        pe, re = select_partition(2, explicit_partition=1, key="user-42")
        self.assertEqual(pe, 1)
        self.assertEqual(re, "explicit")

        # Out of range explicit partition
        with self.assertRaises(ValueError):
            select_partition(2, explicit_partition=5)

    def test_03_offsets_sequential_and_independent(self):
        """Offsets within a partition are sequential from 0, and independent across partitions/topics."""
        self.cluster.start_all()
        leader = self.cluster.wait_for_leader()
        self.assertIsNotNone(leader)

        # Produce 3 records to orders partition 0
        for expected_off in range(3):
            code, resp = self.cluster.produce(
                leader,
                {"topic": "orders", "partition": 0, "messageID": f"ord-0-{expected_off}", "payload": f"item-{expected_off}"}
            )
            self.assertEqual(code, 200)
            self.assertEqual(resp["offset"], expected_off)
            self.assertFalse(resp["duplicate"])

        # Produce to orders partition 1 -> offset starts at 0 independently
        code, resp = self.cluster.produce(
            leader,
            {"topic": "orders", "partition": 1, "messageID": "ord-1-0", "payload": "part1-msg"}
        )
        self.assertEqual(code, 200)
        self.assertEqual(resp["offset"], 0)

        # Produce to payments partition 0 -> offset starts at 0 independently
        code, resp = self.cluster.produce(
            leader,
            {"topic": "payments", "partition": 0, "messageID": "pay-0-0", "payload": "pay-msg"}
        )
        self.assertEqual(code, 200)
        self.assertEqual(resp["offset"], 0)

    def test_04_produce_acknowledged_after_commit_and_readable(self):
        """A produce is only acknowledged after commit; acknowledged offset is readable on leader."""
        self.cluster.start_all()
        leader = self.cluster.wait_for_leader()
        self.assertIsNotNone(leader)

        code, resp = self.cluster.produce(
            leader,
            {"topic": "audit", "partition": 0, "messageID": "audit-1", "payload": {"action": "login"}}
        )
        self.assertEqual(code, 200)
        self.assertEqual(resp["offset"], 0)
        self.assertEqual(resp["hwm"], 1)

        # Read back via fetch
        f_code, f_resp = self.cluster.fetch(leader, topic="audit", partition=0, offset=0)
        self.assertEqual(f_code, 200)
        self.assertEqual(len(f_resp["records"]), 1)
        self.assertEqual(f_resp["records"][0]["messageID"], "audit-1")
        self.assertEqual(f_resp["records"][0]["payload"], {"action": "login"})
        self.assertEqual(f_resp["hwm"], 1)

    def test_05_deduplication_including_concurrent(self):
        """Retrying the same messageID returns original offset with duplicate=true and no new record."""
        self.cluster.start_all()
        leader = self.cluster.wait_for_leader()
        self.assertIsNotNone(leader)

        # Initial produce
        code1, resp1 = self.cluster.produce(
            leader,
            {"topic": "orders", "partition": 0, "messageID": "dedup-fixed-1", "payload": "data-1"}
        )
        self.assertEqual(code1, 200)
        self.assertEqual(resp1["offset"], 0)
        self.assertFalse(resp1["duplicate"])

        # Retry same messageID
        code2, resp2 = self.cluster.produce(
            leader,
            {"topic": "orders", "partition": 0, "messageID": "dedup-fixed-1", "payload": "different-data"}
        )
        self.assertEqual(code2, 200)
        self.assertEqual(resp2["offset"], 0)
        self.assertTrue(resp2["duplicate"])

        # Ensure only 1 record exists
        _, f_resp = self.cluster.fetch(leader, "orders", 0, offset=0)
        self.assertEqual(len(f_resp["records"]), 1)

        # Test concurrent duplicate produces
        msg_id = "concurrent-msg-123"
        def do_produce():
            return self.cluster.produce(leader, {"topic": "orders", "partition": 0, "messageID": msg_id, "payload": "concurrent"})

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as ex:
            f1 = ex.submit(do_produce)
            f2 = ex.submit(do_produce)
            r1 = f1.result()
            r2 = f2.result()

        self.assertEqual(r1[0], 200)
        self.assertEqual(r2[0], 200)
        # Both must have the same offset
        self.assertEqual(r1[1]["offset"], r2[1]["offset"])
        self.assertEqual(r1[1]["offset"], 1)  # next offset after 0
        # Exactly one should be duplicate=False, the other duplicate=True
        duplicates = [r1[1]["duplicate"], r2[1]["duplicate"]]
        self.assertIn(False, duplicates)
        self.assertIn(True, duplicates)

        # Exactly 2 records in partition log total
        _, f_resp_after = self.cluster.fetch(leader, "orders", 0, offset=0)
        self.assertEqual(len(f_resp_after["records"]), 2)

    def test_06_records_replicate_to_all_followers(self):
        """Records replicate to all followers with identical offsets and contents in every partition."""
        self.cluster.start_all()
        leader = self.cluster.wait_for_leader()
        self.assertIsNotNone(leader)

        for i in range(4):
            code, _ = self.cluster.produce(
                leader,
                {"topic": "notifications", "partition": i % 2, "messageID": f"notif-{i}", "payload": f"alert-{i}"}
            )
            self.assertEqual(code, 200)

        # Give followers a moment to apply
        time.sleep(0.5)

        # Verify on all 5 nodes
        for nid in self.cluster.ids:
            for p in (0, 1):
                code, f_resp = self.cluster.fetch(nid, "notifications", p, offset=0)
                self.assertEqual(code, 200, f"Node {nid} partition {p} fetch failed")
                self.assertEqual(len(f_resp["records"]), 2, f"Node {nid} partition {p} count mismatch")
                self.assertEqual(f_resp["records"][0]["offset"], 0)
                self.assertEqual(f_resp["records"][1]["offset"], 1)

    def test_07_fetch_never_returns_at_or_beyond_hwm(self):
        """Fetch never returns records at or beyond the HWM."""
        self.cluster.start_all()
        leader = self.cluster.wait_for_leader()
        self.assertIsNotNone(leader)

        self.cluster.produce(leader, {"topic": "audit", "partition": 0, "messageID": "a1", "payload": "1"})
        self.cluster.produce(leader, {"topic": "audit", "partition": 0, "messageID": "a2", "payload": "2"})

        # HWM is 2
        code, resp = self.cluster.fetch(leader, "audit", 0, offset=2)
        self.assertEqual(code, 200)
        self.assertEqual(resp["records"], [])
        self.assertEqual(resp["hwm"], 2)

        code, resp = self.cluster.fetch(leader, "audit", 0, offset=999)
        self.assertEqual(code, 200)
        self.assertEqual(resp["records"], [])

    def test_08_follower_redirect_and_validation_errors(self):
        """Producing to follower redirects to leader; unknown topic/partition returns proper errors."""
        self.cluster.start_all()
        leader = self.cluster.wait_for_leader()
        self.assertIsNotNone(leader)

        follower = [nid for nid in self.cluster.ids if nid != leader][0]

        # Producing to follower returns 307
        code, resp = self.cluster.produce(follower, {"topic": "orders", "messageID": "m1", "payload": "p1"})
        self.assertEqual(code, 307)
        self.assertEqual(resp["leader_id"], leader)

        # Unknown topic -> 404
        code404, resp404 = self.cluster.produce(leader, {"topic": "unknown_topic", "messageID": "m1", "payload": "p1"})
        self.assertEqual(code404, 404)

        # Partition out of range -> 400
        code400, _ = self.cluster.produce(leader, {"topic": "orders", "partition": 9, "messageID": "m1", "payload": "p1"})
        self.assertEqual(code400, 400)

        # Empty messageID -> 400
        code_empty, _ = self.cluster.produce(leader, {"topic": "orders", "messageID": "", "payload": "p1"})
        self.assertEqual(code_empty, 400)

    def test_09_kill_leader_midstream(self):
        """Kill leader mid-stream: new leader elected, records preserved, new produces continue."""
        self.cluster.start_all()
        l1 = self.cluster.wait_for_leader()
        self.assertIsNotNone(l1)

        # Produce initial records
        self.cluster.produce(l1, {"topic": "orders", "partition": 0, "messageID": "lead-kill-1", "payload": "initial"})

        # Kill leader
        self.cluster.kill(l1)

        # New leader elected
        l2 = self.cluster.wait_for_leader(timeout=14.0)
        self.assertIsNotNone(l2)
        self.assertNotEqual(l1, l2)

        # Committed record is still present
        code, fetch_res = self.cluster.fetch(l2, "orders", 0, offset=0)
        self.assertEqual(code, 200)
        self.assertEqual(len(fetch_res["records"]), 1)
        self.assertEqual(fetch_res["records"][0]["messageID"], "lead-kill-1")

        # New produce continues from offset 1
        p_code, p_res = self.cluster.produce(l2, {"topic": "orders", "partition": 0, "messageID": "lead-kill-2", "payload": "after-kill"})
        self.assertEqual(p_code, 200)
        self.assertEqual(p_res["offset"], 1)

    def test_10_follower_catches_up_after_restart(self):
        """A follower that was down catches up and its partition logs end up identical to leader's."""
        self.cluster.start_all()
        leader = self.cluster.wait_for_leader()
        self.assertIsNotNone(leader)

        follower = [nid for nid in self.cluster.ids if nid != leader][0]
        self.cluster.kill(follower)

        # Produce while follower is down
        for i in range(3):
            self.cluster.produce(leader, {"topic": "payments", "partition": 0, "messageID": f"catchup-{i}", "payload": i})

        # Restart follower
        self.cluster.start(follower)
        time.sleep(1.0)

        # Check follower has all 3 records
        code, f_resp = self.cluster.fetch(follower, "payments", 0, offset=0)
        self.assertEqual(code, 200)
        self.assertEqual(len(f_resp["records"]), 3)
        self.assertEqual(f_resp["hwm"], 3)

    def test_11_restart_all_nodes_persistence(self):
        """Restart ALL nodes: partition logs, offsets, HWMs, and dedup state are restored."""
        self.cluster.start_all()
        leader = self.cluster.wait_for_leader()
        self.assertIsNotNone(leader)

        self.cluster.produce(leader, {"topic": "orders", "partition": 0, "messageID": "persist-1", "payload": "data"})
        self.cluster.produce(leader, {"topic": "orders", "partition": 0, "messageID": "persist-2", "payload": "data"})

        # Allow replication to flush to disk before killing
        time.sleep(0.4)

        # Stop all nodes
        self.cluster.stop_all()
        time.sleep(0.5)

        # Restart all nodes
        self.cluster.start_all()
        new_leader = self.cluster.wait_for_leader(timeout=14.0)
        self.assertIsNotNone(new_leader)
        time.sleep(0.5)

        active_leader = self.cluster.wait_for_leader(timeout=8.0) or new_leader

        # Verify records and HWM
        code, f_resp = self.cluster.fetch(active_leader, "orders", 0, offset=0)
        self.assertEqual(code, 200)
        self.assertEqual(len(f_resp["records"]), 2)
        self.assertEqual(f_resp["hwm"], 2)

        # Verify dedup restored: retry persist-1 returns duplicate=True with offset 0
        p_code = 0
        p_res = None
        for _ in range(10):
            cur_leader = self.cluster.wait_for_leader(timeout=5.0)
            if cur_leader:
                p_code, p_res = self.cluster.produce(cur_leader, {"topic": "orders", "partition": 0, "messageID": "persist-1", "payload": "data"})
                if p_code == 200:
                    break
            time.sleep(0.3)

        self.assertEqual(p_code, 200)
        self.assertEqual(p_res["offset"], 0)
        self.assertTrue(p_res["duplicate"])

    def test_12_minority_partition_cannot_accept_produces(self):
        """A minority partition (2 nodes) cannot accept produces; the majority side can."""
        self.cluster.start_all()
        leader = self.cluster.wait_for_leader()
        self.assertIsNotNone(leader)

        # Partition: {leader, another} vs {3 other nodes}
        others = [nid for nid in self.cluster.ids if nid != leader]
        minority = [leader, others[0]]
        majority = others[1:]

        for m in minority:
            self.cluster.partition_node(m, blocked=majority)
        for maj in majority:
            self.cluster.partition_node(maj, blocked=minority)

        # Minority produce should time out / fail
        m_code, _ = self.cluster.produce(leader, {"topic": "orders", "partition": 0, "messageID": "min-fail", "payload": "no"}, timeout=2.0)
        self.assertIn(m_code, (504, 503, 0))

        # Majority side elects new leader
        deadline = time.monotonic() + 10.0
        maj_leader = None
        while time.monotonic() < deadline:
            for nid in majority:
                code, s = _get(f"http://127.0.0.1:{_port(self.base_port, nid)}/status")
                if code == 200 and s and s.get("role") == "leader":
                    maj_leader = nid
                    break
            if maj_leader:
                break
            time.sleep(0.2)

        self.assertIsNotNone(maj_leader)
        # Majority can produce successfully
        maj_code, maj_res = self.cluster.produce(maj_leader, {"topic": "orders", "partition": 0, "messageID": "maj-ok", "payload": "yes"}, timeout=5.0)
        self.assertEqual(maj_code, 200)
        self.assertFalse(maj_res["duplicate"])


if __name__ == "__main__":
    unittest.main()
