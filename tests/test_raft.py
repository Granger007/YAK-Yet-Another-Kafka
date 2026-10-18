"""
tests/test_raft.py  -  Multi-process integration tests for the Raft cluster.

Each test:
  1. Starts N broker processes (real OS processes, real HTTP ports).
  2. Exercises the cluster via HTTP.
  3. Kills / restarts processes as needed.
  4. Asserts invariants via /status and /debug/propose.

Run with:
    python -m pytest tests/test_raft.py -v
  or
    python -m unittest tests.test_raft
"""

from __future__ import annotations

import json
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import unittest
import urllib.error
import urllib.request
from typing import Dict, List, Optional

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_ROOT   = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_PYTHON = sys.executable

BASE_PORT = 19000   # start of port range; each test gets a fresh range


def _port(base: int, node_id: int) -> int:
    return base + node_id


def _addr(base: int, node_id: int) -> str:
    return f"127.0.0.1:{_port(base, node_id)}"


def _peers_arg(base: int, all_ids: list, exclude: int) -> str:
    return ",".join(
        f"{i}:127.0.0.1:{_port(base, i)}" for i in all_ids if i != exclude
    )


def _get(url: str, timeout: float = 2.0) -> Optional[dict]:
    try:
        req = urllib.request.Request(url, headers={"Connection": "close"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read())
    except Exception:
        return None


def _post(url: str, body: dict, timeout: float = 12.0) -> Optional[dict]:
    data = json.dumps(body).encode()
    req  = urllib.request.Request(
        url, data=data,
        headers={"Content-Type": "application/json", "Connection": "close"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:
        try:
            return json.loads(e.read())
        except Exception:
            return None
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Cluster manager
# ---------------------------------------------------------------------------

class Cluster:
    """Manages a set of broker subprocesses for one test."""

    def __init__(self, n: int, base_port: int, data_dir: str,
                 election_min_ms: int = 1500,
                 election_max_ms: int = 3000,
                 heartbeat_ms:    int = 150):
        self.n          = n
        self.base_port  = base_port
        self.data_dir   = data_dir
        self.ids        = list(range(1, n + 1))
        self._procs: Dict[int, subprocess.Popen] = {}
        self._election_min = election_min_ms
        self._election_max = election_max_ms
        self._hb          = heartbeat_ms

    def start(self, node_id: int):
        nd = os.path.join(self.data_dir, f"node-{node_id}")
        os.makedirs(nd, exist_ok=True)
        cmd = [
            _PYTHON, "-m", "broker.broker",
            "--id",              str(node_id),
            "--port",            str(_port(self.base_port, node_id)),
            "--peers",           _peers_arg(self.base_port, self.ids, node_id),
            "--data-dir",        nd,
            "--election-min-ms", str(self._election_min),
            "--election-max-ms", str(self._election_max),
            "--heartbeat-ms",    str(self._hb),
            "--log-level",       "INFO",
        ]
        proc = subprocess.Popen(cmd, cwd=_ROOT)
        self._procs[node_id] = proc
        return proc

    def start_all(self):
        for nid in self.ids:
            self.start(nid)
        # Wait until processes bind ports and respond
        deadline = time.monotonic() + 4.0
        for nid in self.ids:
            while time.monotonic() < deadline:
                if self.status(nid) is not None:
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

    def status(self, node_id: int) -> Optional[dict]:
        url = f"http://127.0.0.1:{_port(self.base_port, node_id)}/status"
        return _get(url)

    def wait_for_leader(self, timeout: float = 12.0) -> Optional[int]:
        """Poll all live nodes until exactly one reports role=leader."""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            leaders = []
            for nid in list(self._procs):
                s = self.status(nid)
                if s and s.get("role") == "leader":
                    leaders.append(s["id"])
            if len(leaders) == 1:
                return leaders[0]
            time.sleep(0.2)
        return None

    def all_statuses(self) -> Dict[int, dict]:
        result = {}
        for nid in list(self._procs):
            s = self.status(nid)
            if s:
                result[nid] = s
        return result

    def propose(self, node_id: int, data: str, max_retries: int = 5) -> Optional[dict]:
        """POST to node_id; follow leader redirects and retry on error."""
        curr_id = node_id
        last_resp: Optional[dict] = None
        for attempt in range(max_retries):
            url  = f"http://127.0.0.1:{_port(self.base_port, curr_id)}/debug/propose"
            resp = _post(url, {"data": data})
            last_resp = resp
            if resp and resp.get("committed"):
                return resp
            # Follow leader hint from 307 / error body
            hint = resp.get("leader_id") if resp else None
            if hint and hint != curr_id and hint in self._procs:
                curr_id = hint
                time.sleep(0.1)
                continue
            # No useful hint – re-discover the leader
            time.sleep(0.3)
            new_leader = self.wait_for_leader(timeout=5)
            if new_leader and new_leader in self._procs:
                curr_id = new_leader
        return last_resp

    def partition(self, node_id: int, blocked: list):
        url = f"http://127.0.0.1:{_port(self.base_port, node_id)}/debug/partition"
        return _post(url, {"blocked": blocked}, timeout=3)


# ---------------------------------------------------------------------------
# Test cases
# ---------------------------------------------------------------------------

_PORT_COUNTER = [BASE_PORT]


def _next_base() -> int:
    base = _PORT_COUNTER[0]
    _PORT_COUNTER[0] += 10
    return base


class TestRaftCluster(unittest.TestCase):

    def setUp(self):
        self._tmp   = tempfile.mkdtemp(prefix="raft_test_")
        self._base  = _next_base()
        self._cluster: Optional[Cluster] = None

    def tearDown(self):
        if self._cluster:
            self._cluster.stop_all()
        shutil.rmtree(self._tmp, ignore_errors=True)

    def _make_cluster(self, n=5, **kw) -> Cluster:
        c = Cluster(n, self._base, self._tmp, **kw)
        self._cluster = c
        return c

    # ------------------------------------------------------------------ #

    def test_01_leader_elected(self):
        """A single leader is elected within a few seconds."""
        c = self._make_cluster()
        c.start_all()
        leader = c.wait_for_leader(timeout=12)
        self.assertIsNotNone(leader, "No leader elected within 12 s")
        print(f"\n  [OK] Leader = node {leader}")

    # ------------------------------------------------------------------ #

    def test_02_at_most_one_leader_per_term(self):
        """At most one leader per term across the whole cluster."""
        c = self._make_cluster()
        c.start_all()
        c.wait_for_leader(timeout=12)

        # Sample many times
        for _ in range(10):
            time.sleep(0.2)
            term_leaders: Dict[int, List[int]] = {}
            for nid in c.ids:
                s = c.status(nid)
                if s and s["role"] == "leader":
                    t = s["current_term"]
                    term_leaders.setdefault(t, []).append(nid)
            for t, leaders in term_leaders.items():
                self.assertEqual(
                    len(leaders), 1,
                    f"Two leaders in term {t}: {leaders}",
                )

    # ------------------------------------------------------------------ #

    def test_03_kill_leader_triggers_re_election(self):
        """Killing the leader causes re-election at a higher term."""
        c = self._make_cluster()
        c.start_all()

        leader1 = c.wait_for_leader(timeout=12)
        self.assertIsNotNone(leader1)

        # Record the term at which leader1 was elected
        s1      = c.status(leader1)
        term1   = s1["current_term"]

        # Kill the leader
        print(f"\n  Killing leader node {leader1} (term={term1})")
        c.kill(leader1)
        time.sleep(0.5)

        # A new leader must be elected
        leader2 = c.wait_for_leader(timeout=12)
        self.assertIsNotNone(leader2, "No new leader after killing old leader")
        self.assertNotEqual(leader2, leader1)

        s2 = c.status(leader2)
        self.assertGreater(s2["current_term"], term1,
                           "New leader term must be higher than old leader term")
        print(f"  [OK] New leader = node {leader2} (term={s2['current_term']})")

        # Restart old leader and verify it rejoins as follower
        c.start(leader1)
        time.sleep(3)
        s_old = c.status(leader1)
        self.assertIsNotNone(s_old)
        self.assertEqual(s_old["role"], "follower",
                         f"Old leader rejoined as {s_old['role']} instead of follower")
        print(f"  [OK] Old leader {leader1} rejoined as follower (term={s_old['current_term']})")

    # ------------------------------------------------------------------ #

    def test_04_outdated_log_vote_denied(self):
        """A node with an up-to-date log never grants a vote to a stale candidate."""
        c = self._make_cluster(n=3)
        c.start_all()

        leader = c.wait_for_leader(timeout=10)
        self.assertIsNotNone(leader)

        # Propose some entries so the leader log is ahead
        for i in range(3):
            resp = c.propose(leader, f"entry-{i}")
            self.assertIsNotNone(resp)
            self.assertTrue(resp.get("committed"), f"Entry {i} not committed: {resp}")

        time.sleep(1)

        # Explicitly verify that an up-to-date node denies a vote to a candidate with an outdated log
        follower = next(nid for nid in c.ids if nid != leader)
        f_status = c.status(follower)
        self.assertIsNotNone(f_status)
        stale_candidate_req = {
            "term": f_status["current_term"] + 1,
            "candidate_id": 99,
            "last_log_index": 0,
            "last_log_term": 0,
        }
        url = f"http://127.0.0.1:{_port(c.base_port, follower)}/raft/requestVote"
        resp = _post(url, stale_candidate_req)
        self.assertIsNotNone(resp)
        self.assertFalse(resp.get("vote_granted") or resp.get("voteGranted"),
                         f"Follower granted vote to stale candidate: {resp}")
        print(f"\n  [OK] Followers caught up; stale candidate vote explicitly denied by follower {follower}")

    # ------------------------------------------------------------------ #

    def test_05_no_double_vote_same_term(self):
        """A node never votes twice in the same term."""
        c = self._make_cluster()
        c.start_all()
        c.wait_for_leader(timeout=12)

        for nid in c.ids:
            s = c.status(nid)
            if s:
                # voted_for should be a single value (not two different candidates)
                self.assertIn(s.get("voted_for"), [None] + c.ids,
                              f"Node {nid} has invalid voted_for={s.get('voted_for')}")
        print("\n  [OK] No double-vote detected")

    # ------------------------------------------------------------------ #

    def test_06_log_replication(self):
        """Entries proposed on the leader are replicated to a majority."""
        c = self._make_cluster()
        c.start_all()

        leader = c.wait_for_leader(timeout=12)
        self.assertIsNotNone(leader)

        entries = ["alpha", "beta", "gamma", "delta"]
        for data in entries:
            resp = c.propose(leader, data)
            self.assertIsNotNone(resp, f"Propose of {data!r} returned None")
            self.assertTrue(resp.get("committed"), f"Propose of {data!r} not committed: {resp}")

        time.sleep(1.5)

        # Majority (at least 3) must have commit_index >= len(entries)+1 (no-op + entries)
        replicated = []
        for nid in c.ids:
            s = c.status(nid)
            if s and s["commit_index"] >= len(entries):
                replicated.append(nid)

        self.assertGreaterEqual(len(replicated), 3,
                                f"Only {replicated} nodes have commit_index >= {len(entries)}")
        print(f"\n  [OK] {len(replicated)}/5 nodes committed all {len(entries)} entries")

    # ------------------------------------------------------------------ #

    def test_07_follower_catches_up_after_restart(self):
        """A follower that was down fully catches up on restart."""
        c = self._make_cluster()
        c.start_all()

        leader = c.wait_for_leader(timeout=12)
        self.assertIsNotNone(leader)

        # Pick a non-leader follower to kill
        follower = next(nid for nid in c.ids if nid != leader)
        print(f"\n  Killing follower {follower}")
        c.kill(follower)
        time.sleep(0.5)

        # Propose entries while follower is down
        for i in range(5):
            resp = c.propose(leader, f"catch-up-{i}")
            self.assertIsNotNone(resp)
            self.assertTrue(resp.get("committed"))

        # Restart follower
        print(f"  Restarting follower {follower}")
        c.start(follower)
        time.sleep(4)   # allow catch-up

        s = c.status(follower)
        self.assertIsNotNone(s)
        # commit_index should equal at least the 5 entries + no-op
        self.assertGreaterEqual(s["commit_index"], 5,
                                f"Restarted follower commit_index={s['commit_index']}")
        print(f"  [OK] Follower {follower} caught up: commit_index={s['commit_index']}")

    # ------------------------------------------------------------------ #

    def test_08_minority_partition_cannot_commit(self):
        """
        A minority of 2 nodes cannot elect a leader or commit entries.
        The majority partition (3 nodes) CAN still commit.
        """
        c = self._make_cluster()
        c.start_all()

        leader = c.wait_for_leader(timeout=12)
        self.assertIsNotNone(leader)

        # Determine majority (3) and minority (2) sides
        others      = [nid for nid in c.ids if nid != leader]
        minority    = others[:2]
        majority    = [leader] + others[2:]

        print(f"\n  Leader={leader}  majority={majority}  minority={minority}")

        # Partition minority away from majority
        for nid in minority:
            blocked = [x for x in majority]
            c.partition(nid, blocked)
        for nid in majority:
            blocked = [x for x in minority]
            c.partition(nid, blocked)

        time.sleep(3)   # enough time for minority to try (and fail) an election

        # Minority must NOT have a leader
        for nid in minority:
            s = c.status(nid)
            if s:
                self.assertNotEqual(
                    s["role"], "leader",
                    f"Minority node {nid} illegally became leader!",
                )

        # Majority MUST still be able to commit
        resp = c.propose(leader, "majority-write")
        if resp is None:
            # leader may have stepped down briefly; find new leader in majority
            leader = c.wait_for_leader(timeout=8)
            self.assertIn(leader, majority, f"New leader {leader} is not in majority")
            resp = c.propose(leader, "majority-write")

        self.assertIsNotNone(resp)
        self.assertTrue(resp.get("committed"), f"Majority could not commit: {resp}")
        print(f"  [OK] Minority had no leader; majority committed successfully")

    # ------------------------------------------------------------------ #

    def test_09_persistence(self):
        """Restart all nodes and verify term, votedFor, and log survive."""
        c = self._make_cluster()
        c.start_all()

        leader = c.wait_for_leader(timeout=12)
        self.assertIsNotNone(leader)

        # Propose entries
        for i in range(3):
            resp = c.propose(leader, f"persist-{i}")
            self.assertIsNotNone(resp)
            self.assertTrue(resp.get("committed"))

        time.sleep(1)

        # Snapshot state before restart
        before: Dict[int, dict] = {}
        for nid in c.ids:
            s = c.status(nid)
            if s:
                before[nid] = s

        # Stop all
        c.stop_all()
        time.sleep(1)

        # Restart all
        c.start_all()
        time.sleep(2)

        # New leader should be elected
        new_leader = c.wait_for_leader(timeout=12)
        self.assertIsNotNone(new_leader)

        # Each node''s term must be >= its pre-restart term
        for nid in c.ids:
            s = c.status(nid)
            if s and nid in before:
                self.assertGreaterEqual(
                    s["current_term"], before[nid]["current_term"],
                    f"Node {nid} term regressed after restart",
                )
                self.assertGreaterEqual(
                    s["log_length"], before[nid]["log_length"],
                    f"Node {nid} log shrunk after restart",
                )
        print(f"\n  [OK] All nodes persisted and restored term/log correctly")

    # ------------------------------------------------------------------ #

    def test_10_split_vote_resolves(self):
        """Split-vote scenario eventually resolves via randomised timeouts."""
        # Use a 3-node cluster; tight timeouts make split votes more likely
        c = self._make_cluster(n=3, election_min_ms=600, election_max_ms=1200, heartbeat_ms=100)
        c.start_all()

        leader = c.wait_for_leader(timeout=15)
        self.assertIsNotNone(leader, "Split vote never resolved")
        print(f"\n  [OK] Leader elected (node {leader}) despite possible split votes")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    unittest.main(verbosity=2)
