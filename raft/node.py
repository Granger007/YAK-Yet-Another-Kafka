"""
raft/node.py  -  Core Raft state machine.

Implements every rule from the Raft paper (Fig 2) plus:
  - randomised election timeouts (configurable)
  - no-op entry on leader election (section 5.4.2)
  - fast log-conflict back-off (conflict_term / conflict_index)
  - persist-before-respond for currentTerm, votedFor, log
  - propose() for client commands with commit-wait
  - partition simulation (via transport.is_blocked)
"""

from __future__ import annotations

import logging
import random
import threading
import time
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

from .rpc import (
    AppendEntriesRequest,
    AppendEntriesResponse,
    LogEntry,
    RequestVoteRequest,
    RequestVoteResponse,
)
from .storage import RaftStorage
from .transport import RaftTransport

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Role
# ---------------------------------------------------------------------------

class Role(Enum):
    FOLLOWER  = "follower"
    CANDIDATE = "candidate"
    LEADER    = "leader"


# ---------------------------------------------------------------------------
# Per-peer replicator
# ---------------------------------------------------------------------------

class _PeerReplicator:
    """
    One background thread per peer that the leader owns.
    Woken by _trigger (Event) either on heartbeat or on a new entry.
    Handles its own retry loop for log-inconsistency back-off.
    """

    def __init__(self, node: "RaftNode", peer_id: int, peer_addr: str):
        self._node      = node
        self._peer_id   = peer_id
        self._peer_addr = peer_addr
        self._trigger   = threading.Event()
        self._stop      = threading.Event()
        self._thread    = threading.Thread(
            target=self._loop,
            daemon=True,
            name=f"node-{node.node_id}-rep-{peer_id}",
        )

    def start(self):
        self._stop.clear()
        self._trigger.set()   # Send heartbeat/replication immediately
        self._thread.start()

    def stop(self):
        self._stop.set()
        self._trigger.set()

    def wake(self):
        """Signal the replicator to send immediately."""
        self._trigger.set()

    # ------------------------------------------------------------------

    def _loop(self):
        node = self._node
        while not self._stop.is_set():
            # Wait for a heartbeat tick or an explicit wake
            self._trigger.wait(timeout=node.heartbeat_interval)
            self._trigger.clear()
            if self._stop.is_set():
                break
            self._replicate_once()

    def _replicate_once(self):
        """
        Attempt replication.  If the follower rejects due to log mismatch
        decrement nextIndex and retry immediately (within same call) until
        success or until we step down / network fails.
        """
        node      = self._node
        peer_id   = self._peer_id
        peer_addr = self._peer_addr

        while not self._stop.is_set():
            # --- Build request under lock ---
            with node._lock:
                if node.role != Role.LEADER:
                    return
                term           = node.current_term
                next_idx       = node.next_index.get(peer_id, 1)
                prev_log_index = next_idx - 1
                if prev_log_index < len(node.log):
                    prev_log_term = node.log[prev_log_index].term
                else:
                    prev_log_term = 0
                entries        = list(node.log[next_idx:])
                leader_commit  = node.commit_index

            req = AppendEntriesRequest(
                term=term,
                leader_id=node.node_id,
                prev_log_index=prev_log_index,
                prev_log_term=prev_log_term,
                entries=entries,
                leader_commit=leader_commit,
            )

            is_heartbeat = len(entries) == 0
            if not is_heartbeat:
                logger.info(
                    "[Node %d] [T%d] -> peer %d: AppendEntries prevIdx=%d #entries=%d",
                    node.node_id, term, peer_id, prev_log_index, len(entries),
                )
            else:
                logger.debug(
                    "[Node %d] [T%d] -> peer %d: heartbeat (commitIndex=%d)",
                    node.node_id, term, peer_id, leader_commit,
                )

            resp = node.transport.send_append_entries(peer_id, peer_addr, req)

            if resp is None:
                return   # network failure; retry on next heartbeat

            # --- Process response under lock ---
            with node._lock:
                if node.role != Role.LEADER or node.current_term != term:
                    return   # stale

                if resp.term > node.current_term:
                    logger.info(
                        "[Node %d] [T%d] Higher term %d from peer %d -> stepping down",
                        node.node_id, term, resp.term, peer_id,
                    )
                    node._step_down(resp.term)
                    return

                if resp.success:
                    new_match = prev_log_index + len(entries)
                    if new_match > node.match_index.get(peer_id, 0):
                        node.match_index[peer_id] = new_match
                        node._try_advance_commit()
                    if new_match + 1 > node.next_index.get(peer_id, 1):
                        node.next_index[peer_id] = new_match + 1

                    if not is_heartbeat:
                        logger.info(
                            "[Node %d] [T%d] peer %d acked up to index %d",
                            node.node_id, term, peer_id, new_match,
                        )
                    return   # done for this round

                else:
                    # Fast back-off using conflict hint
                    if resp.conflict_term > 0:
                        # Find last entry in our log with conflict_term
                        found = None
                        for i in range(len(node.log) - 1, 0, -1):
                            if node.log[i].term == resp.conflict_term:
                                found = i
                                break
                        node.next_index[peer_id] = (found + 1) if found is not None else resp.conflict_index
                    elif resp.conflict_index > 0:
                        node.next_index[peer_id] = resp.conflict_index
                    else:
                        node.next_index[peer_id] = max(1, node.next_index.get(peer_id, 1) - 1)

                    logger.info(
                        "[Node %d] [T%d] peer %d rejected; retry from nextIndex=%d",
                        node.node_id, term, peer_id, node.next_index[peer_id],
                    )
                    # loop to retry immediately


# ---------------------------------------------------------------------------
# RaftNode
# ---------------------------------------------------------------------------

class RaftNode:
    """
    Full Raft consensus node.

    Persistent state  (survives restart):
        current_term, voted_for, log

    Volatile state:
        commit_index, last_applied, role, leader_id
        next_index, match_index   (leader only)
    """

    def __init__(
        self,
        node_id: int,
        peers: Dict[int, str],           # {peer_id: "host:port"}
        storage: RaftStorage,
        transport: RaftTransport,
        election_timeout_min_ms: int = 1500,
        election_timeout_max_ms: int = 3000,
        heartbeat_interval_ms: int   = 300,
    ):
        self.node_id  = node_id
        # Exclude self from peers list to avoid self-RPCs
        self.peers    = {pid: addr for pid, addr in peers.items() if pid != node_id}
        self.storage  = storage
        self.transport = transport

        self.election_timeout_min = election_timeout_min_ms / 1000.0
        self.election_timeout_max = election_timeout_max_ms / 1000.0
        self.heartbeat_interval   = heartbeat_interval_ms / 1000.0

        # ---- Persistent state ----
        self.current_term: int           = 0
        self.voted_for:    Optional[int] = None
        self.log: List[LogEntry]         = [LogEntry(0, 0, None)]  # sentinel at index 0

        # ---- Volatile state ----
        self.commit_index: int  = 0
        self.last_applied: int  = 0
        self.role: Role         = Role.FOLLOWER
        self.leader_id: Optional[int] = None
        self._is_stopped: bool  = False

        # ---- Leader volatile state ----
        self.next_index:  Dict[int, int] = {}
        self.match_index: Dict[int, int] = {}

        # ---- Synchronisation ----
        self._lock        = threading.Lock()
        self._commit_cond = threading.Condition(self._lock)   # signalled on commit_index advance

        # ---- Election timer ----
        self._election_reset = threading.Event()   # set to reset the timer
        self._stop           = threading.Event()

        # ---- Per-peer replicators (only active when leader) ----
        self._replicators: Dict[int, _PeerReplicator] = {}

        # ---- Background threads ----
        self._election_thread: Optional[threading.Thread] = None
        self._apply_thread:    Optional[threading.Thread] = None

        # ---- Optional apply callback ----
        self.apply_callback: Optional[Callable[[LogEntry], None]] = None

    # ======================================================================
    # Lifecycle
    # ======================================================================

    def start(self):
        self._stop.clear()
        self._election_reset.clear()
        self._is_stopped = False
        with self._lock:
            self.role = Role.FOLLOWER
            self.leader_id = None

        state = self.storage.load()
        if state:
            self.current_term = state["current_term"]
            self.voted_for    = state["voted_for"]
            self.log          = state["log"]
            logger.info(
                "[Node %d] Loaded state: term=%d voted_for=%s log_len=%d",
                self.node_id, self.current_term, self.voted_for, len(self.log),
            )

        self._election_thread = threading.Thread(
            target=self._election_timer_loop, daemon=True,
            name=f"node-{self.node_id}-election",
        )
        self._apply_thread = threading.Thread(
            target=self._apply_loop, daemon=True,
            name=f"node-{self.node_id}-apply",
        )
        self._election_thread.start()
        self._apply_thread.start()

        logger.info(
            "[Node %d] Started as FOLLOWER at term %d", self.node_id, self.current_term
        )

    def stop(self):
        self._is_stopped = True
        self._stop.set()
        self._election_reset.set()
        with self._lock:
            self._commit_cond.notify_all()
        self._stop_replicators()

    def pause(self):
        """Simulate crash or pause without stopping HTTP server."""
        logger.info("[Node %d] Pausing / simulating crash", self.node_id)
        self.stop()

    def resume(self):
        """Resume / restart consensus after pause."""
        logger.info("[Node %d] Resuming node from stopped state", self.node_id)
        self.start()

    def trigger_election(self) -> bool:
        """Force candidate election immediately for testing/demo."""
        with self._lock:
            if self._is_stopped:
                return False
            if self.role == Role.LEADER:
                return False
        logger.info("[Node %d] Manual election trigger requested", self.node_id)
        threading.Thread(target=self._start_election, daemon=True, name=f"node-{self.node_id}-manual-elect").start()
        return True

    def step_down_leader(self) -> bool:
        """Force leader to step down to follower."""
        with self._lock:
            if self.role == Role.LEADER:
                self._step_down(self.current_term + 1)
                return True
            return False

    # ======================================================================
    # Election timer
    # ======================================================================

    def _election_timer_loop(self):
        while not self._stop.is_set():
            with self._lock:
                role = self.role

            if role == Role.LEADER:
                # Leaders heartbeat via replicators; sleep briefly to avoid spin
                time.sleep(0.05)
                continue

            timeout = random.uniform(self.election_timeout_min, self.election_timeout_max)
            logger.debug(
                "[Node %d] [T%d] Election timer set to %.0fms",
                self.node_id, self.current_term, timeout * 1000,
            )
            fired = not self._election_reset.wait(timeout)
            self._election_reset.clear()

            if self._stop.is_set():
                break

            if fired:
                with self._lock:
                    if self.role == Role.LEADER:
                        continue
                logger.info(
                    "[Node %d] [T%d] Election timeout fired (%.0fms)",
                    self.node_id, self.current_term, timeout * 1000,
                )
                self._start_election()

    def _reset_election_timer(self):
        """Reset the timer.  MUST be called with self._lock held."""
        self._election_reset.set()

    # ======================================================================
    # Candidate  -  start election
    # ======================================================================

    def _start_election(self):
        with self._lock:
            if self.role == Role.LEADER:
                return

            self.role         = Role.CANDIDATE
            self.current_term += 1
            self.voted_for    = self.node_id
            self._save_persistent()

            term           = self.current_term
            last_log_index = len(self.log) - 1
            last_log_term  = self.log[-1].term
            peers          = dict(self.peers)
            node_id        = self.node_id

        logger.info(
            "[Node %d] [T%d] Became CANDIDATE (lastLogIdx=%d lastLogTerm=%d)",
            node_id, term, last_log_index, last_log_term,
        )

        quorum = (len(peers) + 1) // 2 + 1    # majority of whole cluster (self + peers)

        # Single-node cluster wins immediately
        if 1 >= quorum:
            with self._lock:
                if self.role == Role.CANDIDATE and self.current_term == term:
                    self._become_leader()
            return

        # Shared mutable vote counter (list so inner function can mutate it)
        votes     = [1]          # already voted for self
        votes_lck = threading.Lock()

        def ask_peer(pid, addr):
            req = RequestVoteRequest(
                term=term,
                candidate_id=node_id,
                last_log_index=last_log_index,
                last_log_term=last_log_term,
            )
            logger.info(
                "[Node %d] [T%d] RequestVote -> peer %d", node_id, term, pid
            )
            resp = self.transport.send_request_vote(pid, addr, req)

            if resp is None:
                logger.debug("[Node %d] [T%d] No response from peer %d", node_id, term, pid)
                return

            with self._lock:
                if resp.term > self.current_term:
                    logger.info(
                        "[Node %d] [T%d] Peer %d returned higher term %d -> step down",
                        node_id, term, pid, resp.term,
                    )
                    self._step_down(resp.term)
                    return

                if self.role != Role.CANDIDATE or self.current_term != term:
                    return   # stale response

                if resp.vote_granted:
                    with votes_lck:
                        votes[0] += 1
                        total = votes[0]
                    logger.info(
                        "[Node %d] [T%d] Vote granted by peer %d  (%d/%d)",
                        node_id, term, pid, total, quorum,
                    )
                    if total >= quorum and self.role == Role.CANDIDATE:
                        self._become_leader()
                else:
                    logger.info(
                        "[Node %d] [T%d] Vote denied by peer %d", node_id, term, pid
                    )

        threads = []
        for pid, addr in peers.items():
            t = threading.Thread(target=ask_peer, args=(pid, addr), daemon=True)
            threads.append(t)
            t.start()

    # ======================================================================
    # Leader
    # ======================================================================

    def _become_leader(self):
        """Transition to leader.  MUST be called with self._lock held."""
        if self.role != Role.CANDIDATE:
            return

        self.role      = Role.LEADER
        self.leader_id = self.node_id

        last_log_index = len(self.log) - 1
        for pid in self.peers:
            self.next_index[pid]  = last_log_index + 1
            self.match_index[pid] = 0

        logger.info(
            "[Node %d] [T%d] Became LEADER  nextIndex=%s",
            self.node_id, self.current_term, dict(self.next_index),
        )

        # Append no-op to commit any entries from previous terms (§5.4.2)
        noop = LogEntry(
            index=len(self.log),
            term=self.current_term,
            command={"type": "no-op"},
        )
        self.log.append(noop)
        self._save_persistent()
        logger.info(
            "[Node %d] [T%d] Appended no-op at index %d",
            self.node_id, self.current_term, noop.index,
        )

        self._reset_election_timer()
        # Start per-peer replicators
        self._start_replicators()

    def _start_replicators(self):
        self._stop_replicators()
        for pid, addr in self.peers.items():
            rep = _PeerReplicator(self, pid, addr)
            self._replicators[pid] = rep
            rep.start()

    def _stop_replicators(self):
        for rep in self._replicators.values():
            rep.stop()
        self._replicators.clear()

    def _wake_replicators(self):
        """Wake all per-peer replicators to send immediately."""
        for rep in self._replicators.values():
            rep.wake()

    # ======================================================================
    # Commit index advancement  (leader only, called under lock)
    # ======================================================================

    def _try_advance_commit(self):
        """
        Advance commitIndex to the highest N such that:
          - N > commitIndex
          - log[N].term == currentTerm   (Raft §5.4.2)
          - matchIndex[peer] >= N for a majority
        """
        if self.role != Role.LEADER:
            return

        last = len(self.log) - 1
        for n in range(last, self.commit_index, -1):
            if self.log[n].term != self.current_term:
                continue
            count = 1  # self
            for pid in self.peers:
                if self.match_index.get(pid, 0) >= n:
                    count += 1
            cluster_size  = len(self.peers) + 1
            quorum        = cluster_size // 2 + 1
            if count >= quorum:
                old = self.commit_index
                self.commit_index = n
                logger.info(
                    "[Node %d] [T%d] commitIndex %d -> %d  (replicated on %d/%d nodes)",
                    self.node_id, self.current_term, old, n, count, cluster_size,
                )
                self._commit_cond.notify_all()
                self._wake_replicators()
                break

    # ======================================================================
    # RPC handlers
    # ======================================================================

    def handle_request_vote(self, req: RequestVoteRequest) -> RequestVoteResponse:
        with self._lock:
            nid = self.node_id

            # Rule §5.1: update term if request carries a higher one
            if req.term > self.current_term:
                logger.info(
                    "[Node %d] [T%d] RequestVote from %d has higher term %d -> step down",
                    nid, self.current_term, req.candidate_id, req.term,
                )
                self._step_down(req.term)

            logger.info(
                "[Node %d] [T%d] RequestVote from candidate=%d term=%d "
                "lastLogIdx=%d lastLogTerm=%d  (my votedFor=%s)",
                nid, self.current_term, req.candidate_id,
                req.term, req.last_log_index, req.last_log_term, self.voted_for,
            )

            # Reject stale term
            if req.term < self.current_term:
                logger.info(
                    "[Node %d] DENY vote: candidate term %d < currentTerm %d",
                    nid, req.term, self.current_term,
                )
                return RequestVoteResponse(term=self.current_term, vote_granted=False)

            # Reject if already voted for a different candidate this term
            if self.voted_for not in (None, req.candidate_id):
                logger.info(
                    "[Node %d] DENY vote: already voted for %d in term %d",
                    nid, self.voted_for, self.current_term,
                )
                return RequestVoteResponse(term=self.current_term, vote_granted=False)

            # Reject if candidate log is less up-to-date
            my_last_idx  = len(self.log) - 1
            my_last_term = self.log[-1].term
            log_ok = (
                req.last_log_term > my_last_term
                or (req.last_log_term == my_last_term and req.last_log_index >= my_last_idx)
            )
            if not log_ok:
                logger.info(
                    "[Node %d] DENY vote: log not up-to-date "
                    "(candidate lastLogTerm=%d lastLogIdx=%d; mine lastLogTerm=%d lastLogIdx=%d)",
                    nid, req.last_log_term, req.last_log_index, my_last_term, my_last_idx,
                )
                return RequestVoteResponse(term=self.current_term, vote_granted=False)

            # Grant vote
            self.voted_for = req.candidate_id
            self._save_persistent()
            self._reset_election_timer()

            logger.info(
                "[Node %d] [T%d] GRANT vote to candidate %d",
                nid, self.current_term, req.candidate_id,
            )
            return RequestVoteResponse(term=self.current_term, vote_granted=True)

    # ------------------------------------------------------------------

    def handle_append_entries(self, req: AppendEntriesRequest) -> AppendEntriesResponse:
        with self._lock:
            nid = self.node_id

            # Rule §5.1
            if req.term > self.current_term:
                self._step_down(req.term)

            # Reject if leader term is stale
            if req.term < self.current_term:
                return AppendEntriesResponse(
                    term=self.current_term, success=False
                )

            # Valid leader heartbeat / replication
            self.leader_id = req.leader_id
            self._reset_election_timer()

            if self.role == Role.CANDIDATE:
                logger.info(
                    "[Node %d] [T%d] Reverting to FOLLOWER: AppendEntries from leader %d",
                    nid, self.current_term, req.leader_id,
                )
                self.role = Role.FOLLOWER

            is_hb = (len(req.entries) == 0)
            if not is_hb:
                logger.info(
                    "[Node %d] [T%d] AppendEntries from leader %d: "
                    "prevIdx=%d prevTerm=%d #entries=%d leaderCommit=%d",
                    nid, self.current_term, req.leader_id,
                    req.prev_log_index, req.prev_log_term,
                    len(req.entries), req.leader_commit,
                )
            else:
                logger.debug(
                    "[Node %d] [T%d] Heartbeat from leader %d (leaderCommit=%d)",
                    nid, self.current_term, req.leader_id, req.leader_commit,
                )

            # --- Log consistency check ---
            if req.prev_log_index > 0:
                if req.prev_log_index >= len(self.log):
                    logger.info(
                        "[Node %d] REJECT AE: prevLogIndex=%d >= log len %d",
                        nid, req.prev_log_index, len(self.log),
                    )
                    return AppendEntriesResponse(
                        term=self.current_term, success=False,
                        conflict_index=len(self.log), conflict_term=0,
                    )

                actual_term = self.log[req.prev_log_index].term
                if actual_term != req.prev_log_term:
                    # Find first index of conflicting term
                    conflict_term = actual_term
                    ci = req.prev_log_index
                    while ci > 1 and self.log[ci - 1].term == conflict_term:
                        ci -= 1
                    logger.info(
                        "[Node %d] REJECT AE: term mismatch at prevLogIndex=%d "
                        "(expected %d got %d)  conflictIndex=%d",
                        nid, req.prev_log_index, req.prev_log_term, actual_term, ci,
                    )
                    return AppendEntriesResponse(
                        term=self.current_term, success=False,
                        conflict_index=ci, conflict_term=conflict_term,
                    )

            # --- Append/overwrite entries ---
            for i, entry in enumerate(req.entries):
                pos = req.prev_log_index + 1 + i
                if pos < len(self.log):
                    if self.log[pos].term != entry.term:
                        logger.info(
                            "[Node %d] Truncating log at index %d (conflict)",
                            nid, pos,
                        )
                        self.log = self.log[:pos]
                        self.log.append(entry)
                else:
                    self.log.append(entry)

            if req.entries:
                self._save_persistent()
                logger.info(
                    "[Node %d] [T%d] Appended %d entries; log now has %d real entries",
                    nid, self.current_term, len(req.entries), len(self.log) - 1,
                )

            # --- Advance commit index ---
            if req.leader_commit > self.commit_index:
                old = self.commit_index
                self.commit_index = min(req.leader_commit, len(self.log) - 1)
                if self.commit_index > old:
                    logger.info(
                        "[Node %d] [T%d] commitIndex %d -> %d (from leaderCommit=%d)",
                        nid, self.current_term, old, self.commit_index, req.leader_commit,
                    )
                    self._commit_cond.notify_all()

            match_idx = req.prev_log_index + len(req.entries)
            return AppendEntriesResponse(
                term=self.current_term, success=True, match_index=match_idx
            )

    # ======================================================================
    # Client proposal  (POST /debug/propose on leader)
    # ======================================================================

    def propose(self, command: Any, timeout: float = 10.0) -> Optional[int]:
        """
        Append command to log and block until committed.
        Returns committed log index, or None if not leader / timed out.
        """
        with self._lock:
            if self.role != Role.LEADER:
                return None
            entry = LogEntry(
                index=len(self.log),
                term=self.current_term,
                command=command,
            )
            self.log.append(entry)
            self._save_persistent()
            log_index = entry.index
            term      = self.current_term
            logger.info(
                "[Node %d] [T%d] Proposed command at index %d: %s",
                self.node_id, term, log_index, command,
            )

        # Immediately wake all replicators
        self._wake_replicators()

        # Wait for commit
        deadline = time.monotonic() + timeout
        with self._lock:
            while self.commit_index < log_index:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    logger.warning(
                        "[Node %d] [T%d] Propose timeout waiting for commit of index %d",
                        self.node_id, term, log_index,
                    )
                    return None
                self._commit_cond.wait(remaining)
                if self.role != Role.LEADER or self.current_term != term:
                    return None

        logger.info(
            "[Node %d] [T%d] Index %d committed", self.node_id, term, log_index
        )
        return log_index

    # ======================================================================
    # Apply loop
    # ======================================================================

    def _apply_loop(self):
        """Background thread: applies committed entries in order."""
        while not self._stop.is_set():
            with self._lock:
                while (
                    self.last_applied >= self.commit_index
                    and not self._stop.is_set()
                ):
                    self._commit_cond.wait(1.0)

                if self._stop.is_set():
                    break

                entries_to_apply = self.log[self.last_applied + 1 : self.commit_index + 1]
                target = self.commit_index

            for entry in entries_to_apply:
                logger.info(
                    "[Node %d] APPLY index=%d term=%d command=%s",
                    self.node_id, entry.index, entry.term, entry.command,
                )
                if self.apply_callback:
                    try:
                        self.apply_callback(entry)
                    except Exception as exc:
                        logger.error("apply_callback error: %s", exc)

            with self._lock:
                if target > self.last_applied:
                    self.last_applied = target

    # ======================================================================
    # State helpers
    # ======================================================================

    def _step_down(self, new_term: int):
        """Step down to follower.  MUST be called with self._lock held."""
        old_role = self.role
        if new_term > self.current_term:
            self.current_term = new_term
            self.voted_for    = None
            self.leader_id    = None
        self.role = Role.FOLLOWER
        self._save_persistent()
        self._reset_election_timer()
        if old_role != Role.FOLLOWER:
            logger.info(
                "[Node %d] [T%d] Stepped down %s -> FOLLOWER",
                self.node_id, self.current_term, old_role.value,
            )
            self._stop_replicators()

    def _save_persistent(self):
        """Persist durable state.  MUST be called with self._lock held."""
        self.storage.save(
            current_term=self.current_term,
            voted_for=self.voted_for,
            log=self.log,
        )

    # ======================================================================
    # Observability
    # ======================================================================

    def get_status(self) -> dict:
        with self._lock:
            last_log_index = len(self.log) - 1
            last_log_term  = self.log[-1].term if self.log else 0
            is_stopped     = getattr(self, "_is_stopped", False)
            role_str       = "stopped" if is_stopped else self.role.value

            entries = [
                {
                    "index": entry.index,
                    "term": entry.term,
                    "command": entry.command,
                    "is_noop": isinstance(entry.command, dict) and entry.command.get("type") == "no-op",
                }
                for entry in self.log[1:]
            ]

            blocked = self.transport.get_blocked() if hasattr(self.transport, "get_blocked") else []

            return {
                "id":             self.node_id,
                "role":           role_str,
                "is_stopped":     is_stopped,
                "current_term":   self.current_term,
                "voted_for":      self.voted_for,
                "leader_id":      self.leader_id,
                "commit_index":   self.commit_index,
                "last_applied":   self.last_applied,
                "log_length":     last_log_index,
                "last_log_index": last_log_index,
                "last_log_term":  last_log_term,
                "blocked":        blocked,
                "peers":          sorted(list(self.peers.keys())),
                "match_index":    dict(self.match_index) if self.role == Role.LEADER and not is_stopped else {},
                "next_index":     dict(self.next_index) if self.role == Role.LEADER and not is_stopped else {},
                "entries":        entries,
                "heartbeat_ms":   int(self.heartbeat_interval * 1000),
                "election_min_ms":int(self.election_timeout_min * 1000),
                "election_max_ms":int(self.election_timeout_max * 1000),
                # camelCase aliases
                "currentTerm":    self.current_term,
                "votedFor":       self.voted_for,
                "leaderId":       self.leader_id,
                "commitIndex":    self.commit_index,
                "lastApplied":    self.last_applied,
                "logLength":      last_log_index,
                "lastLogIndex":   last_log_index,
                "lastLogTerm":    last_log_term,
            }

    def get_leader_addr(self) -> Optional[str]:
        with self._lock:
            if self.leader_id is None:
                return None
            if self.leader_id == self.node_id:
                return None   # I am the leader
            return self.peers.get(self.leader_id)
