"""
server/handlers.py  -  Route handler functions.

Each function receives the parsed JSON body (dict) and the RaftNode,
and returns a (status_code: int, response_body: dict) tuple.
"""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, Any, Dict, Optional, Tuple

if TYPE_CHECKING:
    from raft.node import RaftNode
    from broker.store import BrokerStore

from raft.rpc import AppendEntriesRequest, RequestVoteRequest
from broker.hash import select_partition
import time

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Raft internal RPC handlers
# ---------------------------------------------------------------------------

def handle_request_vote(body: dict, node: "RaftNode") -> Tuple[int, dict]:
    if getattr(node, "_is_stopped", False):
        return 503, {"error": "node stopped / offline"}

    # Reject if partitioned from this candidate
    candidate_id = body.get("candidate_id")
    if candidate_id is not None and node.transport.is_blocked(candidate_id):
        logger.debug(
            "[Node %d] DROP incoming RequestVote from partitioned candidate %d",
            node.node_id, candidate_id,
        )
        return 403, {"error": "partitioned"}

    try:
        req  = RequestVoteRequest.from_dict(body)
        resp = node.handle_request_vote(req)
        return 200, resp.to_dict()
    except Exception as exc:
        logger.exception("handle_request_vote error")
        return 500, {"error": str(exc)}


def handle_append_entries(body: dict, node: "RaftNode") -> Tuple[int, dict]:
    if getattr(node, "_is_stopped", False):
        return 503, {"error": "node stopped / offline"}

    # Reject if partitioned from this sender
    sender_id = body.get("leader_id")
    if sender_id is not None and node.transport.is_blocked(sender_id):
        logger.debug(
            "[Node %d] DROP incoming AppendEntries from partitioned peer %d",
            node.node_id, sender_id,
        )
        return 403, {"error": "partitioned"}

    try:
        req  = AppendEntriesRequest.from_dict(body)
        resp = node.handle_append_entries(req)
        return 200, resp.to_dict()
    except Exception as exc:
        logger.exception("handle_append_entries error")
        return 500, {"error": str(exc)}


# ---------------------------------------------------------------------------
# Observability & Metadata
# ---------------------------------------------------------------------------

def handle_status(node: "RaftNode", store: Optional["BrokerStore"] = None) -> Tuple[int, dict]:
    status = node.get_status()
    if store is not None:
        status.update(store.get_summary())
    return 200, status


def handle_topics(store: Optional["BrokerStore"]) -> Tuple[int, dict]:
    """GET /topics"""
    if store is None:
        return 503, {"error": "broker store not initialized"}
    return 200, {"topics": store.get_topics_metadata()}


def handle_fetch(query_params: dict, store: Optional["BrokerStore"]) -> Tuple[int, dict]:
    """
    GET /debug/fetch?topic=&partition=&offset=&max=
    Allowed on any node to verify follower replication.
    """
    if store is None:
        return 503, {"error": "broker store not initialized"}

    topic = query_params.get("topic")
    if not topic or topic not in store.config.topics:
        return 404, {"error": f"unknown topic: {topic!r}"}

    num_parts = store.config.topics[topic]
    raw_p = query_params.get("partition", "0")
    try:
        partition = int(raw_p)
        if not (0 <= partition < num_parts):
            return 400, {"error": f"partition {partition} out of range [0, {num_parts - 1}]"}
    except (ValueError, TypeError):
        return 400, {"error": f"invalid partition value: {raw_p!r}"}

    raw_off = query_params.get("offset", "0")
    try:
        offset = int(raw_off)
        if offset < 0:
            return 400, {"error": "offset must be >= 0"}
    except (ValueError, TypeError):
        return 400, {"error": f"invalid offset value: {raw_off!r}"}

    raw_max = query_params.get("max", "100")
    try:
        max_records = max(1, min(1000, int(raw_max)))
    except (ValueError, TypeError):
        max_records = 100

    try:
        records, hwm, log_end = store.fetch(topic, partition, offset, max_records)
        return 200, {
            "topic": topic,
            "partition": partition,
            "offset": offset,
            "hwm": hwm,
            "logEndOffset": log_end,
            "records": records,
        }
    except Exception as exc:
        return 400, {"error": str(exc)}


def handle_produce(body: dict, node: "RaftNode", store: Optional["BrokerStore"]) -> Tuple[int, dict]:
    """
    POST /debug/produce
    Body: {"topic": str, "messageID": str, "key": Optional[str], "partition": Optional[int], "payload": Any}
    """
    if getattr(node, "_is_stopped", False):
        return 503, {"error": "node stopped / offline"}

    if store is None:
        return 503, {"error": "broker store not initialized"}

    topic = body.get("topic")
    if not topic or topic not in store.config.topics:
        return 404, {"error": f"unknown topic: {topic!r}"}

    message_id = body.get("messageID")
    if message_id is None or not isinstance(message_id, str) or not message_id.strip():
        return 400, {"error": "messageID is required and cannot be empty"}

    num_parts = store.config.topics[topic]
    explicit_p = body.get("partition")
    if explicit_p is not None:
        try:
            explicit_p = int(explicit_p)
            if not (0 <= explicit_p < num_parts):
                return 400, {"error": f"partition {explicit_p} out of range [0, {num_parts - 1}]"}
        except (ValueError, TypeError):
            return 400, {"error": f"invalid partition value: {explicit_p!r}"}

    # Follower redirection / leader check
    from raft.node import Role
    with node._lock:
        role = node.role
        leader_id = node.leader_id
        if leader_id is not None and leader_id != node.node_id:
            leader_addr = node.peers.get(leader_id)
        else:
            leader_addr = None

    if role != Role.LEADER:
        if leader_addr:
            return 307, {
                "error": "not the leader",
                "leader_id": leader_id,
                "leader_address": leader_addr,
            }
        return 503, {"error": "no known leader", "leader_id": None}

    # Fast pre-check deduplication on leader
    pre_dup = store.precheck_dedup(topic, message_id)
    if pre_dup is not None:
        return 200, pre_dup

    # Partition selection
    key = body.get("key")
    try:
        chosen_partition, reason = select_partition(
            num_parts=num_parts,
            explicit_partition=explicit_p,
            key=key,
            message_id=message_id,
        )
    except ValueError as exc:
        return 400, {"error": str(exc)}

    logger.info(
        "[Node %d] Produce received topic=%s msgId=%s key=%s -> partition=%d (reason=%s)",
        node.node_id, topic, message_id, key, chosen_partition, reason,
    )

    command = {
        "type": "produce",
        "topic": topic,
        "partition": chosen_partition,
        "messageID": message_id,
        "key": key,
        "payload": body.get("payload"),
        "timestamp": time.time(),
    }

    # Propose to Raft
    log_index = node.propose(command, timeout=10.0)
    if log_index is None:
        return 504, {"error": "timed out or lost leadership before commit"}

    # Wait until entry is applied to the replicated state machine
    apply_result = store.wait_applied(log_index, timeout=10.0)
    if apply_result is None:
        return 504, {"error": "timed out waiting for entry to be applied"}

    if "error" in apply_result:
        return 400, apply_result

    return 200, apply_result


def handle_metadata_leader(node: "RaftNode") -> Tuple[int, dict]:
    with node._lock:
        leader_id = node.leader_id
        if leader_id is not None and leader_id == node.node_id:
            # This node is the leader – advertise our own address
            self_addr = node.peers.get(node.node_id)  # may be None for single-node
            leader_addr = self_addr
        elif leader_id is not None:
            leader_addr = node.peers.get(leader_id)
        else:
            leader_addr = None

    if leader_id is None:
        # Election in progress – tell clients to retry
        return 503, {
            "leaderId": None,
            "leaderAddress": None,
            "leader_id": None,
            "leader_address": None,
            "error": "no leader elected",
        }
    return 200, {
        "leaderId": leader_id,
        "leaderAddress": leader_addr,
        "leader_id": leader_id,
        "leader_address": leader_addr,
    }


# ---------------------------------------------------------------------------
# Debug / testing
# ---------------------------------------------------------------------------

def handle_propose(body: dict, node: "RaftNode") -> Tuple[int, dict]:
    """
    POST /debug/propose  {"data": "..."}

    On the leader: appends the entry and blocks until committed.
    On a follower: returns 307 with the known leader address.
    """
    if getattr(node, "_is_stopped", False):
        return 503, {"error": "node stopped / offline"}

    from raft.node import Role

    with node._lock:
        role      = node.role
        leader_id = node.leader_id
        # Inline peer lookup to avoid re-entering node._lock
        if leader_id is not None and leader_id != node.node_id:
            leader_addr = node.peers.get(leader_id)
        else:
            leader_addr = None

    if role != Role.LEADER:
        if leader_addr:
            return 307, {
                "error":          "not the leader",
                "leader_id":      leader_id,
                "leader_address": leader_addr,
            }
        return 503, {"error": "no known leader", "leader_id": None}

    command   = body.get("data", body)   # accept {"data": ...} or raw dict
    log_index = node.propose(command)

    if log_index is None:
        return 504, {"error": "timed out or lost leadership before commit"}

    return 200, {"committed": True, "log_index": log_index}


def handle_partition(body: dict, node: "RaftNode") -> Tuple[int, dict]:
    """
    POST /debug/partition  {"blocked": [2, 3]}

    Installs a set of peer IDs whose traffic this node will silently drop.
    Send an empty list to clear the partition.
    """
    blocked = body.get("blocked", [])
    node.transport.set_blocked(blocked)
    return 200, {"blocked": blocked}


def handle_stop_or_kill(body: dict, node: "RaftNode") -> Tuple[int, dict]:
    """
    POST /debug/kill or POST /debug/stop {"terminate": false}

    Pauses the node state machine (simulating a crash) or optionally terminates the process.
    """
    terminate = body.get("terminate", False)
    if terminate:
        import os
        import threading
        import time

        def _die():
            time.sleep(0.1)
            os._exit(0)

        threading.Thread(target=_die, daemon=True).start()
        return 200, {"success": True, "action": "terminated"}

    node.pause()
    return 200, {"success": True, "action": "paused", "is_stopped": True}


def handle_restart_or_resume(body: dict, node: "RaftNode") -> Tuple[int, dict]:
    """
    POST /debug/restart or POST /debug/start or POST /debug/resume
    """
    node.resume()
    return 200, {"success": True, "action": "resumed", "is_stopped": False}


def handle_trigger_election(node: "RaftNode") -> Tuple[int, dict]:
    """
    POST /debug/trigger-election
    """
    ok = node.trigger_election()
    if ok:
        return 200, {"success": True, "message": "election triggered"}
    return 400, {"error": "cannot trigger election on stopped node or leader"}


def handle_step_down(node: "RaftNode") -> Tuple[int, dict]:
    """
    POST /debug/step-down
    """
    ok = node.step_down_leader()
    if ok:
        return 200, {"success": True, "message": "leader stepped down"}
    return 400, {"error": "node is not a leader"}


# ---------------------------------------------------------------------------
# Production endpoints  (POST /message  and  GET /fetch)
# ---------------------------------------------------------------------------

def handle_message_produce(
    body: dict, node: "RaftNode", store: Optional["BrokerStore"]
) -> Tuple[int, dict]:
    """
    POST /message

    Production producer endpoint.  Identical semantics to /debug/produce:
    - Leader validates, dedup pre-check, proposes to Raft, responds only
      after the entry is committed AND applied to the broker state machine.
    - Response: {topic, partition, offset, duplicate, hwm}
    - Follower: 307 with {leaderId, leaderAddress} in body.
    - No known leader: 503.
    """
    return handle_produce(body, node, store)


def handle_fetch_production(
    query_params: dict,
    node: "RaftNode",
    store: Optional["BrokerStore"],
) -> Tuple[int, dict]:
    """
    GET /fetch?topic=&partition=&offset=&max=

    Production consumer endpoint.  Leader-only: followers return 307.
    Returns records from [offset, HWM).  Never returns records at or
    beyond HWM.  offset >= hwm -> empty list, 200.  offset > hwm -> same.
    """
    if getattr(node, "_is_stopped", False):
        return 503, {"error": "node stopped / offline"}

    if store is None:
        return 503, {"error": "broker store not initialized"}

    # Leader check – followers redirect
    from raft.node import Role
    with node._lock:
        role = node.role
        leader_id = node.leader_id
        if leader_id is not None and leader_id != node.node_id:
            leader_addr = node.peers.get(leader_id)
        else:
            leader_addr = None

    if role != Role.LEADER:
        if leader_id is not None and leader_addr:
            return 307, {
                "error": "not the leader",
                "leaderId": leader_id,
                "leaderAddress": leader_addr,
                "leader_id": leader_id,
                "leader_address": leader_addr,
            }
        return 503, {"error": "no known leader", "leaderId": None}

    # Delegate to the existing /debug/fetch handler (any-node read).
    # We've already asserted leadership above, so this is safe.
    return handle_fetch(query_params, store)
