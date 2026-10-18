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

from raft.rpc import AppendEntriesRequest, RequestVoteRequest

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Raft internal RPC handlers
# ---------------------------------------------------------------------------

def handle_request_vote(body: dict, node: "RaftNode") -> Tuple[int, dict]:
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
# Observability
# ---------------------------------------------------------------------------

def handle_status(node: "RaftNode") -> Tuple[int, dict]:
    return 200, node.get_status()


def handle_metadata_leader(node: "RaftNode") -> Tuple[int, dict]:
    with node._lock:
        leader_id = node.leader_id
        # Inline peer lookup to avoid re-entering node._lock
        if leader_id is not None and leader_id != node.node_id:
            leader_addr = node.peers.get(leader_id)
        else:
            leader_addr = None
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
