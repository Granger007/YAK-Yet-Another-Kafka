"""
raft/transport.py  -  HTTP/JSON transport layer for Raft RPCs.

Sends RequestVote and AppendEntries over plain HTTP POST using only
stdlib urllib.  Also owns the partition-simulation state (a set of
blocked peer IDs that the node will silently drop traffic to/from).
"""

from __future__ import annotations

import json
import logging
import threading
import urllib.error
import urllib.request
from typing import Optional, Set

from .rpc import (
    AppendEntriesRequest,
    AppendEntriesResponse,
    RequestVoteRequest,
    RequestVoteResponse,
)

logger = logging.getLogger(__name__)

_HTTP_TIMEOUT = 1.0   # seconds per RPC call


class RaftTransport:
    """Sends Raft RPCs to peers and manages partition state."""

    def __init__(self, node_id: int):
        self.node_id = node_id
        self._blocked: Set[int] = set()
        self._lock = threading.Lock()

    # ------------------------------------------------------------------
    # Partition simulation
    # ------------------------------------------------------------------

    def set_blocked(self, peer_ids: list) -> None:
        with self._lock:
            self._blocked = set(peer_ids)
        logger.info("[Node %d] Partition: blocking peers %s", self.node_id, peer_ids)

    def is_blocked(self, peer_id: int) -> bool:
        with self._lock:
            return peer_id in self._blocked

    def get_blocked(self) -> list:
        with self._lock:
            return sorted(list(self._blocked))

    # ------------------------------------------------------------------
    # RPC callers
    # ------------------------------------------------------------------

    def send_request_vote(
        self,
        peer_id: int,
        peer_addr: str,
        req: RequestVoteRequest,
    ) -> Optional[RequestVoteResponse]:
        if self.is_blocked(peer_id):
            logger.debug("[Node %d] DROP RequestVote to peer %d (partitioned)", self.node_id, peer_id)
            return None
        url = f"http://{peer_addr}/raft/requestVote"
        resp_data = self._post(url, req.to_dict(), peer_id, "RequestVote")
        if resp_data is None:
            return None
        return RequestVoteResponse.from_dict(resp_data)

    def send_append_entries(
        self,
        peer_id: int,
        peer_addr: str,
        req: AppendEntriesRequest,
    ) -> Optional[AppendEntriesResponse]:
        if self.is_blocked(peer_id):
            logger.debug(
                "[Node %d] DROP AppendEntries to peer %d (partitioned)",
                self.node_id,
                peer_id,
            )
            return None
        url = f"http://{peer_addr}/raft/appendEntries"
        resp_data = self._post(url, req.to_dict(), peer_id, "AppendEntries")
        if resp_data is None:
            return None
        return AppendEntriesResponse.from_dict(resp_data)

    # ------------------------------------------------------------------
    # Internal HTTP helper
    # ------------------------------------------------------------------

    def _post(self, url: str, payload: dict, peer_id: int, rpc_name: str) -> Optional[dict]:
        body = json.dumps(payload).encode()
        req = urllib.request.Request(
            url,
            data=body,
            headers={"Content-Type": "application/json", "Connection": "close"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=_HTTP_TIMEOUT) as resp:
                return json.loads(resp.read().decode())
        except urllib.error.URLError as exc:
            logger.debug("[Node %d] %s -> peer %d failed: %s", self.node_id, rpc_name, peer_id, exc.reason)
        except Exception as exc:
            logger.debug("[Node %d] %s -> peer %d error: %s", self.node_id, rpc_name, peer_id, exc)
        return None
