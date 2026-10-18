"""
raft/storage.py  -  Durable persistence for Raft state.

Writes currentTerm, votedFor and the full log to a single JSON file
inside the node's data directory *before* responding to any RPC,
satisfying the Raft durability requirement (section 5.8).

File format (pretty-printed for debuggability):
{
  "current_term": 3,
  "voted_for": 2,
  "log": [
    {"index": 0, "term": 0, "command": null},
    {"index": 1, "term": 1, "command": {"type": "no-op"}},
    ...
  ]
}
"""

from __future__ import annotations

import json
import logging
import os
import tempfile
from typing import List, Optional

from .rpc import LogEntry

logger = logging.getLogger(__name__)


class RaftStorage:
    """Atomic-write JSON persistence for one Raft node."""

    STATE_FILE = "raft_state.json"

    def __init__(self, data_dir: str):
        os.makedirs(data_dir, exist_ok=True)
        self._path = os.path.join(data_dir, self.STATE_FILE)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def save(
        self,
        current_term: int,
        voted_for: Optional[int],
        log: List[LogEntry],
    ) -> None:
        """Atomically persist Raft durable state."""
        payload = {
            "current_term": current_term,
            "voted_for": voted_for,
            "log": [e.to_dict() for e in log],
        }
        # Write to a temp file then rename for atomic replace.
        dir_ = os.path.dirname(self._path)
        fd, tmp = tempfile.mkstemp(dir=dir_, suffix=".tmp")
        try:
            with os.fdopen(fd, "w") as f:
                json.dump(payload, f, indent=2)
            os.replace(tmp, self._path)
        except Exception:
            os.unlink(tmp)
            raise

    def load(self) -> Optional[dict]:
        """Load persisted state.  Returns None if no state file exists yet."""
        if not os.path.exists(self._path):
            return None
        try:
            with open(self._path, "r") as f:
                data = json.load(f)
            return {
                "current_term": data.get("current_term", 0),
                "voted_for": data.get("voted_for"),
                "log": [LogEntry.from_dict(e) for e in data.get("log", [])],
            }
        except (json.JSONDecodeError, KeyError) as exc:
            logger.warning("Corrupt state file %s: %s  – starting fresh", self._path, exc)
            return None
