"""
raft/rpc.py  -  Raft RPC message types (stdlib only, no third-party deps).

LogEntry.command is intentionally typed as Any so future broker
payloads plug straight in without touching Raft internals.
"""

from __future__ import annotations
from dataclasses import dataclass
from typing import Any, List


# ---------------------------------------------------------------------------
# Log entry
# ---------------------------------------------------------------------------

@dataclass
class LogEntry:
    index: int    # 1-based  (index 0 is the sentinel "dummy" entry)
    term: int
    command: Any  # opaque - dict / str / bytes, must be JSON-serialisable

    def to_dict(self):
        return {"index": self.index, "term": self.term, "command": self.command}

    @staticmethod
    def from_dict(d):
        return LogEntry(index=d["index"], term=d["term"], command=d.get("command"))


# ---------------------------------------------------------------------------
# RequestVote
# ---------------------------------------------------------------------------

@dataclass
class RequestVoteRequest:
    term: int
    candidate_id: int
    last_log_index: int
    last_log_term: int

    def to_dict(self):
        return {
            "term": self.term,
            "candidate_id": self.candidate_id,
            "last_log_index": self.last_log_index,
            "last_log_term": self.last_log_term,
        }

    @staticmethod
    def from_dict(d):
        return RequestVoteRequest(
            term=d["term"],
            candidate_id=d["candidate_id"],
            last_log_index=d["last_log_index"],
            last_log_term=d["last_log_term"],
        )


@dataclass
class RequestVoteResponse:
    term: int
    vote_granted: bool

    def to_dict(self):
        return {"term": self.term, "vote_granted": self.vote_granted}

    @staticmethod
    def from_dict(d):
        return RequestVoteResponse(term=d["term"], vote_granted=d["vote_granted"])


# ---------------------------------------------------------------------------
# AppendEntries
# ---------------------------------------------------------------------------

@dataclass
class AppendEntriesRequest:
    term: int
    leader_id: int
    prev_log_index: int
    prev_log_term: int
    entries: List[LogEntry]
    leader_commit: int

    def to_dict(self):
        return {
            "term": self.term,
            "leader_id": self.leader_id,
            "prev_log_index": self.prev_log_index,
            "prev_log_term": self.prev_log_term,
            "entries": [e.to_dict() for e in self.entries],
            "leader_commit": self.leader_commit,
        }

    @staticmethod
    def from_dict(d):
        return AppendEntriesRequest(
            term=d["term"],
            leader_id=d["leader_id"],
            prev_log_index=d["prev_log_index"],
            prev_log_term=d["prev_log_term"],
            entries=[LogEntry.from_dict(e) for e in d.get("entries", [])],
            leader_commit=d["leader_commit"],
        )


@dataclass
class AppendEntriesResponse:
    term: int
    success: bool
    match_index: int = 0
    conflict_index: int = 0   # first index of conflicting term (fast back-off)
    conflict_term: int = 0    # term at the conflicting position

    def to_dict(self):
        return {
            "term": self.term,
            "success": self.success,
            "match_index": self.match_index,
            "conflict_index": self.conflict_index,
            "conflict_term": self.conflict_term,
        }

    @staticmethod
    def from_dict(d):
        return AppendEntriesResponse(
            term=d["term"],
            success=d["success"],
            match_index=d.get("match_index", 0),
            conflict_index=d.get("conflict_index", 0),
            conflict_term=d.get("conflict_term", 0),
        )
