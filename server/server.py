"""
server/server.py  -  ThreadingHTTPServer that wires URL routes to handlers.

Uses only stdlib http.server.  Each request is handled in its own thread
(ThreadingMixIn) so blocking propose() calls do not stall heartbeats.
"""

from __future__ import annotations

import json
import logging
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from socketserver import ThreadingMixIn
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from raft.node import RaftNode

from server.handlers import (
    handle_append_entries,
    handle_metadata_leader,
    handle_partition,
    handle_propose,
    handle_request_vote,
    handle_status,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Thread-safe HTTP server
# ---------------------------------------------------------------------------

class _ThreadingHTTPServer(ThreadingMixIn, HTTPServer):
    daemon_threads = True    # child threads die with the main process
    allow_reuse_address = True


# ---------------------------------------------------------------------------
# Request handler
# ---------------------------------------------------------------------------

class _RaftHandler(BaseHTTPRequestHandler):
    """Routes GET/POST requests to the appropriate handler function."""

    node: "RaftNode"    # injected by RaftServer

    def log_message(self, fmt, *args):
        # Suppress default access log (we use structured logging)
        pass

    # ---------------------------------------------------------------- GET

    def do_GET(self):
        path = self.path.split("?")[0]
        if path == "/status":
            code, body = handle_status(self.node)
        elif path == "/metadata/leader":
            code, body = handle_metadata_leader(self.node)
        else:
            code, body = 404, {"error": "not found"}
        self._send_json(code, body)

    # ---------------------------------------------------------------- POST

    def do_POST(self):
        path    = self.path.split("?")[0]
        body    = self._read_json()
        if body is None:
            self._send_json(400, {"error": "invalid JSON"})
            return

        if path == "/raft/requestVote":
            code, resp = handle_request_vote(body, self.node)
        elif path == "/raft/appendEntries":
            code, resp = handle_append_entries(body, self.node)
        elif path == "/debug/propose":
            code, resp = handle_propose(body, self.node)
        elif path == "/debug/partition":
            code, resp = handle_partition(body, self.node)
        else:
            code, resp = 404, {"error": "not found"}

        self._send_json(code, resp)

    # ---------------------------------------------------------------- helpers

    def _read_json(self):
        length = int(self.headers.get("Content-Length", 0))
        raw    = self.rfile.read(length)
        try:
            return json.loads(raw.decode()) if raw else {}
        except json.JSONDecodeError:
            return None

    def _send_json(self, code: int, body: dict):
        payload = json.dumps(body).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.send_header("Connection", "close")
        if code == 307 and body.get("leader_address"):
            self.send_header(
                "Location",
                f"http://{body['leader_address']}/debug/propose",
            )
        self.end_headers()
        self.wfile.write(payload)
        self.wfile.flush()
        self.close_connection = True


# ---------------------------------------------------------------------------
# Public interface
# ---------------------------------------------------------------------------

class RaftServer:
    """Wraps _ThreadingHTTPServer and injects the RaftNode into handlers."""

    def __init__(self, host: str, port: int, node: "RaftNode"):
        self._node = node
        self._port = port

        # Build a handler class with the node pre-bound
        handler = type("BoundHandler", (_RaftHandler,), {"node": node})
        self._server = _ThreadingHTTPServer((host, port), handler)

    def start(self):
        """Start serving in a background daemon thread."""
        t = threading.Thread(target=self._server.serve_forever, daemon=True,
                             name=f"http-{self._port}")
        t.start()
        logger.info("HTTP server listening on port %d", self._port)

    def stop(self):
        self._server.shutdown()
