"""
server/server.py  -  ThreadingHTTPServer that wires URL routes to handlers.

Uses only stdlib http.server.  Each request is handled in its own thread
(ThreadingMixIn) so blocking propose() calls do not stall heartbeats.
"""

from __future__ import annotations

import json
import logging
import os
import threading
import urllib.parse
from http.server import BaseHTTPRequestHandler, HTTPServer
from socketserver import ThreadingMixIn
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from raft.node import RaftNode
    from broker.store import BrokerStore

from server.handlers import (
    handle_append_entries,
    handle_fetch,
    handle_fetch_production,
    handle_message_produce,
    handle_metadata_leader,
    handle_partition,
    handle_produce,
    handle_propose,
    handle_request_vote,
    handle_restart_or_resume,
    handle_status,
    handle_step_down,
    handle_stop_or_kill,
    handle_topics,
    handle_trigger_election,
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

    node: "RaftNode"                  # injected by RaftServer
    store: Optional["BrokerStore"] = None  # injected by RaftServer

    def log_message(self, fmt, *args):
        # Suppress default access log (we use structured logging)
        pass

    # ---------------------------------------------------------------- OPTIONS (CORS)

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, Authorization, Accept, X-Requested-With")
        self.send_header("Access-Control-Max-Age", "86400")
        self.send_header("Content-Length", "0")
        self.end_headers()
        self.close_connection = True

    # ---------------------------------------------------------------- GET

    def do_GET(self):
        parts = self.path.split("?", 1)
        path = parts[0]
        qs = parts[1] if len(parts) > 1 else ""

        if path in ("/", "/dashboard"):
            dashboard_path = os.path.join(
                os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                "raft_dashboard.html",
            )
            if os.path.exists(dashboard_path):
                with open(dashboard_path, "rb") as f:
                    html = f.read()
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.send_header("Content-Length", str(len(html)))
                self.send_header("Connection", "close")
                self.end_headers()
                self.wfile.write(html)
                self.wfile.flush()
                self.close_connection = True
                return

        if path == "/status":
            code, body = handle_status(self.node, getattr(self, "store", None))
        elif path == "/metadata/leader":
            code, body = handle_metadata_leader(self.node)
        elif path == "/topics":
            code, body = handle_topics(getattr(self, "store", None))
        elif path == "/debug/fetch":
            parsed = urllib.parse.parse_qs(qs)
            query_dict = {k: v[0] for k, v in parsed.items() if v}
            code, body = handle_fetch(query_dict, getattr(self, "store", None))
        elif path == "/fetch":
            parsed = urllib.parse.parse_qs(qs)
            query_dict = {k: v[0] for k, v in parsed.items() if v}
            code, body = handle_fetch_production(query_dict, self.node, getattr(self, "store", None))
        else:
            code, body = 404, {"error": "not found"}
        self._send_json(code, body, path)

    # ---------------------------------------------------------------- POST

    def do_POST(self):
        path    = self.path.split("?")[0]
        body    = self._read_json()
        if body is None:
            self._send_json(400, {"error": "invalid JSON"}, path)
            return

        if path == "/raft/requestVote":
            code, resp = handle_request_vote(body, self.node)
        elif path == "/raft/appendEntries":
            code, resp = handle_append_entries(body, self.node)
        elif path == "/debug/propose":
            code, resp = handle_propose(body, self.node)
        elif path == "/debug/produce":
            code, resp = handle_produce(body, self.node, getattr(self, "store", None))
        elif path == "/message":
            code, resp = handle_message_produce(body, self.node, getattr(self, "store", None))
        elif path == "/debug/partition":
            code, resp = handle_partition(body, self.node)
        elif path in ("/debug/kill", "/debug/stop"):
            code, resp = handle_stop_or_kill(body, self.node)
        elif path in ("/debug/restart", "/debug/start", "/debug/resume"):
            code, resp = handle_restart_or_resume(body, self.node)
        elif path == "/debug/trigger-election":
            code, resp = handle_trigger_election(self.node)
        elif path == "/debug/step-down":
            code, resp = handle_step_down(self.node)
        else:
            code, resp = 404, {"error": "not found"}

        self._send_json(code, resp, path)

    # ---------------------------------------------------------------- helpers

    def _read_json(self):
        length = int(self.headers.get("Content-Length", 0))
        raw    = self.rfile.read(length)
        try:
            return json.loads(raw.decode()) if raw else {}
        except json.JSONDecodeError:
            return None

    def _send_json(self, code: int, body: dict, req_path: str = ""):
        payload = json.dumps(body).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, Authorization, Accept, X-Requested-With")
        self.send_header("Content-Length", str(len(payload)))
        self.send_header("Connection", "close")
        if code == 307:
            leader_addr = body.get("leader_address") or body.get("leaderAddress")
            if leader_addr:
                redirect_target = self.path if self.path.startswith("/") else f"/{self.path}"
                self.send_header(
                    "Location",
                    f"http://{leader_addr}{redirect_target}",
                )
        try:
            self.end_headers()
            self.wfile.write(payload)
            self.wfile.flush()
        except OSError:
            pass
        self.close_connection = True


# ---------------------------------------------------------------------------
# Public interface
# ---------------------------------------------------------------------------

class RaftServer:
    """Wraps _ThreadingHTTPServer and injects RaftNode and BrokerStore into handlers."""

    def __init__(self, host: str, port: int, node: "RaftNode", store: Optional["BrokerStore"] = None):
        self._node = node
        self._port = port
        self._store = store

        # Build a handler class with the node and store pre-bound
        handler = type("BoundHandler", (_RaftHandler,), {"node": node, "store": store})
        self._server = _ThreadingHTTPServer((host, port), handler)

    def start(self):
        """Start serving in a background daemon thread."""
        t = threading.Thread(target=self._server.serve_forever, daemon=True,
                             name=f"http-{self._port}")
        t.start()
        logger.info("HTTP server listening on port %d", self._port)

    def stop(self):
        self._server.shutdown()

