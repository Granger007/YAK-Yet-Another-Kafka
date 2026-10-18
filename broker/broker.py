#!/usr/bin/env python3
"""
broker/broker.py  -  Entry point for a single Raft broker process.

Usage:
    python -m broker.broker --id 1 --port 8001 --peers 2:localhost:8002,3:localhost:8003,...

Flags
-----
--id                   Integer node ID (required)
--port                 TCP port to listen on (required)
--peers                Comma-separated list of  id:host:port  for all other nodes
--data-dir             Directory for persistent state  (default: ./data/node-<id>)
--election-min-ms      Min election timeout in ms      (default: 1500)
--election-max-ms      Max election timeout in ms      (default: 3000)
--heartbeat-ms         Heartbeat interval in ms        (default: 300)
--log-level            DEBUG / INFO / WARNING           (default: INFO)
"""

import argparse
import logging
import os
import signal
import sys
import time

# Make sure the project root is on the path regardless of CWD
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from raft.node      import RaftNode
from raft.storage   import RaftStorage
from raft.transport import RaftTransport
from server.server  import RaftServer


def parse_peers(raw: str) -> dict:
    """
    Parse  "2:localhost:8002,3:localhost:8003"  -> {2: "localhost:8002", ...}
    """
    peers = {}
    for chunk in raw.split(","):
        chunk = chunk.strip()
        if not chunk:
            continue
        parts = chunk.split(":", 2)   # id : host : port
        if len(parts) != 3:
            raise ValueError(f"Bad peer spec: {chunk!r}  (expected id:host:port)")
        pid, host, port = int(parts[0]), parts[1], int(parts[2])
        peers[pid] = f"{host}:{port}"
    return peers


def main():
    default_id = int(os.environ["RAFT_ID"]) if "RAFT_ID" in os.environ else None
    default_port = int(os.environ["RAFT_PORT"]) if "RAFT_PORT" in os.environ else None

    parser = argparse.ArgumentParser(description="Raft broker node")
    parser.add_argument("--id",              type=int, default=default_id, required=default_id is None,
                        help="Broker ID (or env RAFT_ID)")
    parser.add_argument("--port",            type=int, default=default_port, required=default_port is None,
                        help="HTTP listen port (or env RAFT_PORT)")
    parser.add_argument("--peers",           type=str, default=os.environ.get("RAFT_PEERS", ""),
                        help="id:host:port[,id:host:port,...] (or env RAFT_PEERS)")
    parser.add_argument("--data-dir",        type=str, default=os.environ.get("RAFT_DATA_DIR", ""),
                        help="State directory (or env RAFT_DATA_DIR)")
    parser.add_argument("--election-min-ms", type=int, default=int(os.environ.get("RAFT_ELECTION_MIN_MS", "1500")),
                        help="Min election timeout (or env RAFT_ELECTION_MIN_MS)")
    parser.add_argument("--election-max-ms", type=int, default=int(os.environ.get("RAFT_ELECTION_MAX_MS", "3000")),
                        help="Max election timeout (or env RAFT_ELECTION_MAX_MS)")
    parser.add_argument("--heartbeat-ms",    type=int, default=int(os.environ.get("RAFT_HEARTBEAT_MS", "300")),
                        help="Heartbeat interval (or env RAFT_HEARTBEAT_MS)")
    parser.add_argument("--log-level",       type=str, default=os.environ.get("RAFT_LOG_LEVEL", "INFO"),
                        help="Log level (or env RAFT_LOG_LEVEL)")
    args = parser.parse_args()

    # ---- Logging ----
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s.%(msecs)03d  %(levelname)-7s  %(message)s",
        datefmt="%H:%M:%S",
    )

    # ---- Peers ----
    peers = parse_peers(args.peers) if args.peers else {}

    # ---- Data directory ----
    data_dir = args.data_dir or os.path.join("data", f"node-{args.id}")
    os.makedirs(data_dir, exist_ok=True)

    # ---- Build components ----
    storage   = RaftStorage(data_dir)
    transport = RaftTransport(args.id)
    node      = RaftNode(
        node_id               = args.id,
        peers                 = peers,
        storage               = storage,
        transport             = transport,
        election_timeout_min_ms = args.election_min_ms,
        election_timeout_max_ms = args.election_max_ms,
        heartbeat_interval_ms   = args.heartbeat_ms,
    )
    srv = RaftServer(host="0.0.0.0", port=args.port, node=node)

    # ---- Start ----
    srv.start()
    node.start()

    logging.info("Broker %d running on port %d  peers=%s", args.id, args.port, peers)

    # ---- Graceful shutdown ----
    def _shutdown(sig, frame):
        logging.info("Broker %d shutting down (signal %d)", args.id, sig)
        node.stop()
        srv.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT,  _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        _shutdown(signal.SIGINT, None)


if __name__ == "__main__":
    main()
