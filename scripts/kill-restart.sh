#!/usr/bin/env bash
# kill-restart.sh  -  Kill or restart a specific node.
#
# Usage:
#   bash scripts/kill-restart.sh kill   2
#   bash scripts/kill-restart.sh restart 2
set -euo pipefail
cd "$(dirname "$0")/.."

ACTION=$1
ID=$2
PORT=$((8000 + ID))
PID_FILE="pids/node-${ID}.pid"

ALL_PEERS="1:localhost:8001,2:localhost:8002,3:localhost:8003,4:localhost:8004,5:localhost:8005"

kill_node() {
    if [ -f "$PID_FILE" ]; then
        PID=$(cat "$PID_FILE")
        if kill -0 "$PID" 2>/dev/null; then
            echo "Killing node $ID (PID $PID)"
            kill "$PID"
            sleep 0.3
        fi
        rm -f "$PID_FILE"
    else
        echo "Node $ID not running (no PID file)"
    fi
}

start_node() {
    mkdir -p logs pids
    PEERS=$(python3 -c "
peers = '$ALL_PEERS'.split(',')
print(','.join(p for p in peers if not p.startswith('${ID}:')))
")
    echo "Starting node $ID on port $PORT"
    python3 -m broker.broker \
        --id "$ID" \
        --port "$PORT" \
        --peers "$PEERS" \
        --data-dir "data/node-$ID" \
        --election-min-ms "${ELECTION_MIN_MS:-1500}" \
        --election-max-ms "${ELECTION_MAX_MS:-3000}" \
        --heartbeat-ms    "${HEARTBEAT_MS:-300}" \
        --log-level       INFO \
        > "logs/node-${ID}.log" 2>&1 &
    echo $! > "$PID_FILE"
    echo "Node $ID started (PID $!)"
}

case "$ACTION" in
    kill)    kill_node ;;
    start)   start_node ;;
    restart) kill_node; sleep 0.5; start_node ;;
    *)       echo "Usage: $0 {kill|start|restart} <node_id>"; exit 1 ;;
esac
