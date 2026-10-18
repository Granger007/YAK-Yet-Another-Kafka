#!/usr/bin/env bash
# stop-cluster.sh  -  Kill all 5 broker processes.
set -euo pipefail
cd "$(dirname "$0")/.."
for ID in 1 2 3 4 5; do
    PID_FILE="pids/node-${ID}.pid"
    if [ -f "$PID_FILE" ]; then
        PID=$(cat "$PID_FILE")
        if kill -0 "$PID" 2>/dev/null; then
            echo "Killing node $ID (PID $PID)"
            kill "$PID"
        fi
        rm -f "$PID_FILE"
    fi
done
echo "Cluster stopped."
