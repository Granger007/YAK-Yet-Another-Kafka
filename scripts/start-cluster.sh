#!/usr/bin/env bash
# start-cluster.sh  -  Launch all 5 Raft broker nodes locally.
#
# Usage:
#   bash scripts/start-cluster.sh [--election-min-ms 1500] [--election-max-ms 3000]
#
# Logs go to  logs/node-<id>.log
# PIDs saved to  pids/node-<id>.pid

set -euo pipefail
cd "$(dirname "$0")/.."

ELECTION_MIN=${ELECTION_MIN_MS:-5000}
ELECTION_MAX=${ELECTION_MAX_MS:-8000}
HEARTBEAT=${HEARTBEAT_MS:-300}

mkdir -p logs pids

ALL_PEERS="1:localhost:8001,2:localhost:8002,3:localhost:8003,4:localhost:8004,5:localhost:8005"

for ID in 1 2 3 4 5; do
    PORT=$((8000 + ID))
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
        --election-min-ms "$ELECTION_MIN" \
        --election-max-ms "$ELECTION_MAX" \
        --heartbeat-ms    "$HEARTBEAT" \
        --log-level       INFO \
        > "logs/node-${ID}.log" 2>&1 &
    echo $! > "pids/node-${ID}.pid"
done

echo ""
echo "All 5 nodes started.  Logs: logs/node-<id>.log"
echo "Check status:  curl http://localhost:8001/status | python3 -m json.tool"
echo "Stop cluster:  bash scripts/stop-cluster.sh"
