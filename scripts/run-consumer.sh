#!/usr/bin/env bash
# scripts/run-consumer.sh <id>  -  Launch a consumer instance (1 or 2)
set -euo pipefail
cd "$(dirname "$0")/.."

RAW_ID=${1:-"1"}
if [[ "$RAW_ID" =~ ^[0-9]+$ ]]; then
    CONSUMER_ID="consumer-${RAW_ID}"
else
    CONSUMER_ID="$RAW_ID"
fi

SEEDS=${SEEDS:-"localhost:8001,localhost:8002,localhost:8003,localhost:8004,localhost:8005"}

if [[ "$CONSUMER_ID" == *"2"* ]]; then
    TOPICS=${TOPICS:-"payments,notifications,audit"}
    STATUS_PORT=${STATUS_PORT:-9002}
else
    TOPICS=${TOPICS:-"orders,payments"}
    STATUS_PORT=${STATUS_PORT:-9001}
fi

DATA_DIR=${DATA_DIR:-"data/${CONSUMER_ID}"}
mkdir -p "$DATA_DIR" logs

echo "Starting $CONSUMER_ID (topics=$TOPICS status_port=$STATUS_PORT data_dir=$DATA_DIR)..."
python3 cmd/consumer.py \
    --consumer-id "$CONSUMER_ID" \
    --seeds "$SEEDS" \
    --topics "$TOPICS" \
    --data-dir "$DATA_DIR" \
    --status-port "$STATUS_PORT" \
    "${@:2}"
