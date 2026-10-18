#!/usr/bin/env bash
# scripts/run-producer.sh  -  Launch producer
set -euo pipefail
cd "$(dirname "$0")/.."

PRODUCER_ID=${PRODUCER_ID:-"prod-1"}
SEEDS=${SEEDS:-"localhost:8001,localhost:8002,localhost:8003,localhost:8004,localhost:8005"}
TOPICS=${TOPICS:-"orders,payments,notifications,audit"}
COUNT=${COUNT:-50}
INTERVAL=${INTERVAL:-0.2}
LEDGER_PATH=${LEDGER_PATH:-"data/producer.ledger"}

mkdir -p "$(dirname "$LEDGER_PATH")" logs

echo "Starting producer $PRODUCER_ID (topics=$TOPICS count=$COUNT seeds=$SEEDS)..."
python3 cmd/producer.py \
    --producer-id "$PRODUCER_ID" \
    --seeds "$SEEDS" \
    --topics "$TOPICS" \
    --count "$COUNT" \
    --interval "$INTERVAL" \
    --ledger "$LEDGER_PATH" \
    "$@"
