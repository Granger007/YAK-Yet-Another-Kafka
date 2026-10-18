# YAK(Yet Another Kafka)- Distributed Message Broker System

A Kafka-inspired distributed message broker built from scratch in Python, with no
Raft or messaging libraries. It is a learning project that implements the core
ideas behind Kafka and Raft end to end.

## Features
- **Raft consensus:** 5 brokers with randomized election timeouts, term-based
  voting, majority quorum, heartbeats, log replication, and persistence
- **Topics and partitions:** 4 topics x 2 partitions, applied as a replicated
  state machine on top of the Raft log
- **Exactly-once writes per messageID:** leader-side and apply-time deduplication
- **High water mark:** consumers only see committed records
- **Smart producer:** discovers the leader via `GET /metadata/leader`, caches it,
  and retries safely with stable UUIDs across failovers
- **Consumers:** 2 independent consumers that persist their own offsets, with
  at-least-once delivery and resume after restarts and leader changes
- **Failure testing:** kill the leader, partition the network, and verify with a
  ledger that nothing was lost or reordered

  
## Project layout

```
raft-broker/
+-- raft/
│   +-- rpc.py        # LogEntry, RequestVote*, AppendEntries* dataclasses
│   +-- storage.py    # Atomic-write JSON persistence (currentTerm, votedFor, log)
│   +-- transport.py  # HTTP client (urllib); partition simulation
│   +-- node.py       # Full Raft state machine (election, replication, commit)
+-- broker/
│   +-- broker.py     # CLI entry point (argparse)
│   +-- config.py     # Topic and partition configuration parser
│   +-- hash.py       # Deterministic FNV-1a partition selection
│   +-- partition.py  # Append-only partition log with sequential offsets and HWM
│   +-- store.py      # BrokerStore replicated state machine (apply, dedup, recovery)
+-- client/
│   +-- discovery.py  # LeaderDiscovery: seed querying, leader caching, auto-invalidation
+-- cmd/
│   +-- producer.py   # Production producer binary with deduplication demo & ledger
│   +-- consumer.py   # Production consumer binary with atomic offsets & status HTTP
+-- server/
│   +-- handlers.py   # Route handlers (/message, /fetch, /topics, etc.)
│   +-- server.py     # ThreadingHTTPServer wiring + CORS support + 307 redirects
+-- tests/
│   +-- test_raft.py    # Multi-process Raft tests (10 tests)
│   +-- test_broker.py  # Multi-process Phase 2 Broker tests (12 tests)
│   +-- test_phase3.py  # End-to-end Phase 3 Client & Consumer tests (12 tests)
+-- scripts/
│   +-- run-producer.sh # Launch producer with default settings
│   +-- run-consumer.sh # Launch consumer 1 or 2 with subscription defaults
│   +-- verify.py       # End-to-end verification across ledger & consumer logs
│   +-- start-cluster.ps1  # Windows PowerShell cluster launcher
│   +-- stop-cluster.ps1   # Windows PowerShell cluster stopper
│   +-- start-cluster.bat  # Windows batch equivalents
│   +-- stop-cluster.bat
│   +-- kill-restart.bat
│   +-- start-cluster.sh   # Linux/macOS equivalents
│   +-- stop-cluster.sh
│   +-- kill-restart.sh
+-- raft_dashboard.html    # Live visual control plane
```

---

## Requirements

- Python 3.8+ (stdlib only, no pip installs required for runtime)
- `pytest` for running integration tests (`pip install pytest`)

---

## Architecture: Replicated State Machine

1. **One Raft Group**: The whole cluster runs a single Raft consensus group. The Raft log is the single source of truth; each entry's `command` is an opaque payload.
2. **State Machine (`BrokerStore`)**:
   - Implements `apply(entry)` which Raft calls strictly in commit order.
   - Non-broker commands and Raft no-op entries pass through harmlessly.
   - Offsets, deduplication, and High Watermark (HWM) are decided **inside Apply** (never at proposal time), ensuring every broker arrives at identical state.
3. **Topics & Partitions**:
   - Default: 4 topics with 2 partitions each (`orders:2,payments:2,notifications:2,audit:2`).
   - Configurable via `--topics` flag or `RAFT_TOPICS` environment variable.
   - Persisted as append-only logs under `data/node-<id>/partitions/<topic>-<partition>.log`.
4. **Offsets & High Watermark (HWM)**:
   - Each partition log is append-only with sequential offsets starting at 0.
   - `logEndOffset` = next offset to write = total records in partition log.
   - `HWM = logEndOffset`. Consumers may only read offsets `< HWM`.
   - Idempotency: `Apply` tracks applied Raft indices so replaying after restart never creates duplicates.
5. **Deterministic Partition Selection**:
   - Explicit partition: uses requested partition if in range `[0, numPartitions - 1]`.
   - Key: `partition = fnv1a(key) % numPartitions`.
   - Message ID: `partition = fnv1a(messageID) % numPartitions` (deterministic retry).
6. **Two-Level Deduplication**:
   - Dedup key: `(topic, messageID) -> (partition, offset)`.
   - Leader fast pre-check: returns existing offset immediately without proposing.
   - Authoritative check in Apply: prevents duplicate writes if identical requests were in flight.
   - Dedup map is rebuilt from partition logs on restart.

---

## HTTP Endpoints

### 1. `POST /debug/produce`
Produce a message to the cluster.
- **Request**:
  ```json
  {
    "topic": "orders",
    "messageID": "ord-101",
    "key": "user-42",
    "partition": 0,
    "payload": { "item": "book", "price": 29.99 }
  }
  ```
- **Responses**:
  - `200 OK`: `{"topic": "orders", "partition": 0, "offset": 0, "duplicate": false, "hwm": 1}`
  - `307 Temporary Redirect`: Returned on followers, pointing to the leader.
  - `400 Bad Request`: Invalid partition, empty messageID, or malformed JSON.
  - `404 Not Found`: Unknown topic.
  - `503 Service Unavailable`: No leader currently elected.
  - `504 Gateway Timeout`: Timed out waiting for Raft commit and apply.

### 2. `GET /debug/fetch?topic=orders&partition=0&offset=0&max=10`
Fetch committed records from a partition. Allowed on any node to verify follower replication.
- **Response**:
  ```json
  {
    "topic": "orders",
    "partition": 0,
    "offset": 0,
    "hwm": 1,
    "logEndOffset": 1,
    "records": [
      {
        "offset": 0,
        "topic": "orders",
        "partition": 0,
        "messageID": "ord-101",
        "key": "user-42",
        "payload": { "item": "book", "price": 29.99 },
        "timestamp": 1792300000.0,
        "raftIndex": 2
      }
    ]
  }
  ```

### 3. `GET /topics`
Inspect all topics and per-partition metrics.
- **Response**:
  ```json
  {
    "topics": {
      "orders": {
        "0": { "logEndOffset": 1, "hwm": 1, "recordCount": 1 },
        "1": { "logEndOffset": 0, "hwm": 0, "recordCount": 0 }
      },
      "payments": { ... },
      "notifications": { ... },
      "audit": { ... }
    }
  }
  ```

### 4. `GET /status`
Returns Raft consensus status extended with broker state:
```json
{
  "id": 1,
  "role": "leader",
  "current_term": 2,
  "total_records_applied": 1,
  "partition_hwms": {
    "orders-0": 1,
    "orders-1": 0,
    "payments-0": 0,
    "payments-1": 0,
    "notifications-0": 0,
    "notifications-1": 0,
    "audit-0": 0,
    "audit-1": 0
  }
}
```

---

## Running the Cluster

### Windows (PowerShell)
```powershell
scripts\start-cluster.ps1     # Starts all 5 nodes in background
scripts\stop-cluster.ps1      # Stops all nodes
```

### Linux / macOS
```bash
bash scripts/start-cluster.sh
bash scripts/stop-cluster.sh
```

---

---

## Phase 3: Clients & Production Endpoints

### Production Endpoints

#### `POST /message`
The production producer endpoint. The leader validates topic/partition, checks deduplication cache, proposes the record to Raft, and returns only after the entry is committed and applied. Followers return a `307 Temporary Redirect` with the leader's address.

- **Request**:
  ```json
  {
    "topic": "orders",
    "partition": 0,
    "key": "user-42",
    "messageID": "4e782c5a-3507-448a-bf94-f1791d035418",
    "payload": {"item": "widget", "qty": 2}
  }
  ```
- **Response**: `200 OK`
  ```json
  {
    "topic": "orders",
    "partition": 0,
    "offset": 5,
    "duplicate": false,
    "hwm": 6
  }
  ```

#### `GET /fetch?topic=orders&partition=0&offset=0&max=10`
Leader-only consumer fetch endpoint. Returns records in range `[offset, HWM)`.
- If `offset >= HWM` or `offset > HWM`: returns `200 OK` with `records: []` (does not error).
- Follower node returns `307 Temporary Redirect` with `Location` pointing to the current leader.

---

## Producer Client (`cmd/producer.py`)

A robust producer featuring:
- **Leader Discovery & Cache**: Queries seed brokers via `GET /metadata/leader`, caches leader address in memory, and reuses it for subsequent requests. Automatically invalidates on network timeout, 307 redirect, 503, or "not leader" error.
- **Stable UUIDv4 Message IDs**: Every logical message gets a UUIDv4 on creation, reused on retries so deduplication works safely even if an ack timed out.
- **Append-only Ledger**: Writes `{producerId, messageID, topic, partition, offset, duplicate, hwm}` to an fsynced JSONL ledger file (`data/producer.ledger`) for ground-truth verification.
- **Flags & Environment Variables**:
  - `--producer-id` (`PRODUCER_ID`, default: `prod-1`)
  - `--seeds` (`SEEDS`, default: `localhost:8001,...,localhost:8005`)
  - `--topics` (`TOPICS`, default: `orders,payments,notifications,audit`)
  - `--count` (`COUNT`, default: `0` = infinite)
  - `--interval` (`INTERVAL`, default: `0.5` s)
  - `--timeout` (`ACK_TIMEOUT`, default: `8.0` s)
  - `--max-retries` (`MAX_RETRIES`, default: `10`)
  - `--ledger` (`LEDGER_PATH`, default: `data/producer.ledger`)
  - `--dedup-demo` (`DEDUP_DEMO`, re-sends last N message IDs to verify dedup)

---

## Consumer Client (`cmd/consumer.py`)

Independent consumer instances reading partitions concurrently:
- **Default Subscriptions**:
  - `consumer-1`: `orders,payments`
  - `consumer-2`: `payments,notifications,audit`
  *(The overlap on `payments` demonstrates independent offset tracking on shared topics).*
- **Offset Semantics**:
  - Stored offset is the **NEXT** offset to read (`last_processed + 1`).
  - Fresh partition start positions: `earliest` (offset 0) or `latest` (current HWM).
- **Atomic Offset Persistence**:
  - Persists offsets to `<data-dir>/offsets.json` via atomic write (temp file + fsync + `os.replace`).
  - Corrupt offset files trigger an immediate fail-fast error (`RuntimeError`) rather than silently resetting to 0.
- **At-Least-Once Delivery**:
  - Persists offsets only **after** successfully appending to `<data-dir>/consumer-<id>-processed.log`.
  - On restart / redelivery, records below the stored offset are safely ignored.
- **Status HTTP Endpoint**:
  - `GET /status` returns JSON containing consumer ID, subscriptions, and per-partition offsets, HWM, and lag (`lag = HWM - offset`).
  - `GET /healthz` returns `200 OK`.

---

## End-to-End Verification (`scripts/verify.py`)

Validates correctness across the producer ledger and both consumers' processed logs:
1. **Completeness**: Every non-duplicate acked message in the producer ledger was processed by all consumers subscribed to its topic.
2. **Monotonicity**: Within each consumer and `(topic, partition)`, processed offsets are strictly ascending with no missing records.
3. **Subscription Isolation**: Consumers never process records outside their subscribed topics.
4. **Message ID Uniqueness**: No `messageID` was ever written to multiple different offsets.

---

## Running Phase 3 Demo

```bash
# 1. Start the 5-node cluster
bash scripts/start-cluster.sh

# 2. Launch consumer-1 (orders, payments)
bash scripts/run-consumer.sh 1

# 3. Launch consumer-2 (payments, notifications, audit)
bash scripts/run-consumer.sh 2

# 4. Produce 50 messages across all topics
python cmd/producer.py --count 50 --interval 0.1

# 5. Check consumer lag and status
curl http://localhost:9001/status
curl http://localhost:9002/status

# 6. Run end-to-end verification
python scripts/verify.py
```

---

## Running Tests

```bash
# Phase 1: Raft Core tests (10 tests)
python -m pytest tests/test_raft.py -v

# Phase 2: Broker Replicated State Machine tests (12 tests)
python -m pytest tests/test_broker.py -v

# Phase 3: Producer, Consumer, Offsets & Failover tests (12 tests)
python -m pytest tests/test_phase3.py -v

# Run all 34 tests across all phases
python -m pytest tests/ -v
```

