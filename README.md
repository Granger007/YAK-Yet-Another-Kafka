# raft-broker - Kafka-like Distributed Message Broker

A from-scratch distributed message broker in **pure Python** (stdlib only).
- **Phase 1**: 5-node Raft Consensus Core (elections, log replication, commit, partition simulation).
- **Phase 2**: Replicated State Machine (Topics, Partitions, Offsets, High Watermark, Deduplication).

---

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
+-- server/
│   +-- handlers.py   # Route handlers (/debug/produce, /debug/fetch, /topics, etc.)
│   +-- server.py     # ThreadingHTTPServer wiring + CORS support
+-- tests/
│   +-- test_raft.py    # Multi-process Raft tests (10 tests)
│   +-- test_broker.py  # Multi-process Phase 2 Broker tests (12 tests)
+-- scripts/
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

## Running Tests

```bash
# Run all Phase 1 Raft consensus tests (10 tests)
python -m pytest tests/test_raft.py -v

# Run all Phase 2 Broker tests (12 tests)
python -m pytest tests/test_broker.py -v
```

