# raft-broker  -  Phase 1: Raft Consensus Layer

A from-scratch 5-node Raft consensus cluster in **pure Python** (stdlib only).
This is Phase 1 of a Kafka-like distributed message broker.

---

## Project layout

```
raft-broker/
+-- raft/
¦   +-- rpc.py        # LogEntry, RequestVote*, AppendEntries* dataclasses
¦   +-- storage.py    # Atomic-write JSON persistence (currentTerm, votedFor, log)
¦   +-- transport.py  # HTTP client (urllib); partition simulation
¦   +-- node.py       # Full Raft state machine (election, replication, commit)
+-- server/
¦   +-- handlers.py   # Route handler functions
¦   +-- server.py     # ThreadingHTTPServer wiring
+-- broker/
¦   +-- broker.py     # CLI entry point (argparse)
+-- tests/
¦   +-- test_raft.py  # Multi-process integration tests
+-- scripts/
¦   +-- start-cluster.sh   # Start all 5 nodes (Linux/macOS)
¦   +-- stop-cluster.sh
¦   +-- kill-restart.sh
¦   +-- start-cluster.bat  # Windows equivalents
¦   +-- stop-cluster.bat
¦   +-- kill-restart.bat
+-- Makefile
```

---

## Requirements

- Python 3.8+ (stdlib only, no pip installs)
- `pytest` for running tests (`pip install pytest`)

---

## Running the cluster

### Linux / macOS

```bash
cd raft-broker
bash scripts/start-cluster.sh        # start all 5 nodes
tail -f logs/node-1.log              # watch logs
bash scripts/stop-cluster.sh         # stop all
```

### Windows (PowerShell / cmd)

```bat
cd raft-broker
scripts\start-cluster.bat
scripts\stop-cluster.bat
```

### Manually (any OS)

```bash
# Start nodes in 5 separate terminals:
python -m broker.broker --id 1 --port 8001 --peers 2:localhost:8002,3:localhost:8003,4:localhost:8004,5:localhost:8005
python -m broker.broker --id 2 --port 8002 --peers 1:localhost:8001,3:localhost:8003,4:localhost:8004,5:localhost:8005
python -m broker.broker --id 3 --port 8003 --peers 1:localhost:8001,2:localhost:8002,4:localhost:8004,5:localhost:8005
python -m broker.broker --id 4 --port 8004 --peers 1:localhost:8001,2:localhost:8002,3:localhost:8003,5:localhost:8005
python -m broker.broker --id 5 --port 8005 --peers 1:localhost:8001,2:localhost:8002,3:localhost:8003,4:localhost:8004
```

Optional flags:
| Flag | Default | Description |
|---|---|---|
| `--election-min-ms` | 1500 | Min election timeout |
| `--election-max-ms` | 3000 | Max election timeout |
| `--heartbeat-ms` | 300 | Leader heartbeat interval |
| `--data-dir` | `data/node-<id>` | Persistent state directory |
| `--log-level` | INFO | DEBUG / INFO / WARNING |

---

## Kill & restart a node

```bash
# Linux/macOS
bash scripts/kill-restart.sh kill    3
bash scripts/kill-restart.sh restart 3

# Windows
scripts\kill-restart.bat kill    3
scripts\kill-restart.bat restart 3
```

---

## HTTP endpoints

### `GET /status`
```json
{
  "id": 1,
  "role": "leader",
  "current_term": 2,
  "voted_for": 1,
  "leader_id": 1,
  "commit_index": 4,
  "last_applied": 4,
  "log_length": 4,
  "last_log_index": 4,
  "last_log_term": 2
}
```

### `GET /metadata/leader`
```json
{ "leader_id": 1, "leader_address": null }
```
`leader_address` is null on the leader itself; followers return `"localhost:8001"`.

### `POST /debug/propose`
```bash
curl -s -X POST http://localhost:8001/debug/propose \
     -H 'Content-Type: application/json' \
     -d '{"data": "hello world"}' | python3 -m json.tool
```
Returns `{"committed": true, "log_index": 3}` on success.
Returns HTTP 307 `{"error": "not the leader", "leader_address": "localhost:8001"}` on follower.

### `POST /debug/partition`
```bash
# Isolate node 3 from nodes 4 and 5
curl -s -X POST http://localhost:8003/debug/partition \
     -H 'Content-Type: application/json' \
     -d '{"blocked": [4, 5]}' | python3 -m json.tool

# Clear partition
curl -s -X POST http://localhost:8003/debug/partition \
     -d '{"blocked": []}' -H 'Content-Type: application/json'
```

### Raft internal RPCs (for node-to-node communication)
```
POST /raft/requestVote
POST /raft/appendEntries
```

---

## Running the tests

```bash
cd raft-broker
python -m pytest tests/test_raft.py -v
```

Tests use real OS processes on ports 19001-19060 so they do NOT conflict with a
running cluster on ports 8001-8005.

---

## Demo walkthrough

```bash
# 1. Start cluster
bash scripts/start-cluster.sh
sleep 5

# 2. Find the leader
curl -s http://localhost:8001/status | python3 -m json.tool
curl -s http://localhost:8002/status | python3 -m json.tool
# ... look for "role": "leader"

# 3. Propose some entries (replace 8001 with the leader port)
curl -s -X POST http://localhost:8001/debug/propose \
     -H 'Content-Type: application/json' -d '{"data": "msg-1"}' | python3 -m json.tool
curl -s -X POST http://localhost:8001/debug/propose \
     -H 'Content-Type: application/json' -d '{"data": "msg-2"}' | python3 -m json.tool

# 4. Verify replication on a follower
curl -s http://localhost:8002/status | python3 -m json.tool

# 5. Kill the leader (example: leader is node 1)
bash scripts/kill-restart.sh kill 1
sleep 5

# 6. Watch re-election on the remaining nodes
curl -s http://localhost:8002/status | python3 -m json.tool

# 7. Propose on new leader
curl -s -X POST http://localhost:8002/debug/propose \
     -H 'Content-Type: application/json' -d '{"data": "msg-3"}' | python3 -m json.tool

# 8. Restart old leader - it rejoins as follower
bash scripts/kill-restart.sh restart 1
sleep 3
curl -s http://localhost:8001/status | python3 -m json.tool

# 9. Simulate a minority partition
curl -s -X POST http://localhost:8004/debug/partition -H 'Content-Type: application/json' -d '{"blocked":[1,2,3]}'
curl -s -X POST http://localhost:8005/debug/partition -H 'Content-Type: application/json' -d '{"blocked":[1,2,3]}'
# Nodes 4 & 5 cannot elect a leader (no quorum)
sleep 5
curl -s http://localhost:8004/status | python3 -m json.tool   # should show candidate/follower, NOT leader

# 10. Clear partition
curl -s -X POST http://localhost:8004/debug/partition -d '{"blocked":[]}' -H 'Content-Type: application/json'
curl -s -X POST http://localhost:8005/debug/partition -d '{"blocked":[]}' -H 'Content-Type: application/json'
```

---

## How each Raft rule maps to code

| Rule | Location |
|---|---|
| **Startup (rule 1)**: all nodes begin as Followers, term=0, empty log | `node.py: RaftNode.__init__` |
| **Election timer (rule 2)**: randomised 1500-3000ms, reset on valid AppendEntries or vote granted | `node.py: _election_timer_loop`, `_reset_election_timer` |
| **Become candidate (rule 3)**: increment term, vote self, send RequestVote in parallel | `node.py: _start_election` |
| **RequestVote handler (rule 4)**: term check, one-vote-per-term, log up-to-date check | `node.py: handle_request_vote` |
| **Majority ? leader (rule 5)**: quorum counter in ask_peer closures; no-op appended | `node.py: _become_leader` |
| **Higher term ? step down (rule 6)**: every RPC handler calls `_step_down(new_term)` | `node.py: _step_down` |
| **Log replication (rule 7)**: nextIndex/matchIndex per peer; mismatch ? decrement & retry | `node.py: _PeerReplicator._replicate_once` |
| **Commit rule (rule 8)**: advance only for currentTerm entries; majority matchIndex check | `node.py: _try_advance_commit` |
| **Persistence (rule 9)**: atomic JSON write before every RPC response | `storage.py: RaftStorage.save`, `node.py: _save_persistent` |
| **Opaque command (rule 10)**: `LogEntry.command: Any` accepts any JSON-serialisable value | `rpc.py: LogEntry` |
