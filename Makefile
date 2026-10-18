# Makefile for raft-broker  (works with GNU make on Linux/macOS; use nmake on Windows)

PYTHON   ?= python3
ELECTION_MIN ?= 1500
ELECTION_MAX ?= 3000
HEARTBEAT    ?= 300

.PHONY: cluster stop restart test clean

cluster:
	@bash scripts/start-cluster.sh

stop:
	@bash scripts/stop-cluster.sh

restart:
	@bash scripts/kill-restart.sh restart $(NODE)

test:
	$(PYTHON) -m pytest tests/test_raft.py -v --tb=short 2>&1 | tee test_results.txt

test-unit:
	$(PYTHON) -m unittest discover -s tests -v

clean:
	rm -rf data/ logs/ pids/ __pycache__ raft/__pycache__ server/__pycache__ cmd/__pycache__ tests/__pycache__
	find . -name "*.pyc" -delete

status:
	@for i in 1 2 3 4 5; do \
	    echo "--- Node $$i ---"; \
	    curl -s http://localhost:$$((8000+$$i))/status | python3 -m json.tool 2>/dev/null || echo "(not running)"; \
	done
