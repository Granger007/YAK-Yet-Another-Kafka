"""
broker/store.py  -  Replicated State Machine ("Broker Store").

Handles topics and partitions behind a clean Apply(entry) interface.
Raft remains completely agnostic of broker data structures.
"""

from __future__ import annotations

import logging
import threading
import time
from typing import Any, Dict, Optional, Set, Tuple

from broker.config import BrokerConfig
from broker.partition import PartitionLog

logger = logging.getLogger(__name__)


class BrokerStore:
    """
    Replicated state machine for topics and partitions.
    """

    def __init__(self, node_id: int, data_dir: str, config: BrokerConfig):
        self.node_id = node_id
        self.data_dir = data_dir
        self.config = config

        # Partitions: (topic, partition) -> PartitionLog
        self.partitions: Dict[Tuple[str, int], PartitionLog] = {}
        for topic, num_parts in self.config.topics.items():
            for p in range(num_parts):
                self.partitions[(topic, p)] = PartitionLog(
                    data_dir=data_dir, topic=topic, partition=p, node_id=node_id
                )

        # In-memory deduplication index: (topic, messageID) -> (partition, offset)
        self.dedup_map: Dict[Tuple[str, str], Tuple[int, int]] = {}

        # Raft indices that have been applied to this store (for idempotency)
        self.applied_raft_indices: Set[int] = set()

        # Results of applied commands for leader wait/respond: raft_index -> result_dict
        self._apply_results: Dict[int, Dict[str, Any]] = {}
        self._apply_cond = threading.Condition(threading.Lock())

        self._rebuild_state_from_disk()

    def _rebuild_state_from_disk(self) -> None:
        """
        Scan all partition logs on disk to restore:
          1. Dedup map: (topic, messageID) -> (partition, offset)
          2. applied_raft_indices: set of all raftIndex values present in partition logs
        """
        total_records = 0
        for (topic, p), part_log in self.partitions.items():
            records, _, _ = part_log.fetch(0, max_records=1_000_000)
            for rec in records:
                total_records += 1
                msg_id = rec.get("messageID")
                off = rec.get("offset")
                r_idx = rec.get("raftIndex")
                if msg_id is not None and off is not None:
                    self.dedup_map[(topic, msg_id)] = (p, off)
                if r_idx is not None:
                    self.applied_raft_indices.add(r_idx)

        logger.info(
            "[Node %d] BrokerStore state restored: %d partition logs, %d total records, %d dedup entries, %d applied raft indices",
            self.node_id, len(self.partitions), total_records, len(self.dedup_map), len(self.applied_raft_indices),
        )

    # ------------------------------------------------------------------
    # State Machine Apply Interface
    # ------------------------------------------------------------------

    def apply(self, entry: Any) -> Optional[Dict[str, Any]]:
        """
        Apply an entry committed by Raft in strict log order.
        Raft no-op entries and non-broker commands pass through harmlessly.
        Idempotent: Replaying already applied raftIndex will not append duplicates.
        """
        raft_index = getattr(entry, "index", None)
        command = getattr(entry, "command", None)

        with self._apply_cond:
            # 1. Idempotency check: if this raftIndex was already applied, do not duplicate
            if raft_index is not None and raft_index in self.applied_raft_indices:
                logger.debug(
                    "[Node %d] raftIndex %d already applied; skipping duplicate apply",
                    self.node_id, raft_index,
                )
                res = self._apply_results.get(raft_index)
                self._apply_cond.notify_all()
                return res

            # 2. Pass non-broker commands and no-ops harmlessly
            if not isinstance(command, dict) or command.get("type") != "produce":
                if raft_index is not None:
                    self.applied_raft_indices.add(raft_index)
                    self._apply_cond.notify_all()
                return None

            # 3. Extract produce command fields
            topic = command.get("topic")
            partition = command.get("partition")
            message_id = command.get("messageID")
            key = command.get("key")
            payload = command.get("payload")
            timestamp = command.get("timestamp")

            if (topic, partition) not in self.partitions:
                logger.error(
                    "[Node %d] Apply failed: unknown topic/partition [%s-%s] in raftIndex %d",
                    self.node_id, topic, partition, raft_index,
                )
                result = {"error": f"unknown topic/partition {topic}-{partition}"}
                if raft_index is not None:
                    self.applied_raft_indices.add(raft_index)
                    self._apply_results[raft_index] = result
                    self._apply_cond.notify_all()
                return result

            part_log = self.partitions[(topic, partition)]

            # 4. Authoritative Deduplication Check
            dedup_key = (topic, message_id)
            if dedup_key in self.dedup_map:
                existing_p, existing_off = self.dedup_map[dedup_key]
                existing_hwm = self.partitions[(topic, existing_p)].hwm
                logger.info(
                    "[Node %d] Duplicate detected in Apply: topic=%s messageID=%s -> original partition=%d offset=%d (no write)",
                    self.node_id, topic, message_id, existing_p, existing_off,
                )
                result = {
                    "topic": topic,
                    "partition": existing_p,
                    "offset": existing_off,
                    "duplicate": True,
                    "hwm": existing_hwm,
                }
            else:
                # 5. Assign offset and append to partition log
                rec = part_log.append(
                    message_id=message_id,
                    key=key,
                    payload=payload,
                    raft_index=raft_index if raft_index is not None else 0,
                    timestamp=timestamp,
                )
                assigned_offset = rec["offset"]
                self.dedup_map[dedup_key] = (partition, assigned_offset)
                result = {
                    "topic": topic,
                    "partition": partition,
                    "offset": assigned_offset,
                    "duplicate": False,
                    "hwm": part_log.hwm,
                }

            if raft_index is not None:
                self.applied_raft_indices.add(raft_index)
                self._apply_results[raft_index] = result
                # Prune old results if needed
                if len(self._apply_results) > 10_000:
                    oldest_keys = sorted(self._apply_results.keys())[:2000]
                    for k in oldest_keys:
                        del self._apply_results[k]

            self._apply_cond.notify_all()
            return result

    # ------------------------------------------------------------------
    # Pre-check and Leader Wait Helpers
    # ------------------------------------------------------------------

    def precheck_dedup(self, topic: str, message_id: str) -> Optional[Dict[str, Any]]:
        """
        Fast pre-check on leader before proposing to Raft log.
        If already in dedup map, return original result immediately.
        """
        with self._apply_cond:
            key = (topic, message_id)
            if key in self.dedup_map:
                p, off = self.dedup_map[key]
                hwm = self.partitions[(topic, p)].hwm
                logger.info(
                    "[Node %d] Pre-check duplicate detected: topic=%s msgId=%s -> partition=%d offset=%d",
                    self.node_id, topic, message_id, p, off,
                )
                return {
                    "topic": topic,
                    "partition": p,
                    "offset": off,
                    "duplicate": True,
                    "hwm": hwm,
                }
            return None

    def wait_applied(self, raft_index: int, timeout: float = 10.0) -> Optional[Dict[str, Any]]:
        """
        Leader waits until raft_index has been applied to the broker store.
        """
        deadline = time.monotonic() + timeout
        with self._apply_cond:
            while raft_index not in self._apply_results:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    logger.warning(
                        "[Node %d] Timed out waiting for raftIndex %d to apply",
                        self.node_id, raft_index,
                    )
                    return None
                self._apply_cond.wait(remaining)
            return self._apply_results[raft_index]

    # ------------------------------------------------------------------
    # Observability & Fetch
    # ------------------------------------------------------------------

    def fetch(
        self, topic: str, partition: int, offset: int, max_records: int = 100
    ) -> Tuple[list, int, int]:
        """
        Fetch records from a partition. Allowed on any node.
        Returns (records, hwm, logEndOffset).
        """
        if topic not in self.config.topics:
            raise KeyError(f"Unknown topic: {topic}")
        num_parts = self.config.topics[topic]
        if not (0 <= partition < num_parts):
            raise IndexError(f"Partition {partition} out of range [0, {num_parts - 1}]")

        part_log = self.partitions[(topic, partition)]
        return part_log.fetch(offset, max_records)

    def get_topics_metadata(self) -> Dict[str, Any]:
        """
        Return every topic with each partition's {logEndOffset, hwm, recordCount}.
        """
        data = {}
        for topic, num_parts in self.config.topics.items():
            parts_dict = {}
            for p in range(num_parts):
                part_log = self.partitions[(topic, p)]
                parts_dict[str(p)] = {
                    "logEndOffset": part_log.log_end_offset,
                    "hwm": part_log.hwm,
                    "recordCount": len(part_log._records),
                }
            data[topic] = parts_dict
        return data

    def get_summary(self) -> Dict[str, Any]:
        """
        Summary for GET /status: total records applied and per-partition HWMs.
        """
        total_records = 0
        hwms = {}
        for (topic, p), part_log in self.partitions.items():
            total_records += len(part_log._records)
            hwms[f"{topic}-{p}"] = part_log.hwm
        return {
            "total_records_applied": total_records,
            "partition_hwms": hwms,
        }
