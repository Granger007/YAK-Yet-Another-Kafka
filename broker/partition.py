"""
broker/partition.py  -  Append-only partition log with sequential offsets and HWM.
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


class PartitionLog:
    """
    Manages an append-only log for a single topic-partition.
    File location: <data_dir>/partitions/<topic>-<partition>.log
    """

    def __init__(self, data_dir: str, topic: str, partition: int, node_id: int):
        self.topic = topic
        self.partition = partition
        self.node_id = node_id

        partitions_dir = os.path.join(data_dir, "partitions")
        os.makedirs(partitions_dir, exist_ok=True)
        self.log_path = os.path.join(partitions_dir, f"{topic}-{partition}.log")

        self._lock = threading.Lock()
        self._records: List[Dict[str, Any]] = []
        self._file_handle: Optional[Any] = None

        self._recover()

    def _recover(self) -> None:
        """Read existing partition log from disk on startup."""
        with self._lock:
            self._records = []
            if os.path.exists(self.log_path):
                with open(self.log_path, "r", encoding="utf-8") as f:
                    for line_num, line in enumerate(f):
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            record = json.loads(line)
                            self._records.append(record)
                        except json.JSONDecodeError as exc:
                            logger.error(
                                "[Node %d] Corrupt record in %s line %d: %s",
                                self.node_id, self.log_path, line_num, exc,
                            )

            # Open file handle in append mode with flush on write
            self._file_handle = open(self.log_path, "a", encoding="utf-8")
            logger.info(
                "[Node %d] Partition [%s-%d] restored %d records; logEndOffset=%d, HWM=%d",
                self.node_id, self.topic, self.partition, len(self._records),
                self.log_end_offset, self.hwm,
            )

    @property
    def log_end_offset(self) -> int:
        """Next offset to be written (equal to total records written)."""
        return len(self._records)

    @property
    def hwm(self) -> int:
        """
        High Watermark: all records in the partition log have been committed
        by Raft, so HWM = log_end_offset.
        """
        return len(self._records)

    def append(
        self,
        message_id: str,
        key: Optional[str],
        payload: Any,
        raft_index: int,
        timestamp: Optional[float] = None,
    ) -> Dict[str, Any]:
        """
        Append a new record to the partition log.
        Assigns sequential offset = log_end_offset.
        """
        if timestamp is None:
            timestamp = time.time()

        with self._lock:
            offset = len(self._records)
            record = {
                "offset": offset,
                "topic": self.topic,
                "partition": self.partition,
                "messageID": message_id,
                "key": key,
                "payload": payload,
                "timestamp": timestamp,
                "raftIndex": raft_index,
            }
            # Append-only write to disk with flush
            json_line = json.dumps(record) + "\n"
            self._file_handle.write(json_line)
            self._file_handle.flush()
            os.fsync(self._file_handle.fileno())

            self._records.append(record)
            logger.info(
                "[Node %d] Entry applied to [%s-%d] offset=%d (raftIndex=%d, msgId=%s); HWM=%d",
                self.node_id, self.topic, self.partition, offset, raft_index, message_id, self.hwm,
            )
            return record

    def fetch(self, offset: int, max_records: int = 100) -> Tuple[List[Dict[str, Any]], int, int]:
        """
        Fetch records starting at offset up to HWM (exclusive of HWM).
        Never returns records at or beyond HWM.
        Returns (records, hwm, logEndOffset).
        """
        with self._lock:
            hwm = self.hwm
            log_end = self.log_end_offset

            if offset < 0:
                raise ValueError("Fetch offset must be non-negative")

            if offset >= hwm:
                return [], hwm, log_end

            end = min(hwm, offset + max_records)
            slice_records = self._records[offset:end]
            return slice_records, hwm, log_end

    def close(self):
        with self._lock:
            if self._file_handle:
                try:
                    self._file_handle.close()
                except Exception:
                    pass
                self._file_handle = None
