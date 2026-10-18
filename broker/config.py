"""
broker/config.py  -  Topic and partition configuration.
"""

from __future__ import annotations

import os
import re
from typing import Dict

DEFAULT_TOPICS_SPEC = "orders:2,payments:2,notifications:2,audit:2"
_TOPIC_NAME_REGEX = re.compile(r"^[a-zA-Z0-9_\-]+$")


class BrokerConfig:
    def __init__(self, topics: Dict[str, int]):
        if not topics:
            raise ValueError("Broker configuration must define at least one topic")
        for topic, partitions in topics.items():
            if not isinstance(topic, str) or not _TOPIC_NAME_REGEX.match(topic):
                raise ValueError(f"Invalid topic name: {topic!r}. Must match {_TOPIC_NAME_REGEX.pattern}")
            if not isinstance(partitions, int) or partitions <= 0:
                raise ValueError(f"Invalid partition count for topic {topic!r}: {partitions}. Must be > 0")
        self.topics = topics  # { "orders": 2, ... }

    @classmethod
    def parse(cls, raw: str | None = None) -> BrokerConfig:
        """
        Parse spec in the form: "orders:2,payments:2,notifications:2,audit:2"
        If raw is None, checks env var RAFT_TOPICS, else uses DEFAULT_TOPICS_SPEC.
        """
        if raw is None:
            raw = os.environ.get("RAFT_TOPICS", DEFAULT_TOPICS_SPEC)

        raw = raw.strip()
        if not raw:
            raise ValueError("Empty topic configuration specification")

        topics: Dict[str, int] = {}
        for part in raw.split(","):
            part = part.strip()
            if not part:
                continue
            tokens = part.split(":")
            if len(tokens) != 2:
                raise ValueError(f"Malformed topic spec '{part}'. Expected 'name:partitions'")
            name = tokens[0].strip()
            try:
                p_count = int(tokens[1].strip())
            except ValueError:
                raise ValueError(f"Partition count for topic '{name}' must be integer, got '{tokens[1]}'")
            if name in topics:
                raise ValueError(f"Duplicate topic specification: '{name}'")
            topics[name] = p_count

        return cls(topics)
