"""
broker/hash.py  -  Deterministic FNV-1a 32-bit hash for partition assignment.
"""

def fnv1a_32(key: str) -> int:
    """
    Compute 32-bit FNV-1a hash of a string.
    """
    h = 0x811C9DC5
    for b in key.encode("utf-8"):
        h ^= b
        h = (h * 0x01000193) & 0xFFFFFFFF
    return h


def select_partition(
    num_partitions: int = 0,
    explicit_partition: int | None = None,
    key: str | None = None,
    message_id: str | None = None,
    num_parts: int | None = None,
) -> tuple[int, str]:
    """
    Select partition and return (partition_id, reason).
    Reasons: 'explicit', 'key', 'messageID'.
    """
    n = num_parts if num_parts is not None else num_partitions
    if explicit_partition is not None:
        if not (0 <= explicit_partition < n):
            raise ValueError(
                f"Partition {explicit_partition} out of range [0, {n - 1}]"
            )
        return explicit_partition, "explicit"

    if key is not None and key != "":
        return fnv1a_32(key) % n, "key"

    if message_id is not None and message_id != "":
        return fnv1a_32(message_id) % n, "messageID"

    raise ValueError("Cannot select partition: neither partition, key, nor messageID provided")
