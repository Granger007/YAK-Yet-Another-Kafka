#!/usr/bin/env python3
"""
scripts/verify.py  --  Phase-3 End-to-End Verification Script.

Verifies:
1. Completeness: every non-duplicate acked message in producer ledger is processed
   by every consumer subscribed to that topic (matching topic, partition, offset, messageID).
2. Monotonicity: within each consumer and (topic, partition), processed offsets are ascending.
3. Subscription Isolation: no consumer processed a topic outside its declared subscriptions.
4. Message Uniqueness: no messageID appears at two different offsets.

Exits with 0 if all checks pass, 1 if any check fails.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from collections import defaultdict
from typing import Any, Dict, List, Set, Tuple


def load_jsonl(path: str) -> List[Dict[str, Any]]:
    if not os.path.exists(path):
        return []
    records = []
    with open(path, "r", encoding="utf-8") as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError as exc:
                print(f"[WARN] Failed to parse JSON line {line_num} in {path}: {exc}", file=sys.stderr)
    return records


def verify(
    ledger_path: str,
    c1_log_path: str,
    c2_log_path: str,
    c1_topics: Set[str],
    c2_topics: Set[str],
) -> bool:
    print("=" * 72)
    print("PHASE 3 END-TO-END VERIFICATION")
    print("=" * 72)

    ledger_entries = load_jsonl(ledger_path)
    c1_entries = load_jsonl(c1_log_path)
    c2_entries = load_jsonl(c2_log_path)

    print(f"Producer Ledger:      {ledger_path} ({len(ledger_entries)} total entries)")
    print(f"Consumer-1 Log:       {c1_log_path} ({len(c1_entries)} entries, topics: {sorted(c1_topics)})")
    print(f"Consumer-2 Log:       {c2_log_path} ({len(c2_entries)} entries, topics: {sorted(c2_topics)})")
    print("-" * 72)

    # Filter ledger to non-duplicate acked messages
    acked_ledger = [e for e in ledger_entries if not e.get("duplicate", False)]
    print(f"Non-duplicate acked producer messages: {len(acked_ledger)}")

    results = []

    # -----------------------------------------------------------------------
    # Check 1: Completeness
    # -----------------------------------------------------------------------
    # Build lookups for consumers: (topic, partition, offset, messageID) -> count
    c1_lookup = {(e.get("topic"), e.get("partition"), e.get("offset"), e.get("messageID")) for e in c1_entries}
    c2_lookup = {(e.get("topic"), e.get("partition"), e.get("offset"), e.get("messageID")) for e in c2_entries}

    missing_c1 = []
    missing_c2 = []

    for entry in acked_ledger:
        topic = entry.get("topic")
        part = entry.get("partition")
        off = entry.get("offset")
        msg_id = entry.get("messageID")
        key = (topic, part, off, msg_id)

        if topic in c1_topics and key not in c1_lookup:
            missing_c1.append(key)
        if topic in c2_topics and key not in c2_lookup:
            missing_c2.append(key)

    check1_pass = len(missing_c1) == 0 and len(missing_c2) == 0
    c1_detail = f"missing {len(missing_c1)}" if missing_c1 else "all present"
    c2_detail = f"missing {len(missing_c2)}" if missing_c2 else "all present"
    results.append((
        "Check 1: Completeness",
        check1_pass,
        f"Consumer-1: {c1_detail}; Consumer-2: {c2_detail}",
    ))

    # -----------------------------------------------------------------------
    # Check 2: Monotonic / Ascending Offsets
    # -----------------------------------------------------------------------
    def check_monotonicity(entries: List[Dict[str, Any]], consumer_name: str) -> Tuple[bool, str]:
        by_part: Dict[Tuple[str, int], List[int]] = defaultdict(list)
        for e in entries:
            t = e.get("topic")
            p = e.get("partition")
            off = e.get("offset")
            if t is not None and p is not None and off is not None:
                by_part[(t, p)].append(off)

        violations = []
        for (t, p), offsets in by_part.items():
            for i in range(1, len(offsets)):
                prev = offsets[i - 1]
                curr = offsets[i]
                if curr < prev:
                    violations.append(f"{t}/{p} decreased {prev}->{curr}")

        if violations:
            return False, f"{consumer_name}: {len(violations)} violations ({', '.join(violations[:3])})"
        return True, f"{consumer_name}: all offsets monotonic across {len(by_part)} partitions"

    c1_mono_ok, c1_mono_msg = check_monotonicity(c1_entries, "Consumer-1")
    c2_mono_ok, c2_mono_msg = check_monotonicity(c2_entries, "Consumer-2")
    check2_pass = c1_mono_ok and c2_mono_ok
    results.append((
        "Check 2: Monotonic Offsets",
        check2_pass,
        f"{c1_mono_msg}; {c2_mono_msg}",
    ))

    # -----------------------------------------------------------------------
    # Check 3: Subscription Isolation
    # -----------------------------------------------------------------------
    c1_unexpected = [e for e in c1_entries if e.get("topic") not in c1_topics]
    c2_unexpected = [e for e in c2_entries if e.get("topic") not in c2_topics]
    check3_pass = len(c1_unexpected) == 0 and len(c2_unexpected) == 0
    results.append((
        "Check 3: Subscription Scope",
        check3_pass,
        f"Consumer-1 unexp: {len(c1_unexpected)}; Consumer-2 unexp: {len(c2_unexpected)}",
    ))

    # -----------------------------------------------------------------------
    # Check 4: Message ID Uniqueness
    # -----------------------------------------------------------------------
    # A single messageID should never appear with different offsets
    msg_locations: Dict[str, Set[Tuple[str, int, int]]] = defaultdict(set)
    for e in ledger_entries:
        mid = e.get("messageID")
        t = e.get("topic")
        p = e.get("partition")
        off = e.get("offset")
        if mid and t is not None and p is not None and off is not None:
            msg_locations[mid].add((t, p, off))

    multi_offset_msgs = {mid: locs for mid, locs in msg_locations.items() if len(locs) > 1}
    check4_pass = len(multi_offset_msgs) == 0
    results.append((
        "Check 4: Message ID Uniqueness",
        check4_pass,
        f"{len(multi_offset_msgs)} message IDs with multiple (topic/part/offset) mappings",
    ))

    # -----------------------------------------------------------------------
    # Print Results Table
    # -----------------------------------------------------------------------
    print(f"{'Check':<32} | {'Status':<8} | {'Details'}")
    print("-" * 72)
    all_passed = True
    for name, passed, details in results:
        status_str = "PASS" if passed else "FAIL"
        if not passed:
            all_passed = False
        print(f"{name:<32} | {status_str:<8} | {details}")

    print("=" * 72)
    if all_passed:
        print("VERIFICATION RESULT: ALL CHECKS PASSED")
    else:
        print("VERIFICATION RESULT: ONE OR MORE CHECKS FAILED")
    print("=" * 72)

    return all_passed


def main() -> None:
    parser = argparse.ArgumentParser(description="Verify Phase-3 ledger and consumer logs")
    parser.add_argument(
        "--ledger",
        default=os.environ.get("LEDGER_PATH", os.path.join("data", "producer.ledger")),
        help="Path to producer ledger JSONL",
    )
    parser.add_argument(
        "--consumer-1-log",
        default=os.environ.get("CONSUMER1_LOG", os.path.join("data", "consumer-1", "consumer-1-processed.log")),
        help="Path to consumer-1 processed JSONL",
    )
    parser.add_argument(
        "--consumer-2-log",
        default=os.environ.get("CONSUMER2_LOG", os.path.join("data", "consumer-2", "consumer-2-processed.log")),
        help="Path to consumer-2 processed JSONL",
    )
    parser.add_argument(
        "--consumer-1-topics",
        default=os.environ.get("CONSUMER1_TOPICS", "orders,payments"),
        help="Comma-separated topics for consumer 1",
    )
    parser.add_argument(
        "--consumer-2-topics",
        default=os.environ.get("CONSUMER2_TOPICS", "payments,notifications,audit"),
        help="Comma-separated topics for consumer 2",
    )

    args = parser.parse_args()

    c1_topics = {t.strip() for t in args.consumer_1_topics.split(",") if t.strip()}
    c2_topics = {t.strip() for t in args.consumer_2_topics.split(",") if t.strip()}

    ok = verify(
        ledger_path=args.ledger,
        c1_log_path=args.consumer_1_log,
        c2_log_path=args.consumer_2_log,
        c1_topics=c1_topics,
        c2_topics=c2_topics,
    )

    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
