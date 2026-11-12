#!/usr/bin/env python3
"""Join prepared window, admitted packet, and physical dispatch by request ID."""

import argparse
from collections import Counter
import gzip
import json
from pathlib import Path
import re

IDS = re.compile(r"request_ids=(?:Some\()?\[([^\]]*)\]")
REQUEST = re.compile(r"request_id=(\d+)\b")
GROUP = re.compile(r"group_id=Some\((\d+)\)")
RANGE = re.compile(r"start=(\d+) end=(\d+)")
BLOCK = re.compile(r"block_read\{object_id=(\d+) block=(\d+) start=(\d+) end=(\d+)\}")
EXECUTOR = re.compile(r'executor\{otel.name="([^ "]+)')


def members(line):
    value = IDS.search(line)[1]
    return tuple(int(value) for value in value.split(", ") if value)


def analyze(path, blocks, byte_cap):
    windows = []
    reserved_windows = []
    packets = {}
    submissions = {}
    completed = set()
    selected = {}
    groups = {}
    physical = {}
    successful = set()
    fallback = set()
    counts = Counter()
    examples = {}
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rt") as source:
        for line_number, line in enumerate(source, 1):
            if " DEBUG " not in line:
                continue
            if "prepared window released" in line:
                ids = members(line)
                reason = re.search(r'reason="(\w+)"', line)[1]
                reserved = len(ids) + int(re.search(r"released_permits=(\d+)", line)[1])
                if reserved == blocks:
                    reserved_windows.append((reason, ids))
                    counts[f"reserved_{blocks}_{reason}"] += 1
                    if reason == "window_complete":
                        counts[f"complete_staged_{len(ids)}"] += 1
                        if len(ids) == blocks:
                            windows.append(ids)
            elif "scheduler window packet admitted" in line:
                ids = members(line)
                assert len(ids) == int(re.search(r"size=(\d+)", line)[1]), line
                prep = "uses_preparation=true" in line
                for request in ids:
                    assert request not in packets, line
                    packets[request] = (ids, prep)
            elif "object read submitted" in line:
                if block := BLOCK.search(line):
                    executors = EXECUTOR.findall(line)
                    submissions[int(REQUEST.search(line)[1])] = (
                        tuple(map(int, block.groups())), executors[-1] if executors else None
                    )
            elif "object read completed" in line and "ok=true" in line:
                completed.add(int(REQUEST.search(line)[1]))
            elif "scheduler group selected" in line:
                selected[int(GROUP.search(line)[1])] = line_number
            elif "scheduler physical read started" in line:
                group = int(GROUP.search(line)[1])
                ids = members(line)
                groups[group] = (tuple(map(int, RANGE.search(line).groups())), ids)
                for request in ids:
                    assert request not in physical, line
                    physical[request] = group
            elif "scheduler physical read completed" in line and "ok=true" in line:
                successful.add(int(GROUP.search(line)[1]))
            elif "read scheduler range fallback dispatch" in line and (group := GROUP.search(line)):
                fallback.add(int(group[1]))

    distributions = Counter()
    executors = Counter()
    causes = Counter()
    for ids in windows:
        if not all(
            r in submissions and r in packets and r in physical and r in completed for r in ids
        ):
            counts["unlinked_full_cold"] += 1
            continue
        window_groups = {physical[r] for r in ids}
        if window_groups - selected.keys():
            counts["full_cold_missing_selection"] += 1
            continue
        if window_groups - successful or window_groups & fallback:
            counts["full_cold_io_error_or_retry_fallback"] += 1
            continue
        blocks_and_executors = [submissions[r] for r in ids]
        entries = sorted(block for block, _ in blocks_and_executors)
        object_id, first_block, start, _ = entries[0]
        end = entries[-1][3]
        if (
            any(entry[:2] != (object_id, first_block + i) for i, entry in enumerate(entries))
            or any(left[3] != right[2] for left, right in zip(entries, entries[1:]))
            or end - start > byte_cap
        ):
            counts["full_cold_noncontiguous_or_over_cap"] += 1
            continue
        scopes = {executor for _, executor in blocks_and_executors}
        scope = next(iter(scopes)) if len(scopes) == 1 and None not in scopes else "unknown_or_mixed"
        executors[scope] += 1
        window_packets = {packets[r] for r in ids}
        packet_sizes = sorted(len(packet) for packet, _ in window_packets)
        prep_packets = sum(prep for _, prep in window_packets)
        outcome = f"{blocks} outer reads / {packet_sizes} admitted / {len(window_groups)} GETs"
        distributions[outcome] += 1
        if prep_packets:
            counts["full_cold_with_preparation_admission"] += 1
        ordered_groups = sorted(window_groups, key=selected.__getitem__)
        get_ranges = [groups[group][0] for group in ordered_groups]
        if len(window_groups) == 1:
            outcome = "one_get_with_preparation" if prep_packets else "one_get"
        else:
            combined_span = max(stop for _, stop in get_ranges) - min(begin for begin, _ in get_ranges)
            if len(window_packets) > 1:
                outcome = "multiple_packets"
            elif combined_span > byte_cap:
                outcome = "intact_packet_with_group_span_over_cap"
            else:
                outcome = "intact_packet_split_unexplained"
            causes[outcome] += 1
        examples.setdefault(outcome, {
            "executor": scope,
            "object_id": object_id,
            "first_block": first_block,
            "window_range": [start, end],
            "outer_read_ids": ids,
            "admitted_packets": [
                {"request_ids": packet, "uses_preparation": prep}
                for packet, prep in sorted(window_packets)
            ],
            "gets": [
                {"range": groups[group][0], "request_ids": groups[group][1]}
                for group in ordered_groups
            ],
        })
    staged_outcomes = {}
    successful_without_fallback = successful - fallback
    for reason, ids in reserved_windows:
        outcome = f"{reason}: {len(ids)} staged" if reason == "window_complete" else reason
        stats = staged_outcomes.setdefault(outcome, Counter())
        stats["windows"] += 1
        stats["staged_reads"] += len(ids)
        if not all(r in packets and physical.get(r) in successful_without_fallback for r in ids):
            stats["unlinked_windows"] += 1
            continue
        stats[f"packets_{len({packets[r] for r in ids})}"] += 1
        stats[f"gets_{len({physical[r] for r in ids})}"] += 1

    return {
        "window_counts": counts,
        "staged_read_outcomes": staged_outcomes,
        "matched_full_cold_by_executor": executors,
        "full_cold_distribution": distributions,
        "split_causes": causes,
        "examples": examples,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("trace", type=Path)
    parser.add_argument("--blocks", type=int, default=16)
    parser.add_argument("--byte-cap", type=int, default=1024 * 1024)
    args = parser.parse_args()
    print(json.dumps(analyze(args.trace, args.blocks, args.byte_cap), indent=2, sort_keys=True))
