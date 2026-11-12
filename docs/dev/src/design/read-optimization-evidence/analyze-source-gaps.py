#!/usr/bin/env python3
"""Estimate no-wait merges of complete small StreamScan GETs in a Q18 trace."""

import argparse
from collections import Counter, defaultdict
from datetime import datetime
import gzip
import json
from pathlib import Path
import re

IDS = re.compile(r"request_ids=(?:Some\()?\[([^\]]*)\]")
REQUEST = re.compile(r"object read submitted request_id=(\d+)")
EXECUTOR = re.compile(r'executor\{otel.name="([^ "]+)')
RANGE = re.compile(r"start=(\d+) end=(\d+)")
DEQUEUE = re.compile(r"scheduler=(\d+) request_ids=\[[^\]]*\] dequeued_us=\d+ requests=(.*)")
TUPLE = re.compile(r"\((\d+), (\d+), (\d+), (\d+)\)")
GROUP = re.compile(r"group_id=Some\((\d+)\)")
READY = re.compile(r"\bready=(\d+)")
KIB = 1024
MAX_SPAN = 1024 * KIB
THRESHOLDS = (4, 16, 32, 64, 128, 256, 512)


def read_trace(path, start, cutoff):
    opener = gzip.open if path.suffix == ".gz" else open
    per_block_source_reads = set()
    dequeued = {}
    ready = defaultdict(set)
    selections = {}
    pending_same_object = {}
    actual_ranges = {}
    phase_successes = set()
    with opener(path, "rt") as trace:
        for line in trace:
            if "object read submitted" in line and "block_read{" in line:
                executors = EXECUTOR.findall(line)
                if executors and executors[-1] == "StreamScan":
                    per_block_source_reads.add(int(REQUEST.search(line)[1]))
            elif "scheduler requests dequeued" in line:
                event = DEQUEUE.search(line)
                scheduler = int(event[1])
                ids = tuple(map(int, re.findall(r"Some\((\d+)\)", IDS.search(line)[1])))
                entries = TUPLE.findall(event[2])
                assert len(ids) == len(entries), line
                for request, entry in zip(ids, entries):
                    assert request not in dequeued, line
                    object_hash, left, right, _ = map(int, entry)
                    dequeued[request] = (scheduler, object_hash, left, right)
                    ready[scheduler].add(request)
            elif "scheduler group selected" in line:
                group = int(GROUP.search(line)[1])
                ids = tuple(map(int, IDS.search(line)[1].split(", ")))
                assert group == ids[0] and group not in selections, line
                objects = {dequeued[request][:2] for request in ids}
                assert len(objects) == 1, line
                scheduler, object_hash = objects.pop()
                pending = ready[scheduler]
                assert pending.issuperset(ids), line
                pending.difference_update(ids)
                assert len(pending) == int(READY.search(line)[1]), line
                selections[group] = (ids, scheduler, object_hash)
                pending_same_object[group] = tuple(
                    r for r in pending if dequeued[r][1] == object_hash
                )
            elif "scheduler physical read started" in line:
                group = int(GROUP.search(line)[1])
                assert group not in actual_ranges, line
                span = tuple(map(int, RANGE.search(line).groups()))
                ids = tuple(map(int, IDS.search(line)[1].split(", ")))
                assert ids == selections[group][0], line
                assert span == (
                    min(dequeued[r][2] for r in ids),
                    max(dequeued[r][3] for r in ids),
                ), line
                actual_ranges[group] = span
            elif "scheduler physical read completed" in line and "ok=true" in line:
                completed = datetime.fromisoformat(line.split(" ", 1)[0]).timestamp()
                if start <= completed <= cutoff:
                    phase_successes.add(int(GROUP.search(line)[1]))
    assert all(not members for members in ready.values())
    assert phase_successes <= selections.keys()
    eligible = {
        group: actual_ranges[group]
        for group in phase_successes
        if actual_ranges[group][1] - actual_ranges[group][0] <= 64 * KIB
        and all(r in per_block_source_reads for r in selections[group][0])
    }
    return selections, pending_same_object, actual_ranges, phase_successes, eligible


def analyze(path, start, cutoff):
    selections, pending_same_object, ranges, successful, eligible = read_trace(path, start, cutoff)
    groups_by_request = {
        request: group for group in eligible for request in selections[group][0]
    }
    neighbors = {}
    pair_histogram = Counter()
    small_gap_pairs = []
    examples = {}
    for group in selections:
        if group not in eligible:
            continue
        pending = Counter(
            groups_by_request[r] for r in pending_same_object[group] if r in groups_by_request
        )
        # The entire later GET must be visible at the earlier selection.
        neighbors[group] = [
            other for other, count in pending.items() if count == len(selections[other][0])
        ]
        for other in neighbors[group]:
            first, second = sorted((eligible[group], eligible[other]))
            gap = second[0] - first[1]
            if gap <= 0 or second[1] - first[0] > MAX_SPAN:
                continue
            bucket = next((str(k) for k in THRESHOLDS if gap <= k * KIB), ">512")
            pair_histogram[bucket] += 1
            example = {
                "object_hash": selections[group][2],
                "earlier_selected_get": group,
                "later_selected_get": other,
                "ranges": [first, second],
                "gap_bytes": gap,
            }
            examples.setdefault(bucket, example)
            if gap <= 64 * KIB:
                small_gap_pairs.append(example)

    savings = {}
    for threshold in THRESHOLDS:
        used = set()
        removed = 0
        added_bytes = 0
        for group, others in neighbors.items():
            if group in used:
                continue
            ordered = sorted(
                (g for g in (group, *others) if g not in used), key=eligible.__getitem__
            )
            run = []
            for candidate in ordered:
                begin, end = eligible[candidate]
                if run:
                    gap = begin - eligible[run[-1]][1]
                    if not (0 < gap <= threshold * KIB and end - eligible[run[0]][0] <= MAX_SPAN):
                        if group in run:
                            break
                        run.clear()
                run.append(candidate)
            assert group in run
            used.update(run)
            removed += len(run) - 1
            if len(run) > 1:
                added_bytes += sum(
                    eligible[right][0] - eligible[left][1]
                    for left, right in zip(run, run[1:])
                )
        assert len(used) == len(eligible)
        savings[threshold] = {"saved_gets": removed, "extra_bytes": added_bytes}

    return {
        "start_unix_s": start,
        "source_checkpoint_unix_s": cutoff,
        "scheduled_successes": len(successful),
        "scheduled_successes_at_most_64kib": sum(
            ranges[g][1] - ranges[g][0] <= 64 * KIB for g in successful
        ),
        "stream_scan_small_gets": len(eligible),
        "stream_scan_small_gets_single_request": sum(
            len(selections[g][0]) == 1 for g in eligible
        ),
        "simultaneously_ready_pairs_by_gap_bucket_upper_kib": dict(pair_histogram),
        "examples": examples,
        "pairs_at_most_64kib": small_gap_pairs,
        "no_wait_merge_estimate_by_max_gap_kib": savings,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("trace", type=Path)
    parser.add_argument("--start", type=float, required=True)
    parser.add_argument("--cutoff", type=float, required=True)
    args = parser.parse_args()
    print(json.dumps(analyze(args.trace, args.start, args.cutoff), indent=2))
