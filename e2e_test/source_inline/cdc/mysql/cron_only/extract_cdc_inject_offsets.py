#!/usr/bin/env python3

import argparse
import csv
import json
from pathlib import Path


def extract_offsets(state_file: Path) -> dict[str, str]:
    for raw_line in state_file.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("Table:"):
            continue

        try:
            row = next(csv.reader([line]))
        except Exception:
            continue

        if len(row) < 2:
            continue

        split_id = row[0].strip()
        if not split_id:
            continue

        try:
            split_payload = json.loads(row[1])
        except json.JSONDecodeError:
            continue

        split_info = split_payload.get("split_info", {})
        start_offset = split_info.get("start_offset")
        if not isinstance(start_offset, str) or not start_offset:
            continue

        return {split_id: start_offset}

    raise ValueError(f"failed to extract non-empty CDC start_offset from {state_file}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Extract CDC split_id -> start_offset map from internal_table.mjs output"
    )
    parser.add_argument("--state-file", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()

    offsets = extract_offsets(args.state_file)
    args.output.write_text(json.dumps(offsets, separators=(",", ":"), sort_keys=True))


if __name__ == "__main__":
    main()
