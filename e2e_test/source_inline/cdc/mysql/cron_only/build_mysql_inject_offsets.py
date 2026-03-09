#!/usr/bin/env python3

import argparse
import json
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--split-id", required=True)
    parser.add_argument("--binlog-file", required=True)
    parser.add_argument("--binlog-pos", required=True, type=int)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()

    offset = {
        "sourcePartition": {"server": "test"},
        "sourceOffset": {
            "file": args.binlog_file,
            "pos": args.binlog_pos,
            "snapshot": False,
        },
        "isHeartbeat": False,
    }

    args.output.write_text(
        json.dumps(
            {args.split_id: json.dumps(offset, separators=(",", ":"))},
            separators=(",", ":"),
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
