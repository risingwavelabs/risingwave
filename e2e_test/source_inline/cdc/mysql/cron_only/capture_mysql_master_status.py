#!/usr/bin/env python3

import argparse
import subprocess
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--file-output", required=True, type=Path)
    parser.add_argument("--pos-output", required=True, type=Path)
    parser.add_argument("--file-seq-output", required=True, type=Path)
    args = parser.parse_args()

    result = subprocess.run(
        ["mysql", "--batch", "--raw", "--skip-column-names", "-e", "SHOW MASTER STATUS"],
        check=True,
        capture_output=True,
        text=True,
    )
    first_line = result.stdout.strip().splitlines()[0]
    fields = first_line.split("\t")
    binlog_file = fields[0]
    binlog_pos = int(fields[1])
    binlog_file_seq = int(binlog_file.rsplit(".", 1)[1])

    args.file_output.write_text(binlog_file)
    args.pos_output.write_text(str(binlog_pos))
    args.file_seq_output.write_text(str(binlog_file_seq))


if __name__ == "__main__":
    main()
