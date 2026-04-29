#!/usr/bin/env python3

import argparse
import subprocess
import time
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--source-id-file', required=True, type=Path)
    parser.add_argument('--offsets-file', required=True, type=Path)
    parser.add_argument('--timeout-secs', type=int, default=30)
    parser.add_argument('--interval-secs', type=float, default=1.0)
    args = parser.parse_args()

    source_id = args.source_id_file.read_text().strip()
    offsets = args.offsets_file.read_text().strip()
    deadline = time.time() + args.timeout_secs
    last_error = None

    while time.time() < deadline:
        result = subprocess.run(
            [
                './risedev',
                'ctl',
                'meta',
                'inject-source-offsets',
                '--source-id',
                source_id,
                '--offsets',
                offsets,
            ],
            capture_output=True,
            text=True,
        )
        if result.returncode == 0:
            return
        last_error = result.stderr.strip() or result.stdout.strip() or f'exit code {result.returncode}'
        time.sleep(args.interval_secs)

    raise SystemExit(f'failed to inject source offsets: {last_error}')


if __name__ == '__main__':
    main()
