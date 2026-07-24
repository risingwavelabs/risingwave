#!/usr/bin/env python3

import argparse
import subprocess
import time


def run_psql(host: str, port: int, database: str, query: str) -> str:
    result = subprocess.run(
        [
            'psql',
            '-h',
            host,
            '-p',
            str(port),
            '-d',
            database,
            '-U',
            'root',
            '-t',
            '-A',
            '-c',
            query,
        ],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        return ''
    return result.stdout.strip()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--host', required=True)
    parser.add_argument('--port', required=True, type=int)
    parser.add_argument('--database', required=True)
    parser.add_argument('--table', required=True)
    parser.add_argument('--expected', required=True)
    parser.add_argument('--timeout-secs', type=int, default=180)
    parser.add_argument('--interval-secs', type=float, default=1.0)
    args = parser.parse_args()

    deadline = time.time() + args.timeout_secs
    while time.time() < deadline:
        actual = run_psql(
            args.host,
            args.port,
            args.database,
            f"SELECT string_agg(id || ':' || payload, ',' ORDER BY id) FROM {args.table};",
        )
        if actual == args.expected:
            return
        time.sleep(args.interval_secs)

    raise SystemExit(f'timed out waiting for injected direct CDC rows: expected {args.expected}')


if __name__ == '__main__':
    main()
