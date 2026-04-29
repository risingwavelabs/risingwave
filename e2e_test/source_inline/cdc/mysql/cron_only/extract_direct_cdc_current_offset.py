#!/usr/bin/env python3

import argparse
import subprocess
import time
from pathlib import Path


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
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--host', required=True)
    parser.add_argument('--port', required=True, type=int)
    parser.add_argument('--database', required=True)
    parser.add_argument('--job-name', default='direct_cdc_inject_offset')
    parser.add_argument('--source-id-file', required=True, type=Path)
    parser.add_argument('--output', required=True, type=Path)
    parser.add_argument('--timeout-secs', type=int, default=60)
    parser.add_argument('--interval-secs', type=float, default=1.0)
    args = parser.parse_args()

    state_table = run_psql(
        args.host,
        args.port,
        args.database,
        (
            'SELECT name '
            'FROM rw_catalog.rw_internal_table_info '
            f"WHERE job_name = '{args.job_name}' "
            "AND job_type = 'table' "
            'ORDER BY job_id DESC '
            'LIMIT 1;'
        ),
    )
    if not state_table:
        raise SystemExit(f'failed to find internal state table for job {args.job_name}')

    source_id = args.source_id_file.read_text().strip()
    deadline = time.time() + args.timeout_secs

    while time.time() < deadline:
        offset = run_psql(
            args.host,
            args.port,
            args.database,
            (
                "SELECT offset_info->'split_info'->'mysql_split'->'inner'->>'start_offset' "
                f'FROM {state_table} '
                f"WHERE partition_id = '{source_id}' "
                'LIMIT 1;'
            ),
        )
        if offset and '"isHeartbeat":false' in offset:
            args.output.write_text(offset)
            return
        time.sleep(args.interval_secs)

    raise SystemExit('timed out waiting for non-heartbeat direct CDC offset')


if __name__ == '__main__':
    main()
