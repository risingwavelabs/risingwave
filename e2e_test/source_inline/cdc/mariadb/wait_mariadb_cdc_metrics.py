#!/usr/bin/env python3

import argparse
import os
import re
import subprocess
import time
import urllib.error
import urllib.request


def read_metric(metrics: str, name: str, source_id: str) -> float | None:
    pattern = re.compile(rf"^{re.escape(name)}\{{([^}}]*)\}}\s+(\S+)$", re.MULTILINE)
    for labels, value in pattern.findall(metrics):
        if f'source_id="{source_id}"' in labels.split(","):
            return float(value)
    return None


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-id", required=True, type=int)
    parser.add_argument(
        "--metrics-url",
        default=os.environ.get("RW_META_METRICS_URL", "http://127.0.0.1:1250/metrics"),
    )
    parser.add_argument("--timeout-secs", type=float, default=60)
    args = parser.parse_args()

    # Compare both gauges with the server's actual binlog range.
    output = subprocess.check_output(
        ["mariadb", "-NBe", "SHOW BINARY LOGS"], text=True
    )
    sequences = [int(line.split()[0].rsplit(".", 1)[1]) for line in output.splitlines()]
    assert sequences, "MariaDB has no binary logs"
    expected = {
        "source_status_is_up": 1,
        "mariadb_cdc_binlog_file_seq_min": sequences[0],
        "mariadb_cdc_binlog_file_seq_max": sequences[-1],
    }
    deadline = time.monotonic() + args.timeout_secs
    last = None
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(args.metrics_url, timeout=5) as response:
                metrics = response.read().decode()
            last = {
                name: read_metric(metrics, name, str(args.source_id))
                for name in expected
            }
            if last == expected:
                return
        except (urllib.error.URLError, TimeoutError) as error:
            last = str(error)
        time.sleep(1)
    raise SystemExit(
        f"Timed out waiting for MariaDB CDC metrics for source {args.source_id}: "
        f"expected={expected}, last={last}"
    )


if __name__ == "__main__":
    main()
