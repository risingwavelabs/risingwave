#!/usr/bin/env python3

import argparse
import re
import time
import urllib.error
import urllib.request


def read_metric(metrics: str, metric_name: str, source_id: str) -> float | None:
    pattern = re.compile(rf"^{re.escape(metric_name)}\{{([^}}]*)\}}\s+(\S+)$")
    for line in metrics.splitlines():
        match = pattern.match(line)
        if match and ("source_id", source_id) in re.findall(
            r'(\w+)="([^"]*)"', match.group(1)
        ):
            return float(match.group(2))
    return None


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-id", required=True)
    parser.add_argument("--metrics-url", required=True)
    parser.add_argument("--timeout-secs", type=float, default=60)
    args = parser.parse_args()
    if not args.source_id.isdecimal():
        parser.error("--source-id must be a single numeric source ID")

    deadline = time.monotonic() + args.timeout_secs
    last = None
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(args.metrics_url, timeout=5) as response:
                metrics = response.read().decode()
            up, oldest, newest = (
                read_metric(metrics, name, args.source_id)
                for name in (
                    "source_status_is_up",
                    "mysql_cdc_binlog_file_seq_min",
                    "mysql_cdc_binlog_file_seq_max",
                )
            )
            last = f"source_status_is_up={up}, binlog_min={oldest}, binlog_max={newest}"
            if up == 1 and oldest is not None and newest is not None and 0 < oldest <= newest:
                return
        except (urllib.error.URLError, TimeoutError) as error:
            last = str(error)
        time.sleep(1)

    raise SystemExit(
        f"timed out waiting for MySQL binlog monitoring for source {args.source_id}: {last}"
    )


if __name__ == "__main__":
    main()
