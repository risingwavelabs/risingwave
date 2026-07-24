#!/usr/bin/env python3

import argparse
import re
import time
import urllib.request


def read_metric(metrics: str, metric_name: str, source_id: str) -> int | None:
    pattern = re.compile(rf'^{re.escape(metric_name)}\{{source_id="{re.escape(source_id)}"\}}\s+(\d+)(?:\.0+)?$')
    for line in metrics.splitlines():
        match = pattern.match(line)
        if match:
            return int(match.group(1))
    return None


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-id", required=True)
    parser.add_argument("--file-seq", required=True, type=int)
    parser.add_argument("--pos", required=True, type=int)
    parser.add_argument("--mode", choices=["advance", "equal", "at_least"], required=True)
    parser.add_argument("--metrics-url", default="http://127.0.0.1:1222/metrics")
    parser.add_argument("--timeout-secs", type=int, default=30)
    parser.add_argument("--interval-secs", type=float, default=1.0)
    args = parser.parse_args()

    deadline = time.time() + args.timeout_secs
    last_file_seq = None
    last_pos = None

    while time.time() < deadline:
        with urllib.request.urlopen(args.metrics_url, timeout=5) as response:
            metrics = response.read().decode()

        last_file_seq = read_metric(metrics, "stream_mysql_cdc_state_binlog_file_seq", args.source_id)
        last_pos = read_metric(metrics, "stream_mysql_cdc_state_binlog_position", args.source_id)

        if last_file_seq is not None and last_pos is not None:
            if args.mode == "advance":
                if last_file_seq > args.file_seq or (
                    last_file_seq == args.file_seq and last_pos > args.pos
                ):
                    return
            elif args.mode == "equal":
                if last_file_seq == args.file_seq and last_pos == args.pos:
                    return
            else:
                if last_file_seq > args.file_seq or (
                    last_file_seq == args.file_seq and last_pos >= args.pos
                ):
                    return

        time.sleep(args.interval_secs)

    raise SystemExit(
        f"timed out waiting for MySQL CDC metrics: mode={args.mode}, "
        f"source_id={args.source_id}, target=({args.file_seq}, {args.pos}), "
        f"last=({last_file_seq}, {last_pos})"
    )


if __name__ == "__main__":
    main()
