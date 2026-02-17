#!/usr/bin/env python3
"""Analyze recent Buildkite integration-test failures on a branch.

Example:
  BUILDKITE_COOKIE='bk_logged_in=true; _buildkite_sess=...' \
    ci/scripts/analyze-integration-test-failures.py --branch main --limit 5

If --watch-interval is set, the script keeps polling and only prints newly seen builds.
"""

from __future__ import annotations

import argparse
import html
import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Iterable

DEFAULT_PIPELINE = "risingwavelabs/integration-tests"

# Keep this ordered by specificity.
SIGNATURE_PATTERNS: list[tuple[str, re.Pattern[str]]] = [
    ("missing python distutils", re.compile(r"ModuleNotFoundError: No module named 'distutils'")),
    ("iceberg metastore config", re.compile(r"METASTOREURIS")),
    ("nats protobuf schema", re.compile(r"missing field `consumer\.durable_name`")),
    ("nats barrier flush", re.compile(r"failed to collect barrier")),
    ("clickhouse sql syntax", re.compile(r"Syntax error: failed at position")),
    ("ruby pg option incompatibility", re.compile(r"invalid connection option \"tty\"")),
    ("service bootstrap flakiness", re.compile(r'service \"kafka\" is not running')),
]


@dataclass
class JobFailure:
    step_name: str
    step_key: str | None
    exit_status: str
    signature: str
    evidence: str


def fetch_text(
    url: str,
    cookie: str | None,
    timeout: float = 30.0,
    accept: str = "text/html,application/json;q=0.9,*/*;q=0.8",
) -> str:
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": accept,
    }
    if cookie:
        headers["Cookie"] = cookie

    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=timeout) as resp:  # noqa: S310
        return resp.read().decode("utf-8", errors="replace")


def fetch_json(url: str, cookie: str | None, timeout: float = 30.0) -> dict:
    payload = fetch_text(url, cookie, timeout=timeout, accept="application/json")
    if not payload.strip():
        return {}

    try:
        return json.loads(payload)
    except json.JSONDecodeError as err:
        # The session cookie may be expired and Buildkite can return an HTML page.
        snippet = payload[:200].replace("\n", " ")
        raise ValueError(f"expected JSON but got non-JSON response from {url}: {snippet}") from err


def extract_build_ids(builds_html: str, pipeline: str) -> list[int]:
    pattern = re.compile(rf"/{re.escape(pipeline)}/builds/(\d+)")
    seen: set[int] = set()
    result: list[int] = []
    for m in pattern.finditer(builds_html):
        build_id = int(m.group(1))
        if build_id not in seen:
            seen.add(build_id)
            result.append(build_id)
    return result


def extract_render_json(page_html: str, component_marker: str) -> dict:
    marker = f'"{component_marker}",'  # in render("component", "#id", { ... })
    marker_pos = page_html.find(marker)
    if marker_pos < 0:
        raise ValueError(f"marker not found: {component_marker}")

    start = page_html.find("{", marker_pos)
    if start < 0:
        raise ValueError("json start not found")

    in_string = False
    escaped = False
    depth = 0
    end = -1
    for idx in range(start, len(page_html)):
        ch = page_html[idx]
        if in_string:
            if escaped:
                escaped = False
            elif ch == "\\":
                escaped = True
            elif ch == '"':
                in_string = False
            continue

        if ch == '"':
            in_string = True
        elif ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                end = idx + 1
                break

    if end < 0:
        raise ValueError("json end not found")

    return json.loads(page_html[start:end])


def html_log_to_lines(log_html_payload: str) -> list[str]:
    text = html.unescape(log_html_payload)
    text = re.sub(r"<[^>]+>", "", text)
    return [line.strip() for line in text.splitlines() if line.strip()]


def detect_signature(lines: list[str]) -> tuple[str, str]:
    for signature, pattern in SIGNATURE_PATTERNS:
        for line in lines:
            if pattern.search(line):
                return signature, line

    for line in reversed(lines):
        if "ERROR:" in line or "Error:" in line or "Exception" in line:
            return "generic error", line

    if lines:
        return "unknown", lines[-1]
    return "unknown", "empty log"


def fetch_failed_jobs(build_page: dict) -> list[dict]:
    jobs: list[dict] = build_page.get("build", {}).get("jobs", [])
    failed: list[dict] = []
    for job in jobs:
        if job.get("type") != "script":
            continue
        if job.get("passed") is not False:
            continue
        step_key = job.get("step_key")
        name = (job.get("name") or "").strip()
        if step_key == "test-notify":
            # Intentional notification test step.
            continue
        if name == "trigger failed test notification":
            # Notification pipeline intentionally exits 1 when there are failures.
            continue
        failed.append(job)
    return failed


def summarize_build_failures(pipeline: str, build_id: int, cookie: str | None) -> tuple[dict, list[JobFailure]]:
    build_json_url = f"https://buildkite.com/{pipeline}/builds/{build_id}.json"
    build_meta = fetch_json(build_json_url, cookie)

    show_url = f"https://buildkite.com/{pipeline}/builds/{build_id}"
    show_html = fetch_text(show_url, cookie)
    show_data = extract_render_json(show_html, "app/components/build/Show")

    failures: list[JobFailure] = []
    for job in fetch_failed_jobs(show_data):
        base_path = job.get("base_path")
        if not base_path:
            continue

        log_url = f"https://buildkite.com{base_path}/log"
        try:
            log_json = fetch_json(log_url, cookie)
        except urllib.error.HTTPError as err:
            failures.append(
                JobFailure(
                    step_name=job.get("name") or "(unnamed)",
                    step_key=job.get("step_key"),
                    exit_status=str(job.get("exit_status")),
                    signature="log fetch failed",
                    evidence=f"HTTP {err.code} on {log_url}",
                )
            )
            continue

        lines = html_log_to_lines(log_json.get("output", ""))
        signature, evidence = detect_signature(lines)
        failures.append(
            JobFailure(
                step_name=job.get("name") or "(unnamed)",
                step_key=job.get("step_key"),
                exit_status=str(job.get("exit_status")),
                signature=signature,
                evidence=evidence,
            )
        )

    return build_meta, failures


def iter_target_build_ids(pipeline: str, branch: str, cookie: str | None) -> list[int]:
    query = urllib.parse.urlencode({"branch": branch})
    url = f"https://buildkite.com/{pipeline}/builds?{query}"
    html_text = fetch_text(url, cookie)
    return extract_build_ids(html_text, pipeline)


def print_build_report(build_meta: dict, failures: list[JobFailure]) -> None:
    number = build_meta.get("number")
    state = build_meta.get("state")
    commit = (build_meta.get("commit_id") or "")[:10]
    created = build_meta.get("created_at")

    print(f"\n# Build {number} | state={state} | commit={commit} | created={created}")
    if not failures:
        print("  no actionable failed script steps found")
        return

    for failure in failures:
        step_key = failure.step_key or "(none)"
        print(f"  - {failure.step_name} [step_key={step_key}, exit={failure.exit_status}]")
        print(f"    signature: {failure.signature}")
        print(f"    evidence : {failure.evidence}")


def analyze_once(pipeline: str, branch: str, cookie: str | None, limit: int) -> list[int]:
    build_ids = iter_target_build_ids(pipeline, branch, cookie)
    analyzed: list[int] = []

    for build_id in build_ids[:limit]:
        build_meta, failures = summarize_build_failures(pipeline, build_id, cookie)
        if build_meta.get("branch_name") != branch:
            continue
        analyzed.append(build_id)
        print_build_report(build_meta, failures)

    if not analyzed:
        print("no builds found")
    return analyzed


def run_watch_loop(
    pipeline: str,
    branch: str,
    cookie: str | None,
    limit: int,
    watch_interval: int,
) -> None:
    seen: set[int] = set()
    while True:
        try:
            current_build_ids = iter_target_build_ids(pipeline, branch, cookie)
            new_ids = [bid for bid in current_build_ids if bid not in seen][:limit]
            if new_ids:
                print(f"\n== found {len(new_ids)} new builds on branch '{branch}' ==")
            for build_id in new_ids:
                build_meta, failures = summarize_build_failures(pipeline, build_id, cookie)
                seen.add(build_id)
                print_build_report(build_meta, failures)
            if not new_ids:
                print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] no new builds, sleep {watch_interval}s")
        except Exception as err:  # keep watcher alive
            print(f"watch loop error: {err}", file=sys.stderr)

        time.sleep(watch_interval)


def parse_args(argv: Iterable[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pipeline", default=DEFAULT_PIPELINE, help="Buildkite pipeline path, e.g. risingwavelabs/integration-tests")
    parser.add_argument("--branch", default="main", help="branch name to inspect")
    parser.add_argument("--limit", type=int, default=5, help="max builds to analyze per run")
    parser.add_argument("--cookie", default=None, help="cookie header value. If empty, use BUILDKITE_COOKIE env")
    parser.add_argument("--watch-interval", type=int, default=0, help="seconds between polls; 0 means run once")
    return parser.parse_args(list(argv))


def main(argv: Iterable[str]) -> int:
    args = parse_args(argv)
    cookie = args.cookie or os.environ.get("BUILDKITE_COOKIE") or None

    if args.watch_interval > 0:
        run_watch_loop(args.pipeline, args.branch, cookie, args.limit, args.watch_interval)
        return 0

    analyze_once(args.pipeline, args.branch, cookie, args.limit)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
