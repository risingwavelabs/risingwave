#!/usr/bin/env python3
"""Controlled source bisect; refuse a verdict if endpoint checks are inconsistent."""
import gzip
import json
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import sys
import tempfile
import time

COMMITS = [
    "24cf8ef7be09ff15d33845e6cecccb08f5e920ff",
    "ee5d2019be8b7e7543b2461f5a97c9f53dba5b74",
    "8ac9c69fcc5f9f69003a435fe98840eb150a404f",
    "f97772bbd2a8336db405ebf402d619ea8ad7d884",
    "ec51de1eeaaafaf1a82b16cb506f87d8c279bb9a",
    "3001b8a6a92535d302b3cd50186949982e0ff5fb",
]
NEW_BASE = "ubuntu:24.04@sha256:73b999393d07f2bf65a40373778a44dc9a6d54ad7718d29247d8ff9413c4d6a8"
OLD_BASE = "ubuntu:24.04@sha256:2198e4c65076e9cb0b3d695219df667c0e3b0072da64ca3c2648aca17dcecaf5"


def investigate(run):
    good = run(0, NEW_BASE, "endpoint-good")
    bad = run(5, NEW_BASE, "endpoint-bad")
    if good == "oom" and bad == "oom":
        old = run(0, OLD_BASE, "old-source-old-base")
        return {"verdict": "source_endpoints_not_bracketed", "good": good,
                "bad": bad, "old_source_old_base": old}
    if good != "pass" or bad != "oom":
        repeated = run(5, NEW_BASE, "endpoint-bad-repeat") if bad == "pass" else None
        return {"verdict": "source_endpoints_not_bracketed", "good": good,
                "bad": bad, "bad_repeat": repeated}
    lo, hi = 0, 5
    while hi - lo > 1:
        mid = (lo + hi + 1) // 2
        result = run(mid, NEW_BASE, f"midpoint-{mid}")
        if result == "pass":
            lo = mid
        elif result == "oom":
            hi = mid
        else:
            return {"verdict": "inconclusive_build_error", "index": mid}
    repeat_good = run(lo, NEW_BASE, f"boundary-good-repeat-{lo}")
    repeat_bad = run(hi, NEW_BASE, f"boundary-bad-repeat-{hi}")
    return {
        "verdict": "repeated_boundary" if repeat_good == "pass" and repeat_bad == "oom"
        else "boundary_not_repeatable",
        "last_good": COMMITS[lo], "first_bad": COMMITS[hi],
        "good_repeat": repeat_good, "bad_repeat": repeat_bad,
    }


def archive(revision, destination, repository):
    destination.mkdir(parents=True, exist_ok=True)
    process = subprocess.Popen(["git", "-C", str(repository), "archive", revision],
                               stdout=subprocess.PIPE)
    try:
        subprocess.run(["tar", "-xf", "-", "-C", str(destination)],
                       stdin=process.stdout, check=True)
    finally:
        process.stdout.close()
    if process.wait():
        raise RuntimeError("git archive failed")


def main():
    root = Path.cwd()
    tools = root / "ci/scripts/arm-bisect"
    builder, results_arg = sys.argv[1:]
    results = (root / results_arg).resolve()
    records = []

    def run(index, base, name):
        sha = COMMITS[index]
        directory = Path(tempfile.mkdtemp(prefix="arm-source-bisect-"))
        prefix = results / name
        started = time.time()
        print(f"--- Trial {name}: source={sha}, base={base}, production jobs=32", flush=True)
        archive(sha, directory, root)
        archive("HEAD", directory / "java/debezium", root / "java/debezium")
        wrapper_dir = directory / ".oom-tools"
        wrapper_dir.mkdir()
        shutil.copy2(tools / "rustc-wrapper.py", wrapper_dir / "rustc-wrapper.py")
        text = (directory / "docker/Dockerfile").read_text()
        assert text.count("FROM ubuntu:24.04 AS base") == 1
        text = text.replace("FROM ubuntu:24.04 AS base", f"FROM {base} AS base")
        needle = "RUN cargo fetch && \\\n  cargo build -p risingwave_cmd_all"
        assert text.count(needle) == 1
        text = text.replace(needle,
            "ARG OOM_RUN_ID\nENV CARGO_BUILD_JOBS=32\n"
            "ENV CARGO_PROFILE_PRODUCTION_LTO=thin\n"
            "ENV RUSTC_WRAPPER=/risingwave/.oom-tools/rustc-wrapper.py\n\n"
            + needle)
        dockerfile = prefix.with_suffix(".Dockerfile")
        dockerfile.write_text(text)
        before = subprocess.run(["dmesg"], capture_output=True, text=True).stdout
        memory = prefix.with_suffix(".memory.jsonl")
        monitor = subprocess.Popen(["python3", str(tools / "monitor.py"),
            f"buildx_buildkit_{builder}0", str(memory)])
        try:
            command = ["docker", "buildx", "build", "-f", str(dockerfile),
                "--build-arg", f"GIT_SHA={sha}", "--build-arg", "CARGO_PROFILE=production",
                "--build-arg", f"OOM_RUN_ID={name}-{time.time_ns()}",
                "--builder", builder, "--progress", "plain",
                "--cache-from", "type=registry,ref=ghcr.io/risingwavelabs/risingwave-build-cache:aarch64",
                str(directory)]
            with prefix.with_suffix(".log").open("w", buffering=1) as output:
                process = subprocess.Popen(command, stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT, text=True, bufsize=1)
                for line in process.stdout:
                    print(line, end="", flush=True)
                    output.write(line)
                status = process.wait()
        finally:
            monitor.send_signal(signal.SIGTERM)
            monitor.wait(timeout=15)
        after = subprocess.run(["dmesg"], capture_output=True, text=True).stdout
        old_lines = set(before.splitlines())
        oom_lines = [line for line in after.splitlines() if line not in old_lines
                     and re.search(r"invoked oom-killer|oom-kill:|Out of memory:|oom_reaper:", line)]
        prefix.with_suffix(".kernel-oom.log").write_text("\n".join(oom_lines) + "\n")
        log_path = prefix.with_suffix(".log")
        log = log_path.read_text()
        metrics = [json.loads(x) for x in re.findall(r"OOM_METRIC (\{[^\n]+\})", log)]
        oom = bool(oom_lines) or bool(re.search(r'ResourceExhausted|cannot allocate memory|OOM_METRIC.*"exit_code": -9', log))
        outcome = "pass" if status == 0 and metrics and metrics[-1]["exit_code"] == 0 else "oom" if status != 0 and oom else "error"
        record = {"name": name, "source": sha, "base": base, "jobs": 32,
            "outcome": outcome, "exit_status": status, "metrics": metrics,
            "elapsed_seconds": time.time() - started}
        records.append(record)
        prefix.with_suffix(".result.json").write_text(json.dumps(record, indent=2) + "\n")
        (results / "results.json").write_text(json.dumps(records, indent=2) + "\n")
        if memory.exists():
            with memory.open("rb") as source, gzip.open(str(memory) + ".gz", "wb") as dest:
                shutil.copyfileobj(source, dest)
            memory.unlink()
        # Only controlled diagnostic output is uploaded, never the checkout or environment.
        for path in results.glob(name + ".*"):
            if path.suffix != ".gz":
                content = path.read_text()
                for key in ("GHCR_TOKEN", "GHCR_USERNAME"):
                    secret = os.environ.get(key, "")
                    if secret:
                        content = content.replace(secret, "[REDACTED]")
                path.write_text(content)
        subprocess.run(["buildkite-agent", "artifact", "upload", str(Path(results_arg) / (name + ".*"))], check=True)
        subprocess.run(["buildkite-agent", "artifact", "upload", str(Path(results_arg) / "results.json")], check=True)
        print("BISECT_RESULT " + json.dumps(record), flush=True)
        shutil.rmtree(directory)
        if outcome == "error":
            raise RuntimeError(f"Non-OOM error in {name}; do not classify as a source regression")
        return outcome

    verdict = investigate(run)
    (results / "verdict.json").write_text(json.dumps(verdict, indent=2) + "\n")
    print("BISECT_VERDICT " + json.dumps(verdict), flush=True)


if __name__ == "__main__":
    if sys.argv[1:] == ["--self-test"]:
        for boundary in range(1, 6):
            verdict = investigate(lambda i, b, n: "oom" if i >= boundary else "pass")
            assert verdict["verdict"] == "repeated_boundary"
            assert verdict["first_bad"] == COMMITS[boundary]
        for result in ("pass", "oom"):
            assert investigate(lambda i, b, n: result)["verdict"] == "source_endpoints_not_bracketed"
        print("Bisect driver startup and decision self-tests passed", flush=True)
    else:
        main()
