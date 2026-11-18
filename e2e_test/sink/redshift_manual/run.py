#!/usr/bin/env python3
# Copyright 2026 RisingWave Labs
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Opt-in entry point; no credentials or services are accessed by default."""

import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import uuid

from common import config, redact, require_opt_in

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]


def load_env(path):
    # Parse assignments without evaluating shell code or expanding passwords.
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        key, separator, value = line.removeprefix("export ").partition("=")
        key, value = key.strip(), value.strip()
        if not separator or not key.startswith(("REDSHIFT_", "AWS_", "S3_")):
            raise ValueError(
                "Environment file must contain Redshift/S3 assignments only"
            )
        if len(value) > 1 and value[0] == value[-1] and value[0] in "\"'":
            value = value[1:-1]
        os.environ[key] = value


def run_slt(state, directory):
    env = os.environ.copy()
    env["PATH"] = str(Path(sys.executable).parent) + os.pathsep + env.get("PATH", "")
    # Render only a generated identifier, never credentials. The checked-in SLT
    # has no environment substitutions, so skipped records work without setup.
    slt = directory / "sink.slt"
    slt.write_text(
        (HERE / "sink.slt").read_text().replace("redshift_e2e_schema", state["schema"])
    )
    rw = state["rw"]
    cmd = [
        str(ROOT / "risedev"),
        "slt",
        "-h",
        rw["host"],
        "-p",
        str(rw["port"]),
        "-d",
        rw["dbname"],
        "-u",
        rw["user"],
        "--label",
        "redshift-live",
        str(slt),
    ]
    process = subprocess.Popen(
        cmd,
        cwd=ROOT,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    try:
        for line in process.stdout:
            print(redact(line), end="", flush=True)
        if process.wait():
            raise RuntimeError("Live sink SLT failed")
    finally:
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--run-live",
        action="store_true",
        help="Explicitly enable tests that access AWS",
    )
    parser.add_argument(
        "--env-file",
        type=Path,
        help="Private environment file; never sourced as shell code",
    )
    parser.add_argument("--mode", choices=("jdbc", "s3", "both"), default="both")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=4566)
    parser.add_argument("--database", default="dev")
    parser.add_argument("--user", default="root")
    args = parser.parse_args()
    if not args.run_live:
        print("SKIP live Redshift tests: pass --run-live to enable")
        return
    os.environ["REDSHIFT_E2E_RUN"] = "1"
    require_opt_in()  # Reject CI before reading files or importing optional dependencies.
    if args.env_file:
        load_env(args.env_file)
    modes = ("jdbc", "s3") if args.mode == "both" else (args.mode,)
    configs = {mode: config(mode) for mode in modes}

    from helper import LiveSink
    import regressions

    for mode in modes:
        cfg = configs[mode]
        suffix = uuid.uuid4().hex[:12]
        state = {kind: f"rw_rs_{kind}_{suffix}" for kind in ("cdc", "target", "append")}
        state.update(
            mode=mode,
            schema="rw_rs_" + suffix,
            rw={
                "host": args.host,
                "port": args.port,
                "dbname": args.database,
                "user": args.user,
            },
        )
        if mode == "s3":
            state["prefix"] = cfg["S3_PREFIX"].strip("/") + "/run-" + suffix
        with tempfile.TemporaryDirectory(prefix="rw-redshift-e2e-") as temp:
            directory = Path(temp)
            state_file = directory / "state.json"
            state_file.write_text(json.dumps(state))
            os.environ["REDSHIFT_E2E_STATE"] = str(state_file)
            test = LiveSink(state)
            try:
                print(f"Running live Redshift {mode} tests", flush=True)
                regressions.run(cfg, state, directory)
                run_slt(state, directory)
            finally:
                test.cleanup()
    print("PASS all requested live Redshift tests", flush=True)


if __name__ == "__main__":
    try:
        main()
    except (Exception, KeyboardInterrupt) as exc:
        print(redact(str(exc)) or "Interrupted", file=sys.stderr)
        sys.exit(1)
