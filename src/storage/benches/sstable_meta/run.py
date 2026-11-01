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

"""Build and measure the actual main/serde_bytes SST metadata implementations."""
import argparse
import csv
import datetime
import hashlib
import json
import os
from pathlib import Path
import shutil
import statistics
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[4]
HERE = Path(__file__).resolve().parent
BASE = "a5a57ad283851a2d177f398f263c47cc00dbb903"
FIX = "5cf5dfde307fbb4f423e380744f842a416d24324"
SOURCE = "src/storage/src/hummock/sstable/mod.rs"
BENCH = "src/storage/benches/bench_sstable_meta.rs"
VARIANTS = {"main": BASE, "serde_bytes": FIX}
SAMPLES = ("short_1100k", "short_1300k", "long_160k")


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def utc():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def git_bytes(revision, path):
    return subprocess.check_output(["git", "show", f"{revision}:{path}"], cwd=ROOT)


def environment():
    env = os.environ.copy()
    env.pop("RW_META_BENCH_ALLOC", None)
    env.update(RUSTC_WRAPPER="", CARGO_BUILD_JOBS="4", CARGO_PROFILE_RELEASE_DEBUG="0",
               CARGO_PROFILE_RELEASE_INCREMENTAL="false")
    return env


def build(out):
    original = (ROOT / SOURCE).read_bytes()
    if original != git_bytes(FIX, SOURCE):
        raise RuntimeError("Refusing to overwrite production source differing from the reviewed PR")
    harness_sha = sha(ROOT / BENCH)
    record = {"start_utc": utc(), "base": BASE, "fix": FIX, "bench_sha256": harness_sha,
              "runner_sha256": sha(Path(__file__)), "builds": []}
    (out / "harness.rs").write_bytes((ROOT / BENCH).read_bytes())
    (out / "runner.py").write_bytes(Path(__file__).read_bytes())
    (out / "Cargo.lock").write_bytes((ROOT / "Cargo.lock").read_bytes())
    record["fixed_source_sha256"] = {path: sha(ROOT / path) for path in (
        "src/storage/src/hummock/sstable/xor_filter.rs", "src/storage/Cargo.toml", ".cargo/config.toml", "rust-toolchain")}
    (out / "compiler.txt").write_bytes(subprocess.check_output(["rustc", "+nightly-2026-06-21", "-Vv"]))
    command = ["cargo", "+nightly-2026-06-21", "bench", "-p", "risingwave_storage", "--bench",
               "bench_sstable_meta", "--features", "test", "--profile", "release", "--no-run",
               "--locked", "--message-format=json"]
    try:
        # Build both modes before measurement; option_env changes only the bench crate.
        for variant in ("serde_bytes", "main"):
            (ROOT / SOURCE).write_bytes(git_bytes(VARIANTS[variant], SOURCE))
            for mode in ("timing", "allocation"):
                if sha(ROOT / BENCH) != harness_sha:
                    raise RuntimeError("Harness changed during compilation")
                env = environment()
                if mode == "allocation":
                    env["RW_META_BENCH_ALLOC"] = "1"
                prefix = out / f"{variant}.{mode}"
                started = time.monotonic()
                with prefix.with_suffix(f".{mode}.build.jsonl").open("w") as data, prefix.with_suffix(f".{mode}.build.log").open("w") as log:
                    subprocess.run(command, cwd=ROOT, env=env, stdout=data, stderr=log, check=True, timeout=3600)
                artifacts = [json.loads(line) for line in prefix.with_suffix(f".{mode}.build.jsonl").read_text().splitlines()]
                binaries = [row["executable"] for row in artifacts if row.get("reason") == "compiler-artifact"
                            and row.get("executable") and row["target"]["name"] == "bench_sstable_meta"]
                if len(binaries) != 1:
                    raise RuntimeError(f"Unexpected bench artifacts: {binaries}")
                binary = out / f"{variant}.{mode}.bin"
                shutil.copy2(binaries[0], binary)
                symbols = subprocess.check_output(["nm", "-j", str(binary)], text=True)
                tls_symbols = [line for line in symbols.splitlines() if "ALLOCATIONS" in line]
                if bool(tls_symbols) != (mode == "allocation"):
                    raise RuntimeError("Allocation counter TLS was not compiled out of timing mode")
                record["builds"].append({"variant": variant, "mode": mode, "revision": VARIANTS[variant],
                    "seconds": time.monotonic() - started, "binary_sha256": sha(binary),
                    "source_sha256": sha(ROOT / SOURCE), "counter_tls_symbols": tls_symbols,
                    "command": command, "environment_overrides": {key: env[key] for key in (
                        "RUSTC_WRAPPER", "CARGO_BUILD_JOBS", "CARGO_PROFILE_RELEASE_DEBUG",
                        "CARGO_PROFILE_RELEASE_INCREMENTAL")}, "inherited_configuration": {key: env.get(key) for key in (
                        "RUSTFLAGS", "CARGO_ENCODED_RUSTFLAGS", "CARGO_PROFILE_RELEASE_OPT_LEVEL", "CARGO_PROFILE_RELEASE_LTO",
                        "CARGO_PROFILE_RELEASE_CODEGEN_UNITS")}, "allocation_counter_compiled": mode == "allocation"})
                (out / "build.json").write_text(json.dumps(record, indent=2) + "\n")
    finally:
        (ROOT / SOURCE).write_bytes(original)
        record.update(end_utc=utc(), restored=(ROOT / SOURCE).read_bytes() == original,
                      cargo_lock_sha256=sha(ROOT / "Cargo.lock"))
        (out / "build.json").write_text(json.dumps(record, indent=2) + "\n")


def execute(out, variant, mode, env, name):
    binary = out / f"{variant}.{mode}.bin"
    data_path, err_path = out / f"{name}.csv", out / f"{name}.stderr.log"
    started = time.monotonic()
    start_utc = utc()
    with data_path.open("w") as data, err_path.open("w") as err:
        process = subprocess.Popen([str(binary)], cwd=ROOT, env=env, stdout=data, stderr=err)
        try:
            _, status, usage = os.wait4(process.pid, 0)
        except BaseException:
            process.terminate()
            os.wait4(process.pid, 0)
            raise
        process.returncode = os.waitstatus_to_exitcode(status)
    if process.returncode:
        raise RuntimeError(f"{name} failed ({process.returncode}); see {err_path}")
    return {"name": name, "variant": variant, "mode": mode, "start_utc": start_utc, "end_utc": utc(),
            "seconds": time.monotonic() - started, "binary_sha256": sha(binary),
            "process_user_cpu_s": usage.ru_utime, "process_system_cpu_s": usage.ru_stime,
            "max_rss_bytes": usage.ru_maxrss * (1 if sys.platform == "darwin" else 1024),
            "minor_faults": usage.ru_minflt, "major_faults": usage.ru_majflt,
            "voluntary_switches": usage.ru_nvcsw, "involuntary_switches": usage.ru_nivcsw,
            "cleanup": "process completed and wait4 reaped it"}


def measure(out, fixtures, rounds, iterations, check_only):
    build_record = json.loads((out / "build.json").read_text())
    if sha(ROOT / BENCH) != build_record["bench_sha256"]:
        raise RuntimeError("Measurement harness differs from compiled source")
    env = environment()
    env.update(RW_META_BENCH_FIXTURE_DIR=str(fixtures), RW_META_BENCH_ITERATIONS=str(iterations),
               RW_META_BENCH_ROUNDS="1")
    records = []
    for variant in VARIANTS:
        for mode in ("timing", "allocation"):
            expected = next(row["binary_sha256"] for row in build_record["builds"] if row["variant"] == variant and row["mode"] == mode)
            if sha(out / f"{variant}.{mode}.bin") != expected:
                raise RuntimeError("Compiled binary fingerprint changed")
            check_env = dict(env, RW_META_BENCH_CHECK_ONLY="1")
            records.append(execute(out, variant, mode, check_env, f"{variant}.{mode}.preflight"))
    if check_only:
        (out / "preflight.json").write_text(json.dumps(records, indent=2) + "\n")
        return
    fixture_sha = {p.name: sha(p) for p in fixtures.iterdir() if p.is_file()}
    all_rows = []
    for mode in ("timing", "allocation"):
        for round_id in range(rounds):
            # Alternate AB/BA to reduce monotonic host/temperature drift.
            variants = list(VARIANTS) if round_id % 2 == 0 else list(reversed(VARIANTS))
            for variant in variants:
                name = f"{variant}.{mode}.round{round_id}"
                run_env = dict(env, RW_META_BENCH_FIRST_ROUND=str(round_id), RW_META_BENCH_VARIANT=variant)
                item = execute(out, variant, mode, run_env, name)
                rows = list(csv.DictReader((out / f"{name}.csv").open()))
                if len(rows) != 3 or {row["fixture"] for row in rows} != set(SAMPLES):
                    raise RuntimeError("Incomplete fixture result")
                if any(row["pass"] != mode or int(row["n"]) != iterations for row in rows):
                    raise RuntimeError("Measurement mode or loop count mismatch")
                all_rows.extend(rows)
                records.append(item)
                text = json.dumps({**item, "base": BASE, "fix": FIX, "bench_sha256": build_record["bench_sha256"],
                    "fixture_sha256": fixture_sha, "workload": f"8 warmups, {iterations} decodes per fixture; {rounds} AB/BA batches",
                    "limits": "System allocator, Rust requests only, warm CPU decode; not production EBS or whole-process RSS"}, indent=2)
                table = "\n| Fixture | Wall ns | Thread CPU ns | Alloc calls | Requested B | Peak live B |\n|---|---:|---:|---:|---:|---:|\n"
                for row in rows:
                    table += "| " + " | ".join(row[key] or "-" for key in ("fixture", "wall_ns", "thread_cpu_ns", "alloc_calls", "requested_bytes", "peak_live_bytes")) + " |\n"
                (out / f"{name}.record.txt").write_text(text + "\n" + table)
                with (out / "raw.csv").open("w") as data:
                    writer = csv.DictWriter(data, fieldnames=list(rows[0]))
                    writer.writeheader()
                    writer.writerows(all_rows)
                (out / "measure.json").write_text(json.dumps({"configuration": dict(rounds=rounds, iterations=iterations), "fixture_sha256": fixture_sha, "runs": records}, indent=2) + "\n")
    if fixture_sha != {p.name: sha(p) for p in fixtures.iterdir() if p.is_file()}:
        raise RuntimeError("Fixture changed during measurement")
    lines = ["# Main versus serde_bytes: real SST entry CPU/resources", "", "Each value is the median of batch means; timings exclude output drop.", "",
             "| Fixture | Variant | Wall ms | Thread CPU ms | Alloc calls | Reallocs | Requested MiB | Peak live MiB | Returned live MiB |", "|---|---|---:|---:|---:|---:|---:|---:|---:|"]
    for fixture in SAMPLES:
        for variant in VARIANTS:
            row = []
            for key in ("wall_ns", "thread_cpu_ns", "alloc_calls", "realloc_calls", "requested_bytes", "peak_live_bytes", "returned_live_bytes"):
                values = [float(r[key]) for r in all_rows if r["fixture"] == fixture and r["variant"] == variant and r[key]]
                value = statistics.median(values)
                row.append(value / (1e6 if key.endswith("_ns") else 2**20 if key.endswith("bytes") else 1))
            lines.append(f"| {fixture} | {variant} | " + " | ".join(f"{value:.4f}" for value in row) + " |")
    (out / "summary.md").write_text("\n".join(lines) + "\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=ROOT / "target/sstable-meta-main-ab")
    parser.add_argument("--fixture-dir", type=Path)
    parser.add_argument("--build", action="store_true")
    parser.add_argument("--measure", action="store_true")
    parser.add_argument("--check-only", action="store_true")
    parser.add_argument("--rounds", type=int, default=7)
    parser.add_argument("--iterations", type=int, default=64)
    args = parser.parse_args()
    if args.measure and not args.check_only and (args.rounds < 7 or args.iterations < 1):
        parser.error("Formal measurement requires at least seven batches and positive iterations")
    out = args.output.resolve()
    out.mkdir(parents=True, exist_ok=True)
    fixtures = (args.fixture_dir or out / "fixtures").resolve()
    fixtures.mkdir(parents=True, exist_ok=True)
    if not args.build and not args.measure:
        parser.error("Select --build and/or --measure (keep compilation outside timing windows)")
    if args.build:
        build(out)
    if args.measure:
        measure(out, fixtures, args.rounds, args.iterations, args.check_only)


if __name__ == "__main__":
    main()
