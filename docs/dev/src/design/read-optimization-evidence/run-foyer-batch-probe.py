#!/usr/bin/env python3
"""Run the public Foyer/object-store boundary probe with the workspace's locked libraries.

Requires the normal repository Rust/native build environment. No Cargo manifest or
lockfile is changed. Compiler artifacts identify exact dependency builds; this
avoids creating a second workspace with different patches or feature resolution.
"""
import json
import os
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[5]
TARGET = Path(os.environ.get("CARGO_TARGET_DIR", ROOT / "target")).resolve()
args = sys.argv[1:]
window_probe = "--window16" in args
if window_probe:
    args.remove("--window16")
source = "plug-window-probe.rs" if window_probe else "foyer-batch-probe.rs"
OUT = TARGET / ("plug-window-probe" if window_probe else "foyer-batch-probe")
OUT.mkdir(parents=True, exist_ok=True)
artifacts = OUT / "artifacts.jsonl"
with artifacts.open("w") as log:
    subprocess.run(
        ["cargo", "build", "--locked", "-p", "risingwave_object_store", "--lib", "--message-format=json"],
        cwd=ROOT, stdout=log, check=True,
    )
needed = {"risingwave_object_store", "risingwave_common", "foyer", "tokio", "bytes", "prometheus", "tempfile"}
command = [
    "rustc", "--edition=2024", "--cfg", "tokio_unstable", "-C", "debuginfo=1",
    str(Path(__file__).with_name(source)),
    "-o", str(OUT / "probe"), "-L", "dependency=" + str(TARGET / "debug/deps"),
]
for line in artifacts.read_text().splitlines():
    item = json.loads(line)
    if item.get("reason") == "compiler-artifact" and item["target"]["name"] in needed:
        libraries = [p for p in item["filenames"] if p.endswith(".rlib")]
        if libraries:
            name = item["target"]["name"]
            command += ["--extern", name + "=" + libraries[0]]
            needed.remove(name)
    if item.get("reason") == "build-script-executed":
        for path in item.get("linked_paths", []):
            command += ["-L", path]
if needed:
    raise RuntimeError("Missing library artifacts: " + str(sorted(needed)))
# macOS system Python may strip DYLD_* at startup. Record native library
# locations in the standalone probe's rpath instead of depending on inheritance.
if sys.platform == "darwin":
    for path in os.environ.get("LIBRARY_PATH", "").split(os.pathsep):
        if path:
            command += ["-C", "link-arg=-Wl,-rpath," + path]
subprocess.run(command, cwd=ROOT, check=True)
subprocess.run([str(OUT / "probe"), *args], cwd=ROOT, check=True, timeout=120 if window_probe else 60)
