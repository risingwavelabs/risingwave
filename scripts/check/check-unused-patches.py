#!/usr/bin/env python3

# Cargo only warns when a `[patch]` entry stops matching the dependency graph, and the build
# silently falls back to the unpatched crate. The lockfile records such entries under
# `[[patch.unused]]`.

import sys
import tomllib
from pathlib import Path

lockfile = Path(__file__).resolve().parents[2] / "Cargo.lock"
with open(lockfile, "rb") as f:
    unused = tomllib.load(f).get("patch", {}).get("unused", [])

for entry in unused:
    source = entry.get("source", "path")
    print(f"ERROR: [patch] {entry['name']} {entry['version']} ({source}) is not used")
sys.exit(1 if unused else 0)
