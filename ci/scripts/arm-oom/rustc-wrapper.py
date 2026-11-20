#!/usr/bin/python3.12
"""Measure final binary rustc while preserving inherited jobserver FDs."""
import json
import os
import sys
import time

args = sys.argv[1:]
is_final = (
    "--crate-name" in args
    and args[args.index("--crate-name") + 1] == "risingwave"
    and "--crate-type" in args
    and args[args.index("--crate-type") + 1] == "bin"
)
if not is_final:
    os.execv(args[0], args)
start = time.monotonic()
print("OOM_METRIC final_rustc_start", flush=True, file=sys.stderr)
pid = os.fork()
if pid == 0:
    os.execv(args[0], args)
_, status, usage = os.wait4(pid, 0)
exit_code = os.waitstatus_to_exitcode(status)
print("OOM_METRIC " + json.dumps({
    "kind": "final_rustc", "elapsed_seconds": time.monotonic() - start,
    "max_rss_kib": usage.ru_maxrss, "user_seconds": usage.ru_utime,
    "system_seconds": usage.ru_stime, "exit_code": exit_code,
}), flush=True, file=sys.stderr)
sys.exit(exit_code if exit_code >= 0 else 128 - exit_code)
