#!/usr/bin/env python3
"""Sample memory and process names, excluding command arguments and secrets."""
import json
from pathlib import Path
import signal
import subprocess
import sys
import time

running = True


def stop(*_):
    global running
    running = False


signal.signal(signal.SIGTERM, stop)
builder = sys.argv[1]
group = None
with open(sys.argv[2], "w", buffering=1) as output:
    while running:
        record = {"timestamp": time.time()}
        try:
            record["meminfo"] = dict(
                line.split(":", 1)
                for line in Path("/proc/meminfo").read_text().splitlines()
                if line.split(":", 1)[0] in {
                    "MemTotal", "MemAvailable", "SwapTotal", "SwapFree", "AnonPages"
                }
            )
            processes = []
            for path in Path("/proc").glob("[0-9]*/status"):
                try:
                    fields = dict(line.split(":", 1) for line in path.read_text().splitlines() if ":" in line)
                    processes.append({
                        "pid": int(path.parent.name), "name": fields["Name"].strip(),
                        "rss_kib": int(fields.get("VmRSS", "0 kB").split()[0]),
                        "threads": int(fields.get("Threads", "0").strip()),
                    })
                except (OSError, ValueError, KeyError):
                    pass
            record["top_rss"] = sorted(processes, key=lambda item: item["rss_kib"], reverse=True)[:20]
            if group is None:
                inspect = subprocess.run(
                    ["docker", "inspect", builder, "--format", "{{.State.Pid}}"],
                    capture_output=True, text=True,
                )
                if inspect.returncode == 0 and int(inspect.stdout.strip()) > 0:
                    for line in Path(f"/proc/{int(inspect.stdout.strip())}/cgroup").read_text().splitlines():
                        if line.startswith("0::"):
                            group = Path("/sys/fs/cgroup") / line[3:].lstrip("/")
                            # BuildKit workers can be siblings of the container init scope.
                            for parent in (group, *group.parents):
                                if parent.name.startswith("docker-") and parent.name.endswith(".scope"):
                                    group = parent
                                    break
            if group is not None:
                record["buildkit_cgroup"] = {}
                for name in (
                    "memory.current", "memory.peak", "memory.max", "memory.high",
                    "memory.events", "memory.swap.current", "memory.swap.max",
                ):
                    path = group / name
                    if path.exists():
                        record["buildkit_cgroup"][name] = path.read_text().strip()
        except (OSError, ValueError) as error:
            record["sampling_error"] = type(error).__name__
        output.write(json.dumps(record) + "\n")
        time.sleep(2)
