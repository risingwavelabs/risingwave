#!/usr/bin/python3.12

import csv
import os
from pathlib import Path
import subprocess
import sys
import time


def cpu_sample(stat_file, ticks_per_second):
    raw = stat_file.read_text()
    name, _, rest = raw.partition(" (")[2].rpartition(") ")
    fields = rest.split()
    cpu_seconds = (int(fields[11]) + int(fields[12])) / ticks_per_second
    return name, cpu_seconds


def main():
    compiler = sys.argv[1:]
    if os.environ.get("CARGO_BIN_NAME") != "risingwave":
        os.execvp(compiler[0], compiler)

    output = Path("/risingwave/bin/thinlto-profile")
    output.mkdir(parents=True, exist_ok=True)
    limits = dict(available_cpus=len(os.sched_getaffinity(0)))
    for name in ("cpu.max", "memory.max"):
        path = Path("/sys/fs/cgroup") / name
        if path.is_file():
            limits[name] = path.read_text().strip()
    (output / "thinlto-limits.txt").write_text(
        "\n".join(f"{name}={value}" for name, value in limits.items()) + "\n"
    )
    args = compiler + [
        f"-Zself-profile={output}",
        "-Zself-profile-events=default,args",
        "-Ztime-passes",
    ]
    ticks = os.sysconf("SC_CLK_TCK")
    start = time.monotonic()
    # Keep Cargo's inherited jobserver descriptors open for rustc.
    with subprocess.Popen(args, close_fds=False) as process, (
        output / "thinlto-process.csv"
    ).open("w", newline="") as process_file, (
        output / "thinlto-threads.csv"
    ).open("w", newline="") as threads_file:
        proc = Path("/proc") / str(process.pid)
        processes = csv.writer(process_file)
        threads = csv.writer(threads_file)
        processes.writerow(("elapsed_s", "cpu_s", "rss_kib", "peak_rss_kib"))
        threads.writerow(("elapsed_s", "tid", "name", "cpu_s"))
        while process.poll() is None:
            elapsed = round(time.monotonic() - start, 3)
            try:
                _, cpu = cpu_sample(proc / "stat", ticks)
                memory = {
                    key: int(value.split()[0])
                    for line in (proc / "status").read_text().splitlines()
                    for key, _, value in (line.partition(":"),)
                    if key in ("VmRSS", "VmHWM")
                }
                processes.writerow((elapsed, cpu, memory.get("VmRSS"), memory.get("VmHWM")))
            except (FileNotFoundError, ProcessLookupError):
                break
            for thread in (proc / "task").glob("[0-9]*"):
                try:
                    name, cpu = cpu_sample(thread / "stat", ticks)
                except (FileNotFoundError, ProcessLookupError):
                    continue
                threads.writerow((elapsed, thread.name, name, cpu))
            try:
                process.wait(timeout=1)
            except subprocess.TimeoutExpired:
                pass
        return process.wait()


if __name__ == "__main__":
    sys.exit(main())
