#!/usr/bin/env bash
set -euo pipefail

builder="arm-oom-${BUILDKITE_BUILD_NUMBER}"
container="buildx_buildkit_${builder}0"
results=".context/arm-oom"
mkdir -p "$results"
monitor_pid=""

cleanup() {
  if [[ -n "$monitor_pid" ]]; then
    kill "$monitor_pid" 2>/dev/null || true
    wait "$monitor_pid" 2>/dev/null || true
  fi
  dmesg | grep -E 'invoked oom-killer|oom-kill:|Out of memory:|oom_reaper:' > "$results/kernel-oom.log" || true
  docker inspect "$container" --format 'Memory={{.HostConfig.Memory}} MemorySwap={{.HostConfig.MemorySwap}} NanoCpus={{.HostConfig.NanoCpus}} CpusetCpus={{.HostConfig.CpusetCpus}}' > "$results/buildkit-limits.txt" || true
  gzip -f "$results"/*.jsonl 2>/dev/null || true
  buildkite-agent artifact upload "$results/*" || true
  docker buildx rm "$builder" || true
}
trap cleanup EXIT

echo "--- Original-specification machine inventory"
uname -m
nproc
free -b
if [[ "$(uname -m)" != aarch64 ]]; then
  echo "Expected native ARM64"
  exit 1
fi
mem_kib=$(awk '/MemTotal:/ {print $2}' /proc/meminfo)
if (( mem_kib < 60000000 || mem_kib > 70000000 )); then
  echo "Expected the original 64 GiB memory class; stopping experiment"
  exit 1
fi
docker ps --format '{{.ID}} {{.Names}}'
git submodule update --init java/debezium
printf '%s' "$GHCR_TOKEN" | docker login ghcr.io -u "$GHCR_USERNAME" --password-stdin
docker buildx create --name "$builder" --driver docker-container
docker buildx inspect --builder "$builder" --bootstrap
python3 ci/scripts/arm-oom/monitor.py "$container" "$results/memory.jsonl" &
monitor_pid=$!

# Keep the original multi-stage build and read the shared cache without writing it.
python3 - "$results/Dockerfile" <<'PY'
from pathlib import Path
import sys

text = Path('docker/Dockerfile').read_text()
needle = 'RUN cargo fetch && \\\n  cargo build -p risingwave_cmd_all'
assert text.count(needle) == 1
text = text.replace(
    needle,
    'ARG OOM_JOBS=8\nARG OOM_LTO=thin\n'
    'ENV CARGO_BUILD_JOBS=${OOM_JOBS}\n'
    'ENV CARGO_PROFILE_PRODUCTION_LTO=${OOM_LTO}\n'
    'ENV RUSTC_WRAPPER=/risingwave/ci/scripts/arm-oom/rustc-wrapper.py\n\n'
    + needle,
)
Path(sys.argv[1]).write_text(text)
PY

run_variant() {
  local variant="$1" lto="$2"
  echo "--- Experiment: $variant; production jobs=8 lto=$lto"
  local started status
  started=$(date +%s)
  set +e
  docker buildx build -f "$results/Dockerfile" \
    --build-arg "GIT_SHA=c8e74c2b12f4a3056eedc380606a305453f0a89b" \
    --build-arg CARGO_PROFILE=production \
    --build-arg OOM_JOBS=8 \
    --build-arg "OOM_LTO=$lto" \
    --progress plain --builder "$builder" \
    --cache-from type=registry,ref=ghcr.io/risingwavelabs/risingwave-build-cache:aarch64 \
    . 2>&1 | tee "$results/$variant.log"
  status=${PIPESTATUS[0]}
  set -e
  printf '%s\tstatus=%s\telapsed_seconds=%s\n' "$variant" "$status" "$(( $(date +%s) - started ))" | tee -a "$results/results.tsv"
  return "$status"
}

if run_variant production-jobs8 thin; then
  echo "production with jobs=8 passed; retain LTO and stop comparisons"
elif grep -qE 'ResourceExhausted|cannot allocate memory|signal: 9, SIGKILL|OOM_METRIC.*"exit_code": -9' "$results/production-jobs8.log"; then
  echo "jobs=8 still exhausted memory; compare with only LTO disabled"
  run_variant production-jobs8-no-lto off
else
  echo "Failed without OOM evidence; preserve measurements and do not change LTO"
  exit 1
fi
