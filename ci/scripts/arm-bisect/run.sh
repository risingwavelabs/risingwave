#!/usr/bin/env bash
set -euo pipefail

builder="arm-bisect-${BUILDKITE_BUILD_NUMBER}"
results=".context/arm-bisect"
mkdir -p "$results"

cleanup() {
  buildkite-agent artifact upload "$results/*" || true
  docker buildx rm "$builder" || true
}
trap cleanup EXIT

echo "--- Verify original hardware and source controls"
test "$(uname -m)" = aarch64
test "$(nproc)" = 32
mem_kib=$(awk '/MemTotal:/ {print $2}' /proc/meminfo)
if (( mem_kib < 60000000 || mem_kib > 70000000 )); then
  echo "Expected the original 64 GiB memory class"
  exit 1
fi
free -b
git submodule update --init java/debezium
printf '%s' "$GHCR_TOKEN" | docker login ghcr.io -u "$GHCR_USERNAME" --password-stdin
docker buildx create --name "$builder" --driver docker-container \
  --driver-opt image=moby/buildkit@sha256:92f93f25966c544549b88144406ce8cd9dcdf5df128250711031a4acd9149a8e
docker buildx inspect --builder "$builder" --bootstrap | tee "$results/builder.txt"
python3 ci/scripts/arm-bisect/bisect.py "$builder" "$results"
