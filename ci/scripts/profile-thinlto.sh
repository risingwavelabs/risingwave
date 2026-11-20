#!/usr/bin/env bash

set -euo pipefail

[[ $(uname -m) == x86_64 ]]
candidate=$(git rev-parse HEAD^)
echo "Profiling the production ThinLTO phase for $candidate"
lscpu
grep '^MemTotal:' /proc/meminfo

builder="rw-thinlto-${BUILDKITE_JOB_ID}"
output=$(mktemp -d)
cleanup() {
  docker buildx rm "$builder" >/dev/null 2>&1 || true
  rm -rf -- "$output"
}
trap cleanup EXIT

echo "$DOCKER_TOKEN" | docker login -u risingwavelabs --password-stdin
git submodule update --init java/debezium
docker buildx create --name "$builder" --driver docker-container

time docker buildx build --builder "$builder" --no-cache --progress plain \
  -f docker/Dockerfile --target thinlto-profile \
  --build-arg CARGO_PROFILE=production --build-arg "GIT_SHA=$candidate" \
  --output "type=local,dest=$output" .

(
  cd "$output"
  shopt -s failglob
  test -s thinlto-process.csv
  test -s thinlto-threads.csv
  profiles=(*.mm_profdata)
  test -s "${profiles[0]}"
  buildkite-agent artifact upload 'thinlto-*;*.mm_profdata'
)
