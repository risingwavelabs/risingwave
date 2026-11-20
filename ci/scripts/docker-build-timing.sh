#!/usr/bin/env bash

set -euo pipefail

[[ $(uname -m) == x86_64 ]]
candidate=$(git rev-parse HEAD^)
echo "Measuring $candidate with the production Cargo profile"
lscpu | grep -E '^(Architecture|CPU\(s\)|Model name):'
grep '^MemTotal:' /proc/meminfo

builder="rw-timing-${BUILDKITE_JOB_ID}"
image="rw-timing:${BUILDKITE_JOB_ID}"
registry_id=
smoke_id=
work_dir=$(mktemp -d)
cleanup() {
  if [[ -n "$smoke_id" ]]; then
    docker rm -f "$smoke_id" >/dev/null || true
  fi
  docker image rm "$image" >/dev/null 2>&1 || true
  docker buildx rm "$builder" >/dev/null 2>&1 || true
  if [[ -n "$registry_id" ]]; then
    docker rm -fv "$registry_id" >/dev/null || true
  fi
  rm -rf -- "$work_dir"
}
trap cleanup EXIT

echo "$GHCR_TOKEN" | docker login ghcr.io -u "$GHCR_USERNAME" --password-stdin
echo "$DOCKER_TOKEN" | docker login -u risingwavelabs --password-stdin
git submodule update --init java/debezium

registry_id=$(docker run -d -p 127.0.0.1::5000 registry:2)
registry=$(docker port "$registry_id" 5000/tcp)
curl --fail --silent --show-error --retry 10 --retry-all-errors "http://$registry/v2/"
printf '[registry."%s"]\n  http = true\n' "$registry" > "$work_dir/buildkitd.toml"
docker buildx create --name "$builder" --driver docker-container --driver-opt network=host \
  --buildkitd-config "$work_dir/buildkitd.toml"

build_args=(
  -f docker/Dockerfile --builder "$builder" --progress plain
  --build-arg "GIT_SHA=$candidate" --build-arg CARGO_PROFILE=production
)

echo "--- Cold build and load the smoke image"
time docker buildx build "${build_args[@]}" --no-cache \
  --target risingwave-nodwp --load -t "$image" .

echo "--- Start the image and check the Dashboard"
smoke_id=$(docker run -d --network none "$image" playground)
sleep 10
if [[ $(docker inspect --format='{{.State.Status}}' "$smoke_id") != running ]]; then
  docker logs "$smoke_id"
  exit 1
fi
docker exec "$smoke_id" curl --fail --silent --show-error --retry 20 --retry-all-errors \
  http://127.0.0.1:5691/ -o /tmp/dashboard-index.html
docker exec "$smoke_id" test -s /tmp/dashboard-index.html
docker exec "$smoke_id" curl --fail --silent --show-error \
  http://127.0.0.1:5691/risingwave.svg -o /tmp/dashboard-logo.svg
docker exec "$smoke_id" test -s /tmp/dashboard-logo.svg
docker exec "$smoke_id" test ! -e /risingwave/bin/risingwave.dwp
echo "Dashboard homepage and static assets served offline"

echo "--- Reuse the builder and push the DWP image to the loopback registry"
time docker buildx build "${build_args[@]}" --target risingwave --provenance=false --push \
  -t "$registry/first/risingwave:$candidate" -t "$registry/second/risingwave:$candidate" \
  --cache-to "type=registry,ref=$registry/risingwave-build-cache:x86_64" .

accept='Accept: application/vnd.docker.distribution.manifest.v2+json'
first=$(curl --fail --silent --show-error -H "$accept" "http://$registry/v2/first/risingwave/manifests/$candidate")
second=$(curl --fail --silent --show-error -H "$accept" "http://$registry/v2/second/risingwave/manifests/$candidate")
[[ "$first" == "$second" ]]
echo "Both test tags have the same image manifest"
