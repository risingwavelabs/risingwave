#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

ghcraddr="ghcr.io/risingwavelabs/risingwave"
arch="$(uname -m)"
image="${ghcraddr}:${BUILDKITE_COMMIT}-${arch}"

echo "--- ghcr login"
echo "$GHCR_TOKEN" | docker login ghcr.io -u "$GHCR_USERNAME" --password-stdin

echo "--- dockerhub login"
echo "$DOCKER_TOKEN" | docker login -u "risingwavelabs" --password-stdin

echo "--- pull docker image"
echo "pulling ${image}"
docker pull "${image}"

echo "--- check vulnerabilities"
function docker-scout {
    docker run -it -e DOCKER_SCOUT_HUB_USER=risingwavelabs -e DOCKER_SCOUT_HUB_PASSWORD=$DOCKER_TOKEN -u root -v /var/run/docker.sock:/var/run/docker.sock docker/scout-cli "$@"
}
docker-scout quickview ${image}
docker-scout recommendations "${image}"
docker-scout cves --format sarif -o scout.sarif "${image}"

export SCOUT_REPORT=$(cat scout.sarif)
echo "--- scout report"
cat scout.sarif
