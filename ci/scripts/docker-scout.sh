#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

ghcraddr="ghcr.io/risingwavelabs/risingwave"
arch="$(uname -m)"

echo "--- ghcr login"
echo "$GHCR_TOKEN" | docker login ghcr.io -u "$GHCR_USERNAME" --password-stdin

echo "--- dockerhub login"
echo "$DOCKER_TOKEN" | docker login -u "risingwavelabs" --password-stdin

echo "--- pull docker image ---"
echo "pulling ${ghcraddr}:${BUILDKITE_COMMIT}-${arch}"
docker pull "${ghcraddr}:${BUILDKITE_COMMIT}-${arch}"

echo "--- check vulnerabilities ---"
function docker-scout {
    docker-scout="docker run -it -e DOCKER_SCOUT_HUB_USER=risingwavelabs -e DOCKER_SCOUT_HUB_PASSWORD=$DOCKER_TOKEN docker/scout-cli"
}
docker-scout quickview "${ghcraddr}:${BUILDKITE_COMMIT}-${arch}" | tee scout-quickview.txt
docker-scout cves "${ghcraddr}:${BUILDKITE_COMMIT}-${arch}" | tee scout-cves.txt
docker-scout recommendations "${ghcraddr}:${BUILDKITE_COMMIT}-${arch}" | tee scout-recommendations.txt

export SCOUT_REPORT=$(echo -e "Scout Quickview\n\n" && cat scout-quickview.txt && echo -e "\n\nScout CVEs\n\n" && cat scout-cves.txt && echo -e "\n\nScout Recommendations\n\n" && cat scout-recommendations.txt)