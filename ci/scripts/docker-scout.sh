#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

ghcraddr="ghcr.io/risingwavelabs/risingwave"
arch="$(uname -m)"

echo "--- ghcr login"
echo "$GHCR_TOKEN" | docker login ghcr.io -u "$GHCR_USERNAME" --password-stdin

echo "--- pull docker image ---"
echo "pulling ${ghcraddr}:${BUILDKITE_COMMIT}-${arch}"
docker pull "${ghcraddr}:${BUILDKITE_COMMIT}-${arch}"

echo "--- check vulnerabilities ---"
mkdir -p /scout
docker scout quickview "${ghcraddr}:${BUILDKITE_COMMIT}-${arch}" | tee /scout/scout-quickview.txt
docker scout cves "${ghcraddr}:${BUILDKITE_COMMIT}-${arch}" | tee /scout/scout-cves.txt
docker scout recommendations "${ghcraddr}:${BUILDKITE_COMMIT}-${arch}" | tee /scout/scout-recommendations.txt

export SCOUT_REPORT=$(echo -e "Scout Quickview\n\n" && cat /scout/scout-quickview.txt && echo -e "\n\nScout CVEs\n\n" && cat /scout/scout-cves.txt && echo -e "\n\nScout Recommendations\n\n" && cat /scout/scout-recommendations.txt)