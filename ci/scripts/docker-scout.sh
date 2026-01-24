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
mkdir -p scout
function docker-scout {
    docker run -it -e DOCKER_SCOUT_HUB_USER=risingwavelabs -e DOCKER_SCOUT_HUB_PASSWORD=$DOCKER_TOKEN -u root -v /var/run/docker.sock:/var/run/docker.sock -v $PWD/scout:/scout docker/scout-cli "$@"
}

echo "--- scout quickview"
docker-scout quickview ${image} -o /scout/quickview.output
cat scout/quickview.output
read C H M L <<< $(grep 'Target'  scout/quickview.output | awk -F'[ │ ]+' '{print $4, $5, $6, $7}' | sed 's/[CHML]//g')
cat >> scout/report.output << EOF
Docker Scout Report:
            - Critical: $C
            - High: $H
            - Medium: $M
            - Low: $L
EOF

buildkite-agent meta-data set "SCOUT_REPORT" "$(cat scout/report.output)"

echo "--- scout recommendations"
docker-scout recommendations "${image}"

echo "--- scout cves"
docker-scout cves "${image}"
