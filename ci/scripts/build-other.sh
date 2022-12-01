#!/bin/bash

# Exits as soon as any line fails.
set -euo pipefail

source ci/scripts/common.env.sh

STABLE_VERSION=

echo "--- Build Java connector node"
git clone --depth 1 https://"$GITHUB_TOKEN"@github.com/risingwavelabs/risingwave-connector-node.git
cd risingwave-connector-node
# checkout a stable version
git checkout $STABLE_VERSION
mvn package -Dmaven.test.skip=true
echo "--- Upload Java artifacts"
cp service/target/service-*.jar ./connector-service.jar
buildkite-agent artifact upload ./connector-service.jar
