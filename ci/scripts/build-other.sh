#!/bin/bash

# Exits as soon as any line fails.
set -euo pipefail

source ci/scripts/common.env.sh

# Should set a stable version of connector node
STABLE_VERSION=7d454801e478e86c50a1e94cc139842554a0470d

echo "--- Build Java connector node"
git clone https://"$GITHUB_TOKEN"@github.com/risingwavelabs/risingwave-connector-node.git
cd risingwave-connector-node
# checkout a stable version
git checkout $STABLE_VERSION
mvn package -Dmaven.test.skip=true
echo "--- Upload Java artifacts"
cp service/target/service-*.jar ./connector-service.jar
buildkite-agent artifact upload ./connector-service.jar
