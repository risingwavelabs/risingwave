#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

source ci/scripts/common.env.sh

# Should set a stable version of connector node
STABLE_VERSION=4380fc207d2a76defdcac38754a61606a2e8f83f

echo "--- Build Java connector node"
git clone https://"$GITHUB_TOKEN"@github.com/risingwavelabs/risingwave-connector-node.git
cd risingwave-connector-node
# checkout a stable version
git checkout $STABLE_VERSION
mvn -B package -Dmaven.test.skip=true
echo "--- Upload Java artifacts"
cp assembly/target/risingwave-connector-1.0.0.tar.gz ./risingwave-connector.tar.gz
buildkite-agent artifact upload ./risingwave-connector.tar.gz
