#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

source ci/scripts/common.env.sh


echo "--- Build Java connector node"
cd connector_node

mvn -B package -Dmaven.test.skip=true
echo "--- Upload Java artifacts"
cp assembly/target/risingwave-connector-1.0.0.tar.gz ./risingwave-connector.tar.gz
buildkite-agent artifact upload ./risingwave-connector.tar.gz
