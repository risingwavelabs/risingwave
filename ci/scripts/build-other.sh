#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

source ci/scripts/common.sh


echo "--- Build Java packages"
cd java
mvn -B package -Dmaven.test.skip=true
cd ..

echo "--- Upload Java artifacts"
cp java/connector-node/assembly/target/risingwave-connector-1.0.0.tar.gz ./risingwave-connector.tar.gz
cp java/udf/target/risingwave-udf-example.jar ./risingwave-udf-example.jar
buildkite-agent artifact upload ./risingwave-connector.tar.gz
buildkite-agent artifact upload ./risingwave-udf-example.jar
