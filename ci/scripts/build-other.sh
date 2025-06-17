#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

source ci/scripts/common.sh
unset RW_INSTRUMENT_COVERAGE # disable profiling for wasm udf

echo "--- Build Rust UDF"
cd e2e_test/udf/embedded_wasm
rustup target add wasm32-wasip1
cargo build --release --target wasm32-wasip1
cd ../../..

echo "--- Build Java packages"
cd java
mvn -B package -Dmaven.test.skip=true
mvn -B install -Dmaven.test.skip=true --pl java-binding-integration-test --am
mvn dependency:copy-dependencies --no-transfer-progress --pl java-binding-integration-test
cd ..

echo "--- Build Java UDF"
cd e2e_test/udf/remote_java
mvn -B package
cd ../../..

echo "--- Build rust binary for java binding integration test"
cargo build -p risingwave_java_binding --bin data-chunk-payload-generator --bin data-chunk-payload-convert-generator

echo "--- Create package for java binding integration test"
mkdir bin
mv target/debug/data-chunk-payload-convert-generator bin/data-chunk-payload-convert-generator
mv target/debug/data-chunk-payload-generator bin/data-chunk-payload-generator
tar --zstd -cf java-binding-integration-test.tar.zst bin java/java-binding-integration-test/target/dependency java/java-binding-integration-test/target/classes

echo "--- Upload built artifacts"
cp java/connector-node/assembly/target/risingwave-connector-1.0.0.tar.gz ./risingwave-connector.tar.gz
cp e2e_test/udf/remote_java/target/risingwave-udf-example.jar ./udf.jar
cp e2e_test/udf/embedded_wasm/target/wasm32-wasip1/release/udf.wasm udf.wasm
buildkite-agent artifact upload ./risingwave-connector.tar.gz
buildkite-agent artifact upload ./java-binding-integration-test.tar.zst
buildkite-agent artifact upload ./udf.jar
buildkite-agent artifact upload ./udf.wasm
