#!/bin/bash

# Exits as soon as any line fails.
set -euo pipefail

source ci/scripts/common.env.sh

echo "--- Generate RiseDev CI config"
cp ci/risedev-components.ci.benchmark.env risedev-components.user.env

echo "--- Prepare RiseDev dev cluster"
cargo make pre-start-dev
cargo make link-all-in-one-binaries

echo "--- e2e test w/ Rust frontend - CH-benCHmark"
apt update
apt install -y kafkacat
cargo make clean-data
cargo make ci-start ci-kafka
./scripts/prepare_ci_kafka.sh tpcc
./risedev slt -p 4566 -d dev ./e2e_test/ch-benchmark/ch_benchmark.slt
