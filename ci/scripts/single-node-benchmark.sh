#!/bin/bash

# Exits as soon as any line fails.
set -euo pipefail

echo "--- Generate RiseDev CI config"
cp ci/risedev-components.ci.benchmark.env risedev-components.user.env

echo "--- Download necessary tools"
apt-get -y install golang-go librdkafka-dev
cargo make pre-start-benchmark

echo "--- Start a full risingwave cluster"
./risedev clean-data && ./risedev d full-benchmark

echo "--- Cluter label:"
cat .risingwave/config/prometheus.yml |grep rw_cluster

echo "--- Clone tpch-bench repo"
git clone https://"$GITHUB_TOKEN"@github.com/singularity-data/tpch-bench.git

echo "--- Run tpc-h bench"
cd tpch-bench/
./scripts/build.sh
./scripts/launch.sh