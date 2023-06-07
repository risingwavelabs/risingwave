#!/usr/bin/env bash

# Running it locally:
# RW_COMMIT=$(git rev-parse HEAD) ./ci/scripts/backwards-compat-test.sh

set -euo pipefail

source ci/scripts/common.sh

# TODO(kwannoel): automatically derive this by:
# 1. Fetching major version.
# 2. Find the earliest minor version of that major version.
TAG=v0.18.0
# Duration to wait for recovery (seconds)
RECOVERY_DURATION=20


run_sql () {
    psql -h localhost -p 4566 -d dev -U root -c "$@"
}

assert_not_empty() {
  set +e
  if [[ $(wc -l < "$1" | sed 's/^ *//g') -gt 1 ]]; then
    echo "assert_not_empty PASSED for $1"
  else
    echo "assert_not_empty FAILED for $1"
    buildkite-agent artifact upload "$1"
    exit 1
  fi
  set -e
}

assert_eq() {
  set +e
  if [[ -z $(diff "$1" "$2") ]]; then
    echo "assert_eq PASSED for $1 and $2"
  else
    echo "FAILED"
    buildkite-agent artifact upload "$1"
    buildkite-agent artifact upload "$2"
    exit 1
  fi
  set -e
}

seed_table() {
  for i in $(seq 1 10000)
  do
    run_sql "INSERT into t values ($i);" 1>/dev/null 2>&1
  done
  run_sql "flush;"
}

configure_rw() {
echo "--- Setting up cluster config"
cat <<EOF > risedev-profiles.user.yml
full-without-monitoring:
  steps:
    - use: minio
    - use: etcd
    - use: meta-node
    - use: compute-node
    - use: frontend
    - use: compactor
EOF

cat <<EOF > risedev-components.user.env
RISEDEV_CONFIGURED=true

ENABLE_MINIO=true
ENABLE_ETCD=true
ENABLE_KAFKA=true

# Fetch `risingwave` binary from release.
ENABLE_BUILD_RUST=false

# Ensure it will link the all-in-one binary from our release.
ENABLE_ALL_IN_ONE=true

# ENABLE_RELEASE_PROFILE=true
EOF
}

configure_latest_rw() {
cat <<EOF > risedev-profiles.user.yml
full-without-monitoring:
  steps:
    - use: minio
    - use: etcd
    - use: meta-node
    - use: compute-node
    - use: frontend
    - use: compactor
EOF
}

echo "--- Configuring RW"
configure_rw

echo "--- Build risedev for $TAG, it may not be backwards compatible"
git config --global --add safe.directory /risingwave
git checkout "${TAG}-rc"
cargo build -p risedev

echo "--- Setup old release $TAG"
wget "https://github.com/risingwavelabs/risingwave/releases/download/${TAG}/risingwave-${TAG}-x86_64-unknown-linux.tar.gz"
tar -xvf risingwave-${TAG}-x86_64-unknown-linux.tar.gz
mkdir -p target/debug
cp risingwave target/debug/risingwave

echo "--- Teardown any old cluster"
set +e
./risedev down
set -e

echo "--- Start cluster on tag $TAG"
git config --global --add safe.directory /risingwave
# FIXME(kwannoel): We use this config because kafka encounters errors upon cluster restart.
./risedev d full-without-monitoring && rm .risingwave/log/*
pushd .risingwave/log/
buildkite-agent artifact upload "./*.log"
popd

# TODO(kwannoel): This will be the section for which we run nexmark queries + tpch queries.
echo "--- Running queries"
run_sql "CREATE TABLE t(v1 int);"
seed_table
run_sql "CREATE MATERIALIZED VIEW m as SELECT * from t;" &
CREATE_MV_PID=$!
seed_table
wait $CREATE_MV_PID
run_sql "select * from m ORDER BY v1;" > BEFORE

echo "--- Kill cluster on tag $TAG"
./risedev k

echo "--- Setup Risingwave @ $RW_COMMIT"
download_and_prepare_rw ci-dev common

echo "--- Start cluster on latest"
configure_rw
./risedev d full-without-monitoring

echo "--- Wait ${RECOVERY_DURATION}s for Recovery"
sleep $RECOVERY_DURATION
run_sql "SELECT * from m ORDER BY v1;" > AFTER

echo "--- Comparing results"
assert_eq BEFORE AFTER
assert_not_empty BEFORE
assert_not_empty AFTER