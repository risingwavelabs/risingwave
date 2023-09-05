#!/usr/bin/env bash

# NOTE(kwannoel):
# Do not run this script directly, it is meant to be sourced.
# Backwards compatibility tests consist of the following parts:
#
# 1. Setup old cluster binaries.
# 2. Seed old cluster.
# 3. Setup new cluster binaries.
# 4. Run validation on new cluster.
#
# Steps 1,3 are specific to the execution environment, CI / Local.
# This script only provides utilities for 2, 4.

################################### ENVIRONMENT CONFIG

# Duration to wait for recovery (seconds)
RECOVERY_DURATION=20

# Setup test directory
TEST_DIR=.risingwave/backwards-compat-tests/
mkdir -p $TEST_DIR
cp -r backwards-compat-tests/slt/* $TEST_DIR
cp -r e2e_test/streaming/nexmark $TEST_DIR

################################### TEST UTILIIES

run_sql () {
    psql -h localhost -p 4566 -d dev -U root -c "$@"
}

check_version() {
  local TAG=$1
  local raw_version=$(run_sql "SELECT version();")
  echo "--- Version"
  echo "$raw_version"
  local version=$(echo $raw_version | grep -i risingwave | sed 's/^.*risingwave-\([0-9]*\.[0-9]*\.[0-9]\).*$/\1/i')
  if [[ "$version" != "$TAG" ]]; then
    echo "Version mismatch, expected $TAG, got $version"
    exit 1
  fi
}

################################### Entry Points

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
RISEDEV_CONFIGURED=false

ENABLE_MINIO=true
ENABLE_ETCD=true
ENABLE_KAFKA=false

# Fetch risingwave binary from release.
ENABLE_BUILD_RUST=true

# Ensure it will link the all-in-one binary from our release.
ENABLE_ALL_IN_ONE=true

# ENABLE_RELEASE_PROFILE=true
EOF
}

# Setup table and materialized view.
# Run updates and deletes on the table.
# Get the results.
# TODO: Run nexmark, tpch queries
# TODO(kwannoel): use sqllogictest.
seed_old_cluster() {
  OLD_TAG=$1
  configure_rw
  ./risedev clean-data
  ./risedev d full-without-monitoring && rm .risingwave/log/*

  check_version "$OLD_TAG"

  echo "--- Seeding old cluster with data"
  sqllogictest -d dev -h localhost -p 4566 "$TEST_DIR/basic/seed.slt"

  echo "--- Validating old cluster"
  sqllogictest -d dev -h localhost -p 4566 "$TEST_DIR/basic/validate_original.slt"

  ./risedev k
}

validate_new_cluster() {
  NEW_TAG=$1
  echo "--- Start cluster on latest"
  configure_rw
  ./risedev d full-without-monitoring

  echo "--- Wait ${RECOVERY_DURATION}s for Recovery on Old Cluster Data"
  sleep $RECOVERY_DURATION

  check_version "$NEW_TAG"

  echo "--- Validating new cluster"
  sqllogictest -d dev -h localhost -p 4566 "$TEST_DIR/basic/validate_original.slt"
  sqllogictest -d dev -h localhost -p 4566 "$TEST_DIR/basic/validate_restart.slt"
}