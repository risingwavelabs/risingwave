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

################################### ENVIRONMENT VARIABLES

LOG_DIR=.risingwave/log
mkdir -p "$LOG_DIR"

# TODO(kwannoel): Use the in-built query log.
QUERY_LOG_FILE="$LOG_DIR/query.log"

# TODO(kwannoel): automatically derive this by:
# 1. Fetching major version.
# 2. Find the earliest minor version of that major version.
# Duration to wait for recovery (seconds)
RECOVERY_DURATION=20

################################### TEST UTILIIES

assert_not_empty() {
  set +e
  if [[ $(wc -l < "$1" | sed 's/^ *//g') -gt 1 ]]; then
    echo "assert_not_empty PASSED for $1"
  else
    echo "assert_not_empty FAILED for $1"
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
    echo "LHS: " $1
    echo "RHS: " $2
    exit 1
  fi
  set -e
}

################################### QUERIES

run_sql () {
    psql -h localhost -p 4566 -d dev -U root -c "$@"
}

seed_table() {
  START="$1"
  END="$2"
  for i in $(seq "$START" "$END")
  do
    run_sql "INSERT into t values ($i, $i);" 1>$QUERY_LOG_FILE 2>&1
  done
  run_sql "flush;"
}

random_delete() {
  START=$1
  END=$2
  COUNT=$3
  for i in $(seq 1 "$COUNT")
  do
    run_sql "DELETE FROM t WHERE v1 = $(("$RANDOM" % END));" 1>$QUERY_LOG_FILE 2>&1
  done
  run_sql "flush;"
}

random_update() {
  START=$1
  END=$2
  COUNT=$3
  for _i in $(seq 1 "$COUNT")
  do
    run_sql "UPDATE t SET v2 = v2 + 1 WHERE v1 = $(("$RANDOM" % END));" 1>$QUERY_LOG_FILE 2>&1
  done
  run_sql "flush;"
}

# Just check if the results are the same as old cluster.
run_sql_new_cluster() {
  run_sql "SELECT * from m ORDER BY v1;" > AFTER_1
  run_sql "select * from m2 ORDER BY v1;" > AFTER_2
}

run_updates_and_deletes_new_cluster() {
  random_update 1 20000 1000
  random_delete 1 20000 1000
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
ENABLE_BUILD_RUST=false

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
  configure_rw
  ./risedev clean-data
  ./risedev d full-without-monitoring && rm .risingwave/log/*

  run_sql "CREATE TABLE t(v1 int primary key, v2 int);"

  seed_table 1 10000

  run_sql "CREATE MATERIALIZED VIEW m as SELECT * from t;" &
  CREATE_MV_PID=$!

  seed_table 10001 20000

  random_update 1 20000 1000

  random_delete 1 20000 1000

  wait $CREATE_MV_PID

  run_sql "CREATE MATERIALIZED VIEW m2 as SELECT v1, sum(v2) FROM m GROUP BY v1;"

  run_sql "select * from m ORDER BY v1;" > BEFORE_1
  run_sql "select * from m2 ORDER BY v1;" > BEFORE_2

  ./risedev k
}

validate_new_cluster() {
  echo "--- Start cluster on latest"
  configure_rw
  ./risedev d full-without-monitoring

  echo "--- Wait ${RECOVERY_DURATION}s for Recovery on Old Cluster Data"
  sleep $RECOVERY_DURATION

  echo "--- Running Queries New Cluster"
  run_sql_new_cluster

  echo "--- Sanity Checks"
  echo "AFTER_1"
  cat AFTER_1 | tail -n 100
  echo "AFTER_2"
  cat AFTER_2 | tail -n 100

  echo "--- Comparing results"
  assert_eq BEFORE_1 AFTER_1
  assert_eq BEFORE_2 AFTER_2
  assert_not_empty BEFORE_1
  assert_not_empty BEFORE_2
  assert_not_empty AFTER_1
  assert_not_empty AFTER_2

  echo "--- Running Updates and Deletes on new cluster should not fail"
  run_updates_and_deletes_new_cluster
}
