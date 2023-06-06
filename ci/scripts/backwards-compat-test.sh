#!/usr/bin/env bash

set -euo pipefail

# FIXME(kwannoel): automatically derive this by:
# 1. Fetching major version.
# 2. Find the earliest minor version of that major version.
TAG=v0.18.0-rc
RECOVERY_DURATION=20

rc () {
    psql -h localhost -p 4566 -d dev -U root -c "$@"
}

assert_eq() {
  if [[ -z $(diff "$1" "$2") ]]; then
    echo "No difference"
  else
    echo "Unexpected diff"
    exit 1
  fi
}

seed_table() {
  for i in $(seq 1 10000)
  do
    rc "INSERT into t values ($i);"
  done
  rc "flush;"
}

# Checkout <old-version>
echo "--- Checking out old branch"
git checkout origin/$TAG

echo "--- Teardown any old cluster"
set +e
./risedev down
set -e

echo "--- Start cluster on tag $TAG"
# FIXME(kwannoel): We use this config because kafka encounters errors upon cluster restart.
./risedev d full-without-monitoring

echo "--- Running queries"
rc "CREATE TABLE t(v1 int);"
seed_table
rc "CREATE MATERIALIZED VIEW m as SELECT * from t;" &
CREATE_MV_PID=$!
seed_table
wait $CREATE_MV_PID
rc "select * from m;" > BEFORE

echo "--- Kill cluster on tag $TAG"
./risedev k

echo "--- Checking against $RW_COMMIT"
git checkout $RW_COMMIT

echo "--- Kill cluster on tag $TAG"
./risedev d full-without-monitoring

echo "--- Wait ${RECOVERY_DURATION}s for Recovery"
sleep $RECOVERY_DURATION
rc "SELECT * from m;" > AFTER

echo "--- Comparing results"
assert_eq BEFORE AFTER