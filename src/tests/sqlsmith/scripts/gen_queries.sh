#!/usr/bin/env bash

# USAGE: Script for generating queries via sqlsmith.
# These queries can be used for fuzz testing.
# Requires `$SNAPSHOT_DIR` to be set,
# that will be where queries are stored after generation.

set -euo pipefail

# -x is too verbose, selectively enable it if needed.

export OUTDIR=$SNAPSHOT_DIR
export TEST_NUM=100
export RW_HOME="../../../.."
export LOGDIR=".risingwave/log"
export TESTS_DIR="src/tests/sqlsmith/tests"
export TESTDATA="$TESTS_DIR/testdata"
export MADSIM_BIN="target/sim/ci-sim/risingwave_simulation"

build_madsim() {
  cargo make sslt-build-all --profile ci-sim
}

# $LOGFILE
check_if_crashed() {
  CRASHED=$(grep "note: run with \`MADSIM_TEST_SEED=[0-9]*\` environment variable to reproduce this error" "$1")
  echo "$CRASHED"
}

# Extract queries from $1, write to $2
extract_queries() {
  QUERIES=$(grep "\[EXECUTING .*\]: " < "$1" | sed -E 's/^.*\[EXECUTING .*\]: (.*)$/\1;/')
  CRASHED=$(check_if_crashed "$1")
  if [[ -n "$CRASHED" ]]; then
    echo "Cluster crashed while generating queries. see $1 for more information."
    QUERIES=$(echo -e "$QUERIES" | sed -E '$ s/(.*)/-- \1/')
  fi
  echo -e "$QUERIES" > "$2"
}

# Prefer to use [`generate_deterministic`], it is faster since
# runs with all-in-one binary.
generate_deterministic() {
  . $(which env_parallel.bash)
  # Even if fails early, it should still generate some queries, do not exit script.
  set +e
  seq "$TEST_NUM" | env_parallel "
    mkdir -p $OUTDIR/{}; \
    MADSIM_TEST_SEED={} ./$MADSIM_BIN \
      --sqlsmith 100 \
      --generate-sqlsmith-queries $OUTDIR/{} \
      $TESTDATA \
      2>$LOGDIR/generate-{}.log; \
    extract_queries $LOGDIR/generate-{}.log $OUTDIR/{}/queries.sql; \
    "
  set -e
}

generate_sqlsmith() {
  mkdir -p "$OUTDIR/$1"
  ./risedev d
  ./target/debug/sqlsmith test \
    --testdata ./src/tests/sqlsmith/tests/testdata \
    --generate "$OUTDIR/$1"
}

# Check that queries are different
check_different_queries() {
  if [[ $(diff "$OUTDIR/1/queries.sql" "$OUTDIR/2/queries.sql") ]]; then
    echo "Queries are different."
  else
    echo "Queries are the same! Something went wrong in the generation process." && exit 1
  fi
}

# Check if any query generation step failed, and any query file not generated.
check_failing_queries() {
  echo "Query files generated:"
  ls "$OUTDIR"/* | grep -c queries.sql
}

# Upload step
upload_queries() {
  pushd "$OUTDIR"
  git add .
  git commit --amend -m 'update queries'
  git push -f origin main
  popd
}

# Run it to make sure it should have no errors
run_queries() {
 seq $TEST_NUM | parallel MADSIM_TEST_SEED={} './$MADSIM_BIN  --run-sqlsmith-queries $OUTDIR/{} 2> $LOGDIR/fuzzing-{}.log && rm $LOGDIR/fuzzing-{}.log'
}

check_failed_to_run_queries() {
  FAILED_LOGS=$(ls "$LOGDIR | grep fuzzing")
  if [[ -n "$FAILED_LOGS" ]]; then
    echo -e "FAILING_LOGS: $FAILED_LOGS"
  fi
}

main() {
  pushd $RW_HOME
  build_madsim
  generate_deterministic
  check_different_queries
  check_failing_queries
  run_queries
  check_failed_to_run_queries
  upload_queries
  popd
  echo "successfully generated"
}

main