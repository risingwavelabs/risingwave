#!/usr/bin/env bash

# USAGE: Script for generating queries via sqlsmith.
# These queries can be used for fuzz testing.
# Requires `$SNAPSHOT_DIR` to be set,
# that will be where queries are stored after generation.

export RUST_LOG="info"
export OUTDIR=$SNAPSHOT_DIR
export TEST_NUM=100
export RW_HOME="../../../.."
export LOGDIR=".risingwave/log"
export TESTS_DIR="src/tests/sqlsmith/tests"
export TESTDATA="$TESTS_DIR/testdata"
export MADSIM_BIN="target/sim/ci-sim/risingwave_simulation"
export CRASH_MESSAGE="note: run with \`MADSIM_TEST_SEED=[0-9]*\` environment variable to reproduce this error"

refresh() {
  cd src/tests/sqlsmith/scripts
  source gen_queries.sh
  cd -
}

# Build
build_madsim() {
  cargo make sslt-build-all --profile ci-sim
}

# Get reason for generation crash.
get_failure_reason() {
  grep -B 2 "$CRASH_MESSAGE"
}

# Extract queries from file $1, write to file $2
extract_queries() {
  QUERIES=$(grep "\[EXECUTING .*\]: " < "$1" | sed -E 's/^.*\[EXECUTING .*\]: (.*)$/\1;/')
  FAIL_REASON=$(get_failure_reason < "$1")
  if [[ -n "$FAIL_REASON" ]]; then
    echo "Cluster crashed while generating queries. see $1 for more information."
    QUERIES=$(echo -e "$QUERIES" | sed -E '$ s/(.*)/-- \1/')
  fi
  echo -e "$QUERIES" > "$2"
}

extract_ddl() {
  grep "\[EXECUTING CREATE .*\]: " | sed -E 's/^.*\[EXECUTING CREATE .*\]: (.*)$/\1;/'
}

extract_dml() {
  grep "\[EXECUTING INSERT\]: " | sed -E 's/^.*\[EXECUTING INSERT\]: (.*)$/\1;/'
}

extract_last_session() {
  grep "\[EXECUTING TEST SESSION_VAR\]: " | sed -E 's/^.*\[EXECUTING TEST SESSION_VAR\]: (.*)$/\1;/' | tail -n 1
}

extract_global_session() {
  grep "\[EXECUTING SET_VAR\]: " | sed -E 's/^.*\[EXECUTING SET_VAR\]: (.*)$/\1;/'
}

extract_failing_query() {
  grep "\[EXECUTING .*\]: " | tail -n 1 | sed -E 's/^.*\[EXECUTING .*\]: (.*)$/\1;/'
}

# Extract fail info from file $1
# Fail info: ddl, dml, session var, query, reason
extract_fail_info() {
  REASON=$(get_failure_reason < "$1")
  if [[ -n "$REASON" ]]; then
    DDL=$(extract_ddl < "$1")
    GLOBAL_SESSION=$(extract_global_session < "$1")
    DML=$(extract_dml < "$1")
    TEST_SESSION=$(extract_last_session < "$1")
    QUERY=$(extract_failing_query < "$1")
  fi
}

# Extract fail info from logs in log dir
extract_fail_info_from_logs() {
  for LOGFILENAME in $(ls "$LOGDIR" | grep "generate")
  do
    LOGFILE="$LOGDIR/$LOGFILENAME"
    extract_fail_info "$LOGFILE"
    if [[ -n "$REASON" ]]; then
      SEED=$(echo "$LOGFILENAME" | sed -E 's/generate\-(.*)\.log/\1/')
      echo "$SEED"
      FAIL_DIR="$OUTDIR/failed/$SEED"
      mkdir -p "$FAIL_DIR"
      echo -e "$DDL" "$GLOBAL_SESSION" "$DML" "\n$TEST_SESSION" "\n$QUERY" > "$FAIL_DIR/queries.sql"
      echo -e "$REASON" > "$FAIL_DIR/fail.log"
      cp "$LOGFILE" "$FAIL_DIR/$LOGFILENAME"
    fi
  done
}

# Prefer to use [`generate_deterministic`], it is faster since
# runs with all-in-one binary.
generate_deterministic() {
  # Allows us to use other functions defined in this file within `parallel`.
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
check_failed_to_generate_queries() {
  echo "Query files generated:"
  ls "$OUTDIR"/* | grep -c queries.sql
}

# Upload step
upload_queries() {
  set +x
  pushd "$OUTDIR"
  git checkout -b stage
  git add .
  git commit -m 'update queries'
  git push origin stage
  git branch -D stage
  popd
  set -x
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

################### TOP LEVEL INTERFACE

setup() {
  set -euo pipefail
  # -x is too verbose, selectively enable it if needed.
  pushd $RW_HOME
}

build() {
  build_madsim
}

generate() {
  generate_deterministic
}

validate() {
  check_different_queries
  check_failed_to_generate_queries
  extract_fail_info_from_logs
  run_queries
  check_failed_to_run_queries
}

upload() {
  upload_queries
}

cleanup() {
  popd
  echo "successfully generated"
}

main() {
  setup

  build
  generate
  validate
  upload

  cleanup
}

main