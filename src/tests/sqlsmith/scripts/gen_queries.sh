#!/usr/bin/env bash

# USAGE: Script for generating queries via sqlsmith.
# These queries can be used for fuzz testing.
# Requires `$SNAPSHOT_DIR` to be set,
# that will be where queries are stored after generation.
#
# Example:
# SNAPSHOT_DIR="~/projects/sqlsmith-query-snapshots" ./gen_queries.sh

################# ENV

export RUST_LOG="info"
export OUTDIR=$SNAPSHOT_DIR
export TEST_NUM=100
export RW_HOME="../../../.."
export LOGDIR=".risingwave/log"
export TESTS_DIR="src/tests/sqlsmith/tests"
export TESTDATA="$TESTS_DIR/testdata"
export MADSIM_BIN="target/sim/ci-sim/risingwave_simulation"
export CRASH_MESSAGE="note: run with \`MADSIM_TEST_SEED=[0-9]*\` environment variable to reproduce this error"

################## COMMON

refresh() {
  cd src/tests/sqlsmith/scripts
  source gen_queries.sh
  cd -
}

echo_err() {
  echo "$@" 1>&2
}

################## EXTRACT
# TODO(kwannoel): Write tests for these

# Get reason for generation crash.
get_failure_reason() {
  tac | grep -B 10000 -m1 "\[EXECUTING" | tac | tail -n+2
}

# Extract queries from file $1, write to file $2
extract_queries() {
  QUERIES=$(grep "\[EXECUTING .*\]: " < "$1" | sed -E 's/^.*\[EXECUTING .*\]: (.*)$/\1;/')
  FAIL_REASON=$(get_failure_reason < "$1")
  if [[ -n "$FAIL_REASON" ]]; then
    echo_err "[WARN] Cluster crashed while generating queries. see $1 for more information."
    QUERIES=$(echo -e "$QUERIES" | sed -E '$ s/(.*)/-- \1/')
  fi
  echo -e "$QUERIES" > "$2"
}

extract_ddl() {
  grep "\[EXECUTING CREATE .*\]: " | sed -E 's/^.*\[EXECUTING CREATE .*\]: (.*)$/\1;/' | pg_format || true
}

extract_dml() {
  grep "\[EXECUTING INSERT\]: " | sed -E 's/^.*\[EXECUTING INSERT\]: (.*)$/\1;/' || true
}

extract_last_session() {
  grep "\[EXECUTING TEST SESSION_VAR\]: " | sed -E 's/^.*\[EXECUTING TEST SESSION_VAR\]: (.*)$/\1;/' | tail -n 1 || true
}

extract_global_session() {
  grep "\[EXECUTING SET_VAR\]: " | sed -E 's/^.*\[EXECUTING SET_VAR\]: (.*)$/\1;/' || true
}

extract_failing_query() {
  grep "\[EXECUTING .*\]: " | tail -n 1 | sed -E 's/^.*\[EXECUTING .*\]: (.*)$/\1;/' | pg_format || true
}

# Extract fail info from [`generate-*.log`] in log dir
# $1 := log file name prefix. E.g. if file is generate-XXX.log, prefix will be "generate"
extract_fail_info_from_logs() {
  LOGFILE_PREFIX="$1"
  for LOGFILENAME in $(ls "$LOGDIR" | grep "$LOGFILE_PREFIX")
  do
    LOGFILE="$LOGDIR/$LOGFILENAME"
    REASON=$(get_failure_reason < "$LOGFILE")
    if [[ -n "$REASON" ]]; then
      echo_err "[INFO] $LOGFILE Encountered bug."

      # TODO(Noel): Perhaps add verbose logs here, if any part is missing.
      SEED=$(echo "$LOGFILENAME" | sed -E "s/${LOGFILE_PREFIX}\-(.*)\.log/\1/")
      DDL=$(extract_ddl < "$LOGFILE")
      GLOBAL_SESSION=$(extract_global_session < "$LOGFILE")
      DML=$(extract_dml < "$LOGFILE")
      TEST_SESSION=$(extract_last_session < "$LOGFILE")
      QUERY=$(extract_failing_query < "$LOGFILE")
      FAIL_DIR="$OUTDIR/failed/$SEED"
      mkdir -p "$FAIL_DIR"
      echo -e "$DDL" "\n\n$GLOBAL_SESSION" "\n\n$DML" "\n\n$TEST_SESSION" "\n\n$QUERY" > "$FAIL_DIR/queries.sql"
      echo_err "[INFO] WROTE FAIL QUERY to $FAIL_DIR/queries.sql"
      echo -e "$REASON" > "$FAIL_DIR/fail.log"
      echo_err "[INFO] WROTE FAIL REASON to $FAIL_DIR/fail.log"

      cp "$LOGFILE" "$FAIL_DIR/$LOGFILENAME"
    fi
  done
}

################# Generate

# Prefer to use [`generate_deterministic`], it is faster since
# runs with all-in-one binary.
generate_deterministic() {
  # Allows us to use other functions defined in this file within `parallel`.
  . $(which env_parallel.bash)
  # Even if fails early, it should still generate some queries, do not exit script.
  set +e
  echo "" > $LOGDIR/generate_deterministic.stdout.log
  seq "$TEST_NUM" | env_parallel "
    mkdir -p $OUTDIR/{}; \
    MADSIM_TEST_SEED={} ./$MADSIM_BIN \
      --sqlsmith 100 \
      --generate-sqlsmith-queries $OUTDIR/{} \
      $TESTDATA \
      1>>$LOGDIR/generate_deterministic.stdout.log \
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

############################# Checks

# Check that queries are different
check_different_queries() {
  if [[ -z $(diff "$OUTDIR/1/queries.sql" "$OUTDIR/2/queries.sql") ]]; then
    echo_err "[ERROR] Queries are the same! \
      Something went wrong in the generation process." \
      && exit 1
  fi
}

# Check if any query generation step failed, and any query file not generated.
check_failed_to_generate_queries() {
  if [[ "$(ls "$OUTDIR"/* | grep -c queries.sql)" -lt "$TEST_NUM" ]]; then
    echo_err "Queries not generated: "
    # FIXME(noel): This doesn't list the files which failed to be generated.
    ls "$OUTDIR"/* | grep queries.sql
    exit 1
  fi
}

# Run it to make sure it should have no errors
run_queries() {
  echo "" > $LOGDIR/run_deterministic.stdout.log
  seq $TEST_NUM | parallel "MADSIM_TEST_SEED={} \
    ./$MADSIM_BIN --run-sqlsmith-queries $OUTDIR/{} \
      1>>$LOGDIR/run_deterministic.stdout.log \
      2>$LOGDIR/fuzzing-{}.log \
      && rm $LOGDIR/fuzzing-{}.log"
}

# Generated query sets should not fail.
check_failed_to_run_queries() {
  FAILED_LOGS=$(ls "$LOGDIR" | grep fuzzing || true)
  if [[ -n "$FAILED_LOGS" ]]; then
    echo_err -e "FAILING_LOGS: $FAILED_LOGS" && exit 1
  fi
}

################### TOP LEVEL INTERFACE

setup() {
  set -euo pipefail
  # -x is too verbose, selectively enable it if needed.
  pushd $RW_HOME
}

build_madsim() {
  cargo make sslt-build-all --profile ci-sim
}

build() {
  build_madsim
  echo_err "[INFO] Finished build"
}

generate() {
  generate_deterministic
  echo_err "[INFO] Finished generation"
}

validate() {
  check_different_queries
  echo_err "[CHECK PASSED] Generated queries should be different"
  check_failed_to_generate_queries
  echo_err "[CHECK PASSED] No seeds failed to generate queries"
  extract_fail_info_from_logs "generate"
  echo_err "[INFO] Recorded new bugs from  generated queries"
  run_queries
  echo_err "[INFO] Queries were ran"
  check_failed_to_run_queries
  echo_err "[CHECK PASSED] Queries all ran without failure"
  echo_err "[INFO] Passed checks"
}

# sync step
# Some queries maybe be added
sync_queries() {
  set +x
  pushd $OUTDIR
  git checkout main
  git pull
  set +e
  git branch -D stage
  set -e
  git checkout -b stage
  popd
  set -x
}

sync() {
  sync_queries
  echo_err "[INFO] Synced"
}

# Upload step
upload_queries() {
  set +x
  pushd "$OUTDIR"
  git add .
  git commit -m 'update queries'
  git push -f origin stage
  git checkout -
  git branch -D stage
  popd
  set -x
}

upload() {
  upload_queries
  echo_err "[INFO] Uploaded"
}

cleanup() {
  popd
  echo_err "[INFO] Success!"
}

################### ENTRY POINTS

generate() {
  setup

  build
  sync
  generate
  validate
  upload

  cleanup
}

extract() {
  LOGDIR="$PWD" OUTDIR="$PWD" extract_fail_info_from_logs "fuzzing"
  for QUERY_FOLDER in failed/*
  do
    QUERY_FILE="$QUERY_FOLDER/queries.sql"
    cargo build --bin sqlsmith-reducer
    REDUCER=$RW_HOME/target/debug/sqlsmith-reducer
    if [[ $($REDUCER --input-file "$QUERY_FILE" --output-file "$QUERY_FOLDER") -eq 0 ]]; then
      echo "[INFO] REDUCED QUERY: $PWD/$QUERY_FILE"
      echo "[INFO] WROTE TO DIR: $PWD/$QUERY_FOLDER"
    else
      echo "[INFO] FAILED TO REDUCE QUERY: $QUERY_FILE"
    fi
  done
}

main() {
  if [[ $1 == "extract" ]]; then
    echo "[INFO] Extracting queries"
    extract
  elif [[ $1 == "generate" ]]; then
    generate
  else
    echo "
================================================================
 Extract / Generate Sqlsmith queries
================================================================
 SYNOPSIS
    ./gen_queries.sh [COMMANDS]

 DESCRIPTION
    This script can extract sqlsmith queries from failing logs.
    It can also generate sqlsmith queries and store them in \$SNAPSHOT_DIR.

    You should be in \`risingwave/src/tests/sqlsmith/scripts\`
    when executing this script.

    (@kwannoel: Although eventually this should be integrated into risedev)

 COMMANDS
    generate                      Expects \$SNAPSHOT_DIR to be set.
    extract                       Extracts failing query from logs.
                                  E.g. fuzzing-66.log

 EXAMPLES
    # Generate queries
    SNAPSHOT_DIR=~/projects/sqlsmith-query-snapshots ./gen_queries.sh generate

    # Extract queries from log
    ./gen_queries.sh extract
"
  fi
}

main "$1"