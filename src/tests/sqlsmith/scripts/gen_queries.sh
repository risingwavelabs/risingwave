#!/usr/bin/env bash

set -euxo pipefail

# export SNAPSHOT_DIR=""
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

# Prefer to use [`generate_deterministic`], it is faster since
# runs with all-in-one binary.
generate_deterministic() {
  seq "$TEST_NUM" | parallel "mkdir -p $OUTDIR/{}; \
    MADSIM_TEST_SEED={} $MADSIM_BIN \
      --sqlsmith 500 \
      --generate-sqlsmith-queries $OUTDIR/{} \
      $TESTDATA \
      2> $LOGDIR/fuzzing-{}.log"
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
    echo "Queries should be different"
  else
    echo "no difference!" && exit 1
  fi
}

# Check if any query generation step failed
check_failing_queries() {
  echo "no failing queries"
}

# Upload step
upload_queries() {
  cd "$OUTDIR"
  git add .
  git commit --amend -m 'update queries'
  # git push -f origin main
  cd -
}

main() {
  CUR_DIR="$PWD"
  cd $RW_HOME
  build_madsim
  generate_deterministic
  check_different_queries
  check_failing_queries
  upload_queries
  cd "$CUR_DIR"
}

main