#!/usr/bin/env bash

set -euxo pipefail

export TEST_NUM=100
export RW_HOME="../../../.."
export LOGDIR=".risingwave/log"
export TESTS_FOLDER="src/tests/sqlsmith/tests"
export OUTDIR="$TESTS_FOLDER/freeze"
export TESTDATA="src/tests/sqlsmith/tests/testdata"
export MADSIM_BIN="target/sim/ci-sim/risingwave_simulation"

build_madsim() {
  cargo make sslt-build-all --profile ci-sim
}

generate_deterministic() {
  seq "$TEST_NUM" | \
    parallel "mkdir -p $OUTDIR/{}; \
      MADSIM_TEST_SEED={} $MADSIM_BIN \
        --sqlsmith 100 \
        --generate-sqlsmith-queries $OUTDIR/{} \
        $TESTDATA \
        2> $LOGDIR/fuzzing-{}.log && rm $LOGDIR/fuzzing-{}.log"
}

generate_sqlsmith() {
  mkdir -p "$OUTDIR/$1"
  ./risedev d
  ./target/debug/sqlsmith test \
    --testdata ./src/tests/sqlsmith/tests/testdata \
    --generate "$OUTDIR/$1"
}

main() {
  cd $RW_HOME
  build_madsim
  generate_deterministic
  cd -
}

main