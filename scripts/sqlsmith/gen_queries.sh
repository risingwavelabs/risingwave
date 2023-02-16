#!/usr/bin/env bash

set -euxo pipefail

export TEST_NUM=32
export LOGDIR=".risingwave/log"
export TESTS_FOLDER="./src/tests/sqlsmith/tests"
export BASE_FOLDER="$TESTS_FOLDER/freeze"

generate_deterministic() {
    seq "$TEST_NUM" | \
      parallel "mkdir -p $BASE_FOLDER/{}; \
       MADSIM_TEST_SEED={} \
       ./target/sim/ci-sim/risingwave_simulation \
        --sqlsmith 100 \
        --generate-sqlsmith-queries $BASE_FOLDER/{} \
        ./src/tests/sqlsmith/tests/testdata \
         2> $LOGDIR/fuzzing-{}.log && rm $LOGDIR/fuzzing-{}.log"
}

generate_sqlsmith() {
  mkdir -p "$BASE_FOLDER/$1"
  ./risedev d
  ./target/debug/sqlsmith test \
    --testdata ./src/tests/sqlsmith/tests/testdata \
    --generate "$BASE_FOLDER/$1"
}

cargo make sslt-build-all --profile ci-sim

generate_deterministic

zip -r "$TESTS_FOLDER/freeze.zip" $BASE_FOLDER
rm -r $BASE_FOLDER