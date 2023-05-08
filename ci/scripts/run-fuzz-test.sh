#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

if [[ "$RUN_SQLSMITH_ALL" -eq "1" ]]; then
  RUN_SQLSMITH_PRE_GEN=1
  RUN_SQLSMITH=1
fi

if [[ $RUN_SQLSMITH_FRONTEND -eq "1" ]]; then
    echo "--- Run sqlsmith frontend tests"
    NEXTEST_PROFILE=ci cargo nextest run run_sqlsmith_on_frontend --features "failpoints sync_point enable_sqlsmith_unit_test" 2> >(tee);
fi

if [[ "$RUN_SQLSMITH" -eq "1" ]]; then
    while getopts 'p:' opt; do
        case ${opt} in
            p )
                profile=$OPTARG
                ;;
            \? )
                echo "Invalid Option: -$OPTARG" 1>&2
                exit 1
                ;;
            : )
                echo "Invalid option: $OPTARG requires an argument" 1>&2
                ;;
        esac
    done
    shift $((OPTIND -1))

    download_and_prepare_rw "$profile" common

    echo "--- Download artifacts"
    buildkite-agent artifact download risingwave_simulation .
    chmod +x ./risingwave_simulation

    echo "--- Download sqlsmith e2e bin"
    buildkite-agent artifact download sqlsmith-"$profile" target/debug/
    mv target/debug/sqlsmith-"$profile" target/debug/sqlsmith
    chmod +x ./target/debug/sqlsmith

    echo "--- e2e, ci-3cn-1fe, build"
    cargo make ci-start ci-3cn-1fe

    echo "--- e2e, ci-3cn-1fe, run fuzzing"
    timeout 20m RUST_LOG=info ./target/debug/sqlsmith test \
      --count "$SQLSMITH_COUNT" \
      --testdata ./src/tests/sqlsmith/tests/testdata \
      2>"$LOGDIR/fuzzing.log" && rm "$LOGDIR/fuzzing.log"

    # Using `kill` instead of `ci-kill` avoids storing excess logs.
    # If there's errors, the failing query will be printed to stderr.
    # Use that to reproduce logs on local machine.
    echo "--- Kill cluster"
    cargo make kill

    # FIXME(Noel): Disable for now, deterministic e2e fuzzing should only
    # be ran for pre-generated queries.
    echo "--- deterministic simulation e2e, ci-3cn-2fe, fuzzing (seed)"
    seq $TEST_NUM | parallel MADSIM_TEST_SEED={} './risingwave_simulation --sqlsmith 100 ./src/tests/sqlsmith/tests/testdata 2> $LOGDIR/fuzzing-{}.log && rm $LOGDIR/fuzzing-{}.log'
fi
