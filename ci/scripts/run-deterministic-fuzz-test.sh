#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

export LOGDIR=.risingwave/log
export RUST_LOG=info
mkdir -p $LOGDIR

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

source ci/scripts/common.sh

echo "--- Download artifacts"
download-and-decompress-artifact risingwave_simulation .
chmod +x ./risingwave_simulation

echo "--- deterministic simulation e2e, ci-3cn-2fe, fuzzing (seed)"
seq 32 | parallel './risingwave_simulation --sqlsmith 100 ./src/tests/sqlsmith/tests/testdata 2> $LOGDIR/fuzzing-{}.log && rm $LOGDIR/fuzzing-{}.log'
