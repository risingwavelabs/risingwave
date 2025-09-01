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

extract_error_sql() {
  cat "$1" \
   | grep -E "(\[EXECUTING|\[TEST)" \
   | sed 's/.*\[EXECUTING .*\]: //' \
   | sed 's/.*\[TEST.*\]: //' \
   | sed 's/$/;/' > $LOGDIR/error.sql.log
}

echo "--- Download artifacts"
download-and-decompress-artifact risingwave_simulation .
chmod +x ./risingwave_simulation


echo "--- deterministic simulation e2e, ci-3cn-2fe, fuzzing (seed)"
failed=0
for seed in $(seq 32); do
    log_file="$LOGDIR/fuzzing-$seed.log"
    if ! MADSIM_TEST_SEED=$seed ./risingwave_simulation \
        --sqlsmith 100 ./src/tests/sqlsmith/tests/testdata \
        2> "$log_file"; then
        echo "Seed $seed failed, logs at $log_file"
        extract_error_sql "$log_file"
        failed=1
    else
        rm -f "$log_file"
    fi
done

if [[ $failed -eq 1 ]]; then
    echo "Simulation fuzzing failed, please check $LOGDIR/error.sql.log"
    echo "--- Running reducer on failing queries"
    echo "Reducing queries from $LOGDIR/error.sql.log -> $LOGDIR/error.sql.shrunk.log"
    ./target/debug/sqlsmith-reducer \
      --input-file $LOGDIR/error.sql.log \
      --output-file $LOGDIR/error.sql.shrunk.log \
      --run-rw-cmd './risedev k && ./risedev d ci-3cn-2fe'
    echo "--- Reducer finished"
    echo "Reduced queries saved at $LOGDIR/error.sql.shrunk.log"
    exit 1
fi

echo "Simulation fuzzing complete"
