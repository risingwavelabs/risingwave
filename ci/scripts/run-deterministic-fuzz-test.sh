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
   | sed 's/$/;/'
}

echo "--- Download artifacts"
download-and-decompress-artifact risingwave_simulation .
chmod +x ./risingwave_simulation


echo "--- deterministic simulation e2e, ci-3cn-2fe, fuzzing (seed)"
set +e
seq 32 | parallel 'MADSIM_TEST_SEED={} ./risingwave_simulation --sqlsmith 100 ./src/tests/sqlsmith/tests/testdata 2> $LOGDIR/fuzzing-{}.log && rm $LOGDIR/fuzzing-{}.log'
set -e

failed_logs=$(ls $LOGDIR/fuzzing-*.log 2>/dev/null || true)

if [[ -n "$failed_logs" ]]; then
    echo "Simulation fuzzing failed, see logs in $LOGDIR"
    for log_file in $failed_logs; do
        seed=$(basename "$log_file" .log | cut -d'-' -f2)
        error_sql="$LOGDIR/error-$seed.sql.log"
        shrunk_sql="$LOGDIR/error-$seed.sql.shrunk.log"

        echo "Processing seed $seed (log: $log_file)"
        extract_error_sql "$log_file" > "$error_sql"

        echo "--- Running reducer on failing queries for seed $seed"
        ./target/debug/sqlsmith-reducer \
          --input-file "$error_sql" \
          --output-file "$shrunk_sql" \
          --run-rw-cmd './risedev k && ./risedev d ci-3cn-2fe'
        echo "--- Reducer finished for seed $seed"
        echo "Reduced queries saved at $shrunk_sql"
    done
    exit 1
fi

echo "Simulation fuzzing complete"
