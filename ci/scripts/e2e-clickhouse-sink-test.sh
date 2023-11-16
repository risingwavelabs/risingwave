#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

source ci/scripts/common.sh

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

download_and_prepare_rw "$profile" source

echo "--- starting risingwave cluster"
cargo make ci-start ci-clickhouse-test
sleep 1


echo "--- create clickhouse table"
curl https://clickhouse.com/ | sh
sleep 2
./clickhouse client --host=clickhouse-server --port=9000 --query="CREATE table demo_test(v1 Int32,v2 Int64,v3 String)ENGINE = ReplacingMergeTree PRIMARY KEY (v1);"

echo "--- testing sinks"
sqllogictest -p 4566 -d dev './e2e_test/sink/clickhouse_sink.slt'
sleep 1
./clickhouse client --host=clickhouse-server --port=9000 --query="select * from demo_test FORMAT CSV;" > ./query_result.csv


# check sink destination using shell
if cat ./query_result.csv | sort | awk -F "," '{
if ($1 == 1 && $2 == 50 && $3 == "\"1-50\"") c1++;
 if ($1 == 13 && $2 == 2 && $3 == "\"13-2\"") c2++;
  if ($1 == 2 && $2 == 2 && $3 == "\"2-2\"") c3++;
   if ($1 == 21 && $2 == 2 && $3 == "\"21-2\"") c4++;
    if ($1 == 3 && $2 == 2 && $3 == "\"3-2\"") c5++;
     if ($1 == 5 && $2 == 2 && $3 == "\"5-2\"") c6++;
      if ($1 == 8 && $2 == 2 && $3 == "\"8-2\"") c7++; }
       END { exit !(c1 == 1 && c2 == 1 && c3 == 1 && c4 == 1 && c5 == 1 && c6 == 1 && c7 == 1); }'; then
  echo "Clickhouse sink check passed"
else
  echo "The output is not as expected."
  exit 1
fi

echo "--- Kill cluster"
cargo make ci-kill