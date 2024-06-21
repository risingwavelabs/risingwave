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
risedev ci-start ci-sink-test
sleep 1


echo "--- create clickhouse table"
curl https://clickhouse.com/ | sh
sleep 2
./clickhouse client --host=clickhouse-server --port=9000 --query="CREATE table demo_test_append_only(v1 Int32,v2 Int64,v3 String,v4 Enum16('A'=1,'B'=2), v5 decimal64(3))ENGINE = ReplacingMergeTree PRIMARY KEY (v1);"
./clickhouse client --host=clickhouse-server --port=9000 --query="CREATE table demo_test_upsert1(v1 Int32,v2 Int64,v3 String,ver DateTime64,del UInt8)ENGINE = ReplacingMergeTree(ver, del) PRIMARY KEY (v1);"
./clickhouse client --host=clickhouse-server --port=9000 --query="CREATE table demo_test_upsert2(v1 Int32,v2 Int64,v3 String,del Int8)ENGINE = CollapsingMergeTree(del) PRIMARY KEY (v1);"

echo "--- testing sinks append_only"
sqllogictest -p 4566 -d dev './e2e_test/sink/clickhouse_sink.slt'
sleep 5
./clickhouse client --host=clickhouse-server --port=9000 --query="select * from demo_test_append_only FORMAT CSV;" > ./query_result.csv


# check sink destination using shell
if cat ./query_result.csv | sort | awk -F "," '{
if ($1 == 1 && $2 == 50 && $3 == "\"1-50\"" && $4 == "\"A\"" && $5 == 1.1) c1++;
 if ($1 == 13 && $2 == 2 && $3 == "\"13-2\"" && $4 == "\"B\"" && $5 == 0) c2++;
  if ($1 == 2 && $2 == 2 && $3 == "\"2-2\"" && $4 == "\"B\"" && $5 == 2.2) c3++;
   if ($1 == 21 && $2 == 2 && $3 == "\"21-2\"" && $4 == "\"A\"" && $5 == 0) c4++;
    if ($1 == 3 && $2 == 2 && $3 == "\"3-2\"" && $4 == "\"A\"" && $5 == 3.3) c5++;
     if ($1 == 5 && $2 == 2 && $3 == "\"5-2\"" && $4 == "\"B\"" && $5 == 4.4) c6++;
      if ($1 == 8 && $2 == 2 && $3 == "\"8-2\"" && $4 == "\"A\"" && $5 == 0) c7++; }
       END { exit !(c1 == 1 && c2 == 1 && c3 == 1 && c4 == 1 && c5 == 1 && c6 == 1 && c7 == 1); }'; then
  echo "Clickhouse sink check passed"
else
  echo "The output is not as expected."
  cat ./query_result.csv
  exit 1
fi

echo "--- testing sinks upsert1"
./clickhouse client --host=clickhouse-server --port=9000 --query="select * from demo_test_upsert1 FORMAT CSV final;" > ./query_result2.csv

if cat ./query_result2.csv | sort | awk -F "," '{
 if ($1 == 2 && $2 == 2 && $3 == "\"2-2\"" && $4 == "2013-01-02 01:01:02+01:00" && $4 == 0) c2++;
  if ($1 == 3 && $2 == 2 && $3 == "\"3-2\"" && $4 == "2013-01-03 01:01:02+01:00" && $4 == 0) c3++;
       END { exit !(c2 == 1 && c3 == 1); }'; then
  echo "Clickhouse sink check passed"
else
  echo "The output is not as expected."
  cat ./query_result2.csv
  exit 1
fi

echo "--- testing sinks upsert2"
./clickhouse client --host=clickhouse-server --port=9000 --query="select * from demo_test_upsert2 FORMAT CSV final;" > ./query_result3.csv

if cat ./query_result2.csv | sort | awk -F "," '{
 if ($1 == 2 && $2 == 2 && $3 == "\"2-2\"" && $3 == 1) c2++;
  if ($1 == 3 && $2 == 2 && $3 == "\"3-2\"" && $3 == 1) c3++; }
       END { exit !(c2 == 1 && c3 == 1); }'; then
  echo "Clickhouse sink check passed"
else
  echo "The output is not as expected."
  cat ./query_result3.csv
  exit 1
fi

echo "--- Kill cluster"
risedev ci-kill