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


echo "--- create starrocks table"
sleep 2
mysql -uroot -P 9030 -h starrocks-fe-server -e "CREATE database demo;use demo;
CREATE table demo_bhv_table(v1 int,v2 smallint,v3 bigint,v4 float,v5 double,v6 string,v7 date,v8 datetime,v9 boolean,v10 json,v11 decimal(10,5)) ENGINE=OLAP
PRIMARY KEY(\`v1\`)
DISTRIBUTED BY HASH(\`v1\`) properties(\"replication_num\" = \"1\");
CREATE table demo_agg_table(id int, value string REPLACE_IF_NOT_NULL) ENGINE=OLAP
AGGREGATE KEY(\`id\`)
DISTRIBUTED BY HASH(\`id\`) properties(\"replication_num\" = \"1\");
CREATE TABLE demo_reserved_words(id int, \`order\` string, \`from\` string) ENGINE=OLAP
PRIMARY KEY(id) DISTRIBUTED BY HASH (id) properties(\"replication_num\" = \"1\");
CREATE USER 'users'@'%' IDENTIFIED BY '123456';
GRANT ALL ON *.* TO 'users'@'%';"
sleep 2

echo "--- testing sinks"
sqllogictest -p 4566 -d dev './e2e_test/sink/starrocks_sink.slt'
sleep 1
mysql -uroot -P 9030 -h starrocks-fe-server -e "select * from demo.demo_bhv_table" > ./query_result.csv
mysql -uroot -P 9030 -h starrocks-fe-server -e "select * from demo.demo_agg_table" > ./query_result_agg.csv

if ! cat ./query_result.csv | sed '1d; s/\t/,/g' | awk -F "," '{
    exit !($1 == 1 && $2 == 1 && $3 == 1 && $4 == 1.1 && $5 == 1.2 && $6 == "test" && $7 == "2013-01-01" && $8 == "2013-01-01 01:01:01" && $9 == 0 && $10 = "{"v101": 100}" && $11 == 1.12346); }'
then
  cat ./query_result.csv
  echo "The output is not as expected."
  exit 1
fi

if ! cat ./query_result_agg.csv | sed '1d; s/\t/,/g' | awk -F "," '{
    exit !($1 == 1 && $2 == "v"); }'
then
  cat ./query_result_agg.csv
  echo "The output is not as expected."
  exit 1
fi

echo "Starrocks sink check passed"

echo "--- Kill cluster"
risedev ci-kill
