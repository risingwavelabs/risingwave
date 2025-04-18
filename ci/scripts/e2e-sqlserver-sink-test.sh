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

echo "--- create SQL Server table"
curl https://packages.microsoft.com/keys/microsoft.asc | sudo apt-key add -
curl https://packages.microsoft.com/config/ubuntu/20.04/prod.list | sudo tee /etc/apt/sources.list.d/msprod.list
apt-get update -y
ACCEPT_EULA=Y DEBIAN_FRONTEND=noninteractive apt-get install -y mssql-tools unixodbc-dev
export PATH="/opt/mssql-tools/bin/:$PATH"
sleep 2

sqlcmd -S sqlserver-server -U SA -P SomeTestOnly@SA -Q "
CREATE DATABASE SinkTest;
GO
USE SinkTest;
GO
CREATE SCHEMA test_schema;
GO
CREATE TABLE test_schema.t_many_data_type (
  k1 int, k2 int,
  c_boolean bit,
  c_int16 smallint,
  c_int32 int,
  c_int64 bigint,
  c_float32 float,
  c_float64 float,
  c_decimal decimal,
  c_date date,
  c_time time,
  c_timestamp datetime2,
  c_timestampz datetime2,
  c_nvarchar nvarchar(1024),
  c_varbinary varbinary(1024),
PRIMARY KEY (k1,k2));
GO"
sleep 2

echo "--- testing sinks"
sqllogictest -p 4566 -d dev './e2e_test/sink/sqlserver_sink.slt'
sleep 1
sqlcmd -S sqlserver-server -U SA -P SomeTestOnly@SA -h -1 -Q "
SELECT * FROM SinkTest.test_schema.t_many_data_type;
GO" > ./query_result.txt

mapfile -t actual < <(tr -s '[:space:]' '\n' < query_result.txt)
actual=("${actual[@]:1}")
expected=(0 0 0 NULL NULL NULL NULL NULL NULL NULL NULL NULL NULL NULL NULL 1 1 0 55 55 1 1.0 1.0 1 2022-04-08 18:20:49.0000000 2022-03-13 01:00:00.0000000 2022-03-13 01:00:00.0000000 Hello World! 0xDE00BEEF 1 2 0 66 66 1 1.0 1.0 1 2022-04-08 18:20:49.0000000 2022-03-13 01:00:00.0000000 2022-03-13 01:00:00.0000000 Hello World! 0xDE00BEEF 1 4 0 2 2 1 1.0 1.0 1 2022-04-08 18:20:49.0000000 2022-03-13 01:00:00.0000000 2022-03-13 01:00:00.0000000 Hello World! 0xDE00BEEF "(4" rows "affected)")

if [[ ${#actual[@]} -eq ${#expected[@]} && ${actual[@]} == ${expected[@]} ]]; then
  echo "SQL Server sink check passed"
else
  cat ./query_result.txt
  echo "The output is not as expected."
  exit 1
fi

echo "--- Kill cluster"
risedev ci-kill
