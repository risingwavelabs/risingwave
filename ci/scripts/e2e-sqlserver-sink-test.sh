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

sink_test_env_setup "$profile" --need-connector

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

if [[ "${ENABLE_NATIVE_SQLSERVER_SINK_TEST:-}" == "1" ]]; then
  echo "--- create native SQL Server sink table"
  sqlcmd -S sqlserver-server -U SA -P SomeTestOnly@SA -Q "
USE SinkTest;
GO
DROP TABLE IF EXISTS test_schema.t_native_sink_pk_and_timestamptz;
GO
CREATE TABLE test_schema.t_native_sink_pk_and_timestamptz (
  EntityId nvarchar(100) NOT NULL,
  EventDate date NOT NULL,
  metric_value numeric(20,5) NULL,
  event_ts_offset datetimeoffset(7) NULL,
  event_ts_micros bigint NULL,
  event_ts_int int NULL,
  event_ts_smallint smallint NULL,
  event_ts_tinyint tinyint NULL,
  CONSTRAINT PK_t_native_sink_pk_and_timestamptz PRIMARY KEY CLUSTERED (EntityId, EventDate)
);
GO
CREATE INDEX IX_t_native_sink_pk_and_timestamptz_EventDate
ON test_schema.t_native_sink_pk_and_timestamptz (EventDate);
GO"

  echo "--- testing native SQL Server sink"
  sqllogictest -p 4566 -d dev './e2e_test/sink/sqlserver_native_sink.slt'

  native_check=$(
    sqlcmd -S sqlserver-server -U SA -P SomeTestOnly@SA -h -1 -W -Q "
SET NOCOUNT ON;
SELECT CASE WHEN EXISTS (
  SELECT 1
  FROM SinkTest.test_schema.t_native_sink_pk_and_timestamptz
  WHERE EntityId = N'entity-1'
    AND EventDate = CONVERT(date, '2024-03-10')
    AND metric_value = CONVERT(numeric(20,5), 123.45)
    AND SWITCHOFFSET(event_ts_offset, '+00:00') = CONVERT(datetimeoffset(7), '2024-03-09T16:00:00+00:00')
    AND event_ts_micros = 1710000000000000
    AND event_ts_int = 123
    AND event_ts_smallint = 123
    AND event_ts_tinyint = 123
) THEN 'ok' ELSE 'missing' END;
GO" | tr -d '\r' | awk 'NF {print $1; exit}'
  )

  if [[ "$native_check" == "ok" ]]; then
    echo "Native SQL Server sink check passed"
  else
    echo "Native SQL Server sink check failed: $native_check"
    exit 1
  fi
fi

echo "--- Kill cluster"
risedev ci-kill
