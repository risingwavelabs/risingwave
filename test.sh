#!/usr/bin/env bash

set -euo pipefail

cargo make ci-start ci-backfill-monitored
./risedev psql -c "
CREATE TABLE tomb (v1 int)
WITH (
  connector = 'datagen',
  fields.v1._.kind = 'sequence',
  datagen.rows.per.second = '10000000'
)
FORMAT PLAIN
ENCODE JSON;
"

sleep 30

./test2.sh 1>test.log 2>&1 &


./risedev psql -c "CREATE MATERIALIZED VIEW m1 as select * from tomb;"
./risedev psql -c "SELECT count(*) from m1;"
