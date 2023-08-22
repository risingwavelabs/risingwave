#!/usr/bin/env bash

set -euo pipefail

set +e
./risedev k
./risedev clean-data
set -e

./risedev d

./risedev psql -c "
SET STREAMING_PARALLELISM=1;
create table t(v1 int);
create materialized view mv1 as select v1 from t;
insert into t values (1), (2), (3), (4);
flush;
"

sleep 10

./risedev psql -c "
select * from mv1;

explain (verbose) create index i1 on mv1(v1);
"

./risedev psql -c "
create index i1 on mv1(v1);
select * from i1;
"
#
#./risedev psql -c "
#insert into t values (1), (2), (3), (4);
#flush;
#"
#
#sleep 10
#
#./risedev psql -c "
#select * from i1;
#"

./risedev k