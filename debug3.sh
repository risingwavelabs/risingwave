#!/usr/bin/env bash

set -euox pipefail
set +e
./risedev k
./risedev clean-data
set -e
./risedev d
psql -h localhost -p 4566 -d dev -U root -c "SET RW_IMPLICIT_FLUSH=true;
set rw_streaming_enable_delta_join = true;
create table A      (k1 int, k2 int, k3 int, v int);
create index Ak1   on A(k1) include(k1,k2,k3,v);
create index Ak1k2 on A(k1,k2) include(k1,k2,k3,v);
create table B      (k1 int, k2 int, k3 int, v int);
create index Bk1   on B(k1) include(k1,k2,k3,v);
-- insert into A values(1, 6, 7, 8);
-- insert into B values(1, 2, 3, 4);
CREATE MATERIALIZED VIEW m1 as select A.v, B.v as Bv from A join B using(k1);
select * from m1;
"
./risedev k
