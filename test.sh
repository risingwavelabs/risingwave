#!/usr/bin/env bash

set -euo pipefail

./risedev d full

sleep 8

./risedev psql -c "
CREATE TABLE t (v1 int, v2 int);
INSERT INTO t VALUES (2, 3), (3, 4), (5, 6), (7, 8);
-- flush;
-- CREATE MATERIALIZED VIEW m2 as SELECT v2, v1 FROM t;
CREATE MATERIALIZED VIEW m2 as SELECT * FROM t;
SELECT * FROM t;
"

./risedev psql -c "
EXPLAIN CREATE MATERIALIZED VIEW m3 as SELECT v1 FROM t;
" </dev/null

./risedev psql -c "
CREATE MATERIALIZED VIEW m3 as SELECT v1 FROM t;
"

./risedev psql -c "
SELECT * FROM m2;
"

./risedev psql -c "
SELECT * FROM m3;
"

./risedev psql -c "
SELECT * FROM t;
"


