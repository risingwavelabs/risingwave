#!/usr/bin/env bash
./risedev k
./risedev clean-data
./risedev d
psql -h localhost -p 4566 -d dev -U root -c "SET RW_IMPLICIT_FLUSH=true;
CREATE TABlE t (v1 int, v2 bigint, PRIMARY KEY (v1, v2));
INSERT INTO t VALUES(1, 2); 
INSERT INTO t VALUES(2, 3);
INSERT INTO t VALUES(3, 4); 
INSERT INTO t VALUES(5, 6);
INSERT INTO t VALUES(8, 9);
CREATE MATERIALIZED VIEW mv as SELECT * FROM t;
SELECT * FROM mv;
SHOW INTERNAL TABLES;
SELECT * FROM t ORDER BY v1 ASC, v2 ASC;
SELECT * FROM __internal_mv_3_chain_1003;
"
./risedev k
