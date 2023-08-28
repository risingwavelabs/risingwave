CREATE SCHEMA IF NOT EXISTS s1;

USE s1;

DROP TABLE IF EXISTS t1;

CREATE TABLE t1
(
  id bigint,
  name string,
  distance bigint
) USING iceberg
TBLPROPERTIES ('format-version'='2');

INSERT INTO t1 VALUES (1, "a", 100), (2, "b", 200);



