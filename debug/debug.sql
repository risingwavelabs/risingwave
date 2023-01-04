CREATE TABLE partsupp (
                          ps_partkey INTEGER,
                          ps_suppkey INTEGER,
                          ps_availqty INTEGER,
                          ps_supplycost NUMERIC,
                          ps_comment VARCHAR,
                          PRIMARY KEY (ps_partkey, ps_suppkey)
);

CREATE TABLE orders (
                        o_orderkey BIGINT,
                        o_custkey INTEGER,
                        o_orderstatus VARCHAR,
                        o_totalprice NUMERIC,
                        o_orderdate DATE,
                        o_orderpriority VARCHAR,
                        o_clerk VARCHAR,
                        o_shippriority INTEGER,
                        o_comment VARCHAR,
                        PRIMARY KEY (o_orderkey)
);

CREATE MATERIALIZED VIEW m5 AS SELECT 0 AS col_0 FROM partsupp;

SELECT
    1
FROM
    m5 JOIN orders ON m5.col_0 = orders.o_orderkey
GROUP BY
    m5.col_0,
    orders.o_totalprice
HAVING
    SMALLINT '27538' = m5.col_0;
