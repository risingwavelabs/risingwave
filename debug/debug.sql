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

CREATE MATERIALIZED VIEW m5 AS SELECT 0 AS col_0 FROM partsupp AS t_0 GROUP BY t_0.ps_suppkey HAVING INTERVAL '733678' <> TIME '17:49:13';

SELECT
    1
FROM
    m5 AS t_20
        JOIN orders AS t_21 ON t_20.col_0 = t_21.o_orderkey
GROUP BY
    t_20.col_0,
    t_21.o_totalprice
HAVING
    SMALLINT '27538' = t_20.col_0;
