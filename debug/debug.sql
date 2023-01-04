CREATE TABLE person (
                        id BIGINT,
                        name VARCHAR,
                        email_address VARCHAR,
                        credit_card VARCHAR,
                        city VARCHAR,
                        state VARCHAR,
                        date_time TIMESTAMP,
                        extra VARCHAR,
                        PRIMARY KEY (id)
);

CREATE TABLE auction (
                         id BIGINT,
                         item_name VARCHAR,
                         description VARCHAR,
                         initial_bid BIGINT,
                         reserve BIGINT,
                         date_time TIMESTAMP,
                         expires TIMESTAMP,
                         seller BIGINT,
                         category BIGINT,
                         extra VARCHAR,
                         PRIMARY KEY (id)
);

CREATE TABLE bid (
                     auction BIGINT,
                     bidder BIGINT,
                     price BIGINT,
                     channel VARCHAR,
                     url VARCHAR,
                     date_time TIMESTAMP,
                     extra VARCHAR
);
CREATE TABLE supplier (
                          s_suppkey INTEGER,
                          s_name VARCHAR,
                          s_address VARCHAR,
                          s_nationkey INTEGER,
                          s_phone VARCHAR,
                          s_acctbal NUMERIC,
                          s_comment VARCHAR,
                          PRIMARY KEY (s_suppkey)
);

CREATE TABLE part (
                      p_partkey INTEGER,
                      p_name VARCHAR,
                      p_mfgr VARCHAR,
                      p_brand VARCHAR,
                      p_type VARCHAR,
                      p_size INTEGER,
                      p_container VARCHAR,
                      p_retailprice NUMERIC,
                      p_comment VARCHAR,
                      PRIMARY KEY (p_partkey)
);

CREATE TABLE partsupp (
                          ps_partkey INTEGER,
                          ps_suppkey INTEGER,
                          ps_availqty INTEGER,
                          ps_supplycost NUMERIC,
                          ps_comment VARCHAR,
                          PRIMARY KEY (ps_partkey, ps_suppkey)
);

CREATE TABLE customer (
                          c_custkey INTEGER,
                          c_name VARCHAR,
                          c_address VARCHAR,
                          c_nationkey INTEGER,
                          c_phone VARCHAR,
                          c_acctbal NUMERIC,
                          c_mktsegment VARCHAR,
                          c_comment VARCHAR,
                          PRIMARY KEY (c_custkey)
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

CREATE TABLE lineitem (
                          l_orderkey BIGINT,
                          l_partkey INTEGER,
                          l_suppkey INTEGER,
                          l_linenumber INTEGER,
                          l_quantity NUMERIC,
                          l_extendedprice NUMERIC,
                          l_discount NUMERIC,
                          l_tax NUMERIC,
                          l_returnflag VARCHAR,
                          l_linestatus VARCHAR,
                          l_shipdate DATE,
                          l_commitdate DATE,
                          l_receiptdate DATE,
                          l_shipinstruct VARCHAR,
                          l_shipmode VARCHAR,
                          l_comment VARCHAR,
                          PRIMARY KEY (l_orderkey, l_linenumber)
);

CREATE TABLE nation (
                        n_nationkey INTEGER,
                        n_name VARCHAR,
                        n_regionkey INTEGER,
                        n_comment VARCHAR,
                        PRIMARY KEY (n_nationkey)
);

CREATE TABLE region (
                        r_regionkey INTEGER,
                        r_name VARCHAR,
                        r_comment VARCHAR,
                        PRIMARY KEY (r_regionkey)
);

CREATE MATERIALIZED VIEW m0 AS SELECT REAL '2147483647' AS col_0, REAL '1' AS col_1 FROM supplier AS t_0 GROUP BY t_0.s_phone, t_0.s_suppkey, t_0.s_address HAVING false;
CREATE MATERIALIZED VIEW m1 AS SELECT TIMESTAMP '2022-09-30 17:49:11' AS col_0, t_0.p_retailprice AS col_1 FROM part AS t_0 GROUP BY t_0.p_retailprice, t_0.p_brand, t_0.p_comment, t_0.p_container HAVING false;
CREATE MATERIALIZED VIEW m2 AS SELECT SMALLINT '676' AS col_0, SMALLINT '0' << t_0.ps_suppkey AS col_1 FROM partsupp AS t_0 WHERE true GROUP BY t_0.ps_supplycost, t_0.ps_partkey, t_0.ps_suppkey, t_0.ps_availqty, t_0.ps_comment;
CREATE MATERIALIZED VIEW m3 AS WITH with_0 AS (SELECT coalesce(NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, TIME '18:49:12', NULL) AS col_0 FROM hop(auction, auction.date_time, INTERVAL '1', INTERVAL '604800') AS hop_1 WHERE SMALLINT '29336' <> hop_1.initial_bid GROUP BY hop_1.id) SELECT SMALLINT '16987' AS col_0 FROM with_0;
CREATE MATERIALIZED VIEW m4 AS SELECT (INTERVAL '1' / t_0.o_totalprice) * t_0.o_shippriority AS col_0, DATE '2022-09-29' AS col_1, 'O27Kz64zOz' AS col_2, coalesce(NULL, REAL '1', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL) AS col_3 FROM orders AS t_0 GROUP BY t_0.o_totalprice, t_0.o_orderkey, t_0.o_orderpriority, t_0.o_shippriority, t_0.o_comment;
CREATE MATERIALIZED VIEW m5 AS SELECT 0 AS col_0 FROM partsupp AS t_0 GROUP BY t_0.ps_suppkey HAVING INTERVAL '733678' <> TIME '17:49:13';
CREATE MATERIALIZED VIEW m6 AS SELECT REAL '542102839.6598799' * (REAL '2147483647' * INTERVAL '604800') AS col_0, INT '0' AS col_1 FROM m1 AS t_0 WHERE false GROUP BY t_0.col_0, t_0.col_1 HAVING SMALLINT '32767' > (CASE WHEN true THEN REAL '2147483647' WHEN false THEN REAL '466745366.7850032' WHEN false THEN REAL '1846741975.811943' WHEN false THEN REAL '0' + REAL '1640066110.8567863' WHEN false THEN REAL '1596143087.178205' WHEN true THEN REAL '2147483647' WHEN true THEN REAL '1' ELSE REAL '1971861147.251753' END - REAL '582656508.903337');
CREATE MATERIALIZED VIEW m7 AS SELECT TIMESTAMP '2022-09-30 17:49:14' AS col_0, t_0.col_2 AS col_1, INT '2147483647' AS col_2 FROM m4 AS t_0 GROUP BY t_0.col_3, t_0.col_0, t_0.col_2, t_0.col_1;
CREATE MATERIALIZED VIEW m8 AS SELECT SMALLINT '32767' AS col_0 FROM partsupp AS t_0 WHERE false GROUP BY t_0.ps_partkey, t_0.ps_suppkey, t_0.ps_supplycost HAVING true;
CREATE MATERIALIZED VIEW m9 AS WITH with_0 AS (SELECT SMALLINT '10701' AS col_0, SMALLINT '21526' AS col_1, FLOAT '2147483647' AS col_2 FROM bid AS t_1 GROUP BY t_1.url, t_1.auction, t_1.price HAVING (0 + SMALLINT '32767') < FLOAT '0') SELECT TIMESTAMP '2022-09-30 18:49:14' AS col_0 FROM with_0;

SELECT
    1
FROM
    hop(
            auction, auction.date_time, INTERVAL '1',
            INTERVAL '60'
        ) AS hop_15,
    m4 AS t_16,
    nation AS t_17,
    m6 AS t_18,
    m9 AS t_19,
    m5 AS t_20
        JOIN orders AS t_21 ON t_20.col_0 = t_21.o_orderkey,
    m1 AS t_22,
    customer AS t_25,
    m0 AS t_26,
    m9 AS t_27
GROUP BY
    t_26.col_1,
    t_17.n_comment,
    t_25.c_custkey,
    t_20.col_0,
    t_21.o_orderstatus,
    t_25.c_nationkey,
    t_17.n_regionkey,
    hop_15.date_time,
    t_18.col_1,
    t_25.c_comment,
    hop_15.item_name,
    t_16.col_3,
    t_21.o_orderpriority,
    t_18.col_0,
    t_16.col_0,
    t_25.c_address,
    t_21.o_totalprice,
    t_25.c_mktsegment,
    t_25.c_name
HAVING
    SMALLINT '27538' = t_20.col_0;
