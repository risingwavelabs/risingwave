CREATE TABLE supplier (s_suppkey INT, s_name CHARACTER VARYING, s_address CHARACTER VARYING, s_nationkey INT, s_phone CHARACTER VARYING, s_acctbal NUMERIC, s_comment CHARACTER VARYING, PRIMARY KEY (s_suppkey));
CREATE TABLE part (p_partkey INT, p_name CHARACTER VARYING, p_mfgr CHARACTER VARYING, p_brand CHARACTER VARYING, p_type CHARACTER VARYING, p_size INT, p_container CHARACTER VARYING, p_retailprice NUMERIC, p_comment CHARACTER VARYING, PRIMARY KEY (p_partkey));
CREATE TABLE partsupp (ps_partkey INT, ps_suppkey INT, ps_availqty INT, ps_supplycost NUMERIC, ps_comment CHARACTER VARYING, PRIMARY KEY (ps_partkey, ps_suppkey));
CREATE TABLE customer (c_custkey INT, c_name CHARACTER VARYING, c_address CHARACTER VARYING, c_nationkey INT, c_phone CHARACTER VARYING, c_acctbal NUMERIC, c_mktsegment CHARACTER VARYING, c_comment CHARACTER VARYING, PRIMARY KEY (c_custkey));
CREATE TABLE orders (o_orderkey BIGINT, o_custkey INT, o_orderstatus CHARACTER VARYING, o_totalprice NUMERIC, o_orderdate DATE, o_orderpriority CHARACTER VARYING, o_clerk CHARACTER VARYING, o_shippriority INT, o_comment CHARACTER VARYING, PRIMARY KEY (o_orderkey));
CREATE TABLE lineitem (l_orderkey BIGINT, l_partkey INT, l_suppkey INT, l_linenumber INT, l_quantity NUMERIC, l_extendedprice NUMERIC, l_discount NUMERIC, l_tax NUMERIC, l_returnflag CHARACTER VARYING, l_linestatus CHARACTER VARYING, l_shipdate DATE, l_commitdate DATE, l_receiptdate DATE, l_shipinstruct CHARACTER VARYING, l_shipmode CHARACTER VARYING, l_comment CHARACTER VARYING, PRIMARY KEY (l_orderkey, l_linenumber));
CREATE TABLE nation (n_nationkey INT, n_name CHARACTER VARYING, n_regionkey INT, n_comment CHARACTER VARYING, PRIMARY KEY (n_nationkey));
CREATE TABLE region (r_regionkey INT, r_name CHARACTER VARYING, r_comment CHARACTER VARYING, PRIMARY KEY (r_regionkey));
CREATE TABLE person (id BIGINT, name CHARACTER VARYING, email_address CHARACTER VARYING, credit_card CHARACTER VARYING, city CHARACTER VARYING, state CHARACTER VARYING, date_time TIMESTAMP, extra CHARACTER VARYING, PRIMARY KEY (id));
CREATE TABLE auction (id BIGINT, item_name CHARACTER VARYING, description CHARACTER VARYING, initial_bid BIGINT, reserve BIGINT, date_time TIMESTAMP, expires TIMESTAMP, seller BIGINT, category BIGINT, extra CHARACTER VARYING, PRIMARY KEY (id));
CREATE TABLE bid (auction BIGINT, bidder BIGINT, price BIGINT, channel CHARACTER VARYING, url CHARACTER VARYING, date_time TIMESTAMP, extra CHARACTER VARYING);
CREATE TABLE alltypes1 (c1 BOOLEAN, c2 SMALLINT, c3 INT, c4 BIGINT, c5 REAL, c6 DOUBLE, c7 NUMERIC, c8 DATE, c9 CHARACTER VARYING, c10 TIME, c11 TIMESTAMP, c13 INTERVAL, c14 STRUCT<a INT>, c15 INT[], c16 CHARACTER VARYING[]);
CREATE TABLE alltypes2 (c1 BOOLEAN, c2 SMALLINT, c3 INT, c4 BIGINT, c5 REAL, c6 DOUBLE, c7 NUMERIC, c8 DATE, c9 CHARACTER VARYING, c10 TIME, c11 TIMESTAMP, c13 INTERVAL, c14 STRUCT<a INT>, c15 INT[], c16 CHARACTER VARYING[]);
CREATE MATERIALIZED VIEW m0 AS SELECT t_0.ps_availqty AS col_0, t_0.ps_suppkey AS col_1 FROM partsupp AS t_0 GROUP BY t_0.ps_suppkey, t_0.ps_availqty, t_0.ps_comment HAVING true;
CREATE MATERIALIZED VIEW m1 AS SELECT t_0.col_1 AS col_0 FROM m0 AS t_0 GROUP BY t_0.col_1;
CREATE MATERIALIZED VIEW m2 AS SELECT (coalesce(NULL, NULL, NULL, NULL, t_0.c_name, NULL, NULL, NULL, NULL, NULL)) AS col_0 FROM customer AS t_0 GROUP BY t_0.c_address, t_0.c_mktsegment, t_0.c_name, t_0.c_acctbal HAVING true;
CREATE MATERIALIZED VIEW m4 AS SELECT t_1.c13 AS col_0, t_1.c4 AS col_1, t_1.c4 AS col_2 FROM region AS t_0 LEFT JOIN alltypes1 AS t_1 ON t_0.r_comment = t_1.c9 AND (t_1.c2 >= t_1.c5) GROUP BY t_1.c14, t_1.c13, t_1.c8, t_0.r_regionkey, t_1.c11, t_1.c5, t_1.c16, t_1.c4, t_0.r_comment HAVING true;
CREATE MATERIALIZED VIEW m5 AS SELECT sq_1.col_0 AS col_0 FROM (SELECT t_0.col_0 AS col_0 FROM m1 AS t_0 WHERE (CASE WHEN true THEN (((227) <> t_0.col_0) IS NOT TRUE) WHEN ((699) < (SMALLINT '195')) THEN (true IS FALSE) ELSE false END) GROUP BY t_0.col_0 HAVING false) AS sq_1 GROUP BY sq_1.col_0;
CREATE MATERIALIZED VIEW m6 AS SELECT sq_3.col_1 AS col_0, (66) AS col_1, sq_3.col_1 AS col_2 FROM (WITH with_0 AS (SELECT 'Rx5UHXFMRj' AS col_0, (substr(t_2.c_phone, t_1.r_regionkey)) AS col_1, (t_1.r_regionkey % (CASE WHEN CAST(t_1.r_regionkey AS BOOLEAN) THEN (SMALLINT '0') ELSE (SMALLINT '179') END)) AS col_2 FROM region AS t_1 JOIN customer AS t_2 ON t_1.r_name = t_2.c_phone AND true WHERE true GROUP BY t_2.c_acctbal, t_1.r_regionkey, t_2.c_phone, t_1.r_name, t_2.c_mktsegment, t_2.c_name HAVING false) SELECT (FLOAT '441') AS col_0, (146) AS col_1, TIMESTAMP '2022-09-29 18:58:20' AS col_2 FROM with_0 WHERE true) AS sq_3 GROUP BY sq_3.col_1, sq_3.col_0 HAVING true;
CREATE MATERIALIZED VIEW m7 AS WITH with_0 AS (SELECT (split_part(min((TRIM(TRAILING t_1.l_shipinstruct FROM t_1.l_linestatus))) FILTER(WHERE true), 'OtE3CYlRZl', t_1.l_partkey)) AS col_0, (replace('ta3C4jMRhC', (to_char((t_1.l_suppkey + DATE '2022-09-29'), t_1.l_shipinstruct)), t_1.l_shipinstruct)) AS col_1, 'bgK5b9hGC3' AS col_2, 'qWkrmVWrio' AS col_3 FROM lineitem AS t_1 LEFT JOIN m2 AS t_2 ON t_1.l_returnflag = t_2.col_0 WHERE (t_1.l_shipdate <= TIMESTAMP '2022-09-18 11:28:15') GROUP BY t_2.col_0, t_1.l_suppkey, t_1.l_quantity, t_1.l_shipinstruct, t_1.l_discount, t_1.l_extendedprice, t_1.l_linestatus, t_1.l_tax, t_1.l_partkey HAVING true) SELECT 'rZjhzTuJ6J' AS col_0, TIMESTAMP '2022-09-29 20:09:19' AS col_1, ((REAL '974') - (REAL '933')) AS col_2 FROM with_0;
CREATE MATERIALIZED VIEW m8 AS SELECT (upper(t_0.c_mktsegment)) AS col_0, false AS col_1, t_0.c_phone AS col_2 FROM customer AS t_0 FULL JOIN m2 AS t_1 ON t_0.c_name = t_1.col_0 AND true WHERE false GROUP BY t_0.c_phone, t_0.c_mktsegment, t_0.c_name;
CREATE MATERIALIZED VIEW m9 AS SELECT (substr(t_1.p_brand, ((INT '324')))) AS col_0, t_0.n_nationkey AS col_1 FROM nation AS t_0 FULL JOIN part AS t_1 ON t_0.n_regionkey = t_1.p_size GROUP BY t_1.p_brand, t_1.p_mfgr, t_0.n_comment, t_0.n_nationkey, t_1.p_partkey HAVING true;
