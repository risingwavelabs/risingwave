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
CREATE MATERIALIZED VIEW m0 AS SELECT sq_1.col_2 AS col_0, (lower((TRIM(TRAILING (lower(sq_1.col_2)) FROM sq_1.col_2)))) AS col_1, sq_1.col_2 AS col_2 FROM (SELECT t_0.p_container AS col_0, (substr('VPxFNu6mVj', (INT '725'))) AS col_1, t_0.p_container AS col_2 FROM part AS t_0 GROUP BY t_0.p_size, t_0.p_container HAVING false) AS sq_1 GROUP BY sq_1.col_0, sq_1.col_2 HAVING false;
CREATE MATERIALIZED VIEW m1 AS SELECT sq_3.col_1 AS col_0, (BIGINT '707') AS col_1, sq_3.col_1 AS col_2 FROM (SELECT sq_2.col_2 AS col_0, (0) AS col_1, (((INTERVAL '140322') + sq_2.col_2) - (INTERVAL '1')) AS col_2 FROM (SELECT t_1.n_name AS col_0, (FLOAT '-2147483648') AS col_1, TIME '23:35:45' AS col_2 FROM region AS t_0 LEFT JOIN nation AS t_1 ON t_0.r_name = t_1.n_comment GROUP BY t_1.n_name, t_0.r_name HAVING ((SMALLINT '0') < (FLOAT '157'))) AS sq_2 WHERE false GROUP BY sq_2.col_2, sq_2.col_1 HAVING false) AS sq_3 WHERE false GROUP BY sq_3.col_1;
CREATE MATERIALIZED VIEW m2 AS SELECT (BIGINT '403') AS col_0, hop_0.c1 AS col_1 FROM hop(alltypes1, alltypes1.c11, INTERVAL '493147', INTERVAL '48328406') AS hop_0 GROUP BY hop_0.c1, hop_0.c5 HAVING false;
CREATE MATERIALIZED VIEW m3 AS SELECT (TRIM((to_char(DATE '2022-03-10', (md5(hop_0.url)))))) AS col_0 FROM hop(bid, bid.date_time, INTERVAL '1', INTERVAL '9') AS hop_0 GROUP BY hop_0.url HAVING ((BIGINT '55') >= (SMALLINT '128'));
CREATE MATERIALIZED VIEW m4 AS SELECT tumble_0.c10 AS col_0 FROM tumble(alltypes1, alltypes1.c11, INTERVAL '87') AS tumble_0 GROUP BY tumble_0.c6, tumble_0.c10, tumble_0.c5, tumble_0.c15, tumble_0.c3 HAVING false;
CREATE MATERIALIZED VIEW m6 AS SELECT (CASE WHEN false THEN ((59) - ((INT '-1612165426') | (INT '938'))) ELSE (t_0.p_retailprice + (SMALLINT '158')) END) AS col_0, (SMALLINT '394') AS col_1, 'q1TkSmIkXn' AS col_2, ('v6qxKvB5rf') AS col_3 FROM part AS t_0 GROUP BY t_0.p_retailprice, t_0.p_comment, t_0.p_name HAVING false;
CREATE MATERIALIZED VIEW m7 AS SELECT t_2.col_0 AS col_0 FROM m0 AS t_2 GROUP BY t_2.col_0;
CREATE MATERIALIZED VIEW m8 AS WITH with_0 AS (WITH with_1 AS (SELECT t_2.c15 AS col_0 FROM alltypes2 AS t_2 WHERE t_2.c1 GROUP BY t_2.c5, t_2.c15, t_2.c7, t_2.c11, t_2.c13, t_2.c1) SELECT 'qh9dtyzdoU' AS col_0, TIME '00:34:48' AS col_1 FROM with_1) SELECT TIME '00:35:48' AS col_0, ARRAY[(SMALLINT '936'), (SMALLINT '193')] AS col_1, (concat('IqpWgHSw28', 'MKvmC6FDSA')) AS col_2 FROM with_0;
CREATE MATERIALIZED VIEW m9 AS SELECT ((INT '0') + DATE '2022-03-16') AS col_0, false AS col_1, t_0.l_linestatus AS col_2, (((REAL '688') + (REAL '152')) + (REAL '492')) AS col_3 FROM lineitem AS t_0 GROUP BY t_0.l_linestatus, t_0.l_quantity, t_0.l_linenumber, t_0.l_shipdate, t_0.l_orderkey;
