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
CREATE MATERIALIZED VIEW m0 AS WITH with_0 AS (WITH with_1 AS (SELECT (((SMALLINT '199') & CAST(true AS INT)) % t_3.c_nationkey) AS col_0 FROM region AS t_2 LEFT JOIN customer AS t_3 ON t_2.r_comment = t_3.c_phone AND ((FLOAT '75') <= (25)) WHERE true GROUP BY t_3.c_custkey, t_3.c_nationkey HAVING ((REAL '945') <> (FLOAT '167'))) SELECT (SMALLINT '462') AS col_0, (FLOAT '643') AS col_1, '4Xr0xWNtAS' AS col_2 FROM with_1 WHERE true) SELECT (0) AS col_0, DATE '2022-07-10' AS col_1, ARRAY[(243724146)] AS col_2, (REAL '321853435') AS col_3 FROM with_0;
CREATE MATERIALIZED VIEW m1 AS SELECT (coalesce(NULL, NULL, NULL, NULL, NULL, NULL, NULL, t_0.ps_suppkey, NULL, NULL)) AS col_0 FROM partsupp AS t_0 FULL JOIN auction AS t_1 ON t_0.ps_comment = t_1.extra AND true GROUP BY t_0.ps_suppkey;
CREATE MATERIALIZED VIEW m2 AS WITH with_0 AS (SELECT t_3.id AS col_0 FROM person AS t_3 GROUP BY t_3.id) SELECT CAST(NULL AS STRUCT<a BIGINT, b CHARACTER VARYING, c TIMESTAMP>) AS col_0 FROM with_0;
CREATE MATERIALIZED VIEW m3 AS SELECT (CASE WHEN false THEN t_0.id WHEN (TIME '04:28:10' <> TIME '04:28:10') THEN ((SMALLINT '168') - (BIGINT '421')) WHEN (CASE WHEN false THEN (((SMALLINT '646') % (SMALLINT '705')) < t_0.id) ELSE false END) THEN (BIGINT '-8048938546888278633') ELSE t_0.id END) AS col_0, t_0.expires AS col_1, (CASE WHEN ((t_0.id / (SMALLINT '24')) = (1291045230)) THEN t_0.id WHEN false THEN t_0.id ELSE ((INT '134') % (BIGINT '669')) END) AS col_2 FROM auction AS t_0 LEFT JOIN customer AS t_1 ON t_0.item_name = t_1.c_phone WHERE (false) GROUP BY t_0.id, t_0.date_time, t_0.expires HAVING true;
CREATE MATERIALIZED VIEW m4 AS SELECT t_1.p_size AS col_0 FROM orders AS t_0 LEFT JOIN part AS t_1 ON t_0.o_clerk = t_1.p_brand WHERE true GROUP BY t_1.p_size, t_0.o_shippriority, t_0.o_comment HAVING true;
CREATE MATERIALIZED VIEW m5 AS SELECT t_0.reserve AS col_0, (SMALLINT '386') AS col_1, (INTERVAL '-604800') AS col_2, (FLOAT '1589434493') AS col_3 FROM auction AS t_0 FULL JOIN orders AS t_1 ON t_0.item_name = t_1.o_clerk GROUP BY t_1.o_totalprice, t_1.o_orderdate, t_0.date_time, t_0.id, t_0.description, t_0.reserve, t_0.extra, t_1.o_orderkey, t_1.o_orderstatus HAVING (t_0.extra) IN (t_1.o_orderstatus, t_1.o_orderstatus, t_1.o_orderstatus, (TRIM(LEADING 'RRsf3G01WQ' FROM 'QbOehqNaoL')), t_0.extra, t_0.description, 'xoBbSeSCNJ', (upper(t_0.description)));
CREATE MATERIALIZED VIEW m6 AS SELECT hop_0.bidder AS col_0 FROM hop(bid, bid.date_time, INTERVAL '440064', INTERVAL '11881728') AS hop_0 WHERE ((FLOAT '713') >= (REAL '-349402840')) GROUP BY hop_0.date_time, hop_0.channel, hop_0.bidder, hop_0.price HAVING false;
CREATE MATERIALIZED VIEW m7 AS SELECT t_1.col_1 AS col_0, (FLOAT '687') AS col_1 FROM m0 AS t_0 JOIN m0 AS t_1 ON t_0.col_2 = t_1.col_2 WHERE true GROUP BY t_1.col_1, t_0.col_0 HAVING (CAST((INT '0') AS BOOLEAN) AND false);
CREATE MATERIALIZED VIEW m8 AS SELECT (REAL '223') AS col_0, (DATE '2022-07-10' + t_0.c3) AS col_1 FROM alltypes1 AS t_0 WHERE t_0.c1 GROUP BY t_0.c7, t_0.c5, t_0.c9, t_0.c4, t_0.c3, t_0.c15;
CREATE MATERIALIZED VIEW m9 AS WITH with_0 AS (SELECT 'zyYSdFcSOA' AS col_0 FROM partsupp AS t_3 GROUP BY t_3.ps_availqty, t_3.ps_comment, t_3.ps_suppkey) SELECT (BIGINT '707') AS col_0, (BIGINT '219') AS col_1 FROM with_0;
