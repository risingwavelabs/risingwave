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
CREATE MATERIALIZED VIEW m0 AS SELECT (INT '849') AS col_0 FROM lineitem AS t_0 RIGHT JOIN nation AS t_1 ON t_0.l_suppkey = t_1.n_nationkey AND CAST(((SMALLINT '852') # t_1.n_regionkey) AS BOOLEAN) WHERE false GROUP BY t_1.n_nationkey, t_1.n_comment, t_1.n_regionkey, t_0.l_comment, t_0.l_shipinstruct, t_0.l_orderkey, t_0.l_suppkey;
CREATE MATERIALIZED VIEW m1 AS SELECT hop_0.auction AS col_0, hop_0.auction AS col_1, hop_0.auction AS col_2 FROM hop(bid, bid.date_time, INTERVAL '604800', INTERVAL '24192000') AS hop_0 GROUP BY hop_0.date_time, hop_0.auction;
CREATE MATERIALIZED VIEW m2 AS SELECT hop_0.auction AS col_0 FROM hop(bid, bid.date_time, INTERVAL '3600', INTERVAL '259200') AS hop_0 GROUP BY hop_0.auction, hop_0.extra, hop_0.channel HAVING false;
CREATE MATERIALIZED VIEW m3 AS SELECT ARRAY[TIMESTAMP '2022-04-27 07:31:49', TIMESTAMP '2022-04-27 07:32:48', TIMESTAMP '2022-04-27 06:32:49'] AS col_0, (TRIM(t_0.email_address)) AS col_1, t_0.email_address AS col_2, TIMESTAMP '2022-04-26 07:32:49' AS col_3 FROM person AS t_0 WHERE (NOT true) GROUP BY t_0.date_time, t_0.email_address, t_0.id, t_0.extra;
CREATE MATERIALIZED VIEW m5 AS SELECT true AS col_0, (250) AS col_1, (REAL '157') AS col_2, t_0.url AS col_3 FROM bid AS t_0 GROUP BY t_0.auction, t_0.date_time, t_0.extra, t_0.url;
CREATE MATERIALIZED VIEW m6 AS SELECT sq_2.col_0 AS col_0 FROM (SELECT 'XksbBviBG4' AS col_0 FROM bid AS t_0 JOIN customer AS t_1 ON t_0.channel = t_1.c_address GROUP BY t_0.url, t_0.price, t_1.c_phone, t_1.c_mktsegment, t_1.c_acctbal, t_1.c_nationkey, t_1.c_name, t_1.c_comment) AS sq_2 GROUP BY sq_2.col_0 HAVING true;
CREATE MATERIALIZED VIEW m7 AS SELECT CAST(NULL AS STRUCT<a CHARACTER VARYING, b TIMESTAMP, c CHARACTER VARYING>) AS col_0 FROM hop(m3, m3.col_3, INTERVAL '3600', INTERVAL '291600') AS hop_0 GROUP BY hop_0.col_2, hop_0.col_3 HAVING false;
CREATE MATERIALIZED VIEW m9 AS SELECT min(t_0.n_name) AS col_0, t_0.n_name AS col_1, t_0.n_name AS col_2 FROM nation AS t_0 RIGHT JOIN lineitem AS t_1 ON t_0.n_comment = t_1.l_linestatus WHERE true GROUP BY t_0.n_name HAVING false;
