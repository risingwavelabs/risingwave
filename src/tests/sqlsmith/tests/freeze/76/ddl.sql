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
CREATE MATERIALIZED VIEW m0 AS SELECT (BIGINT '0') AS col_0 FROM tumble(auction, auction.date_time, INTERVAL '67') AS tumble_0 GROUP BY tumble_0.seller, tumble_0.expires, tumble_0.date_time, tumble_0.id;
CREATE MATERIALIZED VIEW m1 AS WITH with_0 AS (SELECT t_1.n_comment AS col_0 FROM nation AS t_1 JOIN nation AS t_2 ON t_1.n_comment = t_2.n_name WHERE (((REAL '1') - (REAL '614')) <= (FLOAT '370')) GROUP BY t_1.n_regionkey, t_1.n_name, t_1.n_comment, t_2.n_nationkey HAVING false) SELECT (SMALLINT '1') AS col_0, DATE '2022-07-14' AS col_1 FROM with_0;
CREATE MATERIALIZED VIEW m2 AS SELECT CAST(NULL AS STRUCT<a INTERVAL, b CHARACTER VARYING[], c INTERVAL>) AS col_0, ARRAY['zpWOnGpbT0', 'gZMC75hqvz', 'b3ziEDoCzN'] AS col_1 FROM tumble(alltypes2, alltypes2.c11, INTERVAL '53') AS tumble_0 WHERE tumble_0.c1 GROUP BY tumble_0.c14, tumble_0.c13, tumble_0.c16 HAVING ((REAL '-2147483648') <= (FLOAT '428'));
CREATE MATERIALIZED VIEW m3 AS SELECT (FLOAT '271') AS col_0, t_0.c_custkey AS col_1, t_0.c_address AS col_2, (md5(t_0.c_address)) AS col_3 FROM customer AS t_0 WHERE true GROUP BY t_0.c_address, t_0.c_custkey, t_0.c_mktsegment HAVING false;
CREATE MATERIALIZED VIEW m4 AS SELECT (TRIM(BOTH 'iKPS6VswLK' FROM 'crWviLOsX2')) AS col_0, t_2.s_acctbal AS col_1 FROM supplier AS t_2 GROUP BY t_2.s_acctbal, t_2.s_nationkey, t_2.s_phone, t_2.s_address;
CREATE MATERIALIZED VIEW m5 AS SELECT 'uy2bLLqc9Z' AS col_0, t_0.credit_card AS col_1, (FLOAT '-303123025') AS col_2, t_0.city AS col_3 FROM person AS t_0 GROUP BY t_0.city, t_0.credit_card, t_0.state HAVING true;
CREATE MATERIALIZED VIEW m6 AS SELECT t_0.c4 AS col_0 FROM alltypes1 AS t_0 GROUP BY t_0.c4, t_0.c9 HAVING CAST((INT '-907476031') AS BOOLEAN);
CREATE MATERIALIZED VIEW m8 AS SELECT hop_0.reserve AS col_0, TIME '06:25:31' AS col_1 FROM hop(auction, auction.date_time, INTERVAL '604800', INTERVAL '53827200') AS hop_0 GROUP BY hop_0.expires, hop_0.initial_bid, hop_0.reserve, hop_0.item_name;
CREATE MATERIALIZED VIEW m9 AS SELECT (t_1.bidder & ((INT '133') % (SMALLINT '381'))) AS col_0, '2FLDO548Ri' AS col_1, (TRIM(LEADING t_1.channel FROM t_1.channel)) AS col_2, t_1.channel AS col_3 FROM customer AS t_0 FULL JOIN bid AS t_1 ON t_0.c_mktsegment = t_1.url GROUP BY t_1.date_time, t_1.channel, t_1.bidder, t_1.url HAVING false;
