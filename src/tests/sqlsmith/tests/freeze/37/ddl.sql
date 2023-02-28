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
CREATE MATERIALIZED VIEW m0 AS SELECT 'jLnGccuhja' AS col_0, (TIME '00:30:52' + DATE '2022-04-29') AS col_1 FROM hop(auction, auction.expires, INTERVAL '141773', INTERVAL '7372196') AS hop_0 WHERE ((INT '-1291429018') <= (INT '354')) GROUP BY hop_0.seller, hop_0.date_time;
CREATE MATERIALIZED VIEW m1 AS SELECT ((SMALLINT '1') + t_0.ps_suppkey) AS col_0, t_0.ps_supplycost AS col_1, (((SMALLINT '32767') | (SMALLINT '1')) + ((SMALLINT '0') + (28))) AS col_2 FROM partsupp AS t_0 WHERE (CASE WHEN ((INT '828') <= t_0.ps_supplycost) THEN true ELSE false END) GROUP BY t_0.ps_suppkey, t_0.ps_supplycost, t_0.ps_partkey HAVING ((SMALLINT '896') >= (((SMALLINT '0') - t_0.ps_suppkey) + t_0.ps_supplycost));
CREATE MATERIALIZED VIEW m2 AS SELECT (BIGINT '649') AS col_0, t_0.col_2 AS col_1 FROM m1 AS t_0 WHERE true GROUP BY t_0.col_2;
CREATE MATERIALIZED VIEW m3 AS SELECT hop_0.c7 AS col_0, (FLOAT '634') AS col_1 FROM hop(alltypes2, alltypes2.c11, INTERVAL '1', INTERVAL '49') AS hop_0 GROUP BY hop_0.c7, hop_0.c6;
CREATE MATERIALIZED VIEW m4 AS SELECT ((INT '81') & (INT '2147483647')) AS col_0 FROM supplier AS t_0 JOIN partsupp AS t_1 ON t_0.s_nationkey = t_1.ps_suppkey WHERE false GROUP BY t_0.s_comment;
CREATE MATERIALIZED VIEW m5 AS SELECT t_1.col_0 AS col_0, ((INT '280975978') / (t_1.col_0 | ((SMALLINT '32767') # (SMALLINT '382')))) AS col_1, (BIGINT '1') AS col_2 FROM m2 AS t_0 RIGHT JOIN m2 AS t_1 ON t_0.col_1 = t_1.col_1 GROUP BY t_1.col_0 HAVING false;
CREATE MATERIALIZED VIEW m6 AS WITH with_0 AS (WITH with_1 AS (SELECT TIME '16:23:07' AS col_0, (INT '590') AS col_1, t_2.col_0 AS col_2 FROM m0 AS t_2 WHERE true GROUP BY t_2.col_0) SELECT (BIGINT '559') AS col_0, (true) AS col_1 FROM with_1 WHERE true) SELECT (SMALLINT '569') AS col_0, false AS col_1 FROM with_0 WHERE true;
CREATE MATERIALIZED VIEW m7 AS SELECT (INT '2147483647') AS col_0, ((INT '622') & (SMALLINT '540')) AS col_1 FROM partsupp AS t_0 RIGHT JOIN m3 AS t_1 ON t_0.ps_supplycost = t_1.col_0 AND (((SMALLINT '100') = ((FLOAT '825'))) < true) GROUP BY t_0.ps_comment, t_0.ps_partkey HAVING true;
CREATE MATERIALIZED VIEW m8 AS SELECT tumble_0.c2 AS col_0, false AS col_1, sum(((FLOAT '555'))) AS col_2 FROM tumble(alltypes1, alltypes1.c11, INTERVAL '12') AS tumble_0 WHERE tumble_0.c1 GROUP BY tumble_0.c2, tumble_0.c14, tumble_0.c3, tumble_0.c4, tumble_0.c5, tumble_0.c1, tumble_0.c8, tumble_0.c6 HAVING (false);
CREATE MATERIALIZED VIEW m9 AS SELECT ((298)) AS col_0, (632) AS col_1, ((t_0.col_2 - (BIGINT '301')) - t_0.col_2) AS col_2 FROM m1 AS t_0 JOIN supplier AS t_1 ON t_0.col_1 = t_1.s_acctbal WHERE TIMESTAMP '2022-04-22 00:31:56' IN (SELECT ((INTERVAL '3600') + DATE '2022-04-29') AS col_0 FROM hop(alltypes2, alltypes2.c11, INTERVAL '1', INTERVAL '10') AS hop_2 GROUP BY hop_2.c7, hop_2.c16, hop_2.c14, hop_2.c4, hop_2.c11, hop_2.c15, hop_2.c9) GROUP BY t_0.col_1, t_1.s_name, t_0.col_2, t_1.s_address, t_1.s_nationkey HAVING true;
