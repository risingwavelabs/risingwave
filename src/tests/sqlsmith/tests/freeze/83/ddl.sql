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
CREATE MATERIALIZED VIEW m0 AS SELECT t_1.c_custkey AS col_0, t_0.c10 AS col_1 FROM alltypes2 AS t_0 FULL JOIN customer AS t_1 ON t_0.c9 = t_1.c_comment GROUP BY t_1.c_custkey, t_0.c7, t_0.c3, t_0.c10, t_0.c16;
CREATE MATERIALIZED VIEW m1 AS SELECT ((hop_0.c3 << hop_0.c2) * hop_0.c3) AS col_0, (0) AS col_1, hop_0.c13 AS col_2 FROM hop(alltypes1, alltypes1.c11, INTERVAL '86400', INTERVAL '7084800') AS hop_0 WHERE hop_0.c1 GROUP BY hop_0.c3, hop_0.c13, hop_0.c2, hop_0.c7;
CREATE MATERIALIZED VIEW m2 AS SELECT tumble_0.initial_bid AS col_0, (BIGINT '314') AS col_1, tumble_0.initial_bid AS col_2, ((INT '603') + tumble_0.initial_bid) AS col_3 FROM tumble(auction, auction.date_time, INTERVAL '71') AS tumble_0 GROUP BY tumble_0.initial_bid;
CREATE MATERIALIZED VIEW m3 AS SELECT t_0.l_shipdate AS col_0, '9kOAMpJ6WD' AS col_1, t_0.l_shipmode AS col_2 FROM lineitem AS t_0 WHERE false GROUP BY t_0.l_discount, t_0.l_shipdate, t_0.l_quantity, t_0.l_extendedprice, t_0.l_shipmode, t_0.l_partkey HAVING true;
CREATE MATERIALIZED VIEW m4 AS SELECT t_1.name AS col_0, (substr('8gCJwJD9FX', (INT '-1442781915'), (coalesce(NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, (INT '502'), NULL)))) AS col_1, t_1.date_time AS col_2, (split_part(t_1.name, t_1.name, (SMALLINT '770'))) AS col_3 FROM nation AS t_0 JOIN person AS t_1 ON t_0.n_name = t_1.email_address GROUP BY t_1.date_time, t_1.name;
CREATE MATERIALIZED VIEW m5 AS SELECT sq_4.col_1 AS col_0, sq_4.col_1 AS col_1 FROM (WITH with_0 AS (SELECT sq_3.col_2 AS col_0, sq_3.col_0 AS col_1, (sq_3.col_2 - ((INTERVAL '-604800') * (SMALLINT '159'))) AS col_2, sq_3.col_0 AS col_3 FROM (SELECT sq_2.col_0 AS col_0, ((INTERVAL '3600') + DATE '2022-06-28') AS col_1, sq_2.col_0 AS col_2, sq_2.col_0 AS col_3 FROM (SELECT hop_1.c11 AS col_0, hop_1.c10 AS col_1 FROM hop(alltypes2, alltypes2.c11, INTERVAL '3600', INTERVAL '108000') AS hop_1 WHERE hop_1.c1 GROUP BY hop_1.c2, hop_1.c15, hop_1.c6, hop_1.c8, hop_1.c11, hop_1.c16, hop_1.c10 HAVING false) AS sq_2 WHERE (true) GROUP BY sq_2.col_0 HAVING true) AS sq_3 WHERE (true) GROUP BY sq_3.col_0, sq_3.col_2 HAVING (DATE '2022-06-28' IS NOT NULL)) SELECT DATE '2022-06-28' AS col_0, (SMALLINT '651') AS col_1 FROM with_0) AS sq_4 WHERE false GROUP BY sq_4.col_1 HAVING false;
CREATE MATERIALIZED VIEW m6 AS SELECT hop_0.name AS col_0, hop_0.id AS col_1 FROM hop(person, person.date_time, INTERVAL '3600', INTERVAL '327600') AS hop_0 WHERE false GROUP BY hop_0.name, hop_0.id, hop_0.extra, hop_0.credit_card HAVING (((coalesce(NULL, (FLOAT '269'), NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)) + (REAL '868')) >= ((SMALLINT '-32768') / (INT '861')));
CREATE MATERIALIZED VIEW m8 AS SELECT (CASE WHEN max(true) THEN t_0.l_suppkey ELSE (INT '136') END) AS col_0, t_0.l_orderkey AS col_1, t_0.l_shipmode AS col_2 FROM lineitem AS t_0 LEFT JOIN bid AS t_1 ON t_0.l_returnflag = t_1.extra WHERE CAST(((SMALLINT '32767') - (INT '800')) AS BOOLEAN) GROUP BY t_0.l_tax, t_0.l_linestatus, t_1.url, t_1.auction, t_0.l_shipmode, t_0.l_commitdate, t_0.l_extendedprice, t_0.l_orderkey, t_0.l_suppkey HAVING false;
CREATE MATERIALIZED VIEW m9 AS SELECT ('ERKL3eVbbP') AS col_0, CAST(NULL AS STRUCT<a DATE, b CHARACTER VARYING, c CHARACTER VARYING>) AS col_1, tumble_0.url AS col_2, (lower(tumble_0.url)) AS col_3 FROM tumble(bid, bid.date_time, INTERVAL '33') AS tumble_0 GROUP BY tumble_0.url;
