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
CREATE MATERIALIZED VIEW m0 AS SELECT t_0.s_suppkey AS col_0, (CAST(NULL AS STRUCT<a CHARACTER VARYING, b INT>)) AS col_1, (FLOAT '238') AS col_2 FROM supplier AS t_0 JOIN region AS t_1 ON t_0.s_address = t_1.r_name GROUP BY t_0.s_suppkey, t_1.r_comment, t_0.s_acctbal, t_0.s_address, t_1.r_regionkey HAVING false;
CREATE MATERIALIZED VIEW m1 AS SELECT ((SMALLINT '552') | (BIGINT '573')) AS col_0, (BIGINT '0') AS col_1, (INTERVAL '0') AS col_2, (BIGINT '1') AS col_3 FROM (SELECT t_1.price AS col_0 FROM customer AS t_0 FULL JOIN bid AS t_1 ON t_0.c_phone = t_1.extra GROUP BY t_1.price HAVING false) AS sq_2 WHERE (((1) - (SMALLINT '32767')) = (sq_2.col_0 & (INT '-2147483648'))) GROUP BY sq_2.col_0;
CREATE MATERIALIZED VIEW m2 AS WITH with_0 AS (SELECT t_2.c5 AS col_0 FROM region AS t_1 LEFT JOIN alltypes1 AS t_2 ON t_1.r_comment = t_2.c9 AND t_2.c1 WHERE t_2.c1 GROUP BY t_2.c6, t_2.c3, t_2.c5, t_2.c16, t_2.c1, t_2.c7, t_2.c2, t_2.c14, t_2.c9 HAVING (true)) SELECT false AS col_0 FROM with_0;
CREATE MATERIALIZED VIEW m3 AS SELECT (t_1.c7 / ((SMALLINT '705') & (SMALLINT '28844'))) AS col_0 FROM nation AS t_0 FULL JOIN alltypes1 AS t_1 ON t_0.n_comment = t_1.c9 WHERE t_1.c1 GROUP BY t_1.c3, t_1.c7, t_1.c6, t_1.c14, t_1.c5 HAVING true;
CREATE MATERIALIZED VIEW m4 AS SELECT (t_0.l_tax * (SMALLINT '402')) AS col_0 FROM lineitem AS t_0 JOIN nation AS t_1 ON t_0.l_partkey = t_1.n_regionkey AND true WHERE false GROUP BY t_0.l_partkey, t_0.l_shipmode, t_0.l_shipdate, t_0.l_tax, t_0.l_quantity HAVING (t_0.l_tax <= (REAL '-1390685415'));
CREATE MATERIALIZED VIEW m5 AS SELECT ((t_0.col_2 * (REAL '1')) / t_0.col_2) AS col_0, t_0.col_2 AS col_1, t_0.col_2 AS col_2, t_0.col_1 AS col_3 FROM m0 AS t_0 GROUP BY t_0.col_1, t_0.col_2 HAVING false;
CREATE MATERIALIZED VIEW m6 AS SELECT (INTERVAL '0') AS col_0, ((INT '86') / hop_0.auction) AS col_1, hop_0.auction AS col_2 FROM hop(bid, bid.date_time, INTERVAL '3600', INTERVAL '334800') AS hop_0 WHERE true GROUP BY hop_0.auction HAVING false;
CREATE MATERIALIZED VIEW m7 AS SELECT (BIGINT '0') AS col_0, t_2.auction AS col_1, t_2.auction AS col_2 FROM bid AS t_2 WHERE ('KaShBi9us8') NOT IN (t_2.url, t_2.extra, t_2.extra, (upper(t_2.channel)), (TRIM(LEADING 'DpFnmFxSgj' FROM 'EbmnXMdgfr')), t_2.channel) GROUP BY t_2.auction;
CREATE MATERIALIZED VIEW m8 AS SELECT (INT '151') AS col_0, t_1.ps_comment AS col_1, (replace(t_1.ps_comment, (OVERLAY(t_1.ps_comment PLACING (TRIM(LEADING 'uOsPdxVUpb' FROM 'HTrxfqeUpq')) FROM (INT '41') FOR t_1.ps_partkey)), 'Ds0hJDo5RU')) AS col_2 FROM m0 AS t_0 FULL JOIN partsupp AS t_1 ON t_0.col_0 = t_1.ps_suppkey AND true WHERE false GROUP BY t_1.ps_comment, t_1.ps_partkey HAVING false;
CREATE MATERIALIZED VIEW m9 AS SELECT (TRIM(TRAILING (split_part('PzyAhwAbhJ', (OVERLAY(tumble_0.city PLACING (OVERLAY(tumble_0.extra PLACING tumble_0.extra FROM (INT '984'))) FROM ((INT '445') # ((SMALLINT '40') - (SMALLINT '32767'))) FOR (INT '111'))), (INT '1625858852'))) FROM '06VCSpeZjE')) AS col_0, (OVERLAY(('C0NwxTT1Xf') PLACING tumble_0.city FROM (INT '0'))) AS col_1 FROM tumble(person, person.date_time, INTERVAL '76') AS tumble_0 WHERE true GROUP BY tumble_0.city, tumble_0.extra, tumble_0.name;
