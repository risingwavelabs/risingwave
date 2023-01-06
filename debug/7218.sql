CREATE TABLE supplier (s_suppkey INT, s_name CHARACTER VARYING, s_address CHARACTER VARYING, s_nationkey INT, s_phone CHARACTER VARYING, s_acctbal NUMERIC, s_comment CHARACTER VARYING, PRIMARY KEY (s_suppkey));CREATE TABLE part (p_partkey INT, p_name CHARACTER VARYING, p_mfgr CHARACTER VARYING, p_brand CHARACTER VARYING, p_type CHARACTER VARYING, p_size INT, p_container CHARACTER VARYING, p_retailprice NUMERIC, p_comment CHARACTER VARYING, PRIMARY KEY (p_partkey));CREATE TABLE partsupp (ps_partkey INT, ps_suppkey INT, ps_availqty INT, ps_supplycost NUMERIC, ps_comment CHARACTER VARYING, PRIMARY KEY (ps_partkey, ps_suppkey));CREATE TABLE customer (c_custkey INT, c_name CHARACTER VARYING, c_address CHARACTER VARYING, c_nationkey INT, c_phone CHARACTER VARYING, c_acctbal NUMERIC, c_mktsegment CHARACTER VARYING, c_comment CHARACTER VARYING, PRIMARY KEY (c_custkey));CREATE TABLE orders (o_orderkey BIGINT, o_custkey INT, o_orderstatus CHARACTER VARYING, o_totalprice NUMERIC, o_orderdate DATE, o_orderpriority CHARACTER VARYING, o_clerk CHARACTER VARYING, o_shippriority INT, o_comment CHARACTER VARYING, PRIMARY KEY (o_orderkey));CREATE TABLE lineitem (l_orderkey BIGINT, l_partkey INT, l_suppkey INT, l_linenumber INT, l_quantity NUMERIC, l_extendedprice NUMERIC, l_discount NUMERIC, l_tax NUMERIC, l_returnflag CHARACTER VARYING, l_linestatus CHARACTER VARYING, l_shipdate DATE, l_commitdate DATE, l_receiptdate DATE, l_shipinstruct CHARACTER VARYING, l_shipmode CHARACTER VARYING, l_comment CHARACTER VARYING, PRIMARY KEY (l_orderkey, l_linenumber));CREATE TABLE nation (n_nationkey INT, n_name CHARACTER VARYING, n_regionkey INT, n_comment CHARACTER VARYING, PRIMARY KEY (n_nationkey));CREATE TABLE region (r_regionkey INT, r_name CHARACTER VARYING, r_comment CHARACTER VARYING, PRIMARY KEY (r_regionkey));CREATE TABLE person (id BIGINT, name CHARACTER VARYING, email_address CHARACTER VARYING, credit_card CHARACTER VARYING, city CHARACTER VARYING, state CHARACTER VARYING, date_time TIMESTAMP, extra CHARACTER VARYING, PRIMARY KEY (id));CREATE TABLE auction (id BIGINT, item_name CHARACTER VARYING, description CHARACTER VARYING, initial_bid BIGINT, reserve BIGINT, date_time TIMESTAMP, expires TIMESTAMP, seller BIGINT, category BIGINT, extra CHARACTER VARYING, PRIMARY KEY (id));CREATE TABLE bid (auction BIGINT, bidder BIGINT, price BIGINT, channel CHARACTER VARYING, url CHARACTER VARYING, date_time TIMESTAMP, extra CHARACTER VARYING);CREATE TABLE alltypes1 (c1 BOOLEAN, c2 SMALLINT, c3 INT, c4 BIGINT, c5 REAL, c6 DOUBLE, c7 NUMERIC, c8 DATE, c9 CHARACTER VARYING, c10 TIME, c11 TIMESTAMP, c13 INTERVAL, c14 STRUCT<a INT>, c15 INT[], c16 CHARACTER VARYING[]);CREATE TABLE alltypes2 (c1 BOOLEAN, c2 SMALLINT, c3 INT, c4 BIGINT, c5 REAL, c6 DOUBLE, c7 NUMERIC, c8 DATE, c9 CHARACTER VARYING, c10 TIME, c11 TIMESTAMP, c13 INTERVAL, c14 STRUCT<a INT>, c15 INT[], c16 CHARACTER VARYING[]);CREATE MATERIALIZED VIEW m0 AS SELECT true AS col_0 FROM auction AS t_0 GROUP BY t_0.initial_bid, t_0.description, t_0.date_time, t_0.seller, t_0.item_name;CREATE MATERIALIZED VIEW m1 AS SELECT TIMESTAMP '2022-02-27 05:10:10' AS col_0, NULL AS col_1, ~ ((SMALLINT '32670' # CASE WHEN false THEN SMALLINT '1057' & SMALLINT '0' WHEN true THEN SMALLINT '32767' WHEN false THEN SMALLINT '1' / coalesce(NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, SMALLINT '1') WHEN true THEN SMALLINT '32767' WHEN true THEN SMALLINT '1' >> SMALLINT '11401' WHEN 2147483647 <= t_0.p_retailprice THEN SMALLINT '1' ELSE SMALLINT '32767' END) & coalesce(NULL, NULL, NULL, NULL, NULL, SMALLINT '11550', NULL, NULL, NULL, NULL)) AS col_2 FROM part AS t_0 GROUP BY t_0.p_mfgr, t_0.p_name, t_0.p_container, t_0.p_size, t_0.p_comment, t_0.p_retailprice, t_0.p_brand;CREATE MATERIALIZED VIEW m2 AS SELECT INTERVAL '60' AS col_0, INTERVAL '604800' <> INTERVAL '60' AS col_1 FROM customer AS t_0 GROUP BY t_0.c_nationkey, t_0.c_acctbal, t_0.c_address, t_0.c_name, t_0.c_phone, t_0.c_custkey, t_0.c_comment;CREATE MATERIALIZED VIEW m3 AS SELECT TIME '05:10:11' AS col_0 FROM tumble(person, person.date_time, INTERVAL '206647') AS tumble_0 GROUP BY tumble_0.id, tumble_0.date_time, tumble_0.credit_card, tumble_0.name, tumble_0.city, tumble_0.extra, tumble_0.email_address, tumble_0.state HAVING false;CREATE MATERIALIZED VIEW m4 AS SELECT ARRAY[INT '2147483647', INT '2147483647', INT '1', INT '1451014492', INT '1', INT '0', INT '2147483647', INT '555425957', INT '0', INT '0', INT '1937923287', INT '0', INT '1', INT '0', INT '2003205374', INT '1', INT '0', INT '0', INT '468705480', INT '1', INT '2147483647', INT '1658121835', INT '1031383982', INT '1', INT '0', INT '2147483647', INT '1128406194', INT '1089949046', INT '1957588474', INT '1753112487', INT '1628151295', INT '1529464022', INT '1', INT '1250500136', INT '0', INT '1', INT '1', INT '1963547774', INT '1495777790', INT '2147483647', INT '383937894', INT '0', INT '1', INT '1', INT '358635313', INT '2147483647', INT '1', INT '0', INT '1363287582', INT '0', INT '0', INT '0', INT '1', INT '1302459441', INT '516832395', INT '2147483647', INT '1416847540', INT '2147483647', INT '0', INT '1353052791', INT '0', INT '1', INT '1', INT '1', INT '1853135679', INT '2147483647', INT '46300865', INT '1560482848', INT '0', INT '1022238042', INT '0', INT '386693543', INT '0', INT '2147483647', INT '0', INT '2147483647', INT '1982988897', INT '265031103', INT '868154780', INT '1', INT '2147483647', INT '2147483647', INT '1881007605', INT '1598758405', INT '6032928', INT '2147483647', INT '334379161', INT '1', INT '0', INT '1932152366', INT '1'] AS col_0, md5(t_0.channel) AS col_1, REAL '777557513.7083797' + REAL '0' AS col_2, TIMESTAMP '2022-02-27 05:10:11' AS col_3 FROM bid AS t_0 GROUP BY t_0.date_time, t_0.channel HAVING false;CREATE MATERIALIZED VIEW m5 AS SELECT TIMESTAMP '2022-03-06 05:10:11' AS col_0, REAL '2147483647' AS col_1 FROM person AS t_0 GROUP BY t_0.city, t_0.name, t_0.state, t_0.credit_card, t_0.email_address, t_0.extra, t_0.id, t_0.date_time HAVING false;CREATE MATERIALIZED VIEW m6 AS SELECT INT '2147483647' AS col_0, DATE '2022-03-06' AS col_1 FROM auction AS t_0 WHERE true GROUP BY t_0.id, t_0.reserve, t_0.initial_bid, t_0.extra, t_0.category;CREATE MATERIALIZED VIEW m7 AS SELECT 6151355912033718930 AS col_0 FROM m5 AS t_0 WHERE false GROUP BY t_0.col_1, t_0.col_0 HAVING count(true) > SMALLINT '11429';CREATE MATERIALIZED VIEW m8 AS SELECT TIME '05:10:12' AS col_0, FLOAT '1095294059.307788' AS col_1, INT '0' AS col_2 FROM customer AS t_0 WHERE false GROUP BY t_0.c_phone HAVING true;CREATE MATERIALIZED VIEW m9 AS SELECT CASE WHEN true THEN FLOAT '51735585.468260735' WHEN 892871132290329136 > (REAL '453612649.7661127' + t_0.col_1) THEN FLOAT '2147483647' WHEN false THEN FLOAT '1' WHEN false THEN t_0.col_1 / t_1.col_1 WHEN true THEN REAL '1330985995.8324142' + FLOAT '2147483647' WHEN false THEN FLOAT '1' WHEN false THEN FLOAT '0' WHEN false THEN t_1.col_1 + (REAL '2147483647' * REAL '0') ELSE t_1.col_1 END AS col_0, coalesce(NULL, NULL, NULL, NULL, NULL, NULL, NULL, TIMESTAMP '2022-03-03 12:13:43', NULL, NULL) AS col_1, INTERVAL '3600' AS col_2, NULL AS col_3 FROM m8 AS t_0 JOIN m8 AS t_1 ON t_0.col_1 = t_1.col_1 WHERE FLOAT '1076057347.8948662' <= INT '1' GROUP BY t_0.col_0, t_1.col_1, t_0.col_2, t_0.col_1, t_1.col_2, t_1.col_0 HAVING true;

-- Query
SELECT
    1 AS col_0
FROM
    m4 AS t_0,
    orders AS t_1,
    m9 AS t_2,
    m2 AS t_3,
    (
        SELECT
            EXISTS (
                    SELECT
                        TIMESTAMP '2022-03-06 04:11:37' AS col_0,
                        TIME '04:11:37' AS col_1,
                        false AS col_2,
                    INT '0' + INT '1' AS col_3
                    FROM
                    alltypes2 AS t_34
                    JOIN m4 AS t_35 ON t_34.c15 = t_35.col_0,
                    m6 AS t_36,
                    tumble(
                            person, person.date_time, INTERVAL '19555'
                        ) AS tumble_37,
                    partsupp AS t_38,
                    tumble(m4, m4.col_3, INTERVAL '86400') AS tumble_39,
                    bid AS t_40
                    JOIN m1 AS t_41 ON t_40.date_time = t_41.col_0,
                    lineitem AS t_42,
                    alltypes1 AS t_43,
                    m7 AS t_44
                    JOIN alltypes2 AS t_45 ON t_44.col_0 = t_45.c4,
                    supplier AS t_46,
                    customer AS t_47
                ) AS col_0
        FROM
            hop(
                    bid, bid.date_time, INTERVAL '1', INTERVAL '651716'
                ) AS hop_4,
            m5 AS t_5,
            region AS t_6,
            hop(
                    person, person.date_time, INTERVAL '1',
                    INTERVAL '1'
                ) AS hop_7,
            partsupp AS t_8,
            nation AS t_9,
            m9 AS t_10,
            (
                SELECT
                    hop_13.c15 AS col_0,
                    REAL '469743484.22073567' AS col_1,
                    7044279885882287709 AS col_2
                FROM
                    nation AS t_11,
                    m4 AS t_12,
                    hop(
                    alltypes1, alltypes1.c11, INTERVAL '1',
                    INTERVAL '771630'
                    ) AS hop_13,
                    partsupp AS t_14,
                    tumble(
                    alltypes2, alltypes2.c11, INTERVAL '604800'
                    ) AS tumble_15,
                    hop(
                    person, person.date_time, INTERVAL '1',
                    INTERVAL '1'
                    ) AS hop_16,
                    lineitem AS t_17,
                    nation AS t_18,
                    customer AS t_19,
                    m8 AS t_20,
                    m3 AS t_21,
                    lineitem AS t_22
                    JOIN partsupp AS t_23 ON t_22.l_linestatus = t_23.ps_comment,
                    bid AS t_24
                GROUP BY
                    t_17.l_shipmode,
                    tumble_15.c5,
                    t_23.ps_comment,
                    tumble_15.c3,
                    hop_16.credit_card,
                    t_24.extra,
                    tumble_15.c14,
                    t_18.n_regionkey,
                    hop_13.c15,
                    t_22.l_orderkey,
                    t_24.bidder,
                    hop_13.c6,
                    t_18.n_name,
                    t_17.l_partkey,
                    tumble_15.c1,
                    t_18.n_comment,
                    hop_13.c10,
                    t_22.l_partkey,
                    t_23.ps_availqty,
                    t_12.col_0,
                    tumble_15.c16,
                    t_20.col_1,
                    t_22.l_discount,
                    t_23.ps_partkey,
                    t_19.c_nationkey,
                    t_19.c_phone,
                    hop_13.c14,
                    t_19.c_address,
                    t_17.l_orderkey,
                    t_11.n_nationkey,
                    tumble_15.c15,
                    tumble_15.c6,
                    hop_13.c16,
                    hop_13.c1,
                    hop_13.c2,
                    t_20.col_2,
                    hop_13.c13
                HAVING
                    hop_13.c1
            ) AS sq_25,
            hop(
                    person, person.date_time, INTERVAL '1',
                    INTERVAL '60'
                ) AS hop_26,
            m5 AS t_27,
            region AS t_28,
            m2 AS t_29,
            partsupp AS t_30,
            m0 AS t_31,
            m3 AS t_32,
            m4 AS t_33
        GROUP BY
            hop_7.date_time,
            t_6.r_comment,
            t_27.col_1,
            t_8.ps_suppkey,
            hop_4.channel,
            hop_26.name,
            t_30.ps_availqty,
            t_29.col_0,
            t_9.n_comment,
            t_31.col_0,
            t_6.r_regionkey,
            t_6.r_name,
            t_28.r_comment,
            t_8.ps_comment
        HAVING
            t_31.col_0
    ) AS sq_48,
    m3 AS t_49,
    m4 AS t_50,
    hop(
            alltypes1, alltypes1.c11, INTERVAL '1',
            INTERVAL '77947'
        ) AS hop_51,
    auction AS t_52,
    m2 AS t_53,
    tumble(
            person, person.date_time, INTERVAL '60'
        ) AS tumble_54,
    part AS t_55,
    bid AS t_56,
    m0 AS t_57,
    m2 AS t_58
WHERE
    t_58.col_1
GROUP BY
    t_52.id,
    t_52.reserve,
    hop_51.c2,
    t_53.col_1,
    t_0.col_0,
    hop_51.c15,
    t_3.col_0
HAVING
    CAST(
            INT '295703504' * INT '1979387192' AS BOOLEAN
        );

---- END