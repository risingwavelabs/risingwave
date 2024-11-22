DROP TABLE IF EXISTS supplier_items CASCADE;
DROP TABLE IF EXISTS supplier CASCADE;
DROP TABLE IF EXISTS orders CASCADE;
DROP TABLE IF EXISTS lineitem CASCADE;

-- 10,000
CREATE TABLE supplier (
        s_suppkey  INTEGER,
        s_name VARCHAR,
        s_address VARCHAR,
        s_nationkey INTEGER,
        s_phone VARCHAR,
        s_acctbal NUMERIC,
        s_comment VARCHAR,
        PRIMARY KEY (s_suppkey)
);

-- 1,500,000
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

-- 6,000,000
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

-- Wide table with all the fields from 3 tables above
CREATE TABLE supplier_items (
    s_suppkey  INTEGER,
    s_name VARCHAR,
    s_address VARCHAR,
    s_nationkey INTEGER,
    s_phone VARCHAR,
    s_acctbal NUMERIC,
    s_comment VARCHAR,

    o_orderkey BIGINT,
    o_custkey INTEGER,
    o_orderstatus VARCHAR,
    o_totalprice NUMERIC,
    o_orderdate DATE,
    o_orderpriority VARCHAR,
    o_clerk VARCHAR,
    o_shippriority INTEGER,
    o_comment VARCHAR,

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

CREATE SINK supplier_items_sink INTO supplier_items AS
  SELECT
    s_suppkey,
    s_name,
    s_address,
    s_nationkey,
    s_phone,
    s_acctbal,
    s_comment,
    o_orderkey,
    o_custkey,
    o_orderstatus,
    o_totalprice,
    o_orderdate,
    o_orderpriority,
    o_clerk,
    o_shippriority,
    o_comment,
    l_orderkey,
    l_partkey,
    l_suppkey,
    l_linenumber,
    l_quantity,
    l_extendedprice,
    l_discount,
    l_tax,
    l_returnflag,
    l_linestatus,
    l_shipdate,
    l_commitdate,
    l_receiptdate,
    l_shipinstruct,
    l_shipmode,
    l_comment
    FROM supplier
    JOIN orders ON s_suppkey = o_custkey
    JOIN lineitem ON o_orderkey = l_orderkey;

-- Populate supplier (insert 10,000 rows)
INSERT INTO supplier SELECT
    1 AS s_suppkey,
    'Supplier#000000001' AS s_name,
    '123456' AS s_address,
    1 AS s_nationkey,
    '8888888' AS s_phone,
    1000 AS s_acctbal,
    'Lorem ipsum dolor sit amet, consectetur adipiscing elit.' AS s_comment
    FROM generate_series(1, 1);

-- Populate orders (insert 1 rows)
INSERT INTO orders SELECT
    1 AS o_orderkey,
    1 AS o_custkey,
    'O' AS o_orderstatus,
    1000 AS o_totalprice,
    DATE '2020-01-01' AS o_orderdate,
    '1-URGENT' AS o_orderpriority,
    'Clerk#000000001' AS o_clerk,
    1 AS o_shippriority,
    'Lorem ipsum dolor sit amet, consectetur adipiscing elit.' AS o_comment
    FROM generate_series(1, 1);

-- Populate lineitem (insert 6,000,000 rows)
INSERT INTO lineitem SELECT
    1 AS l_orderkey,
    1 AS l_partkey,
    1 AS l_suppkey,
    unique_key AS l_linenumber,
    1000 AS l_quantity,
    1000 AS l_extendedprice,
    0.1 AS l_discount,
    0.1 AS l_tax,
    'R' AS l_returnflag,
    'F' AS l_linestatus,
    DATE '2020-01-01' AS l_shipdate,
    DATE '2020-01-01' AS l_commitdate,
    DATE '2020-01-01' AS l_receiptdate,
    'DELIVER IN PERSON' AS l_shipinstruct,
    'AIR' AS l_shipmode,
    'Lorem ipsum dolor sit amet, consectetur adipiscing elit.' AS l_comment
    FROM generate_series(1, 6000000) as t(unique_key);
