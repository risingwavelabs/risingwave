create table person (
    "id" int,
    "name" varchar,
    "email_address" varchar,
    "credit_card" varchar,
    "city" varchar,
    PRIMARY KEY ("id")
) with (
    connector = 'postgres-cdc',
    hostname = 'postgres',
    port = '5432',
    username = 'myuser',
    password = '123456',
    database.name = 'mydb',
    schema.name = 'public',
    table.name = 'person',
    slot.name = 'person'
);

CREATE SOURCE t_auction (
    id BIGINT,
    item_name VARCHAR,
    date_time BIGINT,
    seller INT,
    category INT
) WITH (
    connector = 'kafka',
    topic = 'auction',
    properties.bootstrap.server = 'message_queue:29092',
    scan.startup.mode = 'earliest'
) ROW FORMAT JSON;

CREATE VIEW auction as
SELECT
    id,
    item_name,
    to_timestamp(date_time) as date_time,
    seller,
    category
FROM
    t_auction;

CREATE TABLE lineitem_rw (
    L_ORDERKEY BIGINT,
    L_PARTKEY BIGINT,
    L_SUPPKEY BIGINT,
    L_LINENUMBER BIGINT,
    L_QUANTITY DECIMAL,
    L_EXTENDEDPRICE DECIMAL,
    L_DISCOUNT DECIMAL,
    L_TAX DECIMAL,
    L_RETURNFLAG VARCHAR,
    L_LINESTATUS VARCHAR,
    L_SHIPDATE DATE,
    L_COMMITDATE DATE,
    L_RECEIPTDATE DATE,
    L_SHIPINSTRUCT VARCHAR,
    L_SHIPMODE VARCHAR,
    L_COMMENT VARCHAR,
    PRIMARY KEY(L_ORDERKEY, L_LINENUMBER)
) WITH (
    connector = 'postgres-cdc',
    hostname = 'postgres',
    port = '5432',
    username = 'myuser',
    password = '123456',
    database.name = 'mydb',
    schema.name = 'public',
    table.name = 'lineitem',
    slot.name = 'lineitem'
);