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
    connector = 'mysql-cdc',
    hostname = 'mysql',
    port = '3306',
    username = 'root',
    password = '123456',
    database.name = 'mydb',
    table.name = 'lineitem',
    server.id = '2'
);