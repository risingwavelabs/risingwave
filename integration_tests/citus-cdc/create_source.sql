CREATE TABLE orders_rw (
    o_orderkey bigint,
    o_custkey bigint,
    o_orderstatus varchar,
    o_totalprice decimal,
    o_orderdate date,
    o_orderpriority varchar,
    o_clerk varchar,
    o_shippriority bigint,
    o_comment varchar,
    PRIMARY KEY (o_orderkey)
) WITH (
    connector = 'citus-cdc',
    hostname = 'citus-master',
    port = '5432',
    username = 'myuser',
    password = '123456',
    database.servers = 'citus-worker-1:5432,citus-worker-2:5432',
    database.name = 'mydb',
    schema.name = 'public',
    table.name = 'orders',
    slot.name = 'orders_dbz_slot',
);

DROP TABLE orders_rw;

CREATE TABLE orders_rw (
    o_orderkey bigint,
    o_custkey bigint,
    o_orderstatus varchar,
    o_totalprice decimal,
    o_orderdate date,
    o_orderpriority varchar,
    o_clerk varchar,
    o_shippriority bigint,
    o_comment varchar,
    PRIMARY KEY (o_orderkey)
) WITH (
   connector = 'citus-cdc',
   hostname = 'citus-master',
   port = '5432',
   username = 'myuser',
   password = '123456',
   database.servers = 'citus-worker-1:5432,citus-worker-2:5432',
   database.name = 'mydb',
   schema.name = 'public',
   table.name = 'orders',
   slot.name = 'orders_dbz_slot'
);