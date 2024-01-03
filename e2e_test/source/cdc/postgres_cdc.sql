-- PG
CREATE TABLE shipments (
  shipment_id SERIAL NOT NULL PRIMARY KEY,
  order_id SERIAL NOT NULL,
  origin VARCHAR(255) NOT NULL,
  destination VARCHAR(255) NOT NULL,
  is_arrived BOOLEAN NOT NULL
);
ALTER SEQUENCE public.shipments_shipment_id_seq RESTART WITH 1001;
ALTER TABLE public.shipments REPLICA IDENTITY FULL;
INSERT INTO shipments
VALUES (default,10001,'Beijing','Shanghai',false),
       (default,10002,'Hangzhou','Shanghai',false),
       (default,10003,'Shanghai','Hangzhou',false);


CREATE TABLE orders_tx (
    order_id SERIAL NOT NULL PRIMARY KEY,
    order_date DATE NOT NULL,
    customer_name VARCHAR(255) NOT NULL,
    price DECIMAL NOT NULL,
    product_id INTEGER NOT NULL,
    order_status BOOLEAN NOT NULL
);
ALTER SEQUENCE public.orders_tx_order_id_seq RESTART WITH 10001;

INSERT INTO orders_tx
VALUES (default, '2020-07-30 10:08:22', 'Jark', 50.50, 102, false),
       (default, '2020-07-30 10:11:09', 'Sally', 15.00, 105, false);

INSERT INTO orders_tx
VALUES (default, '2023-12-30 19:11:09', '张三', 15.00, 105, true),
       (default, '2020-07-30 12:00:30', '李四', 25.25, 106, false);

INSERT INTO orders_tx
VALUES (default, '2024-01-01 17:00:00', 'Sam', 1000.20, 110, false);


CREATE TABLE person (
    "id" int,
    "name" varchar(64),
    "email_address" varchar(200),
    "credit_card" varchar(200),
    "city" varchar(200),
    PRIMARY KEY ("id")
);

ALTER TABLE
    public.person REPLICA IDENTITY FULL;

INSERT INTO person VALUES (1000, 'vicky noris', 'yplkvgz@qbxfg.com', '7878 5821 1864 2539', 'cheyenne');
INSERT INTO person VALUES (1001, 'peter white', 'myckhsp@xpmpe.com', '1781 2313 8157 6974', 'boise');
INSERT INTO person VALUES (1002, 'sarah spencer', 'wipvdbm@dkaap.com', '3453 4987 9481 6270', 'los angeles');

INSERT INTO person VALUES (1003, 'Kafka', 'ypl@qbxfg.com', '1864 2539', 'Shanghai');
INSERT INTO person VALUES (1100, 'noris', 'ypl@qbxfg.com', '1864 2539', 'enne');
INSERT INTO person VALUES (1101, 'white', 'myc@xpmpe.com', '8157 6974', 'se');


SELECT COUNT(*) AS person_count
FROM person_tx
UNION ALL
SELECT COUNT(*) AS order_count
FROM orders_tx;

create schema abs;
create table abs.t1 (v1 int primary key, v2 double precision, v3 varchar, v4 numeric);
create publication my_publicaton for table abs.t1 (v1, v3);
insert into abs.t1 values (1, 1.1, 'aaa', '5431.1234');


CREATE TABLE IF NOT EXISTS postgres_all_types(
     c_boolean boolean,
     c_smallint smallint,
     c_integer integer,
     c_bigint bigint,
     c_decimal decimal,
     c_real real,
     c_double_precision double precision,
     c_varchar varchar,
     c_bytea bytea,
     c_date date,
     c_time time,
     c_timestamp timestamp,
     c_timestamptz timestamptz,
     c_interval interval,
     c_jsonb jsonb,
     c_boolean_array boolean[],
     c_smallint_array smallint[],
     c_integer_array integer[],
     c_bigint_array bigint[],
     c_decimal_array decimal[],
     c_real_array real[],
     c_double_precision_array double precision[],
     c_varchar_array varchar[],
     c_bytea_array bytea[],
     c_date_array date[],
     c_time_array time[],
     c_timestamp_array timestamp[],
     c_timestamptz_array timestamptz[],
     c_interval_array interval[],
     c_jsonb_array jsonb[],
     PRIMARY KEY (c_boolean,c_bigint,c_date)
);
INSERT INTO postgres_all_types VALUES ( False, 0, 0, 0, 0, 0, 0, '', '\x00', '0001-01-01', '00:00:00', '0001-01-01 00:00:00'::timestamp, '0001-01-01 00:00:00'::timestamptz, interval '0 second', '{}', array[]::boolean[], array[]::smallint[], array[]::integer[], array[]::bigint[], array[]::decimal[], array[]::real[], array[]::double precision[], array[]::varchar[], array[]::bytea[], array[]::date[], array[]::time[], array[]::timestamp[], array[]::timestamptz[], array[]::interval[], array[]::jsonb[]);
INSERT INTO postgres_all_types VALUES ( False, -32767, -2147483647, -9223372036854775807, -10.0, -9999.999999, -10000.0, '', '\x00', '0001-01-01', '00:00:00', '0001-01-01 00:00:00'::timestamp, '0001-01-01 00:00:00'::timestamptz, interval '0 second', '{}', array[False::boolean]::boolean[], array[-32767::smallint]::smallint[], array[-2147483647::integer]::integer[], array[-9223372036854775807::bigint]::bigint[], array[-10.0::decimal]::decimal[], array[-9999.999999::real]::real[], array[-10000.0::double precision]::double precision[], array[''::varchar]::varchar[], array['\x00'::bytea]::bytea[], array['0001-01-01'::date]::date[], array['00:00:00'::time]::time[], array['0001-01-01 00:00:00'::timestamp::timestamp]::timestamp[], array['0001-01-01 00:00:00'::timestamptz::timestamptz]::timestamptz[], array[interval '0 second'::interval]::interval[], array['{}'::jsonb]::jsonb[]);
