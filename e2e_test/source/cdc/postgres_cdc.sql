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


create schema abs;
create table abs.t1 (v1 int primary key, v2 double precision, v3 varchar, v4 numeric);
create publication my_publicaton for table abs.t1 (v1, v3);
insert into abs.t1 values (1, 1.1, 'aaa', '5431.1234');