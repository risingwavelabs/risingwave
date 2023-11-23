CREATE TABLE counts (
  id INT,
  sum INT,
  primary key(id)
);

CREATE TABLE flinkcounts (
    id bigint,
    sum bigint,
    primary key (id)
);

CREATE TABLE types (
    id int,
    c_boolean boolean,
    c_smallint smallint,
    c_integer integer,
    c_bigint bigint,
    c_decimal text,
    c_real real,
    c_double_precision double precision,
    c_varchar varchar,
    c_bytea bytea,
    c_date date,
    c_time time,
    c_timestamp timestamp,
    c_timestamptz timestamptz,
    c_interval text,
    c_jsonb jsonb,
    primary key (id)
);

CREATE TABLE flink_types (
    id int,
    c_boolean boolean,
    c_smallint smallint,
    c_integer integer,
    c_bigint bigint,
    c_decimal decimal,
    c_real real,
    c_double_precision double precision,
    c_varchar varchar,
    c_bytea bytea,
    primary key (id)
);
