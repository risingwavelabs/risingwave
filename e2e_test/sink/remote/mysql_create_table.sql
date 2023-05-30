CREATE TABLE t_remote (
    id integer PRIMARY KEY,
    v_varchar varchar(100),
    v_smallint smallint,
    v_integer integer,
    v_bigint bigint,
    v_decimal decimal,
    v_float float,
    v_double double,
    v_timestamp timestamp
);


CREATE TABLE t_types (
   id BIGINT PRIMARY KEY,
   varchar_column VARCHAR(100),
   text_column TEXT,
   integer_column INTEGER,
   smallint_column SMALLINT,
   bigint_column BIGINT,
   decimal_column DECIMAL,
   real_column float,
   double_column DOUBLE,
   boolean_column TINYINT,
   date_column DATE,
   time_column TIME,
   timestamp_column TIMESTAMP,
   jsonb_column JSON,
   array_column LONGTEXT,
   array_column2 LONGTEXT
);
