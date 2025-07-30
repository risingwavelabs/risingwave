CREATE TABLE alltypes1 (
    c1 boolean,
    c2 smallint,
    c3 integer,
    c4 bigint,
    c5 real,
    c6 double precision,
    c7 numeric,
    c8 date,
    c9 varchar,
    c10 time without time zone,
    c11 timestamp without time zone,
    -- TODO(Noel): Many timestamptz expressions are unsupported in RW, leave this out for now.
    -- c12 timestamp with time zone,
    c13 interval,
    c14 struct < a integer >,
    c15 integer [],
    c16 varchar []
);

CREATE TABLE alltypes2 (
    c1 boolean,
    c2 smallint,
    c3 integer,
    c4 bigint,
    c5 real,
    c6 double precision,
    c7 numeric,
    c8 date,
    c9 varchar,
    c10 time without time zone,
    c11 timestamp without time zone,
    -- TODO(Noel): Many timestamptz expressions are unsupported in RW, leave this out for now.
    -- c12 timestamp with time zone,
    c13 interval,
    c14 struct < a integer >,
    c15 integer [],
    c16 varchar []
);

CREATE TABLE alltypes3 (
    c1 boolean,
    c2 smallint,
    c3 integer,
    c4 bigint,
    c5 real,
    c6 double precision,
    c7 numeric,
    c8 date,
    c9 varchar,
    c10 time without time zone,
    c11 timestamp without time zone,
    -- TODO(Noel): Many timestamptz expressions are unsupported in RW, leave this out for now.
    -- c12 timestamp with time zone,
    c13 interval,
    c14 struct < a integer >,
    c15 integer [],
    c16 varchar [],
    WATERMARK FOR c11 AS c11 - INTERVAL '5 seconds',
    PRIMARY KEY (c4)
) APPEND ONLY;