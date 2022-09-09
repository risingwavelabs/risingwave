CREATE TABLE IF NOT EXISTS t
(
    id bigint NOT NULL,
    status character varying(25) COLLATE pg_catalog."default",
    CONSTRAINT t_pkey PRIMARY KEY (id)
);

INSERT INTO t values (1, 'PROCESSING');
