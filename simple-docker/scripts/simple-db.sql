
CREATE TABLE IF NOT EXISTS t
(
    id bigint NOT NULL,
    status character varying(25) COLLATE pg_catalog."default",
    CONSTRAINT t_pkey PRIMARY KEY (id)
);

INSERT INTO t values (1, 'COMPLETED');
INSERT INTO t values (2, 'COMPLETED');
INSERT INTO t values (3, 'PROCESSING');
INSERT INTO t values (4, 'COMPLETED');
INSERT INTO t values (5, 'COMPLETED');
INSERT INTO t values (6, 'PROCESSING');
INSERT INTO t values (7, 'PROCESSING');
UPDATE t SET status='COMPLETED' WHERE id=3;
INSERT INTO t values (8, 'PROCESSING');
INSERT INTO t values (9, 'PROCESSING');
UPDATE t SET status='COMPLETED' WHERE id=6;
UPDATE t SET status='COMPLETED' WHERE id=7;
UPDATE t SET status='COMPLETED' WHERE id=8;
UPDATE t SET status='COMPLETED' WHERE id=9;
