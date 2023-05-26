CREATE TABLE t_remote (
    id BIGINT PRIMARY KEY,
    v_varchar VARCHAR(255),
    v_text TEXT,
    v_integer INT,
    v_smallint SMALLINT,
    v_bigint BIGINT,
    v_decimal DECIMAL(10,2),
    v_real FLOAT,
    v_double DOUBLE,
    v_boolean BOOLEAN,
    v_date DATE,
    v_time TIME,
    v_timestamp TIMESTAMP,
    v_jsonb JSON,
    v_bytea BLOB
);