-- Setup schema and table for Postgres CDC test
DROP SCHEMA IF EXISTS ingestion CASCADE;
CREATE SCHEMA ingestion;

CREATE TABLE ingestion.transactions (
    id bigserial NOT NULL,
    merchant_id int4 NOT NULL,
    amount numeric(12, 2) NULL,
    created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_updated_at timestamp NULL,
    new_col int4 NULL,
    CONSTRAINT transactions_pkey PRIMARY KEY (id)
);

-- Insert initial test data
INSERT INTO ingestion.transactions (merchant_id, amount, created_at, last_updated_at, new_col) VALUES
(101, 100.50, '2024-01-01 10:00:00', '2024-01-01 10:00:00', 1),
(102, 200.75, '2024-01-01 11:00:00', '2024-01-01 11:00:00', 2),
(103, 150.25, '2024-01-01 12:00:00', '2024-01-01 12:00:00', 3),
(104, 300.00, '2024-01-01 13:00:00', '2024-01-01 13:00:00', 4),
(105, 450.80, '2024-01-01 14:00:00', '2024-01-01 14:00:00', 5),
(106, 275.90, '2024-01-01 15:00:00', '2024-01-01 15:00:00', 6),
(107, 325.60, '2024-01-01 16:00:00', '2024-01-01 16:00:00', 7),
(108, 180.40, '2024-01-01 17:00:00', '2024-01-01 17:00:00', 8);