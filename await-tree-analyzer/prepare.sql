CREATE TABLE table_a (
    a_id INT,
    a_val INT
) WITH (
    connector = 'datagen',
    fields.a_id.kind = 'sequence',
    fields.a_id.start = '1',

    fields.a_val.kind = 'sequence',
    fields.a_val.start = '100',

    datagen.rows.per.second = '10',
    datagen.split.num = '1'
)
FORMAT PLAIN
ENCODE JSON;


CREATE TABLE table_b (
    b_key INT,
    b_val INT
) WITH (
    connector = 'datagen',
    fields.b_key.kind = 'sequence',
    fields.b_key.start = '1',

    fields.b_val.kind = 'sequence',
    fields.b_val.start = '200',

    datagen.rows.per.second = '10',
    datagen.split.num = '1'
)
FORMAT PLAIN
ENCODE JSON;


CREATE TABLE table_c (
    c_id INT,
    c_category INT,
    c_amount INT
) WITH (
    connector = 'datagen',

    fields.c_id.kind = 'sequence',
    fields.c_id.start = '1',

    fields.c_category.kind = 'sequence',
    fields.c_category.start = '10',

    fields.c_amount.kind = 'sequence',
    fields.c_amount.start = '1000',

    datagen.rows.per.second = '10',
    datagen.split.num = '1'
)
FORMAT PLAIN
ENCODE JSON;


CREATE MATERIALIZED VIEW mv_agg_a AS
SELECT
    a_id,
    COUNT(*)        AS row_count,
    SUM(a_val)      AS sum_val,
    AVG(a_val)      AS avg_val
FROM table_a
GROUP BY a_id;


CREATE MATERIALIZED VIEW mv_join_ab AS
SELECT
    a.a_id,
    a.a_val,
    b.b_key,
    b.b_val
FROM table_a a
JOIN table_b b
  ON a.a_id = b.b_key;


CREATE MATERIALIZED VIEW mv_join_abc AS
SELECT
    a.a_id,
    c.c_category,
    COUNT(*)              AS total_rows,
    SUM(c.c_amount)       AS total_amount
FROM table_a a
JOIN table_b b
  ON a.a_id = b.b_key
JOIN table_c c
  ON c.c_id = (b.b_val % 10)  /* Example mapping for demonstration */
GROUP BY a.a_id, c.c_category;