SELECT
    source_id,
    data_type,
    LEFT(ENCODE(value, 'escape'), 100)
FROM
    t
LIMIT
    10;
