CREATE TABLE delete_test (
    id INTEGER PRIMARY KEY,
    a INT,
    b text
);

INSERT INTO delete_test (id, a) VALUES (1, 10);
INSERT INTO delete_test (id, a, b) VALUES (2, 50, repeat('x', 10000));
--@ INSERT INTO delete_test (a) VALUES (100);

-- allow an alias to be specified for DELETE's target table
--@ DELETE FROM delete_test AS dt WHERE dt.a > 75;

-- if an alias is specified, don't allow the original table name
-- to be referenced
--@ DELETE FROM delete_test dt WHERE delete_test.a > 25;

SELECT id, a, char_length(b) FROM delete_test ORDER BY id;

-- delete a row with a TOASTed value
DELETE FROM delete_test WHERE a > 25;

SELECT id, a, char_length(b) FROM delete_test;

DROP TABLE delete_test;
