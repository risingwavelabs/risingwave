DROP VIEW IF EXISTS hasura_defaults_view;
DROP TABLE IF EXISTS hasura_child;
DROP TABLE IF EXISTS hasura_parent;
DROP TABLE IF EXISTS hasura_defaults;
DROP FUNCTION IF EXISTS hasura_add(INT, INT);

CREATE TABLE hasura_parent (
  id INT PRIMARY KEY,
  name VARCHAR NOT NULL
);

CREATE TABLE hasura_child (
  id INT PRIMARY KEY,
  parent_id INT REFERENCES hasura_parent(id) ON DELETE CASCADE ON UPDATE SET NULL,
  note VARCHAR
);

CREATE TABLE hasura_defaults (
  id INT PRIMARY KEY,
  required_v INT NOT NULL,
  optional_v INT,
  defaulted_v INT DEFAULT 42,
  generated_v INT AS required_v + 1
);

CREATE VIEW hasura_defaults_view AS
SELECT id, required_v, defaulted_v, generated_v FROM hasura_defaults;

CREATE FUNCTION hasura_add(a INT, b INT) RETURNS INT LANGUAGE SQL AS $$
  SELECT a + b
$$;
