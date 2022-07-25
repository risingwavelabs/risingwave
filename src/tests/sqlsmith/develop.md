# Develop

## How it works

There are two phases:
1. Setup phase - Creates relations e.g. `CREATE TABLE`.
2. Query phase - Generate and send queries e.g. `SELECT * FROM t`.
3. Cleanup phase - Cleanup created relations.

### Setup phase

1. `CREATE TABLE` - Created from SQL source files `sqlsmith/tests/testdata`).
2. `CREATE MATERIALIZED VIEW` - Generate materialized views on seeded tables, previously generated materialized views.

### Query phase

Queries are generated at random, with weighted constraints.

Additionally query complexity is determined by `can_recurse`. This controls how deeply queries nest.
We can have better ways to configure this in the future.

### Cleanup phase

1. `DROP MATERIALIZED VIEW` first, to allow tables to be dropped.
2. `DROP TABLE`.

## Table and Column name generation

For setup phase, only `CREATE MATERIALIZED VIEW` requires table name generation. We prefix them with `m{id}`, where `id` is unique for each materialized view.

For both setup and query phase, table factors also need generated names.
To ensure there are no duplicated name conflicts, we always generate a new globally unique `id` for each table name.

For columns, when referencing them, we always use qualified names to do so.
Additionally for `select`, we always generate aliases for each selected item.
These can be locally unique, when they are referenced, we always use qualified form, so we won't have ambiguity.
