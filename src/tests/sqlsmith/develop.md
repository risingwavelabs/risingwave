# Develop

Here we cover:
- The test runner, which is used for e2e sqlsmith tests.
- Query Generation process.

## Test Runner overview

Query execution and generation happen in step. Here's an overview of it.
1. Create Base tables, based on the supplied `test_data` argument to sqlsmith.
2. Generate `INSERT`s and Populate base tables.
3. Generate and Create base materialized views, they will be used to test mv-on-mv.
   These materialized views can either be on Base tables,
   or on a previously created materialize view from this step.
4. Generate `UPDATE / DELETE` statements and Update base tables with them.
   If no PK we will just do `DELETE` for some rows and `INSERT` back statements.
5. Generate and run batch queries e.g. `SELECT * FROM t`, `WITH w AS ... SELECT * FROM w`.
6. Generate and run stream queries. 
   These are immediately removed after they are successfully created.
7. Drop base materialized views.
8. Drop base tables.

## Query Generation

Queries are generated at random, with weighted constraints.

Additionally, query complexity is determined by `can_recurse`. This controls how deeply queries nest.
We can have better ways to configure this in the future.

Query generation happens recursively, in top-down fashion, roughly following the AST.
For both batch and stream queries, we go through roughly the same process.
We use `is_mview` to selectively disable some sql features which are not well supported by stream engine.

Here's a broad example of what happens when generating a complex query:
1. Generate a with clause.
2. Generate the set expression (`SELECT ...`)
3. Generate `ORDER BY`.

For the set expression we just generate a select statement, which then:
1. Generates the `FROM` expression (includes joins).
2. The `WHERE` expression.
3. Other parts of the `SELECT` query.
4. The list of `SELECT` items.

For the list of select items, it first chooses a type and eventually calls `gen_expr` with that type.

This generates either:
1. A scalar value.
2. A column.
3. Functions.
4. Aggregates.
5. Casts.
6. Other kinds of expressions e.g. `CASE ... WHEN`.
 
We mentioned that we call `gen_expr` with a **specific type**.
That should be the return type of calling functions and aggregates.
It should also be the cast target type.

Hence, we have type signature maps which we use to fetch candidate functions / aggregates / casts with **that specific type**.
We just pick one from the candidate set, and generate some arguments for it.
Then we have the function / aggregate call, or the cast expression generated successfully.

You may start from `query.rs` and trace the code for further details.

## Subquery generation

This section is WIP.

## Table and Column name generation

For setup phase, only `CREATE MATERIALIZED VIEW` requires table name generation. We prefix them with `m{id}`, where `id` is unique for each materialized view.

For both setup and query phase, table factors also need generated names.
To ensure there are no duplicated name conflicts, we always generate a new unique `id` for each table name.

For columns, when referencing them, we always use qualified names to do so.
Additionally for `select`, we always generate aliases for each selected item.
These can be locally unique, when they are referenced, we always use qualified form, so we won't have ambiguity.
