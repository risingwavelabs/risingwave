# Planner Test

This module contains a testing tool for binder, planner and optimizer.
Given a sequence of SQL queries as the input, the test runner will check
the logical operator tree if any, and the physical operator tree if any.

The test data in YAML format is organized under `tests/testdata` folder.
The inputs are put under `tests/testdata/input`, and the outputs are
put under `tests/testdata/output`.

## Examples of Test Cases

### SELECT as the test case

You can simply write a `SELECT` query in the `sql` field, and use `expected_outputs` to specify the plans you want to check, including `logical_plan` , `stream_plan` , `binder_error` , etc.

```yaml
- sql: |
    select * from t
  expected_outputs:
  - binder_error
```

This is a simple test case that validates the binder's behavior on an illegal SQL.

```yaml
- sql: |
    create table t (v1 bigint, v2 double precision);
    select * from t;
  expected_outputs:
  - logical_plan
  - batch_plan
```

If the SQL is valid, then test runner will compare the generated logical plan and batch plan with the expected ones.

### EXPLAIN as the test case

Alternatively, you can also write an `EXPLAIN` statement in the `sql` field. In this case, the output field is a single `explain_output` .

```yaml
- sql: explain select 1;
  expected_outputs:
  - explain_output
```

This is helpful when you want to test `EXPLAIN CREATE ...` and `EXPLAIN (options) ...` statements.

## Update Outputs

The outputs can be automatically generated after you update code or add new test cases.

you may run:

```
./risedev do-apply-planner-test
```

To apply the new test results.

## Run a single test

```
./risedev run-planner-test <yaml file name>
./risedev run-planner-test tpch # Run tpch.yaml
./risedev run-planner-test # Run all tests
```
