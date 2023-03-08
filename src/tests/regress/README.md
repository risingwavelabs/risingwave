This program is a rewrite of [postgres regress test framework](https://github.com/postgres/postgres/tree/master/src/test/regress) 
in rust.

# How it works

* When it starts up, it will do some initialization work, e.g. setting up environment variables, creating output 
  directories.
* After initialization, it reads a schedule file, where each line describes a parallel schedule, e.g. test cases that run
  in parallel. You can find an example [here](https://github.com/postgres/postgres/blob/master/src/test/regress/parallel_schedule).
* For each test case, it starts a psql process which executes sqls in input file, and compares outputs of psql with 
  expected output. For example, for a test case named `boolean`, here is its [input file](data/sql/boolean.sql)
  and [expected out](data/expected/boolean.out).

# How to enable a new test file

Just add another line in your schedule file with your test cast name.
```
test: boolean
```

# How to edit a test file

If you want to ignore a certain test query from an input sql file, comment it out with `--@ `. Note the extra `@` after `--`.

Except for the `--@ ` syntax above, all other edits to the input sql file must be applied to the expected output file as well, including extra normal comments `-- `.
In general, these files are not meant to be modified a lot. The test runner has some assumptions over the format and would misbehave otherwise.

# How to run

* Install `psql` and ensure that it's in your path.
* `cd` to the root directory of risingwave.
* Start risingwave cluster.
* Run tests against RisingWave.

```shell
RUST_BACKTRACE=1 target/debug/risingwave_regress_test --host 127.0.0.1 \
  -p 4566 \
  -u root \
  --input `pwd`/src/tests/regress/data \
  --output `pwd`/src/tests/regress/output \
  --schedule `pwd`/src/tests/regress/data/schedule \
  --mode risingwave
```

* Run tests against PostgreSQL. Make sure PostgreSQL is running.

```shell
RUST_BACKTRACE=1 target/debug/risingwave_regress_test --host 127.0.0.1 \
  -p 5432 \
  -u `user name` \
  --database `database name` \
  --input `pwd`/src/tests/regress/data \
  --output `pwd`/src/tests/regress/output \
  --schedule `pwd`/src/tests/regress/data/schedule \
  --mode postgres
```

Please remove the `output` directory before running the test again.

```shell
rm -rf `pwd`/src/tests/regress/output
```

# Reference

The `data` folder contains test cases migrated from [postgres](https://github.com/postgres/postgres/).

# Caveat

This regress test is executed for both Postgres and RisingWave. As the result set of a query without `order by` 
is order-unaware, we need to interpret the output file by ourselves. 
