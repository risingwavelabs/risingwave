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

# How to add a new test

Just add another line in your schedule file with your test cast name.
```
tests: boolean
```

# How to run

* Install `psql` and ensure that it's in your path.
* Start risingwave cluster.
* Enter `rust` folder of your workspace.
* Run 
```shell
RUST_BACKTRACE=1 target/debug/risingwave_regress_test -h 127.0.0.1 \
  -p 4567 \
  --input `pwd`/src/tests/regress/data \
  --output `pwd`/src/tests/regress/output \
  --schedule `pwd`/src/tests/regress/data/schedule
```

# Reference

The `data` folder contains test cases migrated from [postgres](https://github.com/postgres/postgres/).