This program is a rewrite of [postgres regress test framework](postgres/postgres/tree/master/src/test/regress) 
in rust.

# How it works

* When it starts up, it will do some initialization work, e.g. setting up environment variables, creating output 
  directories.
* After initialization, it reads a schedule file, where each line describes a parallel schedule, e.g. test cases that run
  in parallel. You can find an example [here](postgres/postgres/blob/master/src/test/regress/parallel_schedule).
* For each test case, it starts a psql process which executes sqls in input file, and compares outputs of psql with 
  expected output. For example, for a test case named `boolean`, here is its [input file](postgres/postgres/blob/master/src/test/regress/sql/boolean.sql)
  and [expected out](postgres/postgres/blob/master/src/test/regress/expected/boolean.out).

# How to add a new test

TODO.

# How to run

* Install `psql` and ensure that it's in your path.
* Start risingwave cluster.
* Run `./risingwave_regress_test -h 127.0.01 -p 1234`. (Not ready yet)
