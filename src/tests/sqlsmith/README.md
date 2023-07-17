# Sqlsmith

SqlSmith is currently used as a testing tool to discover unexpected panics in RisingWave (It's not designed to generally test every SQL database, as it also tests some special SQL syntax used in RisingWave). It always generates the correct SQL based on the feature set supported so far. Therefore, if a test fails, it can only be due to two causes:

1. There's a bug in SQLSmith, as it generates invalid SQL.
2. There's a bug in RisingWave because it's unable to handle a correct query.

## Adding new features

Take a look at [develop.md](develop.md) for more details.

Typically you will want to test your changes via madsim. See the section below: [Running with Madsim](#running-with-madsim).


## Frontend

SqlSmith has two modes. The first one focuses on testing the frontend, i.e, testing the functionalities of SQL compilation (binding, transforming an AST into a logical plan, transforming a logical plan into a physical plan).

This test will be run as a unit test:

``` sh
./risedev test -E "package(risingwave_sqlsmith)" --features enable_sqlsmith_unit_test
```

## Running with Madsim

You can check [`ci/scripts/build-simulation.sh`](../../../ci/scripts/build-simulation.sh) 
for the latest madsim build instructions.

You can adjust the sample size. Below `100` batch and stream queries are generated (`--sqlsmith 100`).
It can be useful to run with a larger sample size to ensure new features are well tested.

Build madsim and run fuzzing
```sh
cargo make sslt-build-all --profile ci-sim
RUST_LOG=info RUST_BACKTRACE=1 MADSIM_TEST_SEED=1 ./target/sim/ci-sim/risingwave_simulation --sqlsmith 100 ./src/tests/sqlsmith/tests/testdata
```

Build madsim and run fuzzing and save contents, since it takes a while to run each time.
```sh
cargo make sslt-build-all --profile ci-sim
RUST_LOG=info RUST_BACKTRACE=1 MADSIM_TEST_SEED=1 ./target/sim/ci-sim/risingwave_simulation --sqlsmith 100 ./src/tests/sqlsmith/tests/testdata 1>sqlsmith.log 2>&1
cat sqlsmith.log | less
```

## E2E - random fuzzing

This mode will test the entire query handling end-to-end. We provide a CLI tool that represents a Postgres client. You can run this tool via:

```sh
cargo build
./risedev d
./target/debug/sqlsmith test --testdata ./src/tests/sqlsmith/tests/testdata
```

## E2E - differential testing

This mode will generate batch and stream queries and **diff them after sorting**.

You can run this tool like so:

```sh
cargo build
./risedev d
./target/debug/sqlsmith test --testdata ./src/tests/sqlsmith/tests/testdata --differential-testing
```

## Generating function signatures

Additionally, in some cases where you may want to debug whether we have defined some function/operator incorrectly,
you can try:

```sh
cargo build
./target/debug/sqlsmith print-function-table > ft.txt
```

Check out ft.txt that will contain all the function signatures.

## Generate snapshots

These are generated in CI, and published to the [sqlsmith-query-snapshots](https://github.com/risingwavelabs/sqlsmith-query-snapshots) repository.

You can re-run the failed query:
```sh
RUST_BACKTRACE=1 MADSIM_TEST_SEED=22 RUST_LOG=info \
./target/sim/ci-sim/risingwave_simulation \
  --run-sqlsmith-queries $SNAPSHOT_DIR/failed/22
```

The `failed query` is a summary of the full query set.
In case it does not actually fail, it might be wrong.

You can re-run the full query set as well in that case:
```sh
RUST_BACKTRACE=1 MADSIM_TEST_SEED=22 RUST_LOG=info \
./target/sim/ci-sim/risingwave_simulation \
 --run-sqlsmith-queries $SNAPSHOT_DIR/22
```
