# Testing

Before you submit a PR, fully test the code changes and perform necessary checks.

The RisingWave project enforces several checks in CI. Every time the code is modified, you need to perform the checks and ensure they pass.

<!-- toc -->


## Lint

RisingWave requires all code to pass fmt, clippy, sort and hakari checks. Run the following commands to install test tools and perform these checks.

```shell
./risedev install-tools # Install required tools for running unit tests
./risedev c             # Run all checks. Shortcut for ./risedev check
```

## Unit and integration tests

RiseDev runs unit tests with cargo-nextest. To run unit tests:

```shell
./risedev test          # Run unit tests
```

Some ideas and caveats for writing tests:
- Use [expect_test](https://github.com/rust-analyzer/expect-test) to write data driven tests that can automatically update results.
- It's recommended to write new tests as *integration tests* (i.e. in `tests/` directory) instead of *unit tests* (i.e. in `src/` directory).

  Besides, put integration tests under `tests/integration_tests/*.rs`, instead of `tests/*.rs`. See [Delete Cargo Integration Tests](https://matklad.github.io/2021/02/27/delete-cargo-integration-tests.html) and [#9878](https://github.com/risingwavelabs/risingwave/issues/9878), for more details.

You might want to read [How to Test](https://matklad.github.io/2021/05/31/how-to-test.html) for more good ideas on testing.

## Planner tests

RisingWave's SQL frontend has SQL planner tests.

<!-- For more information, see [Planner Test Guide](../src/frontend/planner_test/README.md). -->
<!-- TODO: migrate here -->

## End-to-end tests

We use [sqllogictest-rs](https://github.com/risinglightdb/sqllogictest-rs) to run RisingWave e2e tests.

Refer to Sqllogictest [`.slt` Test File Format Cookbook](https://github.com/risinglightdb/sqllogictest-rs#slt-test-file-format-cookbook) for the syntax.

Before running end-to-end tests, you will need to start a full cluster first:

```shell
./risedev d
```

Then to run the end-to-end tests, you can use one of the following commands according to which component you are developing:

```shell
# run all streaming tests
./risedev slt-streaming -p 4566 -d dev -j 1
# run all batch tests
./risedev slt-batch -p 4566 -d dev -j 1
# run both
./risedev slt-all -p 4566 -d dev -j 1
```

> Use `-j 1` to create a separate database for each test case, which can ensure that previous test case failure won't affect other tests due to table cleanups.

Alternatively, you can also run some specific tests:

```shell
# run a single test
./risedev slt -p 4566 -d dev './e2e_test/path/to/file.slt'
# run all tests under a directory (including subdirectories)
./risedev slt -p 4566 -d dev './e2e_test/path/to/directory/**/*.slt'
```

After running e2e tests, you may kill the cluster and clean data.

```shell
./risedev k  # shortcut for ./risedev kill
./risedev clean-data
```

RisingWave's codebase is constantly changing. The persistent data might not be stable. In case of unexpected decode errors, try `./risedev clean-data` first.

## Fuzzing tests

### SqlSmith

Currently, SqlSmith supports for e2e and frontend fuzzing. Take a look at [Fuzzing tests](https://github.com/risingwavelabs/risingwave/blob/fb60113c2e8a7f0676af545c99f073a335c255f3/src/tests/sqlsmith/README.md#L1) for more details on running it locally.

<!-- TODO: migrate here -->

## DocSlt tests

As introduced in [#5117](https://github.com/risingwavelabs/risingwave/issues/5117), DocSlt tool allows you to write SQL examples in sqllogictest syntax in Rust doc comments. After adding or modifying any such SQL examples, you should run the following commands to generate and run e2e tests for them.

```shell
# generate e2e tests from doc comments for all default packages
./risedev docslt
# or, generate for only modified package
./risedev docslt -p risingwave_expr

# run all generated e2e tests
./risedev slt-generated -p 4566 -d dev
# or, run only some of them
./risedev slt -p 4566 -d dev './e2e_test/generated/docslt/risingwave_expr/**/*.slt'
```

These will be run on CI as well.

## Deterministic simulation tests

Deterministic simulation is a powerful tool to efficiently search bugs and reliably reproduce them.
In case you are not familiar with this technique, here is a [talk](https://www.youtube.com/watch?v=4fFDFbi3toc) and a [blog post](https://sled.rs/simulation.html) for brief introduction.

See also the blog posts for a detailed writeup:
- [Deterministic Simulation: A New Era of Distributed System Testing (Part 1 of 2)](https://www.risingwave.com/blog/deterministic-simulation-a-new-era-of-distributed-system-testing/)
- [Applying Deterministic Simulation: The RisingWave Story (Part 2 of 2)](https://www.risingwave.com/blog/applying-deterministic-simulation-the-risingwave-story-part-2-of-2/)

### Unit and e2e tests

You can run normal unit tests and end-to-end tests in deterministic simulation mode.

```sh
# run deterministic unit test
./risedev stest
# run deterministic end-to-end test
./risedev sslt -- './e2e_test/path/to/directory/**/*.slt'
```

When your program panics, the simulator will print the random seed of this run:

```sh
thread '<unnamed>' panicked at '...',
note: run with `MADSIM_TEST_SEED=1` environment variable to reproduce this error
```

Then you can reproduce the bug with the given seed:

```sh
# set the random seed to reproduce a run
MADSIM_TEST_SEED=1 RUST_LOG=info ./risedev sslt -- './e2e_test/path/to/directory/**/*.slt'
```

More advanced usages are listed below:

```sh
# run multiple times with different seeds to test reliability
# it's recommended to build in release mode for a fast run
MADSIM_TEST_NUM=100 ./risedev sslt --release -- './e2e_test/path/to/directory/**/*.slt'

# configure cluster nodes (by default: 2fe+3cn)
./risedev sslt -- --compute-nodes 2 './e2e_test/path/to/directory/**/*.slt'

# inject failures to test fault recovery
./risedev sslt -- --kill-meta --etcd-timeout-rate=0.01 './e2e_test/path/to/directory/**/*.slt'

# see more usages
./risedev sslt -- --help
```

Deterministic test is included in CI as well. See [CI script](https://github.com/risingwavelabs/risingwave/blob/ee97c1d52a0a27cf6684fd082ebc4065407d71e0/ci/scripts/deterministic-e2e-test.sh) for details.

### Integration tests

`src/tests/simulation` contains some special integration tests that are designed to be run in deterministic simulation mode. In these tests, we have more fine-grained control over the cluster and the execution environment to test some complex cases that are hard to test in normal environments.

To run these tests:
```shell
./risedev sit-test <test_name>
```

Sometimes in CI you may see a backtrace, followed by an error message with a `MADSIM_TEST_SEED`:
```shell
 161: madsim::sim::task::Executor::block_on
             at /risingwave/.cargo/registry/src/index.crates.io-6f17d22bba15001f/madsim-0.2.22/src/sim/task/mod.rs:238:13
 162: madsim::sim::runtime::Runtime::block_on
             at /risingwave/.cargo/registry/src/index.crates.io-6f17d22bba15001f/madsim-0.2.22/src/sim/runtime/mod.rs:126:9
 163: madsim::sim::runtime::builder::Builder::run::{{closure}}::{{closure}}::{{closure}}
             at /risingwave/.cargo/registry/src/index.crates.io-6f17d22bba15001f/madsim-0.2.22/src/sim/runtime/builder.rs:128:35
note: Some details are omitted, run with `RUST_BACKTRACE=full` for a verbose backtrace.
context: node=6 "compute-1", task=2237 (spawned at /risingwave/src/stream/src/task/stream_manager.rs:689:34)
note: run with `MADSIM_TEST_SEED=2` environment variable to reproduce this error
```

You may use that to reproduce it in your local environment. For example:
```shell
MADSIM_TEST_SEED=4 ./risedev sit-test test_backfill_with_upstream_and_snapshot_read
```

## Backwards compatibility tests

This tests backwards compatibility between the earliest minor version
and latest minor version of Risingwave (e.g. 1.0.0 vs 1.1.0).

You can run it locally with:
```bash
./risedev backwards-compat-test
```

In CI, you can make sure the PR runs it by adding the label `ci/run-backwards-compat-tests`.
