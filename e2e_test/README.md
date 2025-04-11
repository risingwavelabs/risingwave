# e2e test

This folder contains sqllogictest source files for e2e. It is running on CI for integration.

e2e test should drop table if created one to avoid table exist conflict (Currently all tests are running in a single database).

## How to write and run e2e tests

Refer to the [RisingWave Developer Guide](https://risingwavelabs.github.io/risingwave/tests/intro.html#end-to-end-tests).

> [!NOTE]
>
> Usually you will just need to run either batch tests or streaming tests. Other tests may need to be run under some specific settings, e.g., ddl tests need to be run on a fresh instance, and database tests need to first create a database and then connect to that database to run tests.
>
> You will never want to run all tests using `./e2e_test/**/*.slt`. You may refer to the [ci script](../ci/scripts/e2e-test-parallel.sh) to see how to run all tests.

## How to test connectors

See the [connector development guide](http://risingwavelabs.github.io/risingwave/connector/intro.html#end-to-end-tests).
