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

## SLT Coverage Check

The `check_slt_coverage.py` script provides a basic assessment of SLT coverage, i.e., whether all SLT files are referenced by CI scripts under `ci/scripts`.

### Usage

```bash
python3 e2e_test/check_slt_coverage.py -d                   # show directory-level analysis
python3 e2e_test/check_slt_coverage.py -d -o output.json    # save detailed results to file

python3 e2e_test/check_slt_coverage.py --help               # for more options
```

### Coverage Ignore File

The `e2e_test/.coverageignore` file uses gitignore-like syntax to specify patterns for files that should be excluded from coverage analysis. This is useful for:

- Test files that are manually executed
- Template files that generate other test files
- Tests only meant for local development
