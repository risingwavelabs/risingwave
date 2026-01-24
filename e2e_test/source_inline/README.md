# "Inline" style source e2e tests

Compared with prior source tests ( `e2e_test/source` ), tests in this directory are expected to be easy to run locally and easy to write.

See the [connector development guide](http://risingwavelabs.github.io/risingwave/connector/intro.html#end-to-end-tests) for more information about how to set up the test environment,
run tests, and write tests.

## Serial Tests

Tests ending with `.slt.serial` can only be run in serial, e.g., it contains `RECOVER` statement which will break other connections, or it has some special `system` commands.

Other tests can be run in parallel.

```bash
# run all parallel tests
risedev slt './e2e_test/source_inline/**/*.slt' -j16
# run all serial tests
risedev slt './e2e_test/source_inline/**/*.slt.serial'
```

## Install Dependencies

Some additional tools are needed to run the `system` commands in tests.

- `rpk`: Redpanda (Kafka) CLI toolbox. https://docs.redpanda.com/current/get-started/rpk-install/
- `zx`: A tool for writing better scripts. `npm install -g zx`

### Python Dependencies

There are also some `system` commands calling scripts written in Python.

You have two options:

1. **Recommended**: Install the package manager `uv`, which will automatically manage the dependencies for you, requiring no further action.

2. Manually install the dependencies listed in `e2e_test/requirements.txt` in your system Python environment or a virtual environment.
