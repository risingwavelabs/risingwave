# "Inline" style source e2e tests

Compared with prior source tests ( `e2e_test/source` ), tests in this directory are expected to be easy to run locally and easy to write.

See the [connector development guide](http://risingwavelabs.github.io/risingwave/connector/intro.html#end-to-end-tests) for more information about how to set up the test environment,
run tests, and write tests.

## Install Dependencies

Some additional tools are needed to run the `system` commands in tests.

- `rpk`: Redpanda (Kafka) CLI toolbox. https://docs.redpanda.com/current/get-started/rpk-install/
- `zx`: A tool for writing better scripts. `npm install -g zx`
