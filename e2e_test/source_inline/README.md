# "Inline" style source e2e tests

Compared with prior source tests ( `e2e_test/source` ), tests in this directory are expected to be easy to run locally and easy to write.

Refer to https://github.com/risingwavelabs/risingwave/issues/12451#issuecomment-2051861048 for more details.

## Install Dependencies

Some additional tools are needed to run the `system` commands in tests.

- `rpk`: Redpanda (Kafka) CLI toolbox. https://docs.redpanda.com/current/get-started/rpk-install/
- `zx`: A tool for writing better scripts. `npm install -g zx`

## Run tests

To run locally, use `risedev d` to start services (including external systems like Kafka and Postgres, or specify `user-managed` to use your own service).
Then use `risedev slt` to run the tests, which will load the environment variables (ports, etc.)
according to the services started by `risedev d` .

```sh
risedev slt 'e2e_test/source_inline/**/*.slt'
```

## Write tests

To write tests, please ensure each file is self-contained and does not depend on running external scripts to setup the environment.

Use `system` command to setup instead.
For simple cases, you can directly write a bash command;
For more complex cases, you can write a test script (with any language like bash, python, zx), and invoke it in the `system` command.
