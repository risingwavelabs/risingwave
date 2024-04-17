# "Inline" style source e2e tests

Compared with prior source tests (`e2e_test/source`), tests in this directory are expected to be easy to run locally and easy to write.

To run locally, use `risedev d` to start services (including external systems like Kafka and Postgres, or specify `user-managed` to use your own service).
Then use `risedev slt` to run the tests, which will load the environment variables (ports, etc.)
according to the services started by `risedev d`.

```sh
risedev slt 'e2e_test/source_inline/**/*.slt'
```

To write tests, please ensure each file is self-contained and does not depend on running external scripts to setup the environment.
Use `system` command to setup instead.

Refer to https://github.com/risingwavelabs/risingwave/issues/12451#issuecomment-2051861048 for more details.
