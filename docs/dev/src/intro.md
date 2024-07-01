# Introduction

This guide is intended to be used by contributors to learn about how to develop RisingWave. The instructions about how to submit code changes are included in [contributing guidelines](../CONTRIBUTING.md).

If you have questions, you can search for existing discussions or start a new discussion in the [Discussions forum of RisingWave](https://github.com/risingwavelabs/risingwave/discussions), or ask in the RisingWave Community channel on Slack. Please use the [invitation link](https://risingwave.com/slack) to join the channel.

To report bugs, create a [GitHub issue](https://github.com/risingwavelabs/risingwave/issues/new/choose).


## Read the design docs

Before you start to make code changes, ensure that you understand the design and implementation of RisingWave. We recommend that you read the design docs listed in [docs/README.md](README.md) first.

You can also read the [crate level documentation](https://risingwavelabs.github.io/risingwave/) for implementation details, or run `./risedev doc` to read it locally. Note that you need to [set up the development environment](#set-up-the-development-environment) first.

## Learn about the code structure

- The `src` folder contains all of the kernel components, refer to [src/README.md](../src/README.md) for more details, which contains more details about Design Patterns in RisingWave.
- The `docker` folder contains Docker files to build and start RisingWave.
- The `e2e_test` folder contains the latest end-to-end test cases.
- The `docs` folder contains the design docs. If you want to learn about how RisingWave is designed and implemented, check out the design docs here.
- The `dashboard` folder contains RisingWave dashboard.


## Update Grafana dashboard

See [README](../grafana/README.md) for more information.

## Add new files

We use [skywalking-eyes](https://github.com/apache/skywalking-eyes) to manage license headers.
If you added new files, please follow the installation guide and run:

```shell
license-eye -c .licenserc.yaml header fix
```

## Add new dependencies

`./risedev check-hakari`: To avoid rebuild some common dependencies across different crates in workspace, use
[cargo-hakari](https://docs.rs/cargo-hakari/latest/cargo_hakari/) to ensure all dependencies
are built with the same feature set across workspace. You'll need to run `cargo hakari generate`
after deps get updated.

`./risedev check-udeps`: Use [cargo-udeps](https://github.com/est31/cargo-udeps) to find unused dependencies in workspace.

`./risedev check-dep-sort`: Use [cargo-sort](https://crates.io/crates/cargo-sort) to ensure all deps are get sorted.

## Benchmarking and Profiling

- [CPU Profiling Guide](../../cpu-profiling.md)
- [Memory (Heap) Profiling Guide](../../memory-profiling.md)
- [Microbench Guide](../../microbenchmarks.md)
