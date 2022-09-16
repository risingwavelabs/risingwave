# Developer guide

This guide is intended to be used by contributors to learn about how to develop RisingWave. The instructions about how to submit code changes are included in [contributing guidelines](../CONTRIBUTING.md).

If you have questions, you can search for existing discussions or start a new discussion in the [Discussions forum of RisingWave](https://github.com/risingwavelabs/risingwave/discussions), or ask in the RisingWave Community channel on Slack. Please use the [invitation link](https://join.slack.com/t/risingwave-community/shared_invite/zt-120rft0mr-d8uGk3d~NZiZAQWPnElOfw) to join the channel.

To report bugs, create a [GitHub issue](https://github.com/risingwavelabs/risingwave/issues/new/choose).


## Table of contents

- [Developer guide](#developer-guide)
  - [Table of contents](#table-of-contents)
  - [Read the design docs](#read-the-design-docs)
  - [Learn about the code structure](#learn-about-the-code-structure)
  - [Set up the development environment](#set-up-the-development-environment)
  - [Start and monitor a dev cluster](#start-and-monitor-a-dev-cluster)
    - [Configure additional components](#configure-additional-components)
    - [Start the playground with RiseDev](#start-the-playground-with-risedev)
    - [Start the playground with cargo](#start-the-playground-with-cargo)
  - [Develop the dashboard](#develop-the-dashboard)
    - [Dashboard v1](#dashboard-v1)
    - [Dashboard v2](#dashboard-v2)
  - [Observability components](#observability-components)
    - [Cluster Control](#cluster-control)
    - [Monitoring](#monitoring)
    - [Tracing](#tracing)
    - [Dashboard](#dashboard)
    - [Logging](#logging)
  - [Test your code changes](#test-your-code-changes)
    - [Lint](#lint)
    - [Unit tests](#unit-tests)
    - [Planner tests](#planner-tests)
    - [End-to-end tests](#end-to-end-tests)
    - [End-to-end tests on CI](#end-to-end-tests-on-ci)
    - [DocSlt tests](#docslt-tests)
    - [Deterministic simulation tests](#deterministic-simulation-tests)
  - [Miscellaneous checks](#miscellaneous-checks)
  - [Update Grafana dashboard](#update-grafana-dashboard)
  - [Add new files](#add-new-files)
  - [Add new dependencies](#add-new-dependencies)
  - [Submit PRs](#submit-prs)


## Read the design docs

Before you start to make code changes, ensure that you understand the design and implementation of RisingWave. We recommend that you read the design docs listed in [docs/README.md](README.md) first.

## Learn about the code structure

- The `src` folder contains all of the kernel components, refer to [src/README.md](../src/README.md) for more details.
- The `docker` folder contains Docker files to build and start RisingWave.
- The `e2e_test` folder contains the latest end-to-end test cases.
- The `docs` folder contains the design docs. If you want to learn about how RisingWave is designed and implemented, check out the design docs here.
- The `dashboard` folder contains RisingWave dashboard v2.

## Set up the development environment

RiseDev is the development mode of RisingWave. To develop RisingWave, you need to build from the source code and run RiseDev. RiseDev can be built on macOS and Linux. It has the following dependencies:

* Rust toolchain
* CMake
* protobuf
* OpenSSL
* PostgreSQL (psql) (>= 14.1)
* Tmux

To install the dependencies on macOS, run:

```shell
brew install postgresql cmake protobuf openssl tmux
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

To install the dependencies on Debian-based Linux systems, run:

```shell
sudo apt install make build-essential cmake protobuf-compiler curl openssl libssl-dev libcurl4-openssl-dev pkg-config postgresql-client tmux lld
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

Then you'll be able to compile and start RiseDev!

## Start and monitor a dev cluster

You can now build RiseDev and start a dev cluster. It is as simple as:

```shell
./risedev d                        # shortcut for ./risedev dev
psql -h localhost -p 4566 -d dev -U root
```

If you detect memory bottlenecks while compiling, either allocate some disk space on your computer as swap memory, or lower the compilation parallelism with [`CARGO_BUILD_JOBS`](https://doc.rust-lang.org/cargo/reference/config.html#buildjobs), e.g. `CARGO_BUILD_JOBS=2`.

The default dev cluster includes metadata-node, compute-node and frontend-node processes, and an embedded volatile in-memory state storage. No data will be persisted. This configuration is intended to make it easier to develop and debug RisingWave.

To stop the cluster:

```shell
./risedev k # shortcut for ./risedev kill
```

To view the logs:

```shell
./risedev l # shortcut for ./risedev logs
```

To clean local data and logs:

```shell
./risedev clean-data
```

### Configure additional components

There are a few components that you can configure in RiseDev.

Use the `./risedev configure` command to start the interactive configuration mode, in which you can enable and disable components.

- Hummock (MinIO + MinIO-CLI): Enable this component to persist state data.
- Prometheus and Grafana: Enable this component to view RisingWave metrics. You can view the metrics through a built-in Grafana dashboard.
- Etcd: Enable this component if you want to persist metadata node data.
- Kafka: Enable this component if you want to create a streaming source from a Kafka topic.
- Jaeger: Use this component for tracing.

To manually add those components into the cluster, you will need to configure RiseDev to download them first. For example,

```shell
./risedev configure enable prometheus-and-grafana # enable Prometheus and Grafana
./risedev configure enable minio                  # enable MinIO
```
> **Note**
>
> Enabling a component with the `./risedev configure enable` command will only download the component to your environment. To allow it to function, you must revise the corresponding configuration setting in `risedev.yml` and restart the dev cluster.

For example, you can modify the default section to:

```yaml
  default:
    - use: minio
    - use: meta-node
      enable-dashboard-v2: false
    - use: compute-node
    - use: frontend
    - use: prometheus
    - use: grafana
    - use: zookeeper
      persist-data: true
    - use: kafka
      persist-data: true
```

> **Note**
>
> The Kafka service depends on the ZooKeeper service. If you want to enable the Kafka component, enable the ZooKeeper component first.

Now you can run `./risedev d` to start a new dev cluster. The new dev cluster will contain components as configured in the yaml file. RiseDev will automatically configure the components to use the available storage service and to monitor the target.

You may also add multiple compute nodes in the cluster. The `ci-3cn-1fe` config is an example.

### Configure system variables

You can check `src/common/src/config.rs` to see all the configurable variables. 
If additional variables are needed, 
include them in the correct sections (such as `[server]` or `[storage]`) in `src/config/risingwave.toml`.


### Start the playground with RiseDev

If you do not need to start a full cluster to develop, you can issue `./risedev p` to start the playground, where the metadata node, compute nodes and frontend nodes are running in the same process. Logs are printed to stdout instead of separate log files.

```shell
./risedev p # shortcut for ./risedev playground
```

For more information, refer to `README.md` under `src/risedevtool`.

### Start the playground with cargo

To start the playground (all-in-one process) from IDE or command line, you can use:

```shell
cargo run --bin risingwave -- playground
```

Then, connect to the playground instance via:

```shell
psql -h localhost -p 4566 -d dev -U root
```

## Develop the dashboard

Currently, RisingWave has two versions of dashboards. You can use RiseDev config to select which version to use.

The dashboard will be available at `http://127.0.0.1:5691/` on meta node.

### Dashboard v1

Dashboard v1 is a single HTML page. To preview and develop this version, install Node.js, and run this command:

```shell
cd src/meta/src/dashboard && npx reload -b
```

Dashboard v1 is bundled by default along with meta node. When the cluster is started, you may use the dashboard without any configuration.

### Dashboard v2

The development instructions for dashboard v2 are available [here](../dashboard/README.md).

## Observability components

RiseDev supports several observability components.

### Cluster Control

`risectl` is the tool for providing internal access to the RisingWave cluster. See

```
cargo run --bin risectl -- --help
```

... or

```
./risingwave risectl --help
```

for more information.

### Monitoring

Uncomment `grafana` and `prometheus` lines in `risedev.yml` to enable Grafana and Prometheus services. 

### Tracing

Compute nodes support streaming tracing. Tracing is not enabled by default. You need to
use `./risedev configure` to download the tracing components first. After that, you will need to uncomment `jaeger`
service in `risedev.yml` and start a new dev cluster to allow the components to work.

### Dashboard

You may use RisingWave Dashboard to see actors in the system. It will be started along with meta node.

### Logging

The Rust components use `tokio-tracing` to handle both logging and tracing. The default log level is set as:

* Third-party libraries: warn
* Other libraries: debug

If you need to adjust log levels, change the logging filters in `utils/runtime/lib.rs`.


## Test your code changes

Before you submit a PR, fully test the code changes and perform necessary checks.

The RisingWave project enforces several checks in CI. Every time the code is modified, you need to perform the checks and ensure they pass.

### Lint

RisingWave requires all code to pass fmt, clippy, sort and hakari checks. Run the following commands to install test tools and perform these checks.

```shell
./risedev install-tools # Install required tools for running unit tests
./risedev c             # Run all checks. Shortcut for ./risedev check
```

### Unit tests

RiseDev runs unit tests with cargo-nextest. To run unit tests:

```shell
./risedev install-tools # Install required tools for running unit tests
./risedev test          # Run unit tests
```

If you want to see the coverage report, run this command:

```shell
./risedev test-cov
```

### Planner tests

RisingWave's SQL frontend has SQL planner tests. For more information, see [Planner Test Guide](../src/frontend/planner_test/README.md).

### End-to-end tests

Use [sqllogictest-rs](https://github.com/risinglightdb/sqllogictest-rs) to run RisingWave e2e tests.

sqllogictest installation is included when you install test tools with the `./risedev install-tools` command. You may also install it with:

```shell
cargo install --git https://github.com/risinglightdb/sqllogictest-rs --bin sqllogictest
```

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

> **Note**
>
> Use `-j 1` to create a separate database for each test case, which can ensure that previous test case failure won’t affect other tests due to table cleanups.

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

### End-to-end tests on CI

Basically, CI is using the following two configurations to run the full e2e test suite:

```shell
./risedev dev ci-3cn-1fe
```

You can adjust the environment variable to enable some specific code to make all e2e tests pass. Refer to GitHub Action workflow for more information.

### DocSlt tests

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

### Deterministic simulation tests

Deterministic simulation is a powerful tool to efficiently search bugs and reliably reproduce them.
In case you are not familiar with this technique, here is a [talk](https://www.youtube.com/watch?v=4fFDFbi3toc) and a [blog post](https://sled.rs/simulation.html) for brief introduction.

In RisingWave, deterministic simulation is supported in both unit test and end-to-end test. You can run them using the following commands:

```sh
# run deterministic unit test
./risedev stest
# run deterministic end-to-end test
./risedev sslt './e2e_test/path/to/directory/**/*.slt'
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

Deterministic test is included in CI as well. See [CI script](../ci/scripts/deterministic-e2e-test.sh) for details.

## Miscellaneous checks

For shell code, please run:

```shell
brew install shellcheck
shellcheck <new file>
```

For Protobufs, we rely on [buf](https://docs.buf.build/installation) for code formatting and linting. Please check out their documents for installation. To check if you violate the rules, please run the commands:

```shell
buf format -d --exit-code
buf lint
```

## Update Grafana dashboard

See [README](../grafana/README.md) for more information.

## Add new files

We use [skywalking-eyes](https://github.com/apache/skywalking-eyes) to manage license headers.
If you added new files, please follow the installation guide and run:

```shell
license-eye -c .licenserc.yaml header fix
```

## Add new dependencies

To avoid rebuild some common dependencies across different crates in workspace, use
[cargo-hakari](https://docs.rs/cargo-hakari/latest/cargo_hakari/) to ensure all dependencies
are built with the same feature set across workspace. You'll need to run `cargo hakari generate`
after deps get updated.

Use [cargo-udeps](https://github.com/est31/cargo-udeps) to find unused dependencies in
workspace.

And use [cargo-sort](https://crates.io/crates/cargo-sort) to ensure all deps are get sorted.

## Submit PRs

Instructions about submitting PRs are included in the [contribution guidelines](../CONTRIBUTING.md).
