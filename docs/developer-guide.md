# Developer guide

This guide is intended to be used by contributors to learn about how to develop RisingWave. The instructions about how to submit code changes are included in [Contributing guidelines](../CONTRIBUTING.md).

If you have questions, please [create a Github issue](https://github.com/singularity-data/risingwave/issues/new/choose) or ask in the RisingWave Community channel on Slack. Please use the [invitation link](https://join.slack.com/t/risingwave-community/shared_invite/zt-120rft0mr-d8uGk3d~NZiZAQWPnElOfw) to join the channel.

- [Read the design docs](#read-the-design-docs)
- [Learn about the code structure](#learn-about-the-code-structure)
- [Set up the development environment](#set-up-the-development-environment)
    - [Start and monitor a dev cluster](#start-and-monitor-a-dev-cluster)
    - [Configure additional components](#configure-additional-components)
    - [Start the playground with RiseDev](#start-the-playground-with-risedev)
    - [Start the playground with cargo](#start-the-playground-with-cargo)
- [Develop the dashboard](#developing-dashboard)
    - [Dashboard V1](#dashboard-v1)
    - [Dashboard v2](#dashboard-v2)
- [Observability components](#observability-components))
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
- [Miscallenous checks](#miscellaneous-checks)


## Read the design docs

Before you start to make code changes, ensure that you understand the design and implementation of RisingWave. We recommend that you read the design docs listed in the [readme.md](readme.md).

## Learn about the code structure

- The `src` folder contains all of the kernal components, refer to [src/README.md](src/README.md) for more details.
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
sudo apt install make build-essential cmake protobuf-compiler curl openssl libssl-dev pkg-config postgresql-client tmux
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

Then you'll be able to compile and start RiseDev!

## Start and monitor a dev cluster

You can now build RiseDev and start a dev cluster. It is as simple as:

```shell
./risedev d                        # shortcut for ./risedev dev
psql -h localhost -p 4566
```

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
- Prometheus and Grafana: Enable this component to view RisingWave thoroughput and performance metrics. You can view the metrics through a built-in Grafana dashboard.
- Etcd: Enable this component if you want to persist metadata node data.
- Jaeger: Use this component for tracing.


To manually add those components into the cluster, you will need to configure RiseDev to download them first. For example,

```shell
./risedev configure enable prometheus-and-grafana # enable Prometheus and Grafana
./risedev configure enable minio                  # enable MinIO
```

After that, you can modify `risedev.yml` to reconfigure the cluster. For example, you can modify the default section to:

```yaml
  default:
    - use: minio
    - use: meta-node
      enable-dashboard-v2: false
    - use: compute-node
    - use: frontend
    - use: prometheus
    - use: grafana
```

Now we can run `./risedev d`. The new dev cluster will contain components as configured in the yaml file. RiseDev will automatically configure the components to use the available storage service and to monitor the target.

You may also add multiple compute nodes in the cluster. The `ci-3cn-1fe` config is an example.

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
psql -h localhost -p 4566
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

The development instructions for dashboard v2 are available [here](https://github.com/singularity-data/risingwave/blob/main/dashboard/README.md).

## Observability components

RiseDev supports several observability components.

### Monitoring

Uncomment `grafana` and `prometheus` lines in `risedev.yml` to enable Grafana and Prometheus services. 

### Tracing

Compute nodes support streaming tracing. Tracing is not enabled by default. You will need to
use `./risedev configure` to enable tracing components. After that, simply uncomment `jaeger`
service in `risedev.yml`.

### Dashboard

You may use RisingWave Dashboard to see actors in the system. It will be started along with meta node.

### Logging

The Rust components use `tokio-tracing` to handle both logging and tracing. The default log level is set as:

* Third-party libraries: warn
* Other libraries: debug

If you need to adjust log levels, change the logging filters in `utils/logging/lib.rs`.


## Test your code changes

Before you submit a PR, we recommend that you fully test the code changes and perform necessary checks.

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

RisingWave's SQL frontend has SQL planner tests. For more information, see [Planner Test Guide](src/frontend/test_runner/README.md).

### End-to-end tests

Currently, we use [sqllogictest-rs](https://github.com/risinglightdb/sqllogictest-rs) to run RisingWave e2e tests.

sqllogictest installation is included when you install test tools with the `./risedev install-tools` command. You may also install it with:

```shell
cargo install --git https://github.com/risinglightdb/sqllogictest-rs --features bin
```

Before running end-to-end tests, you will need to start a full cluster first:

```shell
./risedev d
```

Then run the end-to-end tests (replace `**/*.slt` with the test case directories and files available):

```shell
./risedev slt -p 4566 './e2e_test/streaming/**/*.slt'
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
./risedev dev ci-1cn-1fe
```

We may adjust the environment variable to enable some specific code to make all e2e tests pass. Refer to GitHub Action workflow for more information.

## Miscellaneous checks

For shell code, please run:

```shell
brew install shellcheck
shellcheck <new file>
```

For Protobufs, we rely on [buf](https://docs.buf.build/installation) for code formatting and linting. Please check out their documents for installation. To check if you violate the rule, please run the commands:

```shell
buf format -d --exit-code
buf lint
```