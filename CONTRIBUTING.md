# Contribution and Development Guidelines

Thanks for your interest in contributing to RisingWave! We welcome and appreciate contributions.

If you have questions, please [create a Github issue](https://github.com/singularity-data/risingwave/issues/new/choose) or ask in the RisingWave Community channel on Slack. Please use the [invitation link](https://join.slack.com/t/risingwave-community/shared_invite/zt-120rft0mr-d8uGk3d~NZiZAQWPnElOfw) to join the channel.


- [Contribution and Development Guidelines](#contribution-and-development-guidelines)
  - [Code Structure](#code-structure)
  - [Setting Up Development Environment](#setting-up-development-environment)
  - [Start and Monitor a Dev Cluster](#start-and-monitor-a-dev-cluster)
    - [Additional Components](#additional-components)
    - [Start All-In-One Process](#start-all-in-one-process)
  - [Testing and Lint](#testing-and-lint)
    - [Lint](#lint)
    - [Unit Tests](#unit-tests)
    - [Planner Tests](#planner-tests)
    - [End-to-End Testing](#end-to-end-testing)
    - [End-to-End Testing on CI](#end-to-end-testing-on-ci)
  - [Developing Dashboard](#developing-dashboard)
    - [Dashboard V1](#dashboard-v1)
    - [Dashboard v2](#dashboard-v2)
  - [Observability](#observability)
    - [Monitoring](#monitoring)
    - [Tracing](#tracing)
    - [Dashboard](#dashboard)
    - [Logging](#logging)
  - [Misc Check](#misc-check)
  - [Submit a PR](#submit-a-pr)
    - [Pull Request Title](#pull-request-title)
    - [Pull Request Description](#pull-request-description)
    - [CLA](#cla)
  - [Update CI Workflow](#update-ci-workflow)
  - [When adding new files...](#when-adding-new-files)
  - [When adding new dependencies...](#when-adding-new-dependencies)
  - [To check-in PRs from forks...](#to-check-in-prs-from-forks)

## Code Structure

- The `legacy` folder contains RisingWave legacy frontend code. This is to be deprecated, and should not be used in production environment.
- The `src` folder contains all of the kernal components, refer to [src/README.md](src/README.md) for more details.
- The `docker` folder contains Dockerfiles to build and start RisingWave.
- The `e2e_test` folder contains the latest end-to-end test cases.
- The `docs` folder contains user and developer docs. If you want to learn about RisingWave, it's a good place to go!
- The `dashbaord` folder contains RisingWave dashboard v2.

## Setting Up Development Environment

To contribute to RisingWave, the first thing is to set up the development environment. You'll need:

* OS: macOS, Linux
* Rust toolchain
* CMake
* protobuf
* OpenSSL
* PostgreSQL (psql) (>= 14.1)

To install components in macOS, run:

```shell
brew install postgresql cmake protobuf openssl tmux
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

To install components in Debian-based Linux systems, run:

```shell
sudo apt install make build-essential cmake protobuf-compiler curl openssl libssl-dev pkg-config postgresql-client
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

Then you'll be able to compile and start RisingWave!

## Start and Monitor a Dev Cluster

We use RiseDev to manage the full cycle of development. Using RiseDev requires `tmux` to be installed.

```shell
# macOS
brew install tmux

# Debian
sudo apt install tmux
```

RiseDev can help you compile and start a dev cluster. It is as simple as:

```shell
./risedev d                        # shortcut for ./risedev dev
psql -h localhost -p 4566
```

The default dev cluster includes meta-node, compute-node and frontend-node processes and an embedded volatile in-memory state storage. No data will be persisted. This should be very useful when developing and debugging.

To stop the cluster,

```shell
./risedev k # shortcut for ./risedev kill
```

To view the logs,

```shell
./risedev l # shortcut for ./risedev logs
```

To clean local data and logs,

```shell
./risedev clean-data
```

### Additional Components

RiseDev supports automatic config of some components. We can add components like etcd, MinIO, Grafana, Prometheus and jaeger to the cluster, so as to persist data and monitor the cluster.

To add those components into the cluster, you will need to configure RiseDev to download them at first. For example,

```shell
./risedev configure                               # start the interactive guide to enable components
./risedev configure enable prometheus-and-grafana # enable Prometheus and Grafana
./risedev configure enable minio                  # enable MinIO
```

After that, you can modify `risedev.yml` to compose the cluster. For example, we can modify the default section to:

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

Now we can run `./risedev d`. The new dev cluster will contain components from RisingWave, as well as MinIO to persist storage data, and Grafana to monitor the cluster. RiseDev will automatically configure the components to use the available storage service and monitor target.

You may also add multiple compute nodes in the cluster. The `ci-3node` config is an example.

### Start All-In-One Process

Sometimes, developers might not need to start a full cluster to develop. `./risedev p` can help start an all-in-one process, where meta-node, compute-node and frontend-node are running in the same process. Logs are also printed to stdout instead of separate log files.

```shell
./risedev p # shortcut for ./risedev playground
```

For more information, refer to `README.md` under `src/risedevtool`.

## Testing and Lint

The RisingWave project enforces several checks in CI. Everytime the code gets modified, several checks should be done.

### Lint

RisingWave requires all code to pass fmt, clippy, sort and hakari checks, which can be done in a single command:

```shell
./risedev install-tools # Install required tools for running unit tests
./risedev c             # Run all checks, shortcut for ./risedev check
```

### Unit Tests

RiseDev helps run unit tests fast with cargo-nextest. To run unit tests:

```shell
./risedev install-tools # Install required tools for running unit tests
./risedev test          # Run unit tests
```

If you want to see the coverage report,

```shell
./risedev test-cov
```

### Planner Tests

RisingWave's SQL frontend has some tests for plans of specific SQLs. See [Planner Test Guide](src/frontend/test_runner/README.md) for more information.

### End-to-End Testing

Currently, we use [sqllogictest-rs](https://github.com/risinglightdb/sqllogictest-rs) to run RisingWave e2e tests.

sqllogictest installation is included when running `./risedev install-tools`. You may also install it with:

```shell
cargo install --git https://github.com/risinglightdb/sqllogictest-rs --features bin
```

To run end-to-end test, you will need to start a full cluster first:

```shell
./risedev d
```

Then run some e2e tests:

```shell
./risedev slt -p 4566 './e2e_test/v2/**/*.slt'
```

After running e2e tests, you may kill the cluster and clean data.

```shell
./risedev k
./risedev clean-data
```

As RisingWave's codebase is constantly changing, and all persistent data might not be in a stable format, if there's
some unexpected decode error, try `./risedev clean-data` first.

### End-to-End Testing on CI

As we are in the process of deprecating the legacy Java frontend, CI runs e2e tests with the legacy Java frontend.
We also have some special settings with environment variable to workaround some problems.

Basically, CI is using the following two configuration to run the full e2e test suite:

```shell
./risedev dev ci-3node
./risedev dev ci-1node
```

We may adjust the environment variable to enable some specific code to make all e2e tests pass. Refer to GitHub Action workflow for more information.

## Developing Dashboard

Currently, we have two versions of RisingWave dashboard. You may use RiseDev config to select which dashboard to use.

Dashboard will be available at `http://127.0.0.1:5691/` on meta node.

### Dashboard V1

Dashboard V1 is a single HTML page. To preview and develop the web page, install Node.js, and

```shell
cd src/meta/src/dashboard && npx reload -b
```

Dashboard V1 is bundled by default along with meta node. Without any configuration, you may use the dashboard when
the cluster is started.

### Dashboard v2

The developement instructions for dashboard v2 is in [here](https://github.com/singularity-data/risingwave/blob/main/dashboard/README.md).

## Observability

RiseDev supports several observability components in RisingWave cluster.

### Monitoring

Uncomment `grafana` and `prometheus` services in `risedev.yml`, and you can view the metrics through a built-in Grafana dashboard.

### Tracing

Compute node supports streaming tracing. Tracing is not enabled by default, and you will need to
use `./risedev configure` to enable tracing components. After that, simply uncomment `jaeger`
service in `risedev.yml`.

### Dashboard

You may use RisingWave Dashboard to see actors in the system. It will be started along with meta node.

### Logging

The Java frontend uses `logback.xml` to configure its logging config.

The Rust components use `tokio-tracing` to handle both logging and tracing. The default log level is set as:

* Third-party libraries: warn
* Other libraries: debug

If you need to adjust log levels, simply change the logging filters in `utils/logging/lib.rs`.

## Misc Check

For shell code, please run:

```shell
brew install shellcheck
shellcheck <new file>
```

For Protobufs, we rely on [prototool](https://github.com/uber/prototool#prototool-format) and [buf](https://docs.buf.build/installation) for code formatting and linting. Please check out their documents for installation. To check if you violate the rule, please run the commands:

```shell
prototool format -d
buf lint
```

## Submit a PR

### Pull Request Title

As described in [here](https://github.com/commitizen/conventional-commit-types/blob/master/index.json), a valid PR title should begin with one of the following prefixes:

- `feat`: A new feature
- `fix`: A bug fix
- `docs`: Documentation only changes
- `style`: Changes that do not affect the meaning of the code (white-space, formatting, missing semi-colons, etc)
- `refactor`: A code change that neither fixes a bug nor adds a feature
- `perf`: A code change that improves performance
- `test`: Adding missing tests or correcting existing tests
- `build`: Changes that affect the build system or external dependencies (example scopes: gulp, broccoli, npm)
- `ci`: Changes to RisingWave CI configuration files and scripts (example scopes: Travis, Circle, BrowserStack, SauceLabs)
- `chore`: Other changes that don't modify src or test files
- `revert`: Reverts a previous commit

For example, a PR title could be:

- `refactor: modify executor protobuf package path`
- `feat(execution): enable comparison between nullable data arrays`, where `(execution)` means that this PR mainly focuses on the execution component.

You may also check out previous PRs in the [PR list](https://github.com/singularity-data/risingwave/pulls).

### Pull Request Description

- If your PR is small (such as a typo fix), you can go brief.
- If it is large and you have changed a lot, it's better to write more details.

### CLA

Contributors will need to sign Singularity Data's CLA.

## Update CI Workflow

We use scripts to generate GitHub Action configurations based on templates in `.github/workflow-template`.

To edit the workflow files, you will need to install `yq` >= 4.16.

```shell
> brew install yq
> yq --version
yq (https://github.com/mikefarah/yq/) version 4.16.1
```

Then, you may edit the files in `workflow-template`.

* `template.yml` + `main-override.yml` = `main.yml`
* `template.yml` + `pr-override.yml` = `pull-request.yml`

After that, run `apply-ci-template` to update the final workflow config.

```shell
./risedev apply-ci-template
```

## When adding new files...

We use [skywalking-eyes](https://github.com/apache/skywalking-eyes) to manage license headers.
If you added new files, please follow the installation guide and run:

```
license-eye -c .licenserc.yaml header fix
```

## When adding new dependencies...

To avoid rebuild some common dependencies across different crates in workspace, we use
[cargo-hakari](https://docs.rs/cargo-hakari/latest/cargo_hakari/) to ensure all dependencies
are built with the same feature set across workspace. You'll need to run `cargo hakari generate`
after deps get updated.

Also, we use [cargo-udeps](https://github.com/est31/cargo-udeps) to find unused dependencies in
workspace.

We use [cargo-sort](https://crates.io/crates/cargo-sort) to ensure all deps are get sorted.

## To check-in PRs from forks...

Simply comment

```
bors try
```

in PR to run tests in forks. Note that we don't use bors to merge PRs.
