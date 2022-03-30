# RisingWave

[![Slack](https://badgen.net/badge/Slack/Join%20RisingWave/0abd59?icon=slack)](https://join.slack.com/t/risingwave-community/shared_invite/zt-120rft0mr-d8uGk3d~NZiZAQWPnElOfw)
[![CI](https://github.com/singularity-data/risingwave/actions/workflows/main.yml/badge.svg)](https://github.com/singularity-data/risingwave/actions/workflows/main.yml)
[![codecov](https://codecov.io/gh/singularity-data/risingwave/branch/main/graph/badge.svg?token=EB44K9K38B)](https://codecov.io/gh/singularity-data/risingwave)

## Download
Run:
```shell
git clone https://github.com/singularity-data/risingwave.git
```

## Environment

* OS: macOS, Linux
* Rust toolchain
* CMake
* Protocol Buffers
* OpenSSL
* PostgreSQL (psql) (>= 14.1)

To install components in macOS, run:

```shell
brew install postgresql cmake protobuf openssl tmux
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

To install components in Debian-based linux systems, run:
```shell
sudo apt update
sudo apt upgrade
sudo apt install make build-essential cmake protobuf-compiler curl openssl libssl-dev pkg-config
sudo apt install postgresql-client
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

Note that we only tested our code against Java 11. So please use the specific version!

## Development

You should have already seen multiple folders in our repo:
- The `java` folder contains the system's legacy frontend code. The frontend includes parser, binder, planner,
optimizer, and other components. We use Calcite to serve as our query optimizer.
- The `rust` folder contains the system's backend code. The backend includes the streaming engine, OLAP
engine, storage engine and meta service.
- The `e2e_test` folder contains the latest end-to-end test cases.

### RiseDev

RiseDev is the tool for developing RisingWave. Using RiseDev requires tmux.

```shell
# macOS
brew install tmux

# Debian
sudo apt install tmux
```

Then, in the root directory, simply run:

```shell
./risedev d # shortcut for ./risedev dev
psql -h localhost -p 4567 -d dev
```

Everything will be set for you.

There are a lot of other running configurations, like `ci-1node`, `ci-3node`, `dev-compute-node`. You may find more in `./risedev --help`.

To stop the playground,

```shell
./risedev k # shortcut for ./risedev kill
```

To view the logs,

```shell
./risedev l # shortcut for ./risedev logs
```

And you can configure components for RiseDev.

```shell
./risedev configure
```

For developers who only develop Rust code (e.g., frontend-v2), use the following command to start an all-in-one process:

```shell
./risedev p # shortcut for ./risedev playground
```

For more information, refer to `README.md` under `rust/risedevtool`.

### Dashboard

To preview the web page, install Node.js, and

```shell
cd rust/meta/src/dashboard && npx reload -b
```

### Dashboard v2

The developement instructions for dashboard v2 is in [here](https://github.com/singularity-data/risingwave/blob/main/dashboard/README.md).

## Contributing

Thanks for your interest in contributing to the project, please refer to the [CONTRIBUTING.md](https://github.com/singularity-data/risingwave/blob/main/CONTRIBUTING.md).

## Toolchain

Currently, we are using nightly toolchain `nightly-2022-03-09`. If anyone needs to upgrade
the toolchain, be sure to bump `rust-toolchain` file as well as GitHub workflow.

## Documentation

The Rust codebase is documented with docstring, and you could view the documentation by:

```shell
./risedev docs
cd rust/target/doc
open risingwave/index.html
```
