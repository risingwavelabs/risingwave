# RisingWave

[![Slack](https://badgen.net/badge/Slack/Join%20RisingWave/0abd59?icon=slack)](https://join.slack.com/t/risingwave-community/shared_invite/zt-120rft0mr-d8uGk3d~NZiZAQWPnElOfw)
[![CI](https://github.com/singularity-data/risingwave-dev/actions/workflows/main.yml/badge.svg)](https://github.com/singularity-data/risingwave-dev/actions/workflows/main.yml)
[![codecov](https://codecov.io/gh/singularity-data/risingwave-dev/branch/main/graph/badge.svg?token=EB44K9K38B)](https://codecov.io/gh/singularity-data/risingwave-dev)

## Download
Run:
```shell
git clone https://github.com/singularity-data/risingwave-dev.git
```

## Environment
* OS: macOS, Linux, or Windows (WSL or Cygwin)
* Java 11
* Rust
* CMake
* Protocol Buffers
* OpenSSL
* PostgreSQL (psql) (>= 14.1)

To install compilers in macOS, run:

```shell
brew install java11
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
brew install cmake
brew install protobuf
brew install openssl
```

Note that we only tested our code against Java 11. So please use the specific version!

## Development
You should have already seen multiple folders in our repo:
- The `java` folder contains the system's frontend code. The frontend includes parser, binder, planner,
optimizer, and other components. We use Calcite to serve as our query optimizer.
- The `rust` folder contains the system's backend code. The backend includes the streaming engine, OLAP
engine, storage engine and meta service.
- The `e2e_test` folder contains the latest end-to-end test cases.


### RiseDev

RiseDev is the new tool for developing RisingWave. You'll need to install tmux (>= 3.2a) beforehand.

```
brew install tmux wget
```

Then, simply run:

```
./risedev d # shortcut for ./risedev dev
./risedev ci-streaming
./risedev ci-3node
./risedev dev-compute-node # compute node will need to be started by you
```

Everything will be set for you.

To stop the playground,

```
./risedev k # shortcut for ./risedev kill
```

And you can configure components for RiseDev.

```
./risedev configure
```

For more information, refer to `README.md` under `rust/risedev`.

### Dashboard

To preview the web page, install Node.js, and

```
cd rust/meta/src/dashboard && npx reload -b
```

## Deployment
To run the system, you need to use at least four terminals:
- The Postgres shell is responsible for sending user command to the frontend server and displaying
results returned by the database.
- The frontend server receives the user command, performs parsing, binding, planning, optimization,
and then passes the physical plan to the corresponding to compute server(s).
- The compute server performs computation and then returns results to the frontend server.
- The meta server performs epoch, id generation and metadata management.

To start the meta server, create one terminal and then type:
```shell
make rust_build ## Not necessary if previously built
cd rust
./target/debug/meta-node --log4rs-config config/log4rs.yaml
```

To start the frontend server, create one terminal and then type:
```shell
cd java
./gradlew -p pgserver run 
# Or it can be specified by the parameter  ./gradlew -p pgserver run --args="-c ./src/main/resources/server.properties"
```

To start the compute server, create one terminal and then type:
```shell
make rust_build
cd rust
```
You can choose to use `minio` as the serving remote storage.
```shell
./target/debug/compute-node --log4rs-config config/log4rs.yaml
# With hummock state store
./target/debug/compute-node --log4rs-config config/log4rs.yaml --state-store hummock+minio://key:secret@localhost:2333/bucket
```
Alternatively, you can choose to use `Amazon S3` as the serving remote storage.
```shell
# With Amazon S3 and test accounts, follow https://singularity-data.larksuite.com/docs/docuszYRfc00x6Q0QhqidP2AxEg to set up the test credentials.
./target/debug/compute-node --log4rs-config config/log4rs.yaml --state-store hummock+s3://s3-ut
```

```shell
# With tikv state store
tiup playground
cargo build --features tikv
./target/debug/compute-node --log4rs-config config/log4rs.yaml --state-store "tikv://<pd_address>"
```

```shell
# With rocksdb state store
cargo build --features rocksdb-local
./target/debug/compute-node --log4rs-config config/log4rs.yaml --state-store rocksdb_local:///tmp/default
```

To start the Postgres shell, create one terminal and then type:
```shell
psql -h localhost -p 4567 -d dev
```

Now you can use the Postgres shell to try out your SQL command!

## Contributing
Thanks for your interest in contributing to the project, please refer to the [CONTRIBUTING.md](https://github.com/singularity-data/risingwave-dev/blob/main/CONTRIBUTING.md).

## Toolchain
Currently, we are using nightly toolchain `nightly-2022-01-13`. If anyone needs to upgrade
the toolchain, be sure to bump `rust-toolchain` file as well as GitHub workflow.

## Documentation

The Rust codebase is documented with docstring, and you could view the documentation by:

```shell
make rust_doc
cd rust/target/doc
open risingwave/index.html
```
