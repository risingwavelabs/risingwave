# RisingWave
[![codecov](https://codecov.io/gh/singularity-data/risingwave/branch/master/graph/badge.svg?token=C5ZX0L0GWK)](https://codecov.io/gh/singularity-data/risingwave)
## Download
Run:
```
git clone https://github.com/singularity-data/risingwave.git
```

## Environment
* OS: macOS or Linux
* Java 11
* Rust
* Go
* cmake
* protobuf
* PostgreSQL

To install compilers in macOS, run:
```
brew install java11
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
brew install golang
brew install cmake
brew install protobuf
brew install postgresql
```
Note that we only tested our code against Java 11. So please use the specific version!

## Development
You should have already seen multiple folders in our repo, including `java`, `rust`,
and `e2e_test`.

The `java` folder contains the system's frontend code. The frontend includes parser, binder, planner,
optimizer, and other components. We use Calcite to serve as our query optimizer.

The `rust` folder contains the system's backend code. The backend includes the streaming engine, OLAP
engine, and storage engine.

The `e2e_test` folder contains the latest end-to-end test cases.

## Deployment
To run the system, you need to use at least three terminals, one for frontend server, one for compute server,
and one for Postgres shell. The Postgres shell is responsible for sending user command to the frontend server
and displaying results returned by the database. The frontend server receives the user command, performs parsing,
binding, planning, optimization, and then passes the physical plan to the corresponding compute server(s).
The compute server performs computation and then returns results to the frontend server.

To start the frontend server, create one terminal and then type:
```
cd java
./gradlew -p pgserver run
```

To start the compute server, create one terminal and then type:
```
make rust_build
cd rust
./target/debug/compute-node --log4rs-config config/log4rs.yaml
```

To start the Postgres shell, create one terminal and then type:
```
psql -h localhost -p 4567 -d dev
```

Now you can use the Postgres shell to try out your SQL command!

## Testing

We support both unit tests (for Rust code only) and end-to-end tests.

To run unit tests, run the following commands under the root directory:
```
make rust_fmt
make rust_check
make rust_test
```

To run end-to-end tests, start the frontend server and a compute server, and then run:
```
make sqllogictest
python3 ./scripts/sqllogictest.py -p 4567 -db dev -f ./e2e_test/distributed/
```

## Distributed Testing

To run end-to-end tests with multiple compute-nodes, run the script:

```sh
./scripts/start_cluster.sh 3
```

It will start processes in the background. After testing, you can run the following script
to cleanup:

```sh
./scripts/kill_cluster.sh
```

## Code Formatting
Before submitting your PR, you should format the code first.

For Java code, please run:
```
cd java
./gradlew spotlessApply
```

For Rust code, please run:

```
cd rust
cargo fmt
cargo clippy --all-targets --all-features
```

## Toolchain
Currently, we are using nightly toolchain `nightly-2021-09-10`. If anyone needs to upgrade
the toolchain, be sure to bump `rust-toolchain` file as well as GitHub workflow.

## Documentation

The Rust codebase is documented with docstring, and you could view the documentation by:

```bash
make rust_doc
cd rust/target/doc
open risingwave/index.html
```
