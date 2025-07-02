# How to build and run RisingWave

## Set up the development environment

### macOS

To install the dependencies on macOS, run:

```shell
brew install postgresql cmake protobuf tmux cyrus-sasl lld openssl@3 libomp
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

### Debian-based Linux

To install the dependencies on Debian-based Linux systems, run:

```shell
sudo apt install make build-essential cmake protobuf-compiler curl postgresql-client tmux lld pkg-config libssl-dev libsasl2-dev libblas-dev liblapack-dev libomp-dev
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

> If you encounter CMake error when compiling some third-party crates, it may be because the CMake version installed by package manager (ie. `brew/apt`) is too high and is incompatible with old CMake files in these crates. You can first uninstall, then manually download the required version from the [official source](https://cmake.org/download/).

### nix shell

If you use nix, you can also enter the nix shell via:

```shell
nix develop ./develop/nix
```

All dependencies will be automatically downloaded and configured.

You can also use [direnv](https://github.com/direnv/direnv) to automatically enter the nix shell:

```shell
direnv allow
```

Check out [flake.nix](https://github.com/risingwavelabs/risingwave/blob/3795168b11f216cb035dbd3130992d417ca6f9ea/develop/nix/flake.nix) to read more information!

Then you'll be able to compile and start RisingWave!

> `.cargo/config.toml` contains `rustflags` configurations like `-Clink-arg` and `-Ctarget-feature`. Since it will be [merged](https://doc.rust-lang.org/cargo/reference/config.html#hierarchical-structure) with `$HOME/.cargo/config.toml`, check the config files and make sure they don't conflict if you have global `rustflags` configurations for e.g. linker there.

> If you want to build RisingWave with Embedded Python UDF feature, you need to install Python 3.12.
>
> To install Python 3.12 on macOS, run:
>
> ```shell
> brew install python@3.12
> ```
>
> To install Python 3.12 on Debian-based Linux systems, run:
>
> ```shell
> sudo apt install software-properties-common
> sudo add-apt-repository ppa:deadsnakes/ppa
> sudo apt-get update
> sudo apt-get install python3.12 python3.12-dev
> ```
>
> If the default `python3` version is not 3.12, please set the `PYO3_PYTHON` environment variable:
>
> ```shell
> export PYO3_PYTHON=python3.12
> ```


## Start and monitor a dev cluster

RiseDev is the RisingWave developers' tool. You can now use RiseDev to start a dev cluster. Just run `risedev` script in the repo's root directory. It is as simple as:

```shell
./risedev d                        # shortcut for ./risedev dev
psql -h localhost -p 4566 -d dev -U root
```

The default dev cluster includes meta-node, compute-node and frontend-node processes, and an embedded volatile in-memory state storage. No data will be persisted. This configuration is intended to make it easier to develop and debug RisingWave.

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

### Tips for compilation

If you detect memory bottlenecks while compiling, either allocate some disk space on your computer as swap memory, or lower the compilation parallelism with [`CARGO_BUILD_JOBS`](https://doc.rust-lang.org/cargo/reference/config.html#buildjobs), e.g. `CARGO_BUILD_JOBS=2`.

### Configure additional components

There are a few additional components supported by RiseDev.

Use the `./risedev configure` command to enable and disable components.

- Hummock (MinIO + MinIO-CLI): Enable this component to persist state data.
- Prometheus and Grafana: Enable this component to view RisingWave metrics. You can view the metrics through a built-in Grafana dashboard.
- Postgres/Mysql/Sqlite: Enable this component if you want to persist metadata node data.
- Kafka: Enable this component if you want to create a streaming source from a Kafka topic.
- Grafana Tempo: Use this component for tracing.

> Enabling a component with the `./risedev configure` command will only download the component to your environment. To allow it to function, you must revise the corresponding configuration setting in `risedev.yml` and restart the dev cluster.

For example, you can modify the default section to:

```yaml
  default:
    - use: minio
    - use: meta-node
    - use: compute-node
    - use: frontend
    - use: prometheus
    - use: grafana
    - use: kafka
      persist-data: true
```

Now you can run `./risedev d` to start a new dev cluster. The new dev cluster will contain components as configured in the yaml file. RiseDev will automatically configure the components to use the available storage service and to monitor the target.

You may also add multiple compute nodes in the cluster. The `ci-3cn-1fe` config is an example.

### Configure system variables

You can check `src/common/src/config.rs` to see all the configurable variables.
If additional variables are needed,
include them in the correct sections (such as `[server]` or `[storage]`) in `src/config/risingwave.toml`.


### Start the playground

If you do not need to start a full cluster to develop, you can issue `./risedev p` to start the playground, where the metadata node, compute nodes and frontend nodes are running in the same process. Logs are printed to stdout instead of separate log files.

```shell
./risedev p # shortcut for ./risedev playground
```

For more information, refer to `README.md` under `src/risedevtool`.

You can also start the playground with `cargo` directly:

```shell
cargo run --bin risingwave -- playground
```

Then, connect to the playground instance via:

```shell
psql -h localhost -p 4566 -d dev -U root
```
