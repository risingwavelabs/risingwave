<picture>
<img src='./docs/images/logo-title.svg' alt='RisingWave Logo' style='visibility:visible'>
</picture>

[![Slack](https://badgen.net/badge/Slack/Join%20RisingWave/0abd59?icon=slack)](https://join.slack.com/t/risingwave-community/shared_invite/zt-120rft0mr-d8uGk3d~NZiZAQWPnElOfw)
[![Build status](https://badge.buildkite.com/9394d2bca0f87e2e97aa78b25f765c92d4207c0b65e7f6648f.svg)](https://buildkite.com/risingwavelabs/main)
[![codecov](https://codecov.io/gh/risingwavelabs/risingwave/branch/main/graph/badge.svg?token=EB44K9K38B)](https://codecov.io/gh/risingwavelabs/risingwave)

RisingWave is a distributed SQL database for stream processing. It is designed to reduce the complexity and cost of building real-time applications. RisingWave consumes streaming data, performs incremental computations when new data comes in, and updates results dynamically. As a database system, RisingWave maintains results inside its own storage so that users can access data efficiently.

RisingWave ingests data from sources like Apache Kafka, Apache Pulsar, Amazon Kinesis, Redpanda, and materialized CDC sources.

Data in RisingWave can be output to external targets such as message brokers, data warehouses, and data lakes for storage or additional processing.

*RisingWave has achieved production-readiness and has been effectively deployed in dozens of companies across a diverse range of industries, including entertainment, fintech, social media, and manufacturing.*

Learn more at [Introduction to RisingWave](https://www.risingwave.dev/docs/current/intro/).

## RisingWave Cloud

RisingWave Cloud is the fully managed service of RisingWave Database. It is now in Beta. Feel free to try out at: [risingwave.com/cloud](risingwave.com/cloud).

## Notes on telemetry

RisingWave collects anonymous usage statistics to better understand how the community is using RisingWave. The sole intention of this exercise is to help improve the product. These statistics are related to system resource usage, OS versions and system uptime. RisingWave doesn't have access to any user data or metadata running on RisingWave clusters including source and sink connection parameters, sources, sinks, materialized views, and tables. Users have an option to opt out of this collection using a system parameter. Please refer to the RisingWave user documentation for more details.

## Quick Start

### Installation

There are two ways to install RisingWave: use a pre-built package or compile from source.

**Use a Pre-built Package (Linux)**

```shell
# Download the pre-built binary
wget https://github.com/risingwavelabs/risingwave/releases/download/v0.18.0/risingwave-v0.18.0-x86_64-unknown-linux.tar.gz
# Unzip the binary
tar xvf risingwave-v0.18.0-x86_64-unknown-linux.tar.gz
# Start RisingWave in single-binary playground mode
./risingwave playground
```

**Use Docker (Linux, macOS)**

```shell
# Start RisingWave in single-binary playground mode
docker run -it --pull=always -p 4566:4566 -p 5691:5691 ghcr.io/risingwavelabs/risingwave:v0.18.0 playground
```

**Compile from Source with [RiseDev](docs/developer-guide.md#set-up-the-development-environment) (Linux and macOS)**

```shell
# Install Rust toolchain
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
# Clone the repo
git clone https://github.com/risingwavelabs/risingwave.git && cd risingwave
# Compile and start the playground
./risedev playground
```

To build from source, you need to pre-install several tools in your system. You may use `./risedev configure` to configure compile settings. Please refer to the [developer guide](docs/developer-guide.md) for more information.

You can launch a RisingWave cluster and process streaming data in a distributed manner, and enable other features like metrics collection and data persistence. Please refer to the [developer guide](docs/developer-guide.md) for more information.

### Your First Query

To connect to the RisingWave server, you will need to [install PostgreSQL shell](docs/developer-guide.md#set-up-the-development-environment) (`psql`) in advance.

```shell
# Use psql to connect RisingWave cluster
psql -h localhost -p 4566 -d dev -U root
```

```sql
/* create a table */
create table t1(v1 int);

/* create a materialized view based on the previous table */
create materialized view mv1 as select sum(v1) as sum_v1 from t1;

/* insert some data into the source table */
insert into t1 values (1), (2), (3);

/* (optional) ensure the materialized view has been updated */
flush;

/* the materialized view should reflect the changes in source table */
select * from mv1;
```

If everything works correctly, you should see

```
 sum_v1
--------
      6
(1 row)
```

in the terminal.

### Connecting to an External Source

Please refer to [get started guide](https://www.risingwave.dev/docs/current/get-started/) for more information.

## Documentation

To learn about how to use RisingWave, refer to [RisingWave docs](https://www.risingwave.dev/). To learn about the development process, see the [developer guide](docs/developer-guide.md). To understand the design and implementation of RisingWave, refer to the design docs listed in [readme.md](docs/README.md).

## License

RisingWave is under the Apache License 2.0. Please refer to [LICENSE](LICENSE) for more information.

## Contributing

Thanks for your interest in contributing to the project! Please refer to [contribution guidelines](CONTRIBUTING.md) for more information.
