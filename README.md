# RisingWave

[![Slack](https://badgen.net/badge/Slack/Join%20RisingWave/0abd59?icon=slack)](https://join.slack.com/t/risingwave-community/shared_invite/zt-120rft0mr-d8uGk3d~NZiZAQWPnElOfw)
[![CI](https://github.com/singularity-data/risingwave/actions/workflows/main.yml/badge.svg)](https://github.com/singularity-data/risingwave/actions/workflows/main.yml)
[![codecov](https://codecov.io/gh/singularity-data/risingwave/branch/main/graph/badge.svg?token=EB44K9K38B)](https://codecov.io/gh/singularity-data/risingwave)

RisingWave is a cloud-native streaming database that uses SQL as the interface language. It is designed to reduce the complexity and cost of developing and using a stream processing platform so that developers can build applications more efficiently. It ingests streaming data, performs the processing that you specify (aggregates, joins, maps, enrichment, etc.), and dynamically updates the results. As a streaming database, RisingWave stores the results so that users can access them in real-time.

RisingWave ingests data from sources like Kafka, Apache Pulsar, Amazon Kinesis, Redpanda, and materialized CDC sources.

Learn more in [Introduction to RisingWave](https://singularity-data.com/risingwave-docs/docs/latest/intro/).

## Quick Start

### Installation

You may start RisingWave with our pre-built binary, or build from source.

**Use Pre-built Binary (Linux x86_64)**

```shell
wget https://github.com/singularity-data/risingwave/releases/download/v0.1.4/risingwave-v0.1.4-unknown-linux.tar.gz
tar xvf risingwave-v0.1.4-unknown-linux.tar.gz
./risingwave playground
```

**Build from Source (macOS, Linux)**

```shell
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh                 # Install Rust toolchain
git clone https://github.com/singularity-data/risingwave.git && cd risingwave  # Clone the repo
./risedev playground                                                           # Compile and start the playground
```

Building from source requires several tools to be installed in the system. You may also use `./risedev configure` to adjust compile settings. See [CONTRIBUTING](CONTRIBUTING.md) for more information.

If you want to start a full cluster, enable metrics, and persist data, you may also refer to [CONTRIBUTING](CONTRIBUTING.md) for more information.

### The First Query

To connect to the RisingWave server, you will need to install Postgres client in advance.

```shell
psql -h localhost -p 4566 # Use psql to connect Risingwave cluster
```

```sql
/* create a table */
create table t1(v1 int not null);

/* create a materialized view based on the previous table */
create materialized view mv1 as select sum(v1) as sum_v1 from t1;

/* insert some data into the source table */
insert into t1 values (1), (2), (3);

/* ensure materialized view has been updated */
flush;

/* the materialized view should reflect the changes in source table */
select * from mv1;
```

If everything works, you will see

```
 sum_v1
--------
      6
(1 row)
```

in the Postgres shell.

### Connecting to an External Source

Please refer to the full [getting started guide](https://singularity-data.com/risingwave-docs/docs/latest/getting-started/).

## Documentation

Visit [RisingWave Docs](https://singularity-data.com/risingwave-docs/) and [Developer Docs](https://github.com/singularity-data/risingwave/tree/main/docs) for more information.

## License

RisingWave is under the Apache 2.0 license. See the [LICENSE](LICENSE) file for details.

## Contributing

Thanks for your interest in contributing to the project, please refer to [CONTRIBUTING](CONTRIBUTING.md) for more information.
