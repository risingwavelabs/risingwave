<p align="center">
  <picture>
    <source srcset=".github/RisingWave-logo-dark.svg" width="500px" media="(prefers-color-scheme: dark)">
    <img src=".github/RisingWave-logo-light.svg" width="500px">
  </picture>
</p>

[![Slack](https://badgen.net/badge/Slack/Join%20RisingWave/0abd59?icon=slack)](https://join.slack.com/t/risingwave-community/shared_invite/zt-120rft0mr-d8uGk3d~NZiZAQWPnElOfw)
[![Build status](https://badge.buildkite.com/9394d2bca0f87e2e97aa78b25f765c92d4207c0b65e7f6648f.svg)](https://buildkite.com/risingwavelabs/main)
[![codecov](https://codecov.io/gh/risingwavelabs/risingwave/branch/main/graph/badge.svg?token=EB44K9K38B)](https://codecov.io/gh/risingwavelabs/risingwave)

RisingWave is a distributed SQL streaming database. It is designed to reduce the complexity and cost of building stream processing applications. RisingWave consumes streaming data, performs incremental computations when new data comes in, and updates results dynamically. As a database system, RisingWave maintains results inside its own storage so that users can access data efficiently.

RisingWave offers wire compatibility with PostgreSQL and demonstrates exceptional performance surpassing the previous generation of stream processing systems, including Apache Flink, by several orders of magnitude.
It particularly excels in handling complex stateful operations like multi-stream joins.

RisingWave ingests data from sources like Apache Kafka, Apache Pulsar, Amazon Kinesis, Redpanda, and materialized CDC sources. Data in RisingWave can be delivered to external targets such as message brokers, data warehouses, and data lakes for storage or additional processing.

RisingWave 1.0 is a battle-tested version that has undergone rigorous stress tests and performance evaluations. It has proven its reliability and efficiency through successful deployments in numerous production environments across dozens of companies.

Learn more at [Introduction to RisingWave](https://www.risingwave.dev/docs/current/intro/).

![RisingWave](https://github.com/risingwavelabs/risingwave-docs/blob/0f7e1302b22493ba3c1c48e78810750ce9a5ff42/docs/images/archi_simple.png)


## RisingWave Cloud

RisingWave Cloud is a fully-managed and scalable stream processing platform powered by the open-source RisingWave project. Try it out for free at: [risingwave.com/cloud](https://risingwave.com/cloud).

## Notes on telemetry

RisingWave collects anonymous usage statistics to better understand how the community is using RisingWave. The sole intention of this exercise is to help improve the product. These statistics are related to system resource usage, OS versions and system uptime. RisingWave doesn't have access to any user data or metadata running on RisingWave clusters including source and sink connection parameters, sources, sinks, materialized views, and tables. Users have the option to opt out of this collection using a system parameter. Please refer to the RisingWave user documentation for more details.

## Get started

- To learn about how to install and run RisingWave, see [Get started](https://www.risingwave.dev/docs/current/get-started/).
- To learn about how to ingest data and the supported data sources, see [Sources](https://www.risingwave.dev/docs/current/data-ingestion/).
- To learn about how to transform data using the PostgreSQL-compatible SQL of RisingWave, see [SQL reference](https://www.risingwave.dev/docs/current/sql-references/).
- To learn about how to deliver data and the supported data sinks, see [Sinks](https://www.risingwave.dev/docs/current/data-delivery/).
- To learn about new features and changes in the current and previous versions, see [Release notes](https://www.risingwave.dev/docs/current/release-notes/).

## Documentation

To learn about how to use RisingWave, refer to [RisingWave User Documentation](https://www.risingwave.dev/). To learn about the development process, see the [developer guide](docs/developer-guide.md). To understand the design and implementation of RisingWave, refer to the design docs listed in [readme.md](docs/README.md).

## Community

Looking for help, discussions, collaboration opportunities, or a casual afternoon chat with our fellow engineers and community members? Join our [Slack workspace](https://join.slack.com/t/risingwave-community/shared_invite/zt-120rft0mr-d8uGk3d~NZiZAQWPnElOfw)!

## License

RisingWave is distributed under the Apache License (Version 2.0). Please refer to [LICENSE](LICENSE) for more information.

## Contributing

Thanks for your interest in contributing to the project! Please refer to [contribution guidelines](CONTRIBUTING.md) for more information.
