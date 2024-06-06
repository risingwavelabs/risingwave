
<p align="center">
  <picture>
    <source srcset=".github/RisingWave-logo-dark.svg" width="500px" media="(prefers-color-scheme: dark)">
    <img src=".github/RisingWave-logo-light.svg" width="500px">
  </picture>
</p>


<div align="center">

### 🌊 Reimagine stream processing.

</div>

<p align="center">
  <a
    href="https://docs.risingwave.com/"
    target="_blank"
  ><b>Documentation</b></a>&nbsp;&nbsp;&nbsp;📑&nbsp;&nbsp;&nbsp;
  <a
    href="https://tutorials.risingwave.com/"
    target="_blank"
  ><b>Hands-on Tutorials</b></a>&nbsp;&nbsp;&nbsp;🎯&nbsp;&nbsp;&nbsp;
  <a
    href="https://cloud.risingwave.com/"
    target="_blank"
  ><b>RisingWave Cloud</b></a>&nbsp;&nbsp;&nbsp;🚀&nbsp;&nbsp;&nbsp;
  <a
    href="https://risingwave.com/slack"
    target="_blank"
  >
    <b>Get Instant Help</b>
  </a>
</p>
<div align="center">
  <a
    href="https://risingwave.com/slack"
    target="_blank"
  >
    <img alt="Slack" src="https://badgen.net/badge/Slack/Join%20RisingWave/0abd59?icon=slack" />
  </a>
  <a
    href="https://twitter.com/risingwavelabs"
    target="_blank"
  >
    <img alt="X" src="https://img.shields.io/twitter/follow/risingwavelabs" />
  </a>
  <a
    href="https://www.youtube.com/@risingwave-labs"
    target="_blank"
  >
    <img alt="YouTube" src="https://img.shields.io/youtube/channel/views/UCsHwdyBRxBpmkA5RRd0YNEA" />
  </a>
</div>

RisingWave is a Postgres-compatible streaming database engineered to provide the <i><b>simplest</b></i> and <i><b>most cost-efficient</b></i> approach for <b>processing</b>, <b>analyzing</b>, and <b>managing</b> real-time event streaming data.


![RisingWave](https://github.com/risingwavelabs/risingwave/assets/41638002/10c44404-f78b-43ce-bbd9-3646690acc59)


## Try it out in 60 seconds

Install RisingWave standalone mode:
```shell
curl https://risingwave.com/sh | sh
```

Then follow the prompts to start and connect to RisingWave.

To learn about other installation options, such as using a Docker image, see [Quick Start](https://docs.risingwave.com/docs/current/get-started/).

## Production deployments

[**RisingWave Cloud**](https://cloud.risingwave.com) offers the easiest way to run RisingWave in production, with a _forever-free_ developer tier.

For **Docker deployment**, please refer to [Docker Compose](https://docs.risingwave.com/docs/current/risingwave-docker-compose/).

For **Kubernetes deployment**, please refer to [Kubernetes with Helm](https://docs.risingwave.com/docs/current/risingwave-k8s-helm/) or [Kubernetes with Operator](https://docs.risingwave.com/docs/current/risingwave-kubernetes/).

## Why RisingWave for real-time materialized views?

RisingWave specializes in providing **incrementally updated, consistent materialized views** — a persistent data structure that represents the results of event stream processing. Compared to materialized views, dynamic tables, and live tables in other database and data warehouse systems, RisingWave's materialized view stands out in several key aspects:
* Highly cost-efficient - up to 95% cost savings compared to state-of-the-art solutions
* Synchronous refresh without compromising consistency
* Extensive SQL support including joins, deletes, and updates
* High concurrency in query serving
* Instant fault tolerance
* Transparent dynamic scaling
* Speedy bootstrapping and backfilling

RisingWave's extensive CDC support further enables users to seamlessly offload event-driven workloads such as materialized views and triggers from operational databases (e.g., [PostgreSQL](https://docs.risingwave.com/docs/current/ingest-from-postgres-cdc/)) to RisingWave.


## Why RisingWave for stream processing?

RisingWave provides users with a comprehensive set of frequently used stream processing features, including exactly-once consistency, [time window functions](https://docs.risingwave.com/docs/current/sql-function-time-window/), [watermarks](https://docs.risingwave.com/docs/current/watermarks/), and more. RisingWave significantly reduces the complexity of building stream processing applications by allowing developers to express intricate stream processing logic through cascaded materialized views. Furthermore, it allows users to persist data directly within the system, eliminating the need to deliver results to external databases for storage and query serving.

![Real-time Data Pipelines without or with RisingWave](https://github.com/risingwavelabs/risingwave/assets/100685635/414afbb7-5187-410f-9ba4-9a640c8c6306)

Compared to existing stream processing systems like [Apache Flink](https://flink.apache.org/), [Apache Spark Streaming](https://spark.apache.org/docs/latest/streaming-programming-guide.html), and [ksqlDB](https://ksqldb.io/), RisingWave stands out in two primary dimensions: **Ease-of-use** and **cost efficiency**, thanks to its **[PostgreSQL](https://www.postgresql.org/)-style interaction experience** and  **[Snowflake](https://snowflake.com/)-like architectural design** (i.e., decoupled storage and compute).

|  | RisingWave 🌊 | Traditional stream processing systems |
| :---:        | :---:          | :---:         |
| Learning curve 🎢   | PostgreSQL-style experience  | System-specific concepts   |
| Integration 🔗    | PostgreSQL ecosystem       | System-specific ecosystem      |
| Complex queries (e.g., joins) 💡  | Highly efficient  | Inefficient   |
| Failure recovery 🚨     | Instant       |  Minutes or even hours     |
| Dynamic scaling 🚀     | Transparent       |  Stop-the-world     |
| Bootstrapping and Backfilling ⏪     | Accelerated via dynamic scaling     | Slow     |


### RisingWave as a database
RisingWave is fundamentally a database that **extends beyond basic streaming data processing capabilities**.  It excels in **the effective management of streaming data**, making it a trusted choice for data persistence and powering online applications. RisingWave offers an extensive range of database capabilities, which include:

* High availability
* Serving highly concurrent queries
* Role-based access control (RBAC)
* Integration with data modeling tools, such as [dbt](https://docs.risingwave.com/docs/current/use-dbt/)
* Integration with database management tools, such as [Dbeaver](https://docs.risingwave.com/docs/current/dbeaver-integration/)
* Integration with BI tools, such as [Grafana](https://docs.risingwave.com/docs/current/grafana-integration/)
* Schema change
* Processing of semi-structured data

## In-production use cases
Within your data stack, RisingWave can assist with:

* Processing and transforming event streaming data in real time
* Offloading event-driven queries (e.g., materialized views, triggers) from operational databases
* Performing real-time ETL (Extract, Transform, Load)
* Supporting real-time feature stores

Read more at [use cases](https://risingwave.com/use-cases/). RisingWave is extensively utilized in real-time applications such as monitoring, alerting, dashboard reporting, machine learning, among others. It has already been adopted in fields such as financial trading, manufacturing, new media, logistics, gaming, and more. Check out [customer stories](https://risingwave.com/resources/?filter=customer-stories).

## Community

Looking for help, discussions, collaboration opportunities, or a casual afternoon chat with our fellow engineers and community members? Join our [Slack workspace](https://risingwave.com/slack)!

## Notes on telemetry

RisingWave collects anonymous usage statistics to better understand how the community is using RisingWave. The sole intention of this exercise is to help improve the product. Users may opt out easily at any time. Please refer to the [user documentation](https://docs.risingwave.com/docs/current/telemetry/) for more details.

## License

RisingWave is distributed under the Apache License (Version 2.0). Please refer to [LICENSE](LICENSE) for more information.

## Contributing

Thanks for your interest in contributing to the project! Please refer to [contribution guidelines](CONTRIBUTING.md) for more information.
