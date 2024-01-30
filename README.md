
<p align="center">
  <picture>
    <source srcset=".github/RisingWave-logo-dark.svg" width="500px" media="(prefers-color-scheme: dark)">
    <img src=".github/RisingWave-logo-light.svg" width="500px">
  </picture>
</p>


<div align="center">

### 🌊 Reimagine Stream Processing.

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


![RisingWave](https://github.com/risingwavelabs/risingwave-docs/blob/main/docs/images/new_archi_grey.png)

## Try it out in 5 minutes
Docker pull:
```
docker run -it --pull=always -p 4566:4566 -p 5691:5691 risingwavelabs/risingwave:latest playground
```
Now connect to RisingWave using `psql`:
```
psql -h localhost -p 4566 -d dev -U root
```
Don’t have Docker? Learn how to install RisingWave on Mac, Ubuntu, and other environments at [Quick Start](https://docs.risingwave.com/docs/current/get-started/).

## Production deployments

For **single-node deployment**, please refer to [Docker Compose](https://docs.risingwave.com/docs/current/risingwave-trial/?method=docker-compose).

For **distributed deployment**, please refer to [Kubernetes with Helm](https://docs.risingwave.com/docs/current/risingwave-k8s-helm/) or [Kubernetes with Operator](https://docs.risingwave.com/docs/current/risingwave-kubernetes/).

**RisingWave Cloud** the easiest way to run a fully-fledged RisingWave cluster. Try it out for free at: [cloud.risingwave.com](https://cloud.risingwave.com).


## Why RisingWave for stream processing?

RisingWave provides users with a comprehensive set of frequently used stream processing features, including exactly-once consistency, [time window functions](https://docs.risingwave.com/docs/current/sql-function-time-window/), [watermarks](https://docs.risingwave.com/docs/current/watermarks/), and more. It specializes in providing **incrementally updated, consistent materialized views** — a persistent data structure that represents the results of stream processing. RisingWave significantly reduces the complexity of building stream processing applications by allowing developers to express intricate stream processing logic through cascaded materialized views. Furthermore, it allows users to persist data directly within the system, eliminating the need to deliver results to external databases for storage and query serving.

![Real-time Data Pipelines without or with RisingWave](https://github.com/risingwavelabs/risingwave/assets/100685635/414afbb7-5187-410f-9ba4-9a640c8c6306)

Compared to existing stream processing systems like [Apache Flink](https://flink.apache.org/), [Apache Spark Streaming](https://spark.apache.org/docs/latest/streaming-programming-guide.html), and [ksqlDB](https://ksqldb.io/), RisingWave stands out in two primary dimensions: **Ease-of-use** and **cost efficiency**, thanks to its **[PostgreSQL](https://www.postgresql.org/)-style interaction experience** and  **[Snowflake](https://snowflake.com/)-like architectural design** (i.e., decoupled storage and compute).

### Ease-of-use

* **Simple to learn**
  * RisingWave speaks PostgreSQL-style SQL, enabling users to dive into stream processing in much the same way as operating a PostgreSQL database.
* **Simple to develop**
  * RisingWave operates as a relational database, allowing users to decompose stream processing logic into smaller, manageable, stacked materialized views, rather than dealing with extensive computational programs.
* **Simple to integrate**
  * With integrations to a diverse range of cloud systems and the PostgreSQL ecosystem, RisingWave boasts a rich and expansive ecosystem, making it straightforward to incorporate into existing infrastructures.

### Cost efficiency

* **Highly efficient in complex queries**
  * RisingWave persists internal states in remote storage systems such as S3, and users can confidently and efficiently perform complex streaming queries (for example, joining dozens of data streams) in a production environment, without worrying about state size.
* **Transparent dynamic scaling**
  * RisingWave's state management mechanism enables near-instantaneous dynamic scaling without any service interruptions.
* **Instant failure recovery**
  * RisingWave's state management mechanism also allows it to recover from failure in seconds, not minutes or hours.

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

RisingWave is extensively utilized in real-time applications such as monitoring, alerting, dashboard reporting, machine learning, among others. It has already been adopted in fields such as financial trading, manufacturing, new media, logistics, gaming, and more. Check out [customer stories](https://www.risingwave.com/use-cases/).

## Community

Looking for help, discussions, collaboration opportunities, or a casual afternoon chat with our fellow engineers and community members? Join our [Slack workspace](https://risingwave.com/slack)!

## Notes on telemetry

RisingWave collects anonymous usage statistics to better understand how the community is using RisingWave. The sole intention of this exercise is to help improve the product. Users may opt out easily at any time. Please refer to the [user documentation](https://docs.risingwave.com/docs/current/telemetry/) for more details.

## License

RisingWave is distributed under the Apache License (Version 2.0). Please refer to [LICENSE](LICENSE) for more information.

## Contributing

Thanks for your interest in contributing to the project! Please refer to [contribution guidelines](CONTRIBUTING.md) for more information.
