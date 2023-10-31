
<p align="center">
  <picture>
    <source srcset=".github/RisingWave-logo-dark.svg" width="500px" media="(prefers-color-scheme: dark)">
    <img src=".github/RisingWave-logo-light.svg" width="500px">
  </picture>
</p>


<div align="center">

### 🌊Stream Processing Redefined.

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
    href="https://hub.docker.com/r/risingwavelabs/risingwave"
    target="_blank"
  >
    <img alt="Docker" src="https://img.shields.io/docker/v/risingwavelabs/risingwave" />
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

RisingWave is a distributed SQL streaming database that enables <b>cost-efficient</b> and <b>reliable</b> processing of streaming data.

![RisingWave](https://github.com/risingwavelabs/risingwave-docs/blob/main/docs/images/new_archi_grey.png)

## Try it out in 5 minutes
**Mac**
```
brew tap risingwavelabs/risingwave
brew install risingwave
risingwave playground
```
**Ubuntu**
```
wget https://github.com/risingwavelabs/risingwave/releases/download/v1.3.0/risingwave-v1.3.0-x86_64-unknown-linux.tar.gz
tar xvf risingwave-v1.3.0-x86_64-unknown-linux.tar.gz
./risingwave playground
```
**Docker**
```
docker run -it --pull=always -p 4566:4566 -p 5691:5691 risingwavelabs/risingwave:latest playground
```
Now connect to RisingWave using `psql`:
```
psql -h localhost -p 4566 -d dev -U root
```
Learn more at [Quick Start](https://docs.risingwave.com/docs/current/get-started/).

## Production deployments
For **single-node Docker deployments**, please refer to [Docker Compose](https://docs.risingwave.com/docs/current/risingwave-trial/?method=docker-compose).

For **Kubernetes deployments**, please refer to [Kubernetes with Helm](https://docs.risingwave.com/docs/current/risingwave-k8s-helm/) or [Kubernetes with Operator](https://docs.risingwave.com/docs/current/risingwave-kubernetes/).

**RisingWave Cloud** the easiest way to run a fully-fledged RisingWave cluster. Try it out for free at: [cloud.risingwave.com](https://cloud.risingwave.com).


## Why RisingWave for stream processing?
RisingWave adaptly addresses some of the most challenging problems in stream processing. Compared to existing stream processing systems like [Apache Flink](https://flink.apache.org/), [Apache Spark Streaming](https://spark.apache.org/docs/latest/streaming-programming-guide.html), and [KsqlDB](https://ksqldb.io/), RisingWave stands out in two primary dimensions: **Ease-of-use** and **efficiency**, thanks to its **[PostgreSQL](https://www.postgresql.org/)-style interaction experience** and  **[Snowflake](https://snowflake.com/)-like architectural design** (i.e., compute-storage decoupling).
### Ease-of-use
* **Simple to learn**
  * RisingWave speaks PostgreSQL-style SQL, enabling users to dive into stream processing in much the same way as operating a PostgreSQL database.
* **Simple to verify correctness**
  * RisingWave persists results in materialized views and allow users to break down complex stream computation programs into stacked materialized views, simplifying program development and result verification.
* **Simple to maintain and operate**
  * RisingWave abstracts away unnecessary low-level details, allowing users to concentrate solely on SQL code-level issues.
* **Simple to integrate**
  * With integrations to a diverse range of cloud systems and the PostgreSQL ecosystem, RisingWave boasts a rich and expansive ecosystem, making it straightforward to incorporate into existing infrastructures.

### Efficiency
* **High resource utilization**
  * Queries in RisingWave leverage shared computational resources, eliminating the need for users to manually allocate resources for each query.
* **No compromise on large state management**
  * The decoupled compute-storage architecture of RisingWave ensures remote persistence of internal states, and users never need to worry about the size of internal states when handling complex queries.
* **Highly efficient in multi-stream joins**
  * RisingWave has made significant optimizations for multiple stream join scenarios. Users can easily join 10-20 streams (or more) efficiently in a production environment.
* **Transparent dynamic scaling**
  * RisingWave supports near-instantaneous dynamic scaling without any service interruptions.
* **Instant failure recovery**
  * RisingWave's state management mechanism allows it to recover from failure in seconds, not minutes or hours.
* **Simplified data stack**
  * RisingWave's ability to store data and serve queries eliminates the need for separate maintenance of stream processors and databases. Users can effortlessly connect RisingWave to their preferred BI tools or through client libraries.

## RisingWave's limitations
RisingWave isn’t a panacea for all data engineering hurdles. It has its own set of limitations:
* **No programmable interfaces**
  * RisingWave does not provide low-level APIs in languages like Java and Scala, and does not allow users to manage internal states manually (unless you want to hack!). For coding in Java, Scala, and other languages, please consider using RisingWave's User-Defined Functions (UDF).
* **No support for transaction processing**
  * RisingWave isn’t cut out for transactional workloads, thus it’s not a viable substitute for operational databases dedicated to transaction processing. However, it supports read-only transactions, ensuring data freshness and consistency. It also comprehends the transactional semantics of upstream database Change Data Capture (CDC).
* **Not tailored for ad-hoc analytical queries**
  * RisingWave's row store design is tailored for optimal stream processing performance rather than interactive analytical workloads. Hence, it's not a suitable replacement for OLAP databases. Yet, a reliable integration with many OLAP databases exists, and a collaborative use of RisingWave and OLAP databases is a common practice among many users.


## In-production use cases
Like other stream processing systems, the primary use cases of RisingWave include monitoring, alerting, real-time dashboard reporting, streaming ETL (Extract, Transform, Load), machine learning feature engineering, and more. It has already been adopted in fields such as financial trading, manufacturing, new media, logistics, gaming, and more. Check out [customer stories](https://www.risingwave.com/use-cases/).

## Community

Looking for help, discussions, collaboration opportunities, or a casual afternoon chat with our fellow engineers and community members? Join our [Slack workspace](https://risingwave.com/slack)!

## Notes on telemetry

RisingWave collects anonymous usage statistics to better understand how the community is using RisingWave. The sole intention of this exercise is to help improve the product. Users may opt out easily at any time. Please refer to the [user documentation](https://docs.risingwave.com/docs/current/telemetry/) for more details.

## License

RisingWave is distributed under the Apache License (Version 2.0). Please refer to [LICENSE](LICENSE) for more information.

## Contributing

Thanks for your interest in contributing to the project! Please refer to [contribution guidelines](CONTRIBUTING.md) for more information.
