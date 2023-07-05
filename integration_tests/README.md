# RisingWave Demos

Here is a gallery of demos that present how to use RisingWave alongwith the ecosystem tools.

- `ad-click/`: [Build and Maintain Real-time Applications Faster and Easier with Redpanda and RisingWave](https://singularity-data.com/blog/build-with-Redpanda-and-RisingWave)
- `ad-ctr`: [Perform real-time ad performance analysis](https://www.risingwave.dev/docs/latest/real-time-ad-performance-analysis/)
- `cdn-metrics`: [Server performance anomaly detection](https://www.risingwave.dev/docs/latest/server-performance-anomaly-detection/)
- `clickstream`: [Clickstream analysis](https://www.risingwave.dev/docs/latest/clickstream-analysis/)
- `twitter`: [Fast Twitter events processing](https://www.risingwave.dev/docs/latest/fast-twitter-events-processing/)
- `twitter-pulsar`: [Tutorial: Pulsar + RisingWave for Fast Twitter Event Processing](https://www.risingwave.com/blog/tutorial-pulsar-risingwave-for-fast-twitter-events-processing/)
- `live-stream`: [Live stream metrics analysis](https://www.risingwave.dev/docs/latest/live-stream-metrics-analysis/)

## Demo Runnability Testing

The demos listed above will all run through a series of tests when each PR is merged, including:

- Run the queries mentioned in the demos.
- Ingest the data in various formats, including Protobuf, Avro, and JSON. Each format will be tested individually.
- For each demo test, we check if the sources and MVs have successfully ingested data, meaning that they should have >0 records.

## Workload Generator

The workloads presented in the demos are produced by a golang program in `/datagen`. You can get this tool in multiple ways:

- Download pre-built binaries from [Releases](https://github.com/risingwavelabs/risingwave-demo/releases)
- Pull the latest docker image via `docker pull ghcr.io/risingwavelabs/demo-datagen:v1.0.9`.
- Build the binary from source:
  ```sh
  cd datagen && go build
  ```

To use this tool, you can run the following command:

```sh
./datagen --mode clickstream --qps 10 kafka --brokers 127.0.0.1:57801
```

or

```sh
./datagen --mode ecommerce --qps 10000000 postgres --port 6875 --user materialize --db materialize
```

- `--mode clickstream` indicates that it will produce random clickstream data.
- `--qps 10` sets a QPS limit to 10.
- `kafka | postgres` chooses the destination. For kafka, you will need to specify the brokers.
