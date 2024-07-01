# Observability


RiseDev supports several observability components.

## Cluster Control

`risectl` is the tool for providing internal access to the RisingWave cluster. See

```
cargo run --bin risectl -- --help
```

... or

```
./risedev ctl --help
```

for more information.

## Monitoring

Uncomment `grafana` and `prometheus` lines in `risedev.yml` to enable Grafana and Prometheus services.

## Tracing

Compute nodes support streaming tracing. Tracing is not enabled by default. You need to
use `./risedev configure` to download the tracing components first. After that, you will need to uncomment `tempo`
service in `risedev.yml` and start a new dev cluster to allow the components to work.

Traces are visualized in Grafana. You may also want to uncomment `grafana` service in `risedev.yml` to enable it.

## Dashboard

You may use RisingWave Dashboard to see actors in the system. It will be started along with meta node, and available at `http://127.0.0.1:5691/`.

The development instructions for dashboard are available [here](../dashboard/README.md).

## Logging

The Rust components use `tokio-tracing` to handle both logging and tracing. The default log level is set as:

* Third-party libraries: warn
* Other libraries: debug

To configure log levels, launch RisingWave with the environment variable `RUST_LOG` set as described [here](https://docs.rs/tracing-subscriber/0.3/tracing_subscriber/filter/struct.EnvFilter.html).

There're also some logs designated for debugging purposes with target names starting with `events::`.
For example, by setting `RUST_LOG=events::stream::message::chunk=trace`, all chunk messages will be logged as it passes through the executors in the streaming engine. Search in the codebase to find more of them.
