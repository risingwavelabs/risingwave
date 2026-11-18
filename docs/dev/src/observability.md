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

You may use RisingWave Dashboard to see actors in the system. It will be started along with meta node, and available at `http://127.0.0.1:5691/` .

The development instructions for dashboard are available [here](https://github.com/risingwavelabs/risingwave/blob/fb60113c2e8a7f0676af545c99f073a335c255f3/dashboard/README.md).

## Logging

The Rust components use `tokio-tracing` to handle both logging and tracing. The default log level is set as:

* Third-party libraries: warn
* Other libraries: debug

To configure log levels, launch RisingWave with the environment variable `RUST_LOG` set as described [here](https://docs.rs/tracing-subscriber/0.3/tracing_subscriber/filter/struct.EnvFilter.html).

Use `risectl log` to inspect or change stdout/stderr log filters without restarting a node:

```sh
# Inspect selected workers. --node-id is an alias for --worker-id.
risectl log get --worker-id 1,2

# Enable storage debug logs on one worker.
risectl log set 'info,risingwave_storage=debug' --worker-id 2

# Change all compute nodes, or all nodes of several types.
risectl log set 'risingwave_stream=debug' --worker-type compute-node
risectl log set 'info,risingwave_storage=debug' --worker-type compute-node,compactor

# Restore the original startup filter on every node.
risectl log reset --all
```

Set `RW_META_ADDR` to the meta service address before running these commands.
Worker IDs and worker types can also be repeated. Supported types are `frontend`,
`compute-node` (alias `compute`), `compactor`, and `meta`; the meta node has worker ID 0.
Select IDs, types, or `--all` explicitly. Unknown IDs are rejected before any update.
If a selected node fails, the command still attempts the other selected nodes, reports
each result, and exits with an error.

Each update applies the supplied directives on top of the built-in defaults and
logger settings, replacing both the original `RUST_LOG` directives and any previous
runtime directives. `reset` restores the startup configuration, including the original
`RUST_LOG`. For example, if startup used `RUST_LOG=risingwave_storage=debug`, setting
`info` removes that startup override; resetting restores it.

The console filter applies `rw_tracing=off` after logger settings, `RUST_LOG`, and
runtime directives to keep tracing-only events out of stdout/stderr.
Changes apply to the entire process and are lost on restart.
Query-log files, Tokio console, and OpenTelemetry retain their own filters.
The `release` and `production` profiles compile out `TRACE` events; runtime updates
can enable `DEBUG` and less verbose levels, but cannot restore compiled-out events.
The `ci-release` profile enables debug assertions, so the release-only level cap
does not apply to that profile.

There're also some logs designated for debugging purposes with target names starting with `events::` .
For example, by setting `RUST_LOG=events::stream::message::chunk=trace` , all chunk messages will be logged as it passes through the executors in the streaming engine. Search in the codebase to find more of them.

<!-- TODO: how to add new metrics and how to update dashboard -->
<!-- ## Update Grafana dashboard

See [README](../grafana/README.md) for more information. -->
