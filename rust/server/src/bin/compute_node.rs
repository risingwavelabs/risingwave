use clap::StructOpt;
use log::info;
use risingwave::server::compute_node_serve;
use risingwave::ComputeNodeOpts;
use tracing::Level;
use tracing_subscriber::filter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::prelude::*;

/// Configure log targets for all `RisingWave` crates. When new crates are added and TRACE level
/// logs are needed, add them here.
fn configure_risingwave_targets(targets: filter::Targets) -> filter::Targets {
    targets
        .with_target("risingwave_stream", Level::TRACE)
        .with_target("risingwave_batch", Level::TRACE)
        .with_target("risingwave_storage", Level::TRACE)
}

#[cfg(not(tarpaulin_include))]
#[tokio::main]
async fn main() {
    use isahc::config::Configurable;

    let opts = ComputeNodeOpts::parse();

    let fmt_layer = {
        // Configure log output to stdout
        let fmt_layer = tracing_subscriber::fmt::layer().compact().with_ansi(false);
        let filter = filter::Targets::new()
            // Only enable WARN and ERROR for 3rd-party crates
            .with_target("rusoto_core", Level::WARN)
            .with_target("hyper", Level::WARN)
            .with_target("h2", Level::WARN)
            .with_target("tower", Level::WARN)
            .with_target("isahc", Level::WARN)
            .with_target("sled", Level::WARN);

        // Configure RisingWave's own crates to log at TRACE level, uncomment the following line if
        // needed.

        // let filter = configure_risingwave_targets(filter);

        // Enable DEBUG level for all other crates
        // TODO: remove this in release mode
        let filter = filter.with_default(Level::DEBUG);

        fmt_layer.with_filter(filter)
    };

    if opts.enable_jaeger_tracing {
        // With Jaeger tracing enabled, we should configure opentelemetry endpoints.

        opentelemetry::global::set_text_map_propagator(opentelemetry_jaeger::Propagator::new());

        let tracer = opentelemetry_jaeger::new_pipeline()
            // TODO: use UDP tracing in production environment
            .with_collector_endpoint("http://127.0.0.1:14268/api/traces")
            // TODO: change service name to compute-{port}
            .with_service_name("compute")
            // disable proxy
            .with_http_client(isahc::HttpClient::builder().proxy(None).build().unwrap())
            .install_batch(risingwave::trace_runtime::RwTokio)
            .unwrap();

        let opentelemetry_layer = tracing_opentelemetry::layer().with_tracer(tracer);

        // Configure RisingWave's own crates to log at TRACE level, and ignore all third-party
        // crates
        let filter = filter::Targets::new();
        let filter = configure_risingwave_targets(filter);

        let opentelemetry_layer = opentelemetry_layer.with_filter(filter);

        tracing_subscriber::registry()
            .with(fmt_layer)
            .with(opentelemetry_layer)
            .init();
    } else {
        // Otherwise, simply enable fmt_layer.
        tracing_subscriber::registry().with(fmt_layer).init();
    }

    // TODO: add file-appender tracing subscriber in the future

    info!("meta address: {}", opts.meta_address.clone());

    let addr = opts.host.parse().unwrap();
    info!("Starting server at {}", addr);

    let (join_handle, _shutdown_send) = compute_node_serve(addr, opts).await;
    join_handle.await.unwrap();
}
