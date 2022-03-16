#![feature(let_chains)]

mod trace_runtime;

use tracing::Level;
use tracing_subscriber::filter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::prelude::*;

/// Configure log targets for all `RisingWave` crates. When new crates are added and TRACE level
/// logs are needed, add them here.
fn configure_risingwave_targets_jaeger(targets: filter::Targets) -> filter::Targets {
    targets
        // enable trace for most modules
        .with_target("risingwave_stream", Level::TRACE)
        .with_target("risingwave_batch", Level::TRACE)
        .with_target("risingwave_storage", Level::TRACE)
        .with_target("risingwave_sqlparser", Level::TRACE)
        // disable events that are too verbose
        // if you want to enable any of them, find the target name and set it to `TRACE`
        // .with_target("events::stream::mview::scan", Level::TRACE)
        .with_target("events", Level::ERROR)
}

/// Configure log targets for all `RisingWave` crates. When new crates are added and TRACE level
/// logs are needed, add them here.
fn configure_risingwave_targets_fmt(targets: filter::Targets) -> filter::Targets {
    let targets = targets
        // enable trace for most modules
        .with_target("risingwave_stream", Level::DEBUG)
        .with_target("risingwave_batch", Level::DEBUG)
        .with_target("risingwave_storage", Level::DEBUG)
        .with_target("risingwave_sqlparser", Level::DEBUG)
        // disable events that are too verbose
        // if you want to enable any of them, find the target name and set it to `TRACE`
        // .with_target("events::stream::mview::scan", Level::TRACE)
        .with_target("events", Level::ERROR);

    if let Ok(x) = std::env::var("RW_CI") && x == "true" {
            targets.with_target("events::meta", Level::TRACE)
        } else {
            targets
        }
}

/// Init logger for RisingWave binaries.
pub fn init_risingwave_logger(enable_jaeger_tracing: bool, colorful: bool) {
    use std::panic;

    let default_hook = panic::take_hook();

    panic::set_hook(Box::new(move |info| {
        default_hook(info);
        std::process::abort();
    }));

    use isahc::config::Configurable;

    let fmt_layer = {
        // Configure log output to stdout
        let fmt_layer = tracing_subscriber::fmt::layer()
            .compact()
            .with_ansi(colorful);
        let filter = filter::Targets::new()
            // Only enable WARN and ERROR for 3rd-party crates
            .with_target("aws_endpoint", Level::WARN)
            .with_target("hyper", Level::WARN)
            .with_target("h2", Level::WARN)
            .with_target("tower", Level::WARN)
            .with_target("isahc", Level::WARN);

        // Configure RisingWave's own crates to log at TRACE level, uncomment the following line if
        // needed.

        let filter = configure_risingwave_targets_fmt(filter);

        // Enable DEBUG level for all other crates
        // TODO: remove this in release mode
        let filter = filter.with_default(Level::DEBUG);

        fmt_layer.with_filter(filter)
    };

    if enable_jaeger_tracing {
        // With Jaeger tracing enabled, we should configure opentelemetry endpoints.

        opentelemetry::global::set_text_map_propagator(opentelemetry_jaeger::Propagator::new());

        let tracer = opentelemetry_jaeger::new_pipeline()
            // TODO: use UDP tracing in production environment
            .with_collector_endpoint("http://127.0.0.1:14268/api/traces")
            // TODO: change service name to compute-{port}
            .with_service_name("compute")
            // disable proxy
            .with_http_client(isahc::HttpClient::builder().proxy(None).build().unwrap())
            .install_batch(trace_runtime::RwTokio)
            .unwrap();

        let opentelemetry_layer = tracing_opentelemetry::layer().with_tracer(tracer);

        // Configure RisingWave's own crates to log at TRACE level, and ignore all third-party
        // crates
        let filter = filter::Targets::new();
        let filter = configure_risingwave_targets_jaeger(filter);

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
}

/// Common set-up for all RisingWave binaries. Currently, this includes:
///
/// * Set panic hook to abort the whole process.
pub fn oneshot_common() {
    use std::panic;

    let default_hook = panic::take_hook();

    panic::set_hook(Box::new(move |info| {
        default_hook(info);
        std::process::abort();
    }));
}
