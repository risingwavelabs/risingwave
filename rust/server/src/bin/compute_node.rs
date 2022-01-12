use clap::StructOpt;
use log::info;
use risingwave::server::compute_node_serve;
use risingwave::ComputeNodeOpts;
use risingwave_common::util::addr::get_host_port;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::prelude::*;

#[cfg(not(tarpaulin_include))]
#[tokio::main]
async fn main() {
    let opts = ComputeNodeOpts::parse();
    if opts.enable_tracing {
        let tracer = opentelemetry_jaeger::new_pipeline()
            // TODO: We should use this function in future release of `opentelemetry_jaeger`
            // .with_auto_split_batch(true)
            .with_max_packet_size(65000)
            .with_service_name("compute")
            // TODO: Enable this in release mode
            // .install_batch(opentelemetry::runtime::Tokio)
            .install_simple()
            .unwrap();
        let opentelemetry = tracing_opentelemetry::layer().with_tracer(tracer);

        let fmt_layer = tracing_subscriber::fmt::layer().compact().with_ansi(false);

        let filter_layer = tracing_subscriber::filter::filter_fn(|metadata| {
            let target = metadata.target();
            // For external crates, only log warnings
            if target.starts_with("rusoto_core")
                || target.starts_with("hyper")
                || target.starts_with("h2")
                || target.starts_with("tower")
            {
                return metadata.level() <= &tracing::Level::WARN;
            }
            // For our own crates, log all debug message
            if metadata.level() <= &tracing::Level::DEBUG {
                return true;
            }
            // For risingwave_stream, log all message
            if target.starts_with("risingwave_") {
                return true;
            }
            false
        })
        .with_max_level_hint(tracing::Level::TRACE);

        tracing_subscriber::registry()
            .with(filter_layer)
            .with(opentelemetry)
            .with(fmt_layer)
            .init();
    } else {
        log4rs::init_file(opts.log4rs_config.clone(), Default::default()).unwrap();
    }
    info!("meta address: {}", opts.meta_address.clone());

    let addr = get_host_port(opts.host.as_str()).unwrap();
    info!("Starting server at {}", addr);

    let (join_handle, _shutdown_send) = compute_node_serve(addr, opts).await;
    join_handle.await.unwrap();
}
