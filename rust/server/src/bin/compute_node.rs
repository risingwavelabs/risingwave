use clap::Parser;
use log::info;
use risingwave::server::rpc_serve;
use risingwave::server_context::ServerContext;
use risingwave_common::util::addr::get_host_port;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::prelude::*;

#[derive(Parser)]
struct Opts {
    // The custom log4rs config file.
    #[clap(long, default_value = "config/log4rs.yaml")]
    log4rs_config: String,

    #[clap(long, default_value = "127.0.0.1:5688")]
    host: String,

    #[clap(long, default_value = "in-memory")]
    state_store: String,

    #[clap(long, default_value = "127.0.0.1:1222")]
    prometheus_listener_addr: String,

    #[clap(long, default_value = "0")]
    metrics_level: u32,

    #[clap(long, default_value = "http://127.0.0.1:5690")]
    meta_address: String,

    #[clap(long, default_value = "./config/risingwave.toml")]
    config_path: String,

    /// Use `tokio-tracing` instead of `log4rs` for observability, and enable Jaeger tracing.
    #[clap(long)]
    enable_tracing: bool,
}

#[cfg(not(tarpaulin_include))]
#[tokio::main]
async fn main() {
    let opts: Opts = Opts::parse();

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

    let server_context =
        ServerContext::new(opts.meta_address.as_str(), opts.config_path.as_str()).await;
    server_context.set_log4rs_config(opts.log4rs_config.as_str());
    server_context.set_host(opts.host.as_str());
    server_context.set_state_store(opts.state_store.as_str());
    server_context.set_prometheus_listener_address(opts.prometheus_listener_addr.as_str());

    let _res = server_context.register_to_meta().await;

    let state_store = opts.state_store.parse().unwrap();

    let addr = get_host_port(opts.host.as_str()).unwrap();
    info!("Starting server at {}", addr);
    let (join_handle, _shutdown_send) = rpc_serve(
        addr,
        state_store,
        opts.prometheus_listener_addr.as_str(),
        opts.metrics_level,
    );
    join_handle.await.unwrap();
}
