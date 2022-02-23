use clap::Parser;
use log::info;
use risingwave_meta::rpc::server::{rpc_serve, MetaStoreBackend};
use tracing::Level;
use tracing_subscriber::filter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::prelude::*;

#[derive(Parser)]
struct Opts {
    #[clap(long, default_value = "127.0.0.1:5690")]
    host: String,

    #[clap(long)]
    dashboard_host: Option<String>,

    #[clap(long)]
    prometheus_host: Option<String>,
}

/// Configure log targets for all `RisingWave` crates. When new crates are added and TRACE level
/// logs are needed, add them here.
#[allow(dead_code)]
fn configure_risingwave_targets(targets: filter::Targets) -> filter::Targets {
    targets
        .with_target("risingwave_stream", Level::TRACE)
        .with_target("risingwave_batch", Level::TRACE)
        .with_target("risingwave_storage", Level::TRACE)
}

#[cfg(not(tarpaulin_include))]
#[tokio::main]
async fn main() {
    let opts: Opts = Opts::parse();

    let fmt_layer = {
        // Configure log output to stdout
        let fmt_layer = tracing_subscriber::fmt::layer().compact().with_ansi(false);
        let filter = filter::Targets::new()
            // Only enable WARN and ERROR for 3rd-party crates
            .with_target("rusoto_core", Level::WARN)
            .with_target("hyper", Level::WARN)
            .with_target("h2", Level::WARN)
            .with_target("tower", Level::WARN)
            .with_target("isahc", Level::WARN);

        // Configure RisingWave's own crates to log at TRACE level, uncomment the following line if
        // needed.

        // let filter = configure_risingwave_targets(filter);

        // Enable DEBUG level for all other crates
        // TODO: remove this in release mode
        let filter = filter.with_default(Level::DEBUG);

        fmt_layer.with_filter(filter)
    };

    tracing_subscriber::registry().with(fmt_layer).init();

    let addr = opts.host.parse().unwrap();
    let dashboard_addr = opts.dashboard_host.map(|x| x.parse().unwrap());
    let prometheus_addr = opts.prometheus_host.map(|x| x.parse().unwrap());

    info!("Starting meta server at {}", addr);
    let (join_handle, _shutdown_send) =
        rpc_serve(addr, prometheus_addr, dashboard_addr, MetaStoreBackend::Mem).await;
    join_handle.await.unwrap();
}
