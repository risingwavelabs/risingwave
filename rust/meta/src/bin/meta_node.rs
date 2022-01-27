use clap::Parser;
use log::info;
use risingwave_meta::rpc::server::{rpc_serve, MetaStoreBackend};

#[derive(Parser)]
struct Opts {
    // The custom log4rs config file.
    #[clap(long, default_value = "config/log4rs.yaml")]
    log4rs_config: String,

    #[clap(long, default_value = "127.0.0.1:5690")]
    host: String,

    #[clap(long, default_value = "127.0.0.1:5691")]
    dashboard_host: String,
}

#[cfg(not(tarpaulin_include))]
#[tokio::main]
async fn main() {
    let opts: Opts = Opts::parse();
    log4rs::init_file(opts.log4rs_config, Default::default()).unwrap();

    let addr = opts.host.parse().unwrap();
    let dashboard_addr = opts.dashboard_host.parse().unwrap();
    info!("Starting meta server at {}", addr);
    let (join_handle, _shutdown_send) = rpc_serve(
        addr,
        Some(dashboard_addr),
        None,
        MetaStoreBackend::SledInMem,
    )
    .await;
    join_handle.await.unwrap();
}
