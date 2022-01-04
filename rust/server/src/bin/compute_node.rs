use clap::Parser;
use log::info;
use risingwave::server::rpc_serve;
use risingwave::server_context::ServerContext;
use risingwave_common::util::addr::get_host_port;
/// TODO(xiangyhu): We need to introduce serious `Configuration` framework (#2165).
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
}

#[cfg(not(tarpaulin_include))]
#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    let opts: Opts = Opts::parse();
    log4rs::init_file(opts.log4rs_config.clone(), Default::default()).unwrap();
    info!("meta address: {}", opts.meta_address.clone());

    let server_context =
        ServerContext::new(opts.meta_address.as_str(), opts.config_path.as_str()).await;
    server_context.set_log4rs_config(opts.log4rs_config.as_str());
    server_context.set_host(opts.host.as_str());
    server_context.set_state_store(opts.state_store.as_str());
    server_context.set_prometheus_listener_address(opts.prometheus_listener_addr.as_str());

    let _res = server_context.register_to_meta().await;

    let addr = get_host_port(opts.host.as_str()).unwrap();
    info!("Starting server at {}", addr);
    let (join_handle, _shutdown_send) = rpc_serve(
        addr,
        Some(&opts.state_store),
        opts.prometheus_listener_addr.as_str(),
        opts.metrics_level,
    );
    join_handle.await.unwrap();
}
