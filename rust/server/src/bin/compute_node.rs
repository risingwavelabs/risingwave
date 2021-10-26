use clap::Parser;
use log::info;
use risingwave::rpc::server::rpc_serve;
use risingwave::util::addr::get_host_port;

#[derive(Parser)]
struct Opts {
    // The custom log4rs config file.
    #[clap(long, default_value = "config/log4rs.yaml")]
    log4rs_config: String,

    #[clap(long, default_value = "127.0.0.1:5688")]
    host: String,
}

#[cfg(not(tarpaulin_include))]
#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    let opts: Opts = Opts::parse();
    log4rs::init_file(opts.log4rs_config, Default::default()).unwrap();

    let addr = get_host_port(opts.host.as_str()).unwrap();
    info!("Starting server at {}", addr);
    let (join_handle, _shutdown_send) = rpc_serve(addr);
    join_handle.await.unwrap();
}
