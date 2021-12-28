use clap::Parser;
use frontend::pgwire::pg_server::PgServer;
#[derive(Parser)]
struct Opts {
    // The custom log4rs config file.
    #[clap(long, default_value = "config/log4rs.yaml")]
    log4rs_config: String,

    #[clap(long, default_value = "127.0.0.1:4566")]
    host: String,
}

#[cfg(not(tarpaulin_include))]
#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    let opts: Opts = Opts::parse();
    log4rs::init_file(&opts.log4rs_config, Default::default()).unwrap();

    PgServer::serve(&opts.host).await;
}
