use clap::{AppSettings, Clap};
use futures::executor::block_on;
use log::info;
use risingwave::server::Server;
use risingwave::util::addr::get_host_port;

use std::sync::mpsc::channel;

#[derive(Clap)]
#[clap(setting = AppSettings::ColoredHelp)]
struct Opts {
    // The custom log4rs config file.
    #[clap(long, default_value = "config/log4rs.yaml")]
    log4rs_config: String,

    #[clap(long, default_value = "127.0.0.1:5688")]
    host: String,
}

fn main() {
    let opts: Opts = Opts::parse();
    log4rs::init_file(opts.log4rs_config, Default::default()).unwrap();
    info!("Starting");

    let addr = get_host_port(opts.host.as_str()).unwrap();
    let mut srv = Server::new(addr).unwrap();
    srv.start();
    info!("RPC Server started [{}]", opts.host);
    let (tx, rx) = channel();
    ctrlc::set_handler(move || tx.send(()).expect("Could not send signal on channel."))
        .expect("Error setting Ctrl-C handler");
    rx.recv().expect("Could not receive from channel.");

    let _ = block_on(srv.shutdown());
}
