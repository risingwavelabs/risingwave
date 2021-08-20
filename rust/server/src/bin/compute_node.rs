use clap::{AppSettings, Clap};
use futures::executor::block_on;
use log::info;
use risingwave::server::Server;
use std::sync::mpsc::channel;

#[derive(Clap)]
#[clap(setting = AppSettings::ColoredHelp)]
struct Opts {
    // The custom log4rs config file.
    #[clap(long, default_value = "config/log4rs.yaml")]
    log4rs_config: String,
}

fn main() {
    let opts: Opts = Opts::parse();
    log4rs::init_file(opts.log4rs_config, Default::default()).unwrap();
    info!("Starting");
    let mut srv = Server::new().unwrap();
    srv.start();
    info!("RPC Server started");
    let (tx, rx) = channel();
    ctrlc::set_handler(move || tx.send(()).expect("Could not send signal on channel."))
        .expect("Error setting Ctrl-C handler");
    rx.recv().expect("Could not receive from channel.");

    let _ = block_on(srv.shutdown());
}
