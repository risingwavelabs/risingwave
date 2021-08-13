use futures::executor::block_on;
use log::info;
use risingwave::server::Server;
use std::sync::mpsc::channel;

fn main() {
    log4rs::init_file("config/log4rs.yaml", Default::default()).unwrap();
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
