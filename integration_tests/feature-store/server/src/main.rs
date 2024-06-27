use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use clap::{Arg, ArgMatches, Command};

use crate::feature_store::FeatureStoreServer;
use crate::kafka::KafkaSink;
use crate::server_pb::server_server::ServerServer;

mod feature_store;
mod kafka;
mod model;
mod server_pb;
mod serving;

#[tokio::main]
async fn main() {
    println!("Reading args");
    let args = get_args();
    let kafka_sink = KafkaSink::new(
        args.get_one::<String>("brokers")
            .expect("failed to decode brokers")
            .to_string(),
        args.get_one::<String>("output-topic")
            .expect("failed to decode output_topics")
            .to_string(),
    );
    println!("Testing Kafka payload,args{:?}", args);
    tokio::spawn(KafkaSink::mock_consume());
    kafka_sink
        .send("0".to_string(), "{init: true}".to_string())
        .await;
    let server = ServerServer::new(FeatureStoreServer { kafka: kafka_sink });

    tonic::transport::Server::builder()
        .add_service(server)
        .serve(SocketAddr::new(
            IpAddr::from(Ipv4Addr::new(127, 0, 0, 1)),
            2666,
        ))
        .await
        .unwrap()
}

fn get_args() -> ArgMatches {
    Command::new("feature-store")
        .about("Feature store")
        .arg(
            Arg::new("brokers")
                .short('b')
                .long("brokers")
                .help("Kafka broker list")
                .num_args(1)
                .default_value("kafka:9092"),
        )
        .arg(
            Arg::new("output-topic")
                .long("output-topics")
                .help("Output topics names")
                .default_value("taxi")
                .num_args(1),
        )
        .get_matches()
}
