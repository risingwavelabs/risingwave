use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use clap::{App, Arg, ArgMatches};

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
        args.value_of("brokers")
            .expect("failed to decode brokers")
            .to_string(),
        args.value_of("output-topic")
            .expect("failed to decode output_topics")
            .to_string(),
    );
    println!("Testing Kafka payload,args{:?}",args);
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

fn get_args<'a>() -> ArgMatches<'a> {
    App::new("feature-store")
        .about("Feature store")
        .arg(
            Arg::with_name("brokers")
                .short("b")
                .long("brokers")
                .help("Kafka broker list")
                .takes_value(true)
                .default_value("kafka:9092"),
        )
        .arg(
            Arg::with_name("output-topic")
                .long("output-topics")
                .help("Output topics names")
                .default_value("taxi")
                .takes_value(true),
        )
        .get_matches()
}
