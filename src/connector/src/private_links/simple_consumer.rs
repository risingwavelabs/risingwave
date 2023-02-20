use std::collections::BTreeMap;
use clap::{App, Arg};

use rdkafka::client::BrokerAddr;
use rdkafka::ClientContext;
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::{CommitMode, Consumer, ConsumerContext, Rebalance};
use rdkafka::error::KafkaResult;
use rdkafka::message::{Headers, Message};
use rdkafka::topic_partition_list::TopicPartitionList;
use rdkafka::util::get_rdkafka_version;
use tracing::log::{info, warn};

use std::io::Write;
use std::thread;

use chrono::prelude::*;
use env_logger::fmt::Formatter;
use env_logger::Builder;
use log::{LevelFilter, Record};

fn setup_logger(log_thread: bool, rust_log: Option<&str>) {
    let output_format = move |formatter: &mut Formatter, record: &Record| {
        let thread_name = if log_thread {
            format!("(t: {}) ", thread::current().name().unwrap_or("unknown"))
        } else {
            "".to_string()
        };

        let local_time: DateTime<Local> = Local::now();
        let time_str = local_time.format("%H:%M:%S%.3f").to_string();
        write!(
            formatter,
            "{} {}{} - {} - {}\n",
            time_str,
            thread_name,
            record.level(),
            record.target(),
            record.args()
        )
    };

    let mut builder = Builder::new();
    builder
        .format(output_format)
        .filter(None, LevelFilter::Info);

    rust_log.map(|conf| builder.parse_filters(conf));

    builder.init();
}


// A context can be used to change the behavior of producers and consumers by adding callbacks
// that will be executed by librdkafka.
// This particular context sets up custom callbacks to log rebalancing events.
struct CustomContext {
    brokers: BTreeMap<String, BrokerAddr>,
}

impl CustomContext {
    pub fn new() -> Self {
        Self { brokers: BTreeMap::new() }
    }
    pub fn add_rewrite_broker_addr(&mut self, old_addr: BrokerAddr, new_addr: BrokerAddr) {
        self.brokers.insert(old_addr.host.clone(), new_addr);
    }
}

impl ClientContext for CustomContext {
    fn rewrite_broker_addr(&self, addr: BrokerAddr) -> BrokerAddr {
        match self.brokers.get(&addr.host) {
            None => { addr }
            Some(new_addr) => {
                info!("broker addr {:?} rewrited to {:?}", addr, new_addr);
                new_addr.clone()
            }
        }
    }
}

impl ConsumerContext for CustomContext {
    fn pre_rebalance(&self, rebalance: &Rebalance) {
        info!("Pre rebalance {:?}", rebalance);
    }

    fn post_rebalance(&self, rebalance: &Rebalance) {
        info!("Post rebalance {:?}", rebalance);
    }

    fn commit_callback(&self, result: KafkaResult<()>, _offsets: &TopicPartitionList) {
        info!("Committing offsets: {:?}", result);
    }
}

// A type alias with your custom consumer can be created for convenience.
type LoggingConsumer = StreamConsumer<CustomContext>;

async fn consume_and_print(brokers: &str, group_id: &str, topics: &[&str]) {
    let mut context = CustomContext::new();

    let b1 = BrokerAddr { host: "b-1.sourcedemomsk.pakv5y.c3.kafka.us-east-1.amazonaws.com".to_string(), port: "9092".to_string() };
    let b2 = BrokerAddr { host: "b-2.sourcedemomsk.pakv5y.c3.kafka.us-east-1.amazonaws.com".to_string(), port: "9092".to_string() };
    let b3 = BrokerAddr { host: "b-3.sourcedemomsk.pakv5y.c3.kafka.us-east-1.amazonaws.com".to_string(), port: "9092".to_string() };

    // vpce-0f23e813f5bc8bb4f-y96eipmp. vpce-svc-018d21e01fc2ec2aa.us-east-1.vpce.amazonaws.com
    context.add_rewrite_broker_addr(b1, BrokerAddr { host: "vpce-0f23e813f5bc8bb4f-y96eipmp-us-east-1c.vpce-svc-018d21e01fc2ec2aa.us-east-1.vpce.amazonaws.com".to_string(), port: "9001".to_string() });
    context.add_rewrite_broker_addr(b2, BrokerAddr { host: "vpce-0f23e813f5bc8bb4f-y96eipmp-us-east-1a.vpce-svc-018d21e01fc2ec2aa.us-east-1.vpce.amazonaws.com".to_string(), port: "9002".to_string() });
    context.add_rewrite_broker_addr(b3, BrokerAddr { host: "vpce-0f23e813f5bc8bb4f-y96eipmp-us-east-1b.vpce-svc-018d21e01fc2ec2aa.us-east-1.vpce.amazonaws.com".to_string(), port: "9003".to_string() });

    let consumer: LoggingConsumer = ClientConfig::new()
        .set("group.id", group_id)
        .set("bootstrap.servers", brokers)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "true")
        //.set("statistics.interval.ms", "30000")
        .set("auto.offset.reset", "smallest")
        .set_log_level(RDKafkaLogLevel::Debug)
        .create_with_context(context).await
        .expect("Consumer creation failed");

    consumer
        .subscribe(&topics.to_vec())
        .expect("Can't subscribe to specified topics");

    loop {
        match consumer.recv().await {
            Err(e) => warn!("Kafka error: {}", e),
            Ok(m) => {
                let payload = match m.payload_view::<str>() {
                    None => "",
                    Some(Ok(s)) => s,
                    Some(Err(e)) => {
                        warn!("Error while deserializing message payload: {:?}", e);
                        ""
                    }
                };
                info!("key: '{:?}', payload: '{}', topic: {}, partition: {}, offset: {}, timestamp: {:?}",
                      m.key(), payload, m.topic(), m.partition(), m.offset(), m.timestamp());
                if let Some(headers) = m.headers() {
                    for header in headers.iter() {
                        info!("  Header {:#?}: {:?}", header.key, header.value);
                    }
                }
                consumer.commit_message(&m, CommitMode::Async).await.unwrap();
            }
        };
    }
}

#[tokio::main]
async fn main() {
    let matches = App::new("consumer example")
        .version(option_env!("CARGO_PKG_VERSION").unwrap_or(""))
        .about("Simple command line consumer")
        .arg(
            Arg::with_name("brokers")
                .short("b")
                .long("brokers")
                .help("Broker list in kafka format")
                .takes_value(true)
                .default_value("localhost:9092"),
        )
        .arg(
            Arg::with_name("group-id")
                .short("g")
                .long("group-id")
                .help("Consumer group id")
                .takes_value(true)
                .default_value("example_consumer_group_id"),
        )
        .arg(
            Arg::with_name("log-conf")
                .long("log-conf")
                .help("Configure the logging format (example: 'rdkafka=trace')")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("topics")
                .short("t")
                .long("topics")
                .help("Topic list")
                .takes_value(true)
                .multiple(true)
                .required(true),
        )
        .get_matches();

    setup_logger(true, matches.value_of("log-conf"));

    let (version_n, version_s) = get_rdkafka_version();
    info!("rd_kafka_version: 0x{:08x}, {}", version_n, version_s);

    let topics = matches.values_of("topics").unwrap().collect::<Vec<&str>>();
    let brokers = matches.value_of("brokers").unwrap();
    let group_id = matches.value_of("group-id").unwrap();

    consume_and_print(brokers, group_id, &topics).await
}
