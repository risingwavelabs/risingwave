use std::collections::BTreeMap;
use std::io::Write;
use std::thread;
use std::time::Duration;

use anyhow::anyhow;
use aws_config::retry::RetryConfig;
use aws_sdk_ec2::model::{Filter, VpcEndpointType};
use chrono::prelude::*;
use clap::{value_t, App, Arg};
use env_logger::fmt::Formatter;
use env_logger::Builder;
use log::{info, trace, LevelFilter, Record};
use rdkafka::client::BrokerAddr;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer, ConsumerContext};
use rdkafka::ClientContext;

fn setup_logger(log_thread: bool, rust_log: Option<&str>) {
    let output_format = move |formatter: &mut Formatter, record: &Record<'_>| {
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

struct CustomContext {
    brokers: BTreeMap<String, BrokerAddr>,
}

impl CustomContext {
    pub fn new() -> Self {
        Self {
            brokers: BTreeMap::new(),
        }
    }

    pub fn add_rewrite_broker_addr(&mut self, old_addr: BrokerAddr, new_addr: BrokerAddr) {
        self.brokers.insert(old_addr.host.clone(), new_addr);
    }
}

impl ClientContext for CustomContext {
    fn rewrite_broker_addr(&self, addr: BrokerAddr) -> BrokerAddr {
        match self.brokers.get(&addr.host) {
            None => addr,
            Some(new_addr) => {
                info!("broker addr {:?} rewrited to {:?}", addr, new_addr);
                new_addr.clone()
            }
        }
    }
}

impl ConsumerContext for CustomContext {}

type MyConsumer = BaseConsumer<CustomContext>;

async fn print_metadata(
    brokers: &str,
    topic: Option<&str>,
    timeout: Duration,
    fetch_offsets: bool,
) {
    let mut context = CustomContext::new();

    let b1 = BrokerAddr {
        host: "b-1.sourcedemomsk.pakv5y.c3.kafka.us-east-1.amazonaws.com".to_string(),
        port: "9092".to_string(),
    };
    let b2 = BrokerAddr {
        host: "b-2.sourcedemomsk.pakv5y.c3.kafka.us-east-1.amazonaws.com".to_string(),
        port: "9092".to_string(),
    };
    let b3 = BrokerAddr {
        host: "b-3.sourcedemomsk.pakv5y.c3.kafka.us-east-1.amazonaws.com".to_string(),
        port: "9092".to_string(),
    };

    context.add_rewrite_broker_addr(b1, BrokerAddr { host: "vpce-0f23e813f5bc8bb4f-y96eipmp-us-east-1c.vpce-svc-018d21e01fc2ec2aa.us-east-1.vpce.amazonaws.com".to_string(), port: "9001".to_string() });
    context.add_rewrite_broker_addr(b2, BrokerAddr { host: "vpce-0f23e813f5bc8bb4f-y96eipmp-us-east-1a.vpce-svc-018d21e01fc2ec2aa.us-east-1.vpce.amazonaws.com".to_string(), port: "9002".to_string() });
    context.add_rewrite_broker_addr(b3, BrokerAddr { host: "vpce-0f23e813f5bc8bb4f-y96eipmp-us-east-1b.vpce-svc-018d21e01fc2ec2aa.us-east-1.vpce.amazonaws.com".to_string(), port: "9003".to_string() });

    let consumer: MyConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create_with_context(context)
        .await
        .expect("Consumer creation failed");

    trace!("Consumer created");

    let metadata = consumer
        .fetch_metadata(topic, timeout)
        .await
        .expect("Failed to fetch metadata");

    let mut message_count = 0;

    println!("Cluster information:");
    println!("  Broker count: {}", metadata.brokers().len());
    println!("  Topics count: {}", metadata.topics().len());
    println!("  Metadata broker name: {}", metadata.orig_broker_name());
    println!("  Metadata broker id: {}\n", metadata.orig_broker_id());

    println!("Brokers:");
    for broker in metadata.brokers() {
        println!(
            "  Id: {}  Host: {}:{}  ",
            broker.id(),
            broker.host(),
            broker.port()
        );
    }

    println!("\nTopics:");
    for topic in metadata.topics() {
        println!("  Topic: {}  Err: {:?}", topic.name(), topic.error());
        for partition in topic.partitions() {
            println!(
                "     Partition: {}  Leader: {}  Replicas: {:?}  ISR: {:?}  Err: {:?}",
                partition.id(),
                partition.leader(),
                partition.replicas(),
                partition.isr(),
                partition.error()
            );
            if fetch_offsets {
                let (low, high) = consumer
                    .fetch_watermarks(topic.name(), partition.id(), Duration::from_secs(1))
                    .await
                    .unwrap_or((-1, -1));
                println!(
                    "       Low watermark: {}  High watermark: {} (difference: {})",
                    low,
                    high,
                    high - low
                );
                message_count += high - low;
            }
        }
        if fetch_offsets {
            println!("     Total message count: {}", message_count);
        }
    }
}

async fn example_main() {
    let matches = App::new("metadata fetch example")
        .version(option_env!("CARGO_PKG_VERSION").unwrap_or(""))
        .about("Fetch and print the cluster metadata")
        .arg(
            Arg::with_name("brokers")
                .short("b")
                .long("brokers")
                .help("Broker list in kafka format")
                .takes_value(true)
                .default_value("localhost:9092"),
        )
        .arg(
            Arg::with_name("offsets")
                .long("offsets")
                .help("Enables offset fetching"),
        )
        .arg(
            Arg::with_name("topic")
                .long("topic")
                .help("Only fetch the metadata of the specified topic")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("log-conf")
                .long("log-conf")
                .help("Configure the logging format (example: 'rdkafka=trace')")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("timeout")
                .long("timeout")
                .help("Metadata fetch timeout in milliseconds")
                .takes_value(true)
                .default_value("60000"),
        )
        .get_matches();

    setup_logger(true, matches.value_of("log-conf"));

    let brokers = matches.value_of("brokers").unwrap();
    let timeout = value_t!(matches, "timeout", u64).unwrap();
    let topic = matches.value_of("topic");
    let fetch_offsets = matches.is_present("offsets");

    print_metadata(
        brokers,
        topic,
        Duration::from_millis(timeout),
        fetch_offsets,
    )
    .await;
}

#[tokio::main]
async fn main() {
    println!("start to create private links...");
    let azs = vec![
        "us-east-1a".to_string(),
        "us-east-1b".to_string(),
        "us-east-1c".to_string(),
    ];
    let cli = AwsEc2Client::new("vpc-011e151aa2e9bb1be").await;

    let endpoint_service_name = "com.amazonaws.vpce.us-east-1.vpce-svc-018d21e01fc2ec2aa";

    cli.create_aws_private_link(endpoint_service_name, &azs)
        .await
        .unwrap();
}

struct AwsEc2Client {
    client: aws_sdk_ec2::Client,
    vpc_id: String,
}

impl AwsEc2Client {
    pub async fn new(vpc_id: &str) -> Self {
        let sdk_config = aws_config::from_env()
            .retry_config(RetryConfig::standard().with_max_attempts(4))
            .load()
            .await;
        let client = aws_sdk_ec2::Client::new(&sdk_config);

        Self {
            client,
            vpc_id: vpc_id.to_string(),
        }
    }

    /// vpc_id: The VPC of the running RisingWave instance
    /// service_name: The name of the endpoint service we want to access
    pub async fn create_aws_private_link(
        &self,
        service_name: &str,
        availability_zones: &Vec<String>,
    ) -> anyhow::Result<()> {
        let subnet_ids = self
            .describe_subnets(&self.vpc_id, &availability_zones)
            .await?;

        println!("subnet_ids => {:?}", subnet_ids);
        self.create_vpc_endpoint(&self.vpc_id, service_name, &subnet_ids)
            .await
    }

    async fn describe_subnets(
        &self,
        vpc_id: &str,
        availability_zones: &[String],
    ) -> anyhow::Result<Vec<String>> {
        let vpc_filter = Filter::builder().name("vpc-id").values(vpc_id).build();
        let az_filter = Filter::builder()
            .name("availability-zone")
            .set_values(Some(Vec::from(availability_zones)))
            .build();

        let resp = self
            .client
            .describe_subnets()
            .set_filters(Some(vec![vpc_filter, az_filter]))
            .send()
            .await;

        match resp {
            Ok(output) => {
                let subnets = output
                    .subnets
                    .unwrap_or_default()
                    .into_iter()
                    .map(|s| s.subnet_id.unwrap_or_default())
                    .collect();
                Ok(subnets)
            }
            Err(e) => Err(anyhow!("Error: {:?}", e)),
        }
    }

    async fn create_vpc_endpoint(
        &self,
        vpc_id: &str,
        service_name: &str,
        subnet_ids: &Vec<String>,
    ) -> anyhow::Result<()> {
        let output = self
            .client
            .create_vpc_endpoint()
            .vpc_endpoint_type(VpcEndpointType::Interface)
            .vpc_id(vpc_id)
            .service_name(service_name)
            .set_subnet_ids(Some(subnet_ids.clone()))
            .send()
            .await?;

        println!("endpoint created success");
        if let Some(endpoint) = output.vpc_endpoint() {
            println!("endpoint id => {:?}", endpoint.vpc_endpoint_id());
            if let Some(dns_entries) = endpoint.dns_entries() {
                for dns_entry in dns_entries {
                    println!("dns entry => {:?}", dns_entry)
                }
            }
        }

        // TODO: save mapping of broker_addr => dns entry into metastore

        // match resp {
        //     Ok(output) => {
        //         if let Some(endpoint) = output.vpc_endpoint() {
        //             if let Some(dns_entries) = endpoint.dns_entries() {
        //                 for dns_entry in dns_entries {
        //                     println!("dns entry: {:?}", dns_entry)
        //                 }
        //             }
        //         }
        //     }
        //     Err(e) => {
        //         anyhow!("Error: {:?}", e)
        //     }
        // }

        Ok(())
    }
}
