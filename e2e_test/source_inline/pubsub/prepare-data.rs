#!/usr/bin/env -S cargo -Zscript
---cargo
[dependencies]
anyhow = "1"
google-cloud-googleapis = { version = "0.16", features = ["pubsub"] }
google-cloud-pubsub = "0.30"
tokio = { version = "0.2", package = "madsim-tokio", features = [
    "rt",
    "rt-multi-thread",
    "sync",
    "macros",
    "time",
    "signal",
    "fs",
] }
---

use google_cloud_googleapis::pubsub::v1::PubsubMessage;
use google_cloud_pubsub::client::{Client, ClientConfig};
use google_cloud_pubsub::subscription::SubscriptionConfig;

const TOPIC: &str = "test-topic";

const SUBSCRIPTION_COUNT: usize = 50;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let command = args[1].as_str();

    let use_emulator = std::env::var("PUBSUB_EMULATOR_HOST").is_ok();
    let use_cloud = std::env::var("GOOGLE_APPLICATION_CREDENTIALS_JSON").is_ok();
    if !use_emulator && !use_cloud {
        panic!("either PUBSUB_EMULATOR_HOST or GOOGLE_APPLICATION_CREDENTIALS_JSON must be set");
    }

    let config = ClientConfig::default().with_auth().await?;
    let client = Client::new(config).await?;

    let topic = client.topic(TOPIC);

    if command == "create" {
        // delete and create "test-topic"
        for subscription in topic.subscriptions(None).await? {
            subscription.delete(None).await?;
        }

        let _ = topic.delete(None).await;
        topic.create(Some(Default::default()), None).await?;
        for i in 0..SUBSCRIPTION_COUNT {
            let _ = client
                .create_subscription(
                    format!("test-subscription-{}", i).as_str(),
                    TOPIC,
                    SubscriptionConfig {
                        retain_acked_messages: false,
                        ..Default::default()
                    },
                    None,
                )
                .await?;
        }
    } else if command == "publish" {
        let publisher = topic.new_publisher(Default::default());
        for i in 0..10 {
            let data = format!("{{\"v1\":{i},\"v2\":\"name{i}\"}}");
            let a = publisher
                .publish(PubsubMessage {
                    data: data.to_string().into_bytes(),
                    ..Default::default()
                })
                .await;
            a.get().await?;
            println!("published {}", data);
        }
    } else {
        panic!("unknown command {command}");
    }

    Ok(())
}
