use std::fs::File;
use std::io::prelude::*;
use std::io::BufReader;

use google_cloud_googleapis::pubsub::v1::PubsubMessage;
use google_cloud_pubsub::client::Client;
use google_cloud_pubsub::subscription::SubscriptionConfig;

const TOPIC: &str = "test-topic";

const SUBSCRIPTION_COUNT: usize = 50;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    std::env::set_var("PUBSUB_EMULATOR_HOST", "127.0.0.1:5980");

    let client = Client::new(Default::default()).await?;

    // delete and create "test-topic"
    let topic = client.topic(TOPIC);
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
                    retain_acked_messages: true,
                    ..Default::default()
                },
                None,
            )
            .await?;
    }

    let path = std::env::current_exe()?
        .parent()
        .and_then(|p| p.parent())
        .and_then(|p| p.parent())
        .unwrap()
        .join("scripts/source/test_data/pubsub_1_test_topic.1");

    let file = File::open(path)?;
    let file = BufReader::new(file);

    let publisher = topic.new_publisher(Default::default());
    for line in file.lines().flatten() {
        let a = publisher
            .publish(PubsubMessage {
                data: line.clone().into_bytes(),
                ..Default::default()
            })
            .await;
        a.get().await?;
        println!("published {}", line);
    }

    Ok(())
}
