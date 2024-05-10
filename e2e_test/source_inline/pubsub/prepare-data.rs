#!/usr/bin/env -S cargo -Zscript
```cargo
[dependencies]
anyhow = "1"
google-cloud-googleapis = { version = "0.12", features = ["pubsub"] }
google-cloud-pubsub = "0.24"
tokio = { version = "0.2", package = "madsim-tokio", features = [
    "rt",
    "rt-multi-thread",
    "sync",
    "macros",
    "time",
    "signal",
    "fs",
] }
```

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

    let publisher = topic.new_publisher(Default::default());
    for line in DATA.lines() {
        let a = publisher
            .publish(PubsubMessage {
                data: line.to_string().into_bytes(),
                ..Default::default()
            })
            .await;
        a.get().await?;
        println!("published {}", line);
    }

    Ok(())
}

const DATA: &str = r#"{"v1":1,"v2":"name0"}
{"v1":2,"v2":"name0"}
{"v1":6,"v2":"name3"}
{"v1":0,"v2":"name5"}
{"v1":5,"v2":"name8"}
{"v1":6,"v2":"name4"}
{"v1":8,"v2":"name9"}
{"v1":9,"v2":"name2"}
{"v1":4,"v2":"name6"}
{"v1":5,"v2":"name3"}
{"v1":8,"v2":"name8"}
{"v1":9,"v2":"name2"}
{"v1":2,"v2":"name3"}
{"v1":4,"v2":"name7"}
{"v1":7,"v2":"name0"}
{"v1":0,"v2":"name9"}
{"v1":3,"v2":"name2"}
{"v1":7,"v2":"name5"}
{"v1":1,"v2":"name7"}
{"v1":3,"v2":"name9"}
"#;
