// Copyright 2023 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;
use std::time::SystemTime;

use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::producer::{BaseProducer, BaseRecord};
use rdkafka::ClientConfig;

/// Create a kafka topic
pub async fn create_topics(broker_addr: &str, topics: HashMap<String, i32>) {
    let admin = ClientConfig::new()
        .set("bootstrap.servers", broker_addr)
        .create::<AdminClient<_>>()
        .await
        .expect("failed to create kafka admin client");

    for (topic, partition) in topics {
        println!("creating topic {}", topic);
        admin
            .create_topics(
                &[NewTopic::new(
                    topic.as_str(),
                    partition,
                    TopicReplication::Fixed(1),
                )],
                &AdminOptions::default(),
            )
            .await
            .expect("failed to create topic");
    }
}

/// Create a kafka producer for the topics and data in `datadir`.
pub async fn producer(broker_addr: &str, datadir: String) {
    let admin = ClientConfig::new()
        .set("bootstrap.servers", broker_addr)
        .create::<AdminClient<_>>()
        .await
        .expect("failed to create kafka admin client");

    let producer = ClientConfig::new()
        .set("bootstrap.servers", broker_addr)
        .create::<BaseProducer>()
        .await
        .expect("failed to create kafka producer");

    for file in std::fs::read_dir(datadir).unwrap() {
        let file = file.unwrap();
        let name = file.file_name().into_string().unwrap();
        let Some((topic, partitions)) = name.split_once('.') else {
            tracing::warn!("ignore file: {name:?}. expected format \"topic.partitions\"");
            continue;
        };
        admin
            .create_topics(
                &[NewTopic::new(
                    topic,
                    partitions.parse().unwrap(),
                    TopicReplication::Fixed(1),
                )],
                &AdminOptions::default(),
            )
            .await
            .expect("failed to create topic");

        let content = std::fs::read(file.path()).unwrap();
        let msgs: Box<dyn Iterator<Item = &[u8]> + Send> = if topic.ends_with("bin") {
            // binary message data, a file is a message
            Box::new(std::iter::once(content.as_slice()))
        } else {
            Box::new(content.split(|&b| b == b'\n'))
        };
        for msg in msgs {
            loop {
                let ts = SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as i64;
                let record = BaseRecord::<(), _>::to(topic).payload(msg).timestamp(ts);
                match producer.send(record) {
                    Ok(_) => break,
                    Err((KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull), _)) => {
                        producer.flush(None).await.expect("failed to flush");
                    }
                    Err((e, _)) => panic!("failed to send message: {}", e),
                }
            }
        }
        producer.flush(None).await.expect("failed to flush");
    }
}
