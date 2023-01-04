// Copyright 2023 Singularity Data
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

use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::producer::{BaseProducer, BaseRecord};
use rdkafka::ClientConfig;

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
        let (topic, partitions) = name.split_once('.').unwrap();
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
        // binary message data, a file is a message
        if topic.ends_with("bin") {
            loop {
                let record = BaseRecord::<(), _>::to(topic).payload(&content);
                match producer.send(record) {
                    Ok(_) => break,
                    Err((KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull), _)) => {
                        producer.flush(None).await;
                    }
                    Err((e, _)) => panic!("failed to send message: {}", e),
                }
            }
        } else {
            for line in content.split(|&b| b == b'\n') {
                loop {
                    let record = BaseRecord::<(), _>::to(topic).payload(line);
                    match producer.send(record) {
                        Ok(_) => break,
                        Err((KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull), _)) => {
                            producer.flush(None).await;
                        }
                        Err((e, _)) => panic!("failed to send message: {}", e),
                    }
                }
            }
        }
        producer.flush(None).await;
    }
}
