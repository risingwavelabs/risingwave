// Copyright 2022 RisingWave Labs
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

use super::docker_service::{DockerService, DockerServiceConfig};
use crate::KafkaConfig;

impl DockerServiceConfig for KafkaConfig {
    fn id(&self) -> String {
        self.id.clone()
    }

    fn is_user_managed(&self) -> bool {
        self.user_managed
    }

    fn image(&self) -> String {
        self.image.clone()
    }

    fn envs(&self) -> Vec<(String, String)> {
        vec![
            ("KAFKA_NODE_ID".to_owned(), self.node_id.to_string()),
            (
                "KAFKA_PROCESS_ROLES".to_owned(),
                "controller,broker".to_owned(),
            ),
            (
                "KAFKA_LISTENERS".to_owned(),
                "HOST://:9092,CONTROLLER://:9093,DOCKER://:9094".to_owned(),
            ),
            (
                "KAFKA_ADVERTISED_LISTENERS".to_owned(),
                format!(
                    "HOST://{}:{},DOCKER://host.docker.internal:{}",
                    self.address, self.port, self.docker_port
                ),
            ),
            (
                "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP".to_owned(),
                "HOST:PLAINTEXT,CONTROLLER:PLAINTEXT,DOCKER:PLAINTEXT".to_owned(),
            ),
            (
                "KAFKA_CONTROLLER_QUORUM_VOTERS".to_owned(),
                format!("{}@localhost:9093", self.node_id),
            ),
            (
                "KAFKA_CONTROLLER_LISTENER_NAMES".to_owned(),
                "CONTROLLER".to_owned(),
            ),
            (
                "KAFKA_INTER_BROKER_LISTENER_NAME".to_owned(),
                "HOST".to_owned(),
            ),
            // https://docs.confluent.io/platform/current/installation/docker/config-reference.html#example-configurations
            (
                "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR".to_owned(),
                "1".to_owned(),
            ),
            ("CLUSTER_ID".to_owned(), "RiseDevRiseDevRiseDev1".to_owned()),
        ]
    }

    fn ports(&self) -> Vec<(String, String)> {
        vec![
            (self.port.to_string(), "9092".to_owned()),
            (self.docker_port.to_string(), "9094".to_owned()),
        ]
    }

    fn data_path(&self) -> Option<String> {
        self.persist_data.then(|| "/var/lib/kafka/data".to_owned())
    }
}

/// Docker-backed Kafka service.
pub type KafkaService = DockerService<KafkaConfig>;
