// Copyright 2024 RisingWave Labs
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
        "bitnami/kafka:3.7".to_owned()
    }

    fn envs(&self) -> Vec<(String, String)> {
        vec![
            ("KAFKA_CFG_NODE_ID".to_owned(), self.broker_id.to_string()),
            ("KAFKA_CFG_BROKER_ID".to_owned(), self.broker_id.to_string()),
            (
                "KAFKA_CFG_PROCESS_ROLES".to_owned(),
                "controller,broker".to_owned(),
            ),
            (
                "KAFKA_CFG_LISTENERS".to_owned(),
                "PLAINTEXT://:9092,CONTROLLER://:9093".to_owned(),
            ),
            (
                "KAFKA_CFG_ADVERTISED_LISTENERS".to_owned(),
                format!("PLAINTEXT://{}:{}", self.address, self.port),
            ),
            (
                "KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP".to_owned(),
                "PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT".to_owned(),
            ),
            (
                "KAFKA_CFG_CONTROLLER_QUORUM_VOTERS".to_owned(),
                format!("{}@localhost:9093", self.broker_id),
            ),
            (
                "KAFKA_CFG_CONTROLLER_LISTENER_NAMES".to_owned(),
                "CONTROLLER".to_owned(),
            ),
            ("KAFKA_KRAFT_CLUSTER_ID".to_owned(), "risedev".to_owned()),
        ]
    }

    fn ports(&self) -> Vec<(String, String)> {
        vec![(
            format!("{}:{}", self.listen_address, self.port),
            "9092".to_owned(),
        )]
    }

    fn data_path(&self) -> Option<String> {
        self.persist_data.then(|| "/bitnami/kafka".to_owned())
    }
}

pub type KafkaService = DockerService<KafkaConfig>;
