// Copyright 2026 RisingWave Labs
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

use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use super::docker_service::{DockerService, DockerServiceConfig};
use crate::CassandraConfig;

impl DockerServiceConfig for CassandraConfig {
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
            ("CASSANDRA_DC".to_owned(), self.datacenter.clone()),
            (
                "CASSANDRA_ENDPOINT_SNITCH".to_owned(),
                "GossipingPropertyFileSnitch".to_owned(),
            ),
        ]
    }

    fn ports(&self) -> Vec<(String, String)> {
        let address = self
            .address
            .parse::<IpAddr>()
            .unwrap_or(Ipv4Addr::LOCALHOST.into());
        vec![(
            SocketAddr::new(address, self.port).to_string(),
            "9042".to_owned(),
        )]
    }

    fn data_path(&self) -> Option<String> {
        self.persist_data.then(|| "/var/lib/cassandra".to_owned())
    }
}

/// Docker-backed Cassandra service.
pub type CassandraService = DockerService<CassandraConfig>;
