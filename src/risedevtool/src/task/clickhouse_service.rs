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

use super::docker_service::{DockerService, DockerServiceConfig};
use crate::ClickHouseConfig;

impl DockerServiceConfig for ClickHouseConfig {
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
            ("CLICKHOUSE_USER".to_owned(), self.user.clone()),
            ("CLICKHOUSE_PASSWORD".to_owned(), self.password.clone()),
            ("CLICKHOUSE_DB".to_owned(), self.database.clone()),
        ]
    }

    fn ports(&self) -> Vec<(String, String)> {
        vec![
            (self.http_port.to_string(), "8123".to_owned()),
            (self.native_port.to_string(), "9000".to_owned()),
        ]
    }

    fn data_path(&self) -> Option<String> {
        self.persist_data.then(|| "/var/lib/clickhouse".to_owned())
    }
}

/// Docker-backed ClickHouse service.
pub type ClickHouseService = DockerService<ClickHouseConfig>;
