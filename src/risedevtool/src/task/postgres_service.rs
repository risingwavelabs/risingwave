// Copyright 2025 RisingWave Labs
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

use crate::PostgresConfig;
use crate::task::docker_service::{DockerService, DockerServiceConfig};

impl DockerServiceConfig for PostgresConfig {
    fn id(&self) -> String {
        self.id.clone()
    }

    fn is_user_managed(&self) -> bool {
        self.user_managed
    }

    fn image(&self) -> String {
        self.image.clone()
    }

    fn args(&self) -> Vec<String> {
        // Enable CDC.
        [
            "-c",
            "wal_level=logical",
            "-c",
            "max_replication_slots=30",
            "-c",
            "log_statement=all",
        ]
        .map(String::from)
        .to_vec()
    }

    fn envs(&self) -> Vec<(String, String)> {
        vec![
            ("POSTGRES_HOST_AUTH_METHOD".to_owned(), "trust".to_owned()),
            ("POSTGRES_USER".to_owned(), self.user.clone()),
            ("POSTGRES_PASSWORD".to_owned(), self.password.clone()),
            ("POSTGRES_DB".to_owned(), self.database.clone()),
            (
                "POSTGRES_INITDB_ARGS".to_owned(),
                "--encoding=UTF-8 --lc-collate=C --lc-ctype=C".to_owned(),
            ),
        ]
    }

    fn ports(&self) -> Vec<(String, String)> {
        vec![(self.port.to_string(), "5432".to_owned())]
    }

    fn data_path(&self) -> Option<String> {
        self.persist_data
            .then(|| "/var/lib/postgresql/data".to_owned())
    }
}

/// Docker-backed PostgreSQL service.
pub type PostgresService = DockerService<PostgresConfig>;
