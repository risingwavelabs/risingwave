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
use crate::MySqlConfig;

impl DockerServiceConfig for MySqlConfig {
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
        let mut envs = vec![("MYSQL_DATABASE".to_owned(), self.database.clone())];
        if self.user == "root" {
            if self.password.is_empty() {
                envs.push(("MYSQL_ALLOW_EMPTY_PASSWORD".to_owned(), "1".to_owned()));
            } else {
                envs.push(("MYSQL_ROOT_PASSWORD".to_owned(), self.password.clone()));
            }
        } else {
            envs.extend([
                ("MYSQL_ALLOW_EMPTY_PASSWORD".to_owned(), "1".to_owned()),
                ("MYSQL_USER".to_owned(), self.user.clone()),
                ("MYSQL_PASSWORD".to_owned(), self.password.clone()),
            ]);
        }
        envs
    }

    fn ports(&self) -> Vec<(String, String)> {
        vec![(self.port.to_string(), "3306".to_owned())]
    }

    fn data_path(&self) -> Option<String> {
        self.persist_data.then(|| "/var/lib/mysql".to_owned())
    }
}

/// Docker-backed MySQL service.
pub type MySqlService = DockerService<MySqlConfig>;
