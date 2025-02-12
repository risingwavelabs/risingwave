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

use crate::task::docker_service::{DockerService, DockerServiceConfig};
use crate::MongoDbConfig;

impl DockerServiceConfig for MongoDbConfig {
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
            ("MONGO_INITDB_ROOT_USERNAME".to_owned(), self.user.clone()),
            (
                "MONGO_INITDB_ROOT_PASSWORD".to_owned(),
                self.password.clone(),
            ),
            ("MONGO_INITDB_DATABASE".to_owned(), self.database.clone()),
        ]
    }

    fn ports(&self) -> Vec<(String, String)> {
        vec![(self.port.to_string(), "27017".to_owned())]
    }

    fn data_path(&self) -> Option<String> {
        self.persist_data.then(|| "/data/db".to_owned())
    }
}

/// Docker-backed `MongoDb` service.
pub type MongoDbService = DockerService<MongoDbConfig>;
