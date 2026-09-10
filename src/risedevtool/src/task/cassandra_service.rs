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
        vec![(self.port.to_string(), "9042".to_owned())]
    }

    fn data_path(&self) -> Option<String> {
        self.persist_data.then(|| "/var/lib/cassandra".to_owned())
    }
}

/// Docker-backed Cassandra service.
pub type CassandraService = DockerService<CassandraConfig>;

#[cfg(test)]
mod tests {
    use yaml_rust::YamlLoader;

    use super::*;
    use crate::{ConfigExpander, ServiceConfig, TaskGroup};

    const CONFIG: &str = "
- use: cassandra
  id: cassandra-test
  address: localhost
  port: 19042
  datacenter: test-dc
  image: cassandra:4.0
  user-managed: false
  persist-data: false
";

    #[test]
    fn test_cassandra_config_and_docker_options() {
        let yaml = YamlLoader::load_from_str(CONFIG).unwrap();
        let services = ConfigExpander::deserialize(&yaml[0]).unwrap();
        let service = &services[0];
        assert_eq!(service.id(), "cassandra-test");
        assert_eq!(service.port(), Some(19042));
        assert_eq!(service.task_group(), TaskGroup::Cassandra);
        assert!(!service.user_managed());

        let ServiceConfig::Cassandra(config) = service else {
            panic!("expected Cassandra service");
        };
        assert_eq!(config.image(), "cassandra:4.0");
        assert_eq!(config.ports(), vec![("19042".into(), "9042".into())]);
        assert_eq!(
            config.envs(),
            vec![
                ("CASSANDRA_DC".into(), "test-dc".into()),
                (
                    "CASSANDRA_ENDPOINT_SNITCH".into(),
                    "GossipingPropertyFileSnitch".into()
                ),
            ]
        );
        assert_eq!(config.data_path(), None);
        assert_eq!(config.cqlsh, None);

        let mut config = config.clone();
        config.persist_data = true;
        assert_eq!(config.data_path().as_deref(), Some("/var/lib/cassandra"));
    }

    #[test]
    fn test_cassandra_user_managed_config() {
        let yaml = YamlLoader::load_from_str(&format!(
            "{}  cqlsh: /opt/cassandra/bin/cqlsh\n",
            CONFIG.replace("user-managed: false", "user-managed: true")
        ))
        .unwrap();
        let services = ConfigExpander::deserialize(&yaml[0]).unwrap();
        assert!(services[0].user_managed());
        let ServiceConfig::Cassandra(config) = &services[0] else {
            panic!("expected Cassandra service");
        };
        assert!(config.is_user_managed());
        assert_eq!(config.cqlsh.as_deref(), Some("/opt/cassandra/bin/cqlsh"));
    }

    #[test]
    fn test_cassandra_rejects_unknown_options() {
        let yaml = YamlLoader::load_from_str(&format!("{CONFIG}  unknown-option: true\n")).unwrap();
        assert!(ConfigExpander::deserialize(&yaml[0]).is_err());
    }
}
