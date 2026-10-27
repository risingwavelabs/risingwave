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

use std::collections::BTreeMap;
use std::fmt::Debug;

use super::client::ElasticSearchOpenSearchClient;
use super::config::ElasticSearchOpenSearchConfig;
use crate::sink::Result;

/// Backend-specific configuration, capabilities, and client construction for search sinks.
pub trait SearchBackend: Debug + Send + Sync + 'static {
    type Config: Debug + Clone + Send + Sync;

    const SINK_NAME: &'static str;
    const SUPPORT_SCHEMA_CHANGE: bool;

    fn parse_config(properties: BTreeMap<String, String>) -> Result<Self::Config>;

    fn common_config(config: &Self::Config) -> &ElasticSearchOpenSearchConfig;

    fn build_client(config: &Self::Config) -> Result<ElasticSearchOpenSearchClient>;

    fn check_available() -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sink::elasticsearch_opensearch::elasticsearch::ElasticSearchBackend;
    use crate::sink::elasticsearch_opensearch::opensearch::OpenSearchBackend;

    fn properties(url: &str, credentials: &[(&str, &str)]) -> BTreeMap<String, String> {
        std::iter::once(("url", url))
            .chain(credentials.iter().copied())
            .map(|(key, value)| (key.to_owned(), value.to_owned()))
            .collect()
    }

    #[test]
    fn test_backend_client_types() {
        // Client construction does not require a sink index or contact the server.
        for credentials in [vec![], vec![("username", "user"), ("password", "password")]] {
            let properties = properties("http://localhost:9200", &credentials);
            let config = ElasticSearchBackend::parse_config(properties.clone()).unwrap();
            assert!(matches!(
                ElasticSearchBackend::build_client(&config).unwrap(),
                ElasticSearchOpenSearchClient::ElasticSearch(_)
            ));
            let config = OpenSearchBackend::parse_config(properties).unwrap();
            assert!(matches!(
                OpenSearchBackend::build_client(&config).unwrap(),
                ElasticSearchOpenSearchClient::OpenSearch(_)
            ));
        }
    }

    fn check_client_config_errors<B: SearchBackend>() {
        for credentials in [vec![("username", "user")], vec![("password", "password")]] {
            let config =
                B::parse_config(properties("http://localhost:9200", &credentials)).unwrap();
            assert!(B::build_client(&config).is_err());
        }

        let config = B::parse_config(properties("invalid url", &[("username", "user")])).unwrap();
        assert!(B::build_client(&config).is_err());

        let mut properties = properties("http://localhost:9200", &[]);
        properties.insert("concurrent_requests".to_owned(), "invalid".to_owned());
        assert!(B::parse_config(properties).is_err());
    }

    #[test]
    fn test_backend_client_config_errors() {
        check_client_config_errors::<ElasticSearchBackend>();
        check_client_config_errors::<OpenSearchBackend>();
    }
}
