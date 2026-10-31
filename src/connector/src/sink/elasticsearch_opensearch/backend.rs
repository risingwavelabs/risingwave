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

use super::client::SearchClient;
use super::config::ElasticSearchOpenSearchConfig;
use crate::sink::Result;

/// Backend-specific configuration, capabilities, and client construction for search sinks.
pub trait SearchBackend: Debug + Send + Sync + 'static {
    type Config: Debug + Clone + Send + Sync;
    type Client: SearchClient;

    const SINK_NAME: &'static str;
    const SUPPORT_SCHEMA_CHANGE: bool;

    fn parse_config(properties: BTreeMap<String, String>) -> Result<Self::Config>;

    fn common_config(config: &Self::Config) -> &ElasticSearchOpenSearchConfig;

    fn build_client(config: &Self::Config) -> Result<Self::Client>;

    fn check_available() -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use bytes::BytesMut;
    use serde_json::{Value, json};

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
            let _: elasticsearch::Elasticsearch =
                ElasticSearchBackend::build_client(&config).unwrap();
            let config = OpenSearchBackend::parse_config(properties).unwrap();
            let _: opensearch::OpenSearch = OpenSearchBackend::build_client(&config).unwrap();
        }
    }

    fn check_bulk_operations<B: SearchBackend>(
        serialize: impl Fn(&<B::Client as SearchClient>::BulkOperation, &mut BytesMut),
    ) {
        let config = B::parse_config(properties("http://localhost:9200", &[])).unwrap();
        let client = B::build_client(&config).unwrap();
        for routing in [None, Some("route".to_owned())] {
            let document = json!({"doc": {"id": 1}, "doc_as_upsert": true});
            let update = client.new_update(
                "1".to_owned(),
                "index".to_owned(),
                5,
                routing.clone(),
                document.clone(),
            );
            let delete = client.new_delete("2".to_owned(), "index".to_owned(), routing.clone());
            let mut body = BytesMut::new();
            serialize(&update, &mut body);
            serialize(&delete, &mut body);
            assert!(body.ends_with(b"\n"));
            let lines: Vec<Value> = std::str::from_utf8(&body)
                .unwrap()
                .lines()
                .map(|line| serde_json::from_str(line).unwrap())
                .collect();
            let mut update_meta = json!({"_id": "1", "_index": "index", "retry_on_conflict": 5});
            let mut delete_meta = json!({"_id": "2", "_index": "index"});
            if let Some(routing) = routing {
                update_meta["routing"] = json!(routing);
                delete_meta["routing"] = json!(routing);
            }
            assert_eq!(
                lines,
                vec![
                    json!({"update": update_meta}),
                    document,
                    json!({"delete": delete_meta})
                ]
            );
        }
    }

    #[test]
    fn test_backend_bulk_operations() {
        check_bulk_operations::<ElasticSearchBackend>(|bulk, body| {
            elasticsearch::http::request::Body::write(bulk, body).unwrap();
        });
        check_bulk_operations::<OpenSearchBackend>(|bulk, body| {
            opensearch::http::request::Body::write(bulk, body).unwrap();
        });
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
