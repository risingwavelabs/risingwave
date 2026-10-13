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

use std::sync::Arc;

use anyhow::Context;
use apache_avro::Schema;
use moka::future::Cache;
use risingwave_common::bail;

use crate::error::ConnectorResult;
use crate::schema::pulsar_schema::{Client, PulsarSchemaInfo};
use crate::source::pulsar::topic::{Topic, parse_topic};

/// Fetch schemas of a single Pulsar topic and cache writer schemas by version.
/// Each parser configuration owns a resolver and shares it with its clones through `Arc`.
#[derive(Debug)]
pub struct PulsarSchemaCache {
    /// The topic without partition suffix, as schemas are shared by all partitions.
    topic: Topic,
    writer_schemas: Cache<i64, Arc<Schema>>,
    pulsar_client: Client,
}

impl PulsarSchemaCache {
    pub fn new(pulsar_client: Client, topic: &str) -> ConnectorResult<Self> {
        let mut topic = parse_topic(topic)?;
        topic.topic = topic.topic_str_without_partition()?;
        topic.partition_index = None;
        Ok(Self {
            topic,
            writer_schemas: Cache::new(u64::MAX),
            pulsar_client,
        })
    }

    fn parse_schema(raw_schema: PulsarSchemaInfo) -> ConnectorResult<Arc<Schema>> {
        if !raw_schema.r#type.eq_ignore_ascii_case("AVRO") {
            bail!("expected Pulsar AVRO schema, got {}", raw_schema.r#type);
        }
        let schema =
            Schema::parse_str(&raw_schema.data).context("failed to parse Pulsar Avro schema")?;
        Ok(Arc::new(schema))
    }

    /// Fetches the current latest schema for a parser's reader schema and caches its version.
    pub async fn get_latest(&self) -> ConnectorResult<Arc<Schema>> {
        let raw_schema = self.pulsar_client.get_schema(&self.topic, None).await?;
        let version = raw_schema.version;
        let schema = Self::parse_schema(raw_schema)?;
        self.writer_schemas
            .insert(version, Arc::clone(&schema))
            .await;
        Ok(schema)
    }

    /// Gets a specific writer schema. Concurrent misses share one lookup; failed lookups can
    /// be retried because errors are not cached.
    pub async fn get_by_version(&self, version: i64) -> ConnectorResult<Arc<Schema>> {
        self.writer_schemas
            .try_get_with(version, async {
                let raw_schema = self
                    .pulsar_client
                    .get_schema(&self.topic, Some(version))
                    .await?;
                if raw_schema.version != version {
                    bail!(
                        "expected Pulsar schema version {version}, got {}",
                        raw_schema.version
                    );
                }
                Self::parse_schema(raw_schema)
            })
            .await
            .map_err(Into::into)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    #[cfg(not(madsim))]
    use std::io::{Read, Write};
    #[cfg(not(madsim))]
    use std::net::TcpListener;
    #[cfg(not(madsim))]
    use std::thread;

    use super::*;
    use crate::schema::pulsar_schema::{
        PULSAR_SCHEMA_AUTH_TOKEN_KEY, PULSAR_SCHEMA_URL_KEY, PulsarSchemaConfig,
    };

    fn config(url: &str, token: Option<&str>) -> PulsarSchemaConfig {
        let mut options = BTreeMap::from([(PULSAR_SCHEMA_URL_KEY.to_owned(), url.to_owned())]);
        if let Some(token) = token {
            options.insert(PULSAR_SCHEMA_AUTH_TOKEN_KEY.to_owned(), token.to_owned());
        }
        PulsarSchemaConfig::from_options(&options).unwrap().unwrap()
    }

    #[tokio::test]
    async fn resolver_clones_share_schemas_but_new_resolvers_are_independent() {
        let config = config("https://cache-lifetime-test:8443", Some("token-a"));
        let first =
            Arc::new(PulsarSchemaCache::new(Client::new(&config).unwrap(), "events").unwrap());
        let cloned = Arc::clone(&first);
        let second = PulsarSchemaCache::new(Client::new(&config).unwrap(), "events").unwrap();
        let schema =
            Arc::new(Schema::parse_str(r#"{"type":"record","name":"Event","fields":[]}"#).unwrap());
        first.writer_schemas.insert(0, Arc::clone(&schema)).await;
        assert!(Arc::ptr_eq(
            &schema,
            &cloned.get_by_version(0).await.unwrap()
        ));
        assert!(second.writer_schemas.get(&0_i64).await.is_none());

        let weak = Arc::downgrade(&first);
        drop(first);
        assert!(weak.upgrade().is_some());
        drop(cloned);
        assert!(weak.upgrade().is_none());
    }

    #[test]
    fn topic_is_normalized_once_to_the_non_partitioned_topic() {
        let config = config("http://topic-cache-test:8080", None);
        for topic in [
            "events",
            "public/default/events",
            "persistent://public/default/events",
            "persistent://public/default/events-partition-2",
        ] {
            let cache = PulsarSchemaCache::new(Client::new(&config).unwrap(), topic).unwrap();
            assert_eq!(
                cache.topic.to_string(),
                "persistent://public/default/events"
            );
            assert_eq!(cache.topic.partition_index, None);
        }
    }

    #[cfg(not(madsim))]
    fn spawn_schema_server(
        responses: Vec<(u16, String)>,
    ) -> (String, thread::JoinHandle<Vec<String>>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let handle = thread::spawn(move || {
            let mut requests = Vec::new();
            for (status, body) in responses {
                let (mut stream, _) = listener.accept().unwrap();
                let mut request = Vec::new();
                while !request.ends_with(b"\r\n\r\n") {
                    let mut byte = [0];
                    stream.read_exact(&mut byte).unwrap();
                    request.push(byte[0]);
                }
                requests.push(String::from_utf8(request).unwrap());
                write!(
                    stream,
                    "HTTP/1.1 {status} Test\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                )
                .unwrap();
            }
            requests
        });
        (url, handle)
    }

    #[cfg(not(madsim))]
    fn schema_response(version: i64, data: &str) -> (u16, String) {
        (
            200,
            serde_json::json!({"version": version, "type": "AVRO", "data": data}).to_string(),
        )
    }

    #[cfg(not(madsim))]
    #[tokio::test]
    async fn concurrent_misses_share_a_lookup_and_failures_can_be_retried() {
        let data = r#"{"type":"record","name":"Event","fields":[]}"#;
        let (url, server) =
            spawn_schema_server(vec![(404, String::new()), schema_response(0, data)]);
        let config = config(&url, None);
        let first = Arc::new(
            PulsarSchemaCache::new(
                Client::new(&config).unwrap(),
                "persistent://public/default/events-partition-0",
            )
            .unwrap(),
        );
        let second = Arc::clone(&first);
        assert!(first.get_by_version(0).await.is_err());

        let (a, b) = tokio::join!(first.get_by_version(0), second.get_by_version(0));
        assert!(Arc::ptr_eq(&a.unwrap(), &b.unwrap()));
        let requests = server.join().unwrap();
        assert_eq!(requests.len(), 2);
        for request in requests {
            assert!(request.starts_with("GET /admin/v2/schemas/public/default/events/schema/0 "));
        }
    }

    #[cfg(not(madsim))]
    #[tokio::test]
    async fn old_writer_schema_resolves_to_the_latest_reader() {
        use apache_avro::types::Value;
        use apache_avro::{from_avro_datum, to_avro_datum};

        let v0 = r#"{"type":"record","name":"Event","fields":[{"name":"id","type":"int"}]}"#;
        let v1 = r#"{"type":"record","name":"Event","fields":[{"name":"id","type":"int"},{"name":"name","type":"string","default":"unknown"}]}"#;
        let (url, server) =
            spawn_schema_server(vec![schema_response(1, v1), schema_response(0, v0)]);
        let cache =
            PulsarSchemaCache::new(Client::new(&config(&url, None)).unwrap(), "events").unwrap();
        let reader = cache.get_latest().await.unwrap();
        let writer = cache.get_by_version(0).await.unwrap();
        assert!(Arc::ptr_eq(
            &writer,
            &cache.get_by_version(0).await.unwrap()
        ));
        assert!(Arc::ptr_eq(
            &reader,
            &cache.get_by_version(1).await.unwrap()
        ));
        let bytes =
            to_avro_datum(&writer, Value::Record(vec![("id".into(), Value::Int(42))])).unwrap();
        assert_eq!(
            from_avro_datum(&writer, &mut bytes.as_slice(), Some(&reader)).unwrap(),
            Value::Record(vec![
                ("id".into(), Value::Int(42)),
                ("name".into(), Value::String("unknown".into())),
            ])
        );
        let requests = server.join().unwrap();
        assert_eq!(requests.len(), 2);
        assert!(requests[0].starts_with("GET /admin/v2/schemas/public/default/events/schema "));
        assert!(requests[1].starts_with("GET /admin/v2/schemas/public/default/events/schema/0 "));
    }
}
