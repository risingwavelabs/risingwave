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
use crate::source::pulsar::topic::parse_topic;

type SchemaVersions = Cache<i64, Arc<Schema>>;

/// Fetch schemas from Pulsar and cache writer schemas by topic, then version.
/// Each parser configuration owns a resolver and shares it with its clones through `Arc`.
#[derive(Debug)]
pub struct PulsarSchemaCache {
    writer_schemas: Cache<String, SchemaVersions>,
    pulsar_client: Client,
}

impl PulsarSchemaCache {
    pub fn new(pulsar_client: Client) -> Self {
        Self {
            writer_schemas: Cache::new(u64::MAX),
            pulsar_client,
        }
    }

    pub(super) fn normalize_topic(topic: &str) -> ConnectorResult<String> {
        let mut topic = parse_topic(topic)?;
        topic.topic = topic.topic_str_without_partition()?;
        topic.partition_index = None;
        Ok(topic.to_string())
    }

    async fn topic_schemas(&self, topic: &str) -> ConnectorResult<SchemaVersions> {
        if let Some(schemas) = self.writer_schemas.get(topic).await {
            return Ok(schemas);
        }
        Ok(self
            .writer_schemas
            .get_with(Self::normalize_topic(topic)?, async {
                Cache::new(u64::MAX)
            })
            .await)
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
    pub async fn get_latest(&self, topic: &str) -> ConnectorResult<Arc<Schema>> {
        let schemas = self.topic_schemas(topic).await?;
        let raw_schema = self.pulsar_client.get_schema(topic, None).await?;
        let version = raw_schema.version;
        let schema = Self::parse_schema(raw_schema)?;
        schemas.insert(version, Arc::clone(&schema)).await;
        Ok(schema)
    }

    /// Gets a specific writer schema. Concurrent misses share one lookup; failed lookups can
    /// be retried because errors are not cached.
    pub async fn get_by_version(&self, topic: &str, version: i64) -> ConnectorResult<Arc<Schema>> {
        let schemas = self.topic_schemas(topic).await?;
        schemas
            .try_get_with(version, async {
                let raw_schema = self.pulsar_client.get_schema(topic, Some(version)).await?;
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
        let first = Arc::new(PulsarSchemaCache::new(Client::new(&config).unwrap()));
        let cloned = Arc::clone(&first);
        let second = PulsarSchemaCache::new(Client::new(&config).unwrap());
        let schema =
            Arc::new(Schema::parse_str(r#"{"type":"record","name":"Event","fields":[]}"#).unwrap());
        first
            .topic_schemas("events")
            .await
            .unwrap()
            .insert(0, Arc::clone(&schema))
            .await;
        assert!(Arc::ptr_eq(
            &schema,
            &cloned.get_by_version("events", 0).await.unwrap()
        ));
        assert!(
            second
                .writer_schemas
                .get("persistent://public/default/events")
                .await
                .is_none()
        );

        let weak = Arc::downgrade(&first);
        drop(first);
        assert!(weak.upgrade().is_some());
        drop(cloned);
        assert!(weak.upgrade().is_none());
    }

    #[tokio::test]
    async fn topic_versions_are_isolated_and_topic_aliases_share_entries() {
        let cache = PulsarSchemaCache::new(
            Client::new(&config("http://topic-cache-test:8080", None)).unwrap(),
        );
        let events =
            Arc::new(Schema::parse_str(r#"{"type":"record","name":"Event","fields":[]}"#).unwrap());
        let orders =
            Arc::new(Schema::parse_str(r#"{"type":"record","name":"Order","fields":[]}"#).unwrap());
        for (topic, version, schema) in [
            ("events", 0, &events),
            ("events", 1, &orders),
            ("orders", 0, &orders),
        ] {
            cache
                .topic_schemas(topic)
                .await
                .unwrap()
                .insert(version, Arc::clone(schema))
                .await;
        }
        for topic in [
            "events",
            "public/default/events",
            "persistent://public/default/events",
            "persistent://public/default/events-partition-2",
        ] {
            assert!(Arc::ptr_eq(
                &events,
                &cache.get_by_version(topic, 0).await.unwrap()
            ));
        }
        for (topic, version) in [("orders", 0), ("events", 1)] {
            assert!(Arc::ptr_eq(
                &orders,
                &cache.get_by_version(topic, version).await.unwrap()
            ));
        }
        cache.writer_schemas.run_pending_tasks().await;
        assert_eq!(cache.writer_schemas.entry_count(), 2);
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
        let first = Arc::new(PulsarSchemaCache::new(Client::new(&config).unwrap()));
        let second = Arc::clone(&first);
        assert!(first.get_by_version("events", 0).await.is_err());

        let (a, b) = tokio::join!(
            first.get_by_version("events", 0),
            second.get_by_version("persistent://public/default/events-partition-0", 0),
        );
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
        let cache = PulsarSchemaCache::new(Client::new(&config(&url, None)).unwrap());
        let reader = cache.get_latest("events").await.unwrap();
        let writer = cache.get_by_version("events", 0).await.unwrap();
        assert!(Arc::ptr_eq(
            &writer,
            &cache.get_by_version("events", 0).await.unwrap()
        ));
        assert!(Arc::ptr_eq(
            &reader,
            &cache.get_by_version("events", 1).await.unwrap()
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
