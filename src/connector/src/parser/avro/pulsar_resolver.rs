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

use std::sync::Arc;

use anyhow::Context as _;
use apache_avro::Schema;
use moka::future::Cache;

use crate::error::ConnectorResult;
use crate::schema::pulsar_schema::Client;

#[derive(Debug)]
pub struct PulsarSchemaCache {
    writer_schemas: Cache<i64, Arc<Schema>>,
    pulsar_client: Client,
    topic: Box<str>,
}

impl PulsarSchemaCache {
    /// Create a new `GlueSchemaCache`
    pub fn new(base: url::Url, token: Option<String>, topic: &str) -> ConnectorResult<Self> {
        let client = Client::new(base, token);
        Ok(Self {
            writer_schemas: Cache::new(u64::MAX),
            pulsar_client: client,
            topic: topic.into(),
        })
    }

    async fn cache_schema(
        &self,
        schema_version: i64,
        content: Schema,
    ) -> ConnectorResult<Arc<Schema>> {
        let schema = Arc::new(content);
        self.writer_schemas
            .insert(schema_version, Arc::clone(&schema))
            .await;
        Ok(schema)
    }
}

impl PulsarSchemaCache {
    pub async fn get(&self, schema_version: Option<i64>) -> ConnectorResult<Arc<Schema>> {
        if let Some(schema_version) = schema_version
            && let Some(schema) = self.writer_schemas.get(&schema_version).await
        {
            return Ok(schema);
        }
        let res = self
            .pulsar_client
            .get_schema(&self.topic, schema_version)
            .await
            .context("pulsar rest error")?;
        let schema_version = res.version;
        let crate::schema::pulsar_schema::PulsarSchema::Avro(definition) = res.schema else {
            unreachable!("glue sdk response without definition");
        };
        self.cache_schema(schema_version, definition).await
    }
}
