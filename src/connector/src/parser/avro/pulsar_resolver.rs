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

use anyhow::{Context, ensure};
use apache_avro::Schema;
use moka::future::Cache;

use crate::error::ConnectorResult;
use crate::schema::pulsar::{PulsarSchema, PulsarSchemaClient, PulsarSchemaRegistryConfig};

#[derive(Debug)]
pub struct PulsarSchemaCache {
    writer_schemas: Cache<i64, Arc<Schema>>,
    pulsar_client: PulsarSchemaClient,
}

impl PulsarSchemaCache {
    pub fn new(config: PulsarSchemaRegistryConfig) -> ConnectorResult<Self> {
        Ok(Self {
            writer_schemas: Cache::new(u64::MAX),
            pulsar_client: PulsarSchemaClient::new(config)?,
        })
    }

    async fn parse_and_cache_schema(
        &self,
        raw_schema: PulsarSchema,
    ) -> ConnectorResult<Arc<Schema>> {
        ensure!(
            raw_schema.schema_type.eq_ignore_ascii_case("AVRO"),
            "expected Pulsar AVRO schema, got {}",
            raw_schema.schema_type
        );
        let schema =
            Schema::parse_str(&raw_schema.data).context("failed to parse Pulsar avro schema")?;
        let schema = Arc::new(schema);
        self.writer_schemas
            .insert(raw_schema.version, Arc::clone(&schema))
            .await;
        Ok(schema)
    }

    pub async fn get_latest(&self) -> ConnectorResult<Arc<Schema>> {
        self.parse_and_cache_schema(self.pulsar_client.get_latest_schema().await?)
            .await
    }

    pub async fn get_by_version(&self, version: i64) -> ConnectorResult<Arc<Schema>> {
        if let Some(schema) = self.writer_schemas.get(&version).await {
            return Ok(schema);
        }
        self.parse_and_cache_schema(self.pulsar_client.get_schema_by_version(version).await?)
            .await
    }
}
