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
use tokio::sync::OnceCell;

use crate::error::ConnectorResult;
use crate::schema::pulsar_schema_registry::{
    PulsarSchemaInfo, PulsarSchemaRegistryConfig, PulsarSchemaSupplier, PulsarSchemaType,
};

#[derive(Debug)]
pub struct PulsarSchemaCache {
    schema_cache: Cache<i64, Arc<Schema>>,
    latest_schema: OnceCell<Arc<Schema>>,
    schema_supplier: PulsarSchemaSupplier,
}

impl PulsarSchemaCache {
    pub fn new(config: PulsarSchemaRegistryConfig) -> ConnectorResult<Self> {
        Ok(Self {
            schema_cache: Cache::new(u64::MAX),
            latest_schema: OnceCell::new(),
            schema_supplier: PulsarSchemaSupplier::new(config)?,
        })
    }

    async fn parse_and_cache_schema(
        &self,
        raw_schema: PulsarSchemaInfo,
    ) -> ConnectorResult<Arc<Schema>> {
        if let PulsarSchemaType::Unsupported(schema_type) = raw_schema.schema_type() {
            bail!("expected Pulsar AVRO schema, got {schema_type}");
        }
        let schema =
            Schema::parse_str(&raw_schema.data).context("failed to parse Pulsar avro schema")?;
        let schema = Arc::new(schema);
        self.schema_cache
            .insert(raw_schema.version, Arc::clone(&schema))
            .await;
        Ok(schema)
    }

    pub async fn get_latest(&self) -> ConnectorResult<Arc<Schema>> {
        let schema = self
            .latest_schema
            .get_or_try_init(|| async {
                self.parse_and_cache_schema(self.schema_supplier.get_latest_schema().await?)
                    .await
            })
            .await?;
        Ok(Arc::clone(schema))
    }

    pub async fn get_by_version(&self, version: i64) -> ConnectorResult<Arc<Schema>> {
        if let Some(schema) = self.schema_cache.get(&version).await {
            return Ok(schema);
        }
        self.parse_and_cache_schema(self.schema_supplier.get_schema_by_version(version).await?)
            .await
    }
}
