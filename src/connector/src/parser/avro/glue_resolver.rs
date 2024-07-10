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

use anyhow::Context;
use apache_avro::Schema;
use aws_sdk_glue::types::{SchemaId, SchemaVersionNumber};
use aws_sdk_glue::Client;
use moka::future::Cache;

use crate::error::ConnectorResult;

/// Fetch schemas from AWS Glue schema registry and cache them.
///
/// Background: This is mainly used for Avro **writer schema** (during schema evolution): When decoding an Avro message,
/// we must get the message's schema id, and use the *exactly same schema* to decode the message, and then
/// convert it with the reader schema. (This is also why Avro has to be used with a schema registry instead of a static schema file.)
///
/// TODO: support protobuf (not sure if it's needed)
#[derive(Debug)]
pub struct GlueSchemaCache {
    writer_schemas: Cache<uuid::Uuid, Arc<Schema>>,
    glue_client: Client,
}

impl GlueSchemaCache {
    /// Create a new `GlueSchemaCache`
    pub fn new(client: Client) -> Self {
        Self {
            writer_schemas: Cache::new(u64::MAX),
            glue_client: client,
        }
    }

    /// Gets the a specific schema by id, which is used as *writer schema*.
    pub async fn get_by_id(&self, schema_version_id: uuid::Uuid) -> ConnectorResult<Arc<Schema>> {
        if let Some(schema) = self.writer_schemas.get(&schema_version_id).await {
            return Ok(schema);
        }
        let res = self
            .glue_client
            .get_schema_version()
            .schema_version_id(schema_version_id)
            .send()
            .await
            .context("glue sdk error")?;
        let definition = res
            .schema_definition()
            .context("glue sdk response without definition")?;
        self.parse_and_cache_schema(schema_version_id, definition)
            .await
    }

    /// Gets the latest schema by arn, which is used as *reader schema*.
    pub async fn get_by_name(&self, schema_arn: &str) -> ConnectorResult<Arc<Schema>> {
        let res = self
            .glue_client
            .get_schema_version()
            .schema_id(SchemaId::builder().schema_arn(schema_arn).build())
            .schema_version_number(SchemaVersionNumber::builder().latest_version(true).build())
            .send()
            .await
            .context("glue sdk error")?;
        let schema_version_id = res
            .schema_version_id()
            .context("glue sdk response without schema version id")?
            .parse()
            .context("glue sdk response invalid schema version id")?;
        let definition = res
            .schema_definition()
            .context("glue sdk response without definition")?;
        self.parse_and_cache_schema(schema_version_id, definition)
            .await
    }

    async fn parse_and_cache_schema(
        &self,
        schema_version_id: uuid::Uuid,
        content: &str,
    ) -> ConnectorResult<Arc<Schema>> {
        let schema = Schema::parse_str(content).context("failed to parse avro schema")?;
        let schema = Arc::new(schema);
        self.writer_schemas
            .insert(schema_version_id, Arc::clone(&schema))
            .await;
        Ok(schema)
    }
}
