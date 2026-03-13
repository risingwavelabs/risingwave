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
use moka::future::Cache;

use crate::error::ConnectorResult;
use crate::schema::schema_registry::{Client, ConfluentSchema};

/// Fetch schemas from confluent schema registry and cache them.
///
/// Background: This is mainly used for Avro **writer schema** (during schema evolution): When decoding an Avro message,
/// we must get the message's schema id, and use the *exactly same schema* to decode the message, and then
/// convert it with the reader schema. (This is also why Avro has to be used with a schema registry instead of a static schema file.)
///
/// TODO: support protobuf (not sure if it's needed)
#[derive(Debug)]
pub struct ConfluentSchemaCache {
    writer_schemas: Cache<i32, Arc<Schema>>,
    confluent_client: Client,
}

impl ConfluentSchemaCache {
    async fn parse_and_cache_schema(
        &self,
        raw_schema: ConfluentSchema,
    ) -> ConnectorResult<Arc<Schema>> {
        let schema =
            Schema::parse_str(&raw_schema.content).context("failed to parse avro schema")?;
        let schema = Arc::new(schema);
        self.writer_schemas
            .insert(raw_schema.id, Arc::clone(&schema))
            .await;
        Ok(schema)
    }

    /// Create a new `ConfluentSchemaResolver`
    pub fn new(client: Client) -> Self {
        ConfluentSchemaCache {
            writer_schemas: Cache::new(u64::MAX),
            confluent_client: client,
        }
    }

    /// Gets the latest schema by subject name, which is used as *reader schema*.
    pub async fn get_by_subject(&self, subject_name: &str) -> ConnectorResult<(Arc<Schema>, i32, String)> {
        let raw_schema = self
            .confluent_client
            .get_schema_by_subject(subject_name)
            .await?;
        let id = raw_schema.id;
        let content = raw_schema.content.clone();
        let schema = self.parse_and_cache_schema(raw_schema).await?;
        Ok((schema, id, content))
    }

    /// Gets the a specific schema by id, which is used as *writer schema*.
    pub async fn get_by_id(&self, schema_id: i32) -> ConnectorResult<Arc<Schema>> {
        // TODO: use `get_with`
        if let Some(schema) = self.writer_schemas.get(&schema_id).await {
            Ok(schema)
        } else {
            let raw_schema = self.confluent_client.get_schema_by_id(schema_id).await?;
            self.parse_and_cache_schema(raw_schema).await
        }
    }
}
