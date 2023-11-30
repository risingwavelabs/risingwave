// Copyright 2023 RisingWave Labs
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

use apache_avro::Schema;
use moka::future::Cache;
use risingwave_common::error::ErrorCode::ProtocolError;
use risingwave_common::error::{Result, RwError};

use crate::schema::schema_registry::{Client, ConfluentSchema};

#[derive(Debug)]
pub struct ConfluentSchemaResolver {
    writer_schemas: Cache<i32, Arc<Schema>>,
    confluent_client: Client,
}

impl ConfluentSchemaResolver {
    async fn parse_and_cache_schema(&self, raw_schema: ConfluentSchema) -> Result<Arc<Schema>> {
        let schema = Schema::parse_str(&raw_schema.content)
            .map_err(|e| RwError::from(ProtocolError(format!("Avro schema parse error {}", e))))?;
        let schema = Arc::new(schema);
        self.writer_schemas
            .insert(raw_schema.id, Arc::clone(&schema))
            .await;
        Ok(schema)
    }

    /// Create a new `ConfluentSchemaResolver`
    pub fn new(client: Client) -> Self {
        ConfluentSchemaResolver {
            writer_schemas: Cache::new(u64::MAX),
            confluent_client: client,
        }
    }

    pub async fn get_by_subject_name(&self, subject_name: &str) -> Result<Arc<Schema>> {
        let raw_schema = self.get_raw_schema_by_subject_name(subject_name).await?;
        self.parse_and_cache_schema(raw_schema).await
    }

    pub async fn get_raw_schema_by_subject_name(
        &self,
        subject_name: &str,
    ) -> Result<ConfluentSchema> {
        self.confluent_client
            .get_schema_by_subject(subject_name)
            .await
    }

    // get the writer schema by id
    pub async fn get(&self, schema_id: i32) -> Result<Arc<Schema>> {
        // TODO: use `get_with`
        if let Some(schema) = self.writer_schemas.get(&schema_id).await {
            Ok(schema)
        } else {
            let raw_schema = self.confluent_client.get_schema_by_id(schema_id).await?;
            self.parse_and_cache_schema(raw_schema).await
        }
    }
}
