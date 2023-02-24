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

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use apache_avro::Schema;
use moka::future::Cache;
use risingwave_common::error::ErrorCode::{InternalError, InvalidConfigValue, ProtocolError};
use risingwave_common::error::{Result, RwError};
use url::Url;

use crate::aws_utils::{default_conn_config, s3_client, AwsConfigV2};
use crate::parser::schema_registry::{Client, ConfluentSchema};
use crate::parser::util::download_from_http;

const AVRO_SCHEMA_LOCATION_S3_REGION: &str = "region";

/// Read schema from s3 bucket.
/// S3 file location format: <s3://bucket_name/file_name>
pub(super) async fn read_schema_from_s3(
    url: &Url,
    properties: &HashMap<String, String>,
) -> Result<String> {
    let bucket = url
        .domain()
        .ok_or_else(|| RwError::from(InternalError(format!("Illegal Avro schema path {}", url))))?;
    if properties.get(AVRO_SCHEMA_LOCATION_S3_REGION).is_none() {
        return Err(RwError::from(InvalidConfigValue {
            config_entry: AVRO_SCHEMA_LOCATION_S3_REGION.to_string(),
            config_value: "NONE".to_string(),
        }));
    }
    let key = url.path().replace('/', "");
    let config = AwsConfigV2::from(properties.clone());
    let sdk_config = config.load_config(None).await;
    let s3_client = s3_client(&sdk_config, Some(default_conn_config()));
    let response = s3_client
        .get_object()
        .bucket(bucket.to_string())
        .key(key)
        .send()
        .await
        .map_err(|e| RwError::from(InternalError(e.to_string())))?;
    let body_bytes = response.body.collect().await.map_err(|e| {
        RwError::from(InternalError(format!(
            "Read Avro schema file from s3 {}",
            e
        )))
    })?;
    let schema_bytes = body_bytes.into_bytes().to_vec();
    String::from_utf8(schema_bytes)
        .map_err(|e| RwError::from(InternalError(format!("Avro schema not valid utf8 {}", e))))
}

/// Read avro schema file from local file.For on-premise or testing.
pub(super) fn read_schema_from_local(path: impl AsRef<Path>) -> Result<String> {
    std::fs::read_to_string(path.as_ref()).map_err(|e| e.into())
}

/// Read avro schema file from local file.For common usage.
pub(super) async fn read_schema_from_http(location: &Url) -> Result<String> {
    let schema_bytes = download_from_http(location).await?;

    String::from_utf8(schema_bytes.into()).map_err(|e| {
        RwError::from(InternalError(format!(
            "read schema string from https failed {}",
            e
        )))
    })
}

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
        let raw_schema = self
            .confluent_client
            .get_schema_by_subject(subject_name)
            .await?;
        self.parse_and_cache_schema(raw_schema).await
    }

    // get the writer schema by id
    pub async fn get(&self, schema_id: i32) -> Result<Arc<Schema>> {
        if let Some(schema) = self.writer_schemas.get(&schema_id) {
            Ok(schema)
        } else {
            let raw_schema = self.confluent_client.get_schema_by_id(schema_id).await?;
            self.parse_and_cache_schema(raw_schema).await
        }
    }
}
