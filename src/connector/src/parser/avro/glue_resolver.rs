// Copyright 2025 RisingWave Labs
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
use std::sync::Arc;

use anyhow::Context;
use apache_avro::Schema;
use aws_sdk_glue::Client;
use aws_sdk_glue::types::{SchemaId, SchemaVersionNumber};
use moka::future::Cache;

use crate::connector_common::AwsAuthProps;
use crate::error::ConnectorResult;

/// Fetch schemas from AWS Glue schema registry and cache them.
///
/// Background: This is mainly used for Avro **writer schema** (during schema evolution): When decoding an Avro message,
/// we must get the message's schema id, and use the *exactly same schema* to decode the message, and then
/// convert it with the reader schema. (This is also why Avro has to be used with a schema registry instead of a static schema file.)
///
/// TODO: support protobuf (not sure if it's needed)
pub trait GlueSchemaCache {
    /// Gets the a specific schema by id, which is used as *writer schema*.
    async fn get_by_id(&self, schema_version_id: uuid::Uuid) -> ConnectorResult<Arc<Schema>>;
    /// Gets the latest schema by arn, which is used as *reader schema*.
    async fn get_by_name(&self, schema_arn: &str) -> ConnectorResult<Arc<Schema>>;
}

#[derive(Debug)]
pub enum GlueSchemaCacheImpl {
    Real(RealGlueSchemaCache),
    Mock(MockGlueSchemaCache),
}

impl GlueSchemaCacheImpl {
    pub async fn new(
        aws_auth_props: &AwsAuthProps,
        mock_config: Option<&str>,
    ) -> ConnectorResult<Self> {
        if let Some(mock_config) = mock_config {
            return Ok(Self::Mock(MockGlueSchemaCache::new(mock_config)));
        }
        Ok(Self::Real(RealGlueSchemaCache::new(aws_auth_props).await?))
    }
}

impl GlueSchemaCache for GlueSchemaCacheImpl {
    async fn get_by_id(&self, schema_version_id: uuid::Uuid) -> ConnectorResult<Arc<Schema>> {
        match self {
            Self::Real(inner) => inner.get_by_id(schema_version_id).await,
            Self::Mock(inner) => inner.get_by_id(schema_version_id).await,
        }
    }

    async fn get_by_name(&self, schema_arn: &str) -> ConnectorResult<Arc<Schema>> {
        match self {
            Self::Real(inner) => inner.get_by_name(schema_arn).await,
            Self::Mock(inner) => inner.get_by_name(schema_arn).await,
        }
    }
}

#[derive(Debug)]
pub struct RealGlueSchemaCache {
    writer_schemas: Cache<uuid::Uuid, Arc<Schema>>,
    glue_client: Client,
}

impl RealGlueSchemaCache {
    /// Create a new `GlueSchemaCache`
    pub async fn new(aws_auth_props: &AwsAuthProps) -> ConnectorResult<Self> {
        let client = Client::new(&aws_auth_props.build_config().await?);
        Ok(Self {
            writer_schemas: Cache::new(u64::MAX),
            glue_client: client,
        })
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

impl GlueSchemaCache for RealGlueSchemaCache {
    /// Gets the a specific schema by id, which is used as *writer schema*.
    async fn get_by_id(&self, schema_version_id: uuid::Uuid) -> ConnectorResult<Arc<Schema>> {
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
    async fn get_by_name(&self, schema_arn: &str) -> ConnectorResult<Arc<Schema>> {
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
}

#[derive(Debug)]
pub struct MockGlueSchemaCache {
    by_id: HashMap<uuid::Uuid, Arc<Schema>>,
    arn_to_latest_id: HashMap<String, uuid::Uuid>,
}

impl MockGlueSchemaCache {
    pub fn new(mock_config: &str) -> Self {
        // The `mock_config` accepted is a JSON that looks like:
        // {
        //   "by_id": {
        //     "4dc80ccf-2d0c-4846-9325-7e1c9e928121": {
        //       "type": "record",
        //       "name": "MyEvent",
        //       "fields": [...]
        //     },
        //     "3df022f4-b16d-4afe-bdf7-cf4baf8d01d3": {
        //       ...
        //     }
        //   },
        //   "arn_to_latest_id": {
        //     "arn:aws:glue:ap-southeast-1:123456123456:schema/default-registry/MyEvent": "3df022f4-b16d-4afe-bdf7-cf4baf8d01d3"
        //   }
        // }
        //
        // The format is not public and we can make breaking changes to it.
        // Current format only supports avsc.
        let parsed: serde_json::Value =
            serde_json::from_str(mock_config).expect("mock config shall be valid json");
        let by_id = parsed
            .get("by_id")
            .unwrap()
            .as_object()
            .unwrap()
            .iter()
            .map(|(schema_version_id, schema)| {
                let schema_version_id = schema_version_id.parse().unwrap();
                let schema = Schema::parse(schema).unwrap();
                (schema_version_id, Arc::new(schema))
            })
            .collect();
        let arn_to_latest_id = parsed
            .get("arn_to_latest_id")
            .unwrap()
            .as_object()
            .unwrap()
            .iter()
            .map(|(arn, latest_id)| (arn.clone(), latest_id.as_str().unwrap().parse().unwrap()))
            .collect();
        Self {
            by_id,
            arn_to_latest_id,
        }
    }
}

impl GlueSchemaCache for MockGlueSchemaCache {
    async fn get_by_id(&self, schema_version_id: uuid::Uuid) -> ConnectorResult<Arc<Schema>> {
        Ok(self
            .by_id
            .get(&schema_version_id)
            .context("schema version id not found in mock registry")?
            .clone())
    }

    async fn get_by_name(&self, schema_arn: &str) -> ConnectorResult<Arc<Schema>> {
        let schema_version_id = self
            .arn_to_latest_id
            .get(schema_arn)
            .context("schema arn not found in mock registry")?;
        self.get_by_id(*schema_version_id).await
    }
}
