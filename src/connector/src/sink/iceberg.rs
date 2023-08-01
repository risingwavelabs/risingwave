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
use std::fmt::Debug;

use anyhow::anyhow;
use async_trait::async_trait;
use icelake::Table;
use opendal::services::S3;
use risingwave_common::array::StreamChunk;
use risingwave_common::buffer::Bitmap;
use risingwave_common::catalog::Schema;
use risingwave_rpc_client::ConnectorClient;
use serde_derive::Deserialize;

use super::{
    DummySinkCommitCoordinator, Sink, SinkError, SinkWriter, SinkWriterParam,
    SINK_TYPE_APPEND_ONLY, SINK_TYPE_OPTION, SINK_TYPE_UPSERT,
};
use crate::deserialize_bool_from_string;
use crate::sink::Result;

/// This iceberg sink is WIP. When it ready, we will change this name to "iceberg".
pub const ICEBERG_SINK: &str = "iceberg_v2";

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IcebergConfig {
    #[serde(skip_serializing)]
    pub connector: String, // Must be "kafka" here.

    pub r#type: String, // accept "append-only" or "upsert"

    #[serde(default, deserialize_with = "deserialize_bool_from_string")]
    pub force_append_only: bool,

    #[serde(rename = "warehouse.path")]
    pub path: String,

    #[serde(rename = "s3.region")]
    pub region: Option<String>,

    #[serde(rename = "s3.endpoint")]
    pub endpoint: String,

    #[serde(rename = "s3.access.key")]
    pub access_key: String,

    #[serde(rename = "s3.secret.key")]
    pub secret_key: String,

    #[serde(rename = "database.name")]
    pub database_name: String,

    #[serde(rename = "table.name")]
    pub table_name: String,
}

impl IcebergConfig {
    pub fn from_hashmap(values: HashMap<String, String>) -> Result<Self> {
        let config = serde_json::from_value::<IcebergConfig>(serde_json::to_value(values).unwrap())
            .map_err(|e| SinkError::Config(anyhow!(e)))?;

        if config.r#type != SINK_TYPE_APPEND_ONLY && config.r#type != SINK_TYPE_UPSERT {
            return Err(SinkError::Config(anyhow!(
                "`{}` must be {}, or {}",
                SINK_TYPE_OPTION,
                SINK_TYPE_APPEND_ONLY,
                SINK_TYPE_UPSERT
            )));
        }
        Ok(config)
    }
}

pub struct IcebergSink {
    config: IcebergConfig,
    table: Table,
    schema: Schema,
}

impl Debug for IcebergSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IcebergSink")
            .field("config", &self.config)
            .finish()
    }
}

impl IcebergSink {
    fn parse_bucket_and_root_from_path(path: &str) -> Result<(String, String)> {
        let prefix = if path.starts_with("s3://") {
            "s3://"
        } else if path.starts_with("s3a://") {
            "s3a://"
        } else if path.starts_with("s3n://") {
            "s3n://"
        } else {
            return Err(SinkError::Config(anyhow!(
                "Invalid path prefix: {}. Only support s3://, s3a://, s3n:// now.",
                path
            )));
        };

        let path = path.trim_start_matches(prefix);
        let (bucket, root) = path.split_once('/').unwrap_or((path, ""));
        Ok((bucket.to_string(), format!("/{}", root)))
    }

    pub async fn new(config: IcebergConfig, schema: Schema) -> Result<Self> {
        let (bucket, root) = Self::parse_bucket_and_root_from_path(&config.path)?;

        let mut builder = S3::default();

        // Sink will not load config from file.
        builder.disable_config_load();

        builder
            .root(&root)
            .bucket(&bucket)
            .endpoint(&config.endpoint)
            .access_key_id(&config.access_key)
            .secret_access_key(&config.secret_key);

        if let Some(region) = &config.region {
            builder.region(region);
        }

        let op = opendal::Operator::new(builder)
            .map_err(|err| SinkError::Config(anyhow!("{}", err)))?
            .finish();

        let table = Table::open_with_op(op)
            .await
            .map_err(|err| SinkError::Iceberg(format!("Create table fail: {}", err)))?;

        Ok(Self {
            config,
            table,
            schema,
        })
    }
}

#[async_trait::async_trait]
impl Sink for IcebergSink {
    /// TODO: Can't support commit coordinator now.
    type Coordinator = DummySinkCommitCoordinator;
    type Writer = IcebergWriter;

    async fn validate(&self, _client: Option<ConnectorClient>) -> Result<()> {
        // We used icelake to write data into storage directly, don't need to validate.
        Ok(())
    }

    async fn new_writer(&self, _writer_param: SinkWriterParam) -> Result<Self::Writer> {
        Ok(IcebergWriter {})
    }
}

/// TODO(ZENOTME): Just a placeholder, we will implement it later.(#10642)
pub struct IcebergWriter {}

#[async_trait]
impl SinkWriter for IcebergWriter {
    /// Begin a new epoch
    async fn begin_epoch(&mut self, _epoch: u64) -> Result<()> {
        todo!()
    }

    /// Write a stream chunk to sink
    async fn write_batch(&mut self, _chunk: StreamChunk) -> Result<()> {
        todo!()
    }

    /// Receive a barrier and mark the end of current epoch. When `is_checkpoint` is true, the sink
    /// writer should commit the current epoch.
    async fn barrier(&mut self, _is_checkpoint: bool) -> Result<()> {
        todo!()
    }

    /// Clean up
    async fn abort(&mut self) -> Result<()> {
        todo!()
    }

    /// Update the vnode bitmap of current sink writer
    async fn update_vnode_bitmap(&mut self, _vnode_bitmap: Bitmap) -> Result<()> {
        todo!()
    }
}
