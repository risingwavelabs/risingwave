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
use arrow_array::RecordBatch;
use async_trait::async_trait;
use icelake::transaction::Transaction;
use icelake::Table;
use itertools::Itertools;
use opendal::services::S3;
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::buffer::Bitmap;
use risingwave_common::catalog::Schema;
use risingwave_rpc_client::ConnectorClient;
use serde_derive::Deserialize;
use url::Url;

use super::{
    DummySinkCommitCoordinator, Sink, SinkError, SinkWriter, SinkWriterParam,
    SINK_TYPE_APPEND_ONLY, SINK_TYPE_OPTION, SINK_TYPE_UPSERT,
};
use crate::deserialize_bool_from_string;
use crate::sink::remote::{CoordinatedRemoteSink, RemoteConfig};
use crate::sink::Result;

/// This iceberg sink is WIP. When it ready, we will change this name to "iceberg".
pub const ICEBERG_SINK: &str = "iceberg_v2";
pub const REMOTE_ICEBERG_SINK: &str = "iceberg";

pub type RemoteIcebergSink = CoordinatedRemoteSink;
pub type RemoteIcebergConfig = RemoteConfig;

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
    schema: Schema,
    table_root: String,
    bucket_name: String,
}

impl Debug for IcebergSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IcebergSink")
            .field("config", &self.config)
            .finish()
    }
}

impl IcebergSink {
    async fn create_table(&self) -> Result<Table> {
        let mut builder = S3::default();

        // Sink will not load config from file.
        builder.disable_config_load();

        builder
            .root(&self.table_root)
            .bucket(&self.bucket_name)
            .endpoint(&self.config.endpoint)
            .access_key_id(&self.config.access_key)
            .secret_access_key(&self.config.secret_key);

        if let Some(region) = &self.config.region {
            builder.region(region);
        }

        let op = opendal::Operator::new(builder)
            .map_err(|err| SinkError::Config(anyhow!("{}", err)))?
            .finish();

        let table = Table::open_with_op(op)
            .await
            .map_err(|err| SinkError::Iceberg(format!("Create table fail: {}", err)))?;

        let iceberg_schema = table
            .current_table_metadata()
            .current_schema()
            .map_err(|err| SinkError::Iceberg(err.to_string()))?
            .clone()
            .try_into()
            .map_err(|err: icelake::Error| SinkError::Iceberg(err.to_string()))?;
        if !self.schema.same_as_arrow_schema(&iceberg_schema) {
            return Err(SinkError::Iceberg(format!(
                "Schema not match, expect: {:?}, actual: {:?}",
                self.schema, iceberg_schema
            )));
        }

        Ok(table)
    }

    /// Parse bucket name and table root path.
    ///
    /// return (bucket name, table root path)
    fn parse_bucket_and_root_from_path(config: &IcebergConfig) -> Result<(String, String)> {
        let url = Url::parse(&config.path).map_err(|err| {
            SinkError::Config(anyhow!(
                "Fail to parse Invalid path: {}, caused by: {}",
                &config.path,
                err
            ))
        })?;

        let scheme = url.scheme();
        if scheme != "s3a" && scheme != "s3" && scheme != "s3n" {
            return Err(SinkError::Config(anyhow!(
                "Invalid path: {}, only support s3a,s3,s3n prefix",
                &config.path
            )));
        }

        let bucket = url
            .host_str()
            .ok_or_else(|| SinkError::Config(anyhow!("Invalid path: {}", &config.path)))?;
        let root = url.path();

        let table_root_path = if root.is_empty() {
            format!("/{}/{}", config.database_name, config.table_name)
        } else {
            format!("{}/{}/{}", root, config.database_name, config.table_name)
        };

        Ok((bucket.to_string(), table_root_path))
    }

    pub fn new(config: IcebergConfig, schema: Schema) -> Result<Self> {
        let (bucket_name, table_root) = Self::parse_bucket_and_root_from_path(&config)?;
        // TODO(ZENOTME): Only support append-only mode now.
        if !config.force_append_only {
            return Err(SinkError::Iceberg(
                "Iceberg sink only support append-only mode now.".to_string(),
            ));
        }

        Ok(Self {
            config,
            schema,
            table_root,
            bucket_name,
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
        let table = self.create_table().await?;

        Ok(IcebergWriter {
            is_append_only: self.config.force_append_only,
            writer: table
                .task_writer()
                .await
                .map_err(|err| SinkError::Iceberg(err.to_string()))?,
            table,
        })
    }
}

/// TODO(ZENOTME): Just a placeholder, we will implement it later.(#10642)
pub struct IcebergWriter {
    is_append_only: bool,
    table: Table,
    writer: icelake::io::task_writer::TaskWriter,
}

impl IcebergWriter {
    async fn append_only_write(&mut self, chunk: StreamChunk) -> Result<()> {
        let (mut chunk, ops) = chunk.into_parts();

        let filters =
            Bitmap::from_bool_slice(&ops.iter().map(|op| *op == Op::Insert).collect_vec());
        let filters = if let Some(ori_vis) = chunk.visibility() {
            ori_vis & &filters
        } else {
            filters
        };

        chunk.set_visibility(filters);
        let chunk = RecordBatch::try_from(&chunk.compact())
            .map_err(|err| SinkError::Iceberg(err.to_string()))?;

        self.writer.write(&chunk).await.map_err(|err| {
            SinkError::Iceberg(format!("Write chunk fail: {}, chunk: {:?}", err, chunk))
        })?;

        Ok(())
    }
}

#[async_trait]
impl SinkWriter for IcebergWriter {
    /// Begin a new epoch
    async fn begin_epoch(&mut self, _epoch: u64) -> Result<()> {
        // Just skip it.
        Ok(())
    }

    /// Write a stream chunk to sink
    async fn write_batch(&mut self, chunk: StreamChunk) -> Result<()> {
        if self.is_append_only {
            self.append_only_write(chunk).await
        } else {
            return Err(SinkError::Iceberg(
                "Iceberg sink only support append-only mode now.".to_string(),
            ));
        }
    }

    /// Receive a barrier and mark the end of current epoch. When `is_checkpoint` is true, the sink
    /// writer should commit the current epoch.
    async fn barrier(&mut self, is_checkpoint: bool) -> Result<()> {
        // Skip it if not checkpoint
        if !is_checkpoint {
            return Ok(());
        }

        let old_writer = std::mem::replace(
            &mut self.writer,
            self.table
                .task_writer()
                .await
                .map_err(|err| SinkError::Iceberg(err.to_string()))?,
        );

        let mut tx = Transaction::new(&mut self.table);

        let data_files = old_writer
            .close()
            .await
            .map_err(|err| SinkError::Iceberg(format!("Close writer fail: {}", err)))?;
        if data_files.is_empty() {
            return Ok(());
        }

        tx.append_file(data_files.into_iter());
        tx.commit()
            .await
            .map_err(|err| SinkError::Iceberg(err.to_string()))?;

        Ok(())
    }

    /// Clean up
    async fn abort(&mut self) -> Result<()> {
        // TODO: abort should clean up all the data written in this epoch.
        Ok(())
    }

    /// Update the vnode bitmap of current sink writer
    async fn update_vnode_bitmap(&mut self, _vnode_bitmap: Bitmap) -> Result<()> {
        // Just skip it.
        Ok(())
    }
}
