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

use core::num::NonZeroU64;
use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::{Context, anyhow};
use async_trait::async_trait;
use lance::dataset::fragment::FileFragment;
use lance::dataset::transaction::Operation;
use lance::Dataset;
use lance_table::format::Fragment;
use lancedb::Connection as LanceDbConnection;
use lancedb::connection::ConnectBuilder;
use risingwave_common::array::StreamChunk;
use risingwave_common::array::arrow::LanceDbConvert;
use risingwave_common::catalog::Schema;
use risingwave_pb::connector_service::SinkMetadata;
use risingwave_pb::connector_service::sink_metadata::Metadata::Serialized;
use risingwave_pb::connector_service::sink_metadata::SerializedMetadata;
use serde::Deserialize;
use serde_with::{DisplayFromStr, serde_as};
use tokio::sync::mpsc::UnboundedSender;
use with_options::WithOptions;

use crate::connector_common::IcebergSinkCompactionUpdate;
use crate::enforce_secret::EnforceSecret;
use crate::sink::coordinate::CoordinatedLogSinker;
use crate::sink::decouple_checkpoint_log_sink::default_commit_checkpoint_interval;
use crate::sink::writer::SinkWriter;
use crate::sink::{
    Result, SINK_TYPE_APPEND_ONLY, SINK_USER_FORCE_APPEND_ONLY_OPTION,
    SinglePhaseCommitCoordinator, Sink, SinkCommitCoordinator, SinkError, SinkParam,
    SinkWriterParam,
};

pub const LANCEDB_SINK: &str = "lancedb";

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

#[serde_as]
#[derive(Deserialize, Debug, Clone, WithOptions)]
pub struct LanceDbCommon {
    /// URI of the LanceDB database (e.g., "/tmp/lancedb", "s3://bucket/path/db")
    #[serde(rename = "lancedb.uri")]
    pub uri: String,

    /// Table name in the LanceDB database
    #[serde(rename = "lancedb.table")]
    pub table: String,

    /// Commit every n(>0) checkpoints, default is 10.
    #[serde(default = "default_commit_checkpoint_interval")]
    #[serde_as(as = "DisplayFromStr")]
    #[with_option(allow_alter_on_fly)]
    pub commit_checkpoint_interval: u64,
}

impl LanceDbCommon {
    pub async fn create_connection(&self) -> Result<LanceDbConnection> {
        let conn = ConnectBuilder::new(&self.uri)
            .execute()
            .await
            .context("failed to connect to LanceDB")
            .map_err(SinkError::LanceDb)?;
        Ok(conn)
    }

    /// Get the lance Dataset URI for the table.
    /// LanceDB stores each table as a lance dataset at `{db_uri}/{table_name}.lance`.
    pub fn dataset_uri(&self) -> String {
        format!("{}/{}.lance", self.uri.trim_end_matches('/'), self.table)
    }
}

#[serde_as]
#[derive(Clone, Debug, Deserialize, WithOptions)]
pub struct LanceDbConfig {
    #[serde(flatten)]
    pub common: LanceDbCommon,

    pub r#type: String,
}

impl LanceDbConfig {
    pub fn from_btreemap(properties: BTreeMap<String, String>) -> Result<Self> {
        let config = serde_json::from_value::<LanceDbConfig>(
            serde_json::to_value(properties).map_err(|e| SinkError::LanceDb(e.into()))?,
        )
        .map_err(|e| SinkError::Config(anyhow!(e)))?;
        Ok(config)
    }
}

// ---------------------------------------------------------------------------
// Sink
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub struct LanceDbSink {
    pub config: LanceDbConfig,
    param: SinkParam,
}

impl EnforceSecret for LanceDbSink {
    fn enforce_secret<'a>(
        _prop_iter: impl Iterator<Item = &'a str>,
    ) -> crate::error::ConnectorResult<()> {
        // LanceDB currently has no secret properties.
        Ok(())
    }
}

impl LanceDbSink {
    pub fn new(config: LanceDbConfig, param: SinkParam) -> Result<Self> {
        Ok(Self { config, param })
    }
}

impl Sink for LanceDbSink {
    type LogSinker = CoordinatedLogSinker<LanceDbSinkWriter>;

    const SINK_NAME: &'static str = LANCEDB_SINK;

    async fn new_log_sinker(&self, writer_param: SinkWriterParam) -> Result<Self::LogSinker> {
        let inner =
            LanceDbSinkWriter::new(self.config.clone(), self.param.schema().clone()).await?;

        let commit_checkpoint_interval =
            NonZeroU64::new(self.config.common.commit_checkpoint_interval).expect(
                "commit_checkpoint_interval should be greater than 0, and it should be checked in config validation",
            );

        let writer = CoordinatedLogSinker::new(
            &writer_param,
            self.param.clone(),
            inner,
            commit_checkpoint_interval,
        )
        .await?;

        Ok(writer)
    }

    fn validate_alter_config(config: &BTreeMap<String, String>) -> Result<()> {
        LanceDbConfig::from_btreemap(config.clone())?;
        Ok(())
    }

    async fn validate(&self) -> Result<()> {
        // Only append-only is supported
        if self.config.r#type != SINK_TYPE_APPEND_ONLY
            && self.config.r#type != SINK_USER_FORCE_APPEND_ONLY_OPTION
        {
            return Err(SinkError::Config(anyhow!(
                "only append-only LanceDB sink is supported",
            )));
        }

        if self.config.common.commit_checkpoint_interval == 0 {
            return Err(SinkError::Config(anyhow!(
                "`commit_checkpoint_interval` must be greater than 0"
            )));
        }

        // Validate connection
        let conn = self.config.common.create_connection().await?;

        // Validate table exists and schema is compatible
        let table = conn
            .open_table(&self.config.common.table)
            .execute()
            .await
            .context("failed to open LanceDB table")
            .map_err(SinkError::LanceDb)?;

        // Get the Lance table schema (Arrow schema)
        let lance_schema = table
            .schema()
            .await
            .context("failed to get LanceDB table schema")
            .map_err(SinkError::LanceDb)?;

        // Convert RW schema to arrow schema and compare
        let rw_schema = self.param.schema();
        let rw_arrow_schema = LanceDbConvert
            .rw_schema_to_arrow_schema(&rw_schema)
            .map_err(|e| SinkError::LanceDb(anyhow!(e)))?;

        // Check field count matches
        if rw_arrow_schema.fields().len() != lance_schema.fields().len() {
            return Err(SinkError::LanceDb(anyhow!(
                "Columns mismatch. RisingWave schema has {} fields, LanceDB table has {} fields",
                rw_arrow_schema.fields().len(),
                lance_schema.fields().len()
            )));
        }

        // Check each field name exists and type is compatible
        for rw_field in rw_arrow_schema.fields() {
            match lance_schema.field_with_name(rw_field.name()) {
                Ok(lance_field) => {
                    if rw_field.data_type() != lance_field.data_type() {
                        return Err(SinkError::LanceDb(anyhow!(
                            "column '{}' type mismatch: LanceDB type is {:?}, RisingWave type is {:?}",
                            rw_field.name(),
                            lance_field.data_type(),
                            rw_field.data_type()
                        )));
                    }
                }
                Err(_) => {
                    return Err(SinkError::LanceDb(anyhow!(
                        "column '{}' not found in LanceDB table",
                        rw_field.name()
                    )));
                }
            }
        }

        Ok(())
    }

    fn is_coordinated_sink(&self) -> bool {
        true
    }

    async fn new_coordinator(
        &self,
        _iceberg_compact_stat_sender: Option<UnboundedSender<IcebergSinkCompactionUpdate>>,
    ) -> Result<SinkCommitCoordinator> {
        let committer = LanceDbSinkCommitter::new(self.config.clone()).await?;
        Ok(SinkCommitCoordinator::SinglePhase(Box::new(committer)))
    }
}

impl TryFrom<SinkParam> for LanceDbSink {
    type Error = SinkError;

    fn try_from(param: SinkParam) -> std::result::Result<Self, Self::Error> {
        let config = LanceDbConfig::from_btreemap(param.properties.clone())?;
        LanceDbSink::new(config, param)
    }
}

// ---------------------------------------------------------------------------
// Writer
// ---------------------------------------------------------------------------

// Re-export arrow types from the LanceDb arrow module so they're used consistently.
use risingwave_common::array::arrow::arrow_array_lancedb as arrow_array;
use risingwave_common::array::arrow::arrow_schema_lancedb as arrow_schema;

/// The writer writes data files directly to the Lance dataset storage using the
/// low-level `FileFragment::create_fragments()` API. On checkpoint, it returns
/// lightweight `Fragment` metadata (file paths + row counts) instead of the
/// actual data payload. The coordinator then commits these fragments atomically
/// via `Dataset::commit()`.
///
/// This follows the same pattern as the Iceberg sink, where writers handle I/O
/// and the coordinator only performs a metadata-only commit.
pub struct LanceDbSinkWriter {
    pub config: LanceDbConfig,
    #[expect(dead_code)]
    schema: Schema,
    arrow_schema: Arc<arrow_schema::Schema>,
    /// Dataset URI for the target Lance table (e.g., "/tmp/lancedb/my_table.lance")
    dataset_uri: String,
    /// Buffered record batches for the current epoch
    batches: Vec<arrow_array::RecordBatch>,
}

impl LanceDbSinkWriter {
    pub async fn new(config: LanceDbConfig, schema: Schema) -> Result<Self> {
        let arrow_schema = LanceDbConvert
            .rw_schema_to_arrow_schema(&schema)
            .map_err(|e| SinkError::LanceDb(anyhow!(e)))?;

        let dataset_uri = config.common.dataset_uri();

        Ok(Self {
            config,
            schema,
            arrow_schema: Arc::new(arrow_schema),
            dataset_uri,
            batches: Vec::new(),
        })
    }

    /// Flush buffered batches by writing them as lance data files directly to storage.
    /// Returns a list of `Fragment` metadata describing the written files.
    async fn flush_to_fragments(&mut self) -> Result<Vec<Fragment>> {
        if self.batches.is_empty() {
            return Ok(Vec::new());
        }

        let batches = std::mem::take(&mut self.batches);
        let schema = batches[0].schema();
        let reader =
            arrow_array::RecordBatchIterator::new(batches.into_iter().map(Ok), schema);

        let fragments = FileFragment::create_fragments(
            &self.dataset_uri,
            reader,
            None, // default WriteParams
        )
        .await
        .context("failed to write lance data files")
        .map_err(SinkError::LanceDb)?;

        Ok(fragments)
    }
}

#[async_trait]
impl SinkWriter for LanceDbSinkWriter {
    type CommitMetadata = Option<SinkMetadata>;

    async fn write_batch(&mut self, chunk: StreamChunk) -> Result<()> {
        let record_batch = LanceDbConvert
            .to_record_batch(self.arrow_schema.clone(), &chunk)
            .context("failed to convert DataChunk to RecordBatch for LanceDB")
            .map_err(SinkError::LanceDb)?;
        self.batches.push(record_batch);
        Ok(())
    }

    async fn begin_epoch(&mut self, _epoch: u64) -> Result<()> {
        Ok(())
    }

    async fn abort(&mut self) -> Result<()> {
        self.batches.clear();
        Ok(())
    }

    async fn barrier(&mut self, is_checkpoint: bool) -> Result<Option<SinkMetadata>> {
        if !is_checkpoint {
            return Ok(None);
        }

        if self.batches.is_empty() {
            return Ok(None);
        }

        // Write data files directly to storage and get fragment metadata.
        let fragments = self.flush_to_fragments().await?;

        if fragments.is_empty() {
            return Ok(None);
        }

        // Serialize fragment metadata as JSON — this is lightweight (file paths + row counts).
        let metadata = serde_json::to_vec(&fragments)
            .context("failed to serialize fragment metadata")
            .map_err(SinkError::LanceDb)?;

        Ok(Some(SinkMetadata {
            metadata: Some(Serialized(SerializedMetadata { metadata })),
        }))
    }
}

// ---------------------------------------------------------------------------
// Committer (Coordinator)
// ---------------------------------------------------------------------------

/// The coordinator collects lightweight `Fragment` metadata from all writers
/// and commits them atomically via `Dataset::commit(Operation::Append { fragments })`.
/// No data payload flows through the coordinator — only file-level metadata.
pub struct LanceDbSinkCommitter {
    config: LanceDbConfig,
    conn: LanceDbConnection,
}

impl LanceDbSinkCommitter {
    pub async fn new(config: LanceDbConfig) -> Result<Self> {
        let conn = config.common.create_connection().await?;
        Ok(Self { config, conn })
    }
}

#[async_trait::async_trait]
impl SinglePhaseCommitCoordinator for LanceDbSinkCommitter {
    async fn init(&mut self) -> Result<()> {
        tracing::info!(
            "LanceDB commit coordinator initialized for table '{}'",
            self.config.common.table
        );
        Ok(())
    }

    async fn commit_data(&mut self, epoch: u64, metadata: Vec<SinkMetadata>) -> Result<()> {
        tracing::debug!("Starting LanceDB commit in epoch {epoch}.");

        // Collect all Fragment metadata from all writers.
        let mut all_fragments: Vec<Fragment> = Vec::new();
        for meta in &metadata {
            if let Some(Serialized(s)) = &meta.metadata {
                let fragments: Vec<Fragment> = serde_json::from_slice(&s.metadata)
                    .context("failed to deserialize fragment metadata")
                    .map_err(SinkError::LanceDb)?;
                all_fragments.extend(fragments);
            }
        }

        if all_fragments.is_empty() {
            tracing::debug!("No fragments to commit in epoch {epoch}, skipping.");
            return Ok(());
        }

        // Open the table to get the underlying lance Dataset.
        let table = self
            .conn
            .open_table(&self.config.common.table)
            .execute()
            .await
            .context("failed to open LanceDB table for commit")
            .map_err(SinkError::LanceDb)?;

        let dataset_wrapper = table.dataset().ok_or_else(|| {
            SinkError::LanceDb(anyhow!(
                "failed to get underlying lance Dataset (table may be remote)"
            ))
        })?;

        let dataset_guard = dataset_wrapper
            .get()
            .await
            .map_err(|e| SinkError::LanceDb(anyhow!(e)))?;

        let read_version = dataset_guard.version().version;
        let session = dataset_guard.session();

        // Drop the read guard before commit to avoid holding the lock.
        drop(dataset_guard);

        // Commit fragments via low-level Dataset::commit with Operation::Append.
        // This is a metadata-only operation — the data files were already written by workers.
        let operation = Operation::Append {
            fragments: all_fragments,
        };

        let dataset_uri = self.config.common.dataset_uri();

        let new_dataset = Dataset::commit(
            &dataset_uri as &str,
            operation,
            Some(read_version),
            None, // store_params
            None, // commit_handler
            session,
            false, // enable_v2_manifest_paths
        )
        .await
        .context("failed to commit fragments to lance dataset")
        .map_err(SinkError::LanceDb)?;

        // Update the lancedb Table's internal dataset to the new version.
        dataset_wrapper.set_latest(new_dataset).await;

        tracing::debug!(
            "Succeeded to commit fragments to LanceDB table in epoch {epoch}."
        );
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Error conversion
// ---------------------------------------------------------------------------

impl From<lancedb::Error> for SinkError {
    fn from(value: lancedb::Error) -> Self {
        SinkError::LanceDb(anyhow!(value))
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(all(test, not(madsim)))]
mod tests {
    use risingwave_common::array::{Array, I32Array, Op, StreamChunk, Utf8Array};
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::types::DataType;

    use super::*;
    use crate::sink::SinglePhaseCommitCoordinator;
    use crate::sink::writer::SinkWriter;

    #[tokio::test]
    async fn test_lancedb_sink_roundtrip() {
        // 1. Create a temp directory for LanceDB
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_str().unwrap();

        // 2. Create a LanceDB table with a schema
        let conn = lancedb::connect(uri).execute().await.unwrap();

        // Create initial data to define the table schema
        let arrow_schema = Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("id", arrow_schema::DataType::Int32, false),
            arrow_schema::Field::new("name", arrow_schema::DataType::Utf8, false),
        ]));
        let id_array = arrow_array::Int32Array::from(vec![0i32]);
        let name_array = arrow_array::StringArray::from(vec!["init"]);
        let init_batch = arrow_array::RecordBatch::try_new(
            arrow_schema.clone(),
            vec![Arc::new(id_array), Arc::new(name_array)],
        )
        .unwrap();
        let reader =
            arrow_array::RecordBatchIterator::new(vec![Ok(init_batch)], arrow_schema.clone());
        conn.create_table("test_table", reader)
            .execute()
            .await
            .unwrap();

        // 3. Create a LanceDB sink writer
        let properties: BTreeMap<String, String> = [
            ("connector".to_owned(), "lancedb".to_owned()),
            ("type".to_owned(), "append-only".to_owned()),
            ("lancedb.uri".to_owned(), uri.to_owned()),
            ("lancedb.table".to_owned(), "test_table".to_owned()),
        ]
        .into();

        let schema = Schema::new(vec![
            Field {
                data_type: DataType::Int32,
                name: "id".into(),
            },
            Field {
                data_type: DataType::Varchar,
                name: "name".into(),
            },
        ]);

        let config = LanceDbConfig::from_btreemap(properties).unwrap();
        let mut writer = LanceDbSinkWriter::new(config.clone(), schema).await.unwrap();

        // 4. Write a chunk
        let chunk = StreamChunk::new(
            vec![Op::Insert, Op::Insert, Op::Insert],
            vec![
                I32Array::from_iter(vec![1, 2, 3]).into_ref(),
                Utf8Array::from_iter(vec!["Alice", "Bob", "Clare"]).into_ref(),
            ],
        );
        writer.write_batch(chunk).await.unwrap();

        // 5. Barrier → get SinkMetadata (lightweight fragment metadata, not data payload)
        let metadata = writer.barrier(true).await.unwrap().unwrap();

        // Verify the metadata is lightweight JSON (not Arrow IPC payload)
        if let Some(Serialized(s)) = &metadata.metadata {
            let fragments: Vec<Fragment> = serde_json::from_slice(&s.metadata).unwrap();
            assert!(!fragments.is_empty(), "should have at least one fragment");
            // Fragment metadata should be much smaller than actual data
            assert!(
                s.metadata.len() < 4096,
                "fragment metadata should be lightweight, got {} bytes",
                s.metadata.len()
            );
        } else {
            panic!("expected serialized metadata");
        }

        // 6. Commit via coordinator (metadata-only commit)
        let mut committer = LanceDbSinkCommitter::new(config).await.unwrap();
        committer.init().await.unwrap();
        committer.commit_data(1, vec![metadata]).await.unwrap();

        // 7. Verify data was written by reading back
        let table = conn.open_table("test_table").execute().await.unwrap();
        let count = table.count_rows(None).await.unwrap();
        // 1 initial row + 3 new rows = 4
        assert_eq!(count, 4);
    }
}
