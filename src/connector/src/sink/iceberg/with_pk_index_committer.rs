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

//! V3 Iceberg sink committer implementing `TwoPhaseCommitCoordinator`.
//!
//! Replaces the meta-side `IcebergCommitHandler` for V3 sinks by running the same
//! commit logic inside the `CoordinatorWorker` on the compute node.

use std::sync::Arc;

use anyhow::{Context, anyhow};
use async_trait::async_trait;
use iceberg::Catalog;
use iceberg::spec::{MAIN_BRANCH, SerializedDataFile};
use iceberg::table::Table;
use iceberg::transaction::FastAppendAction;
use risingwave_pb::connector_service::SinkMetadata;
use risingwave_pb::connector_service::coordinate_request::CoordinationRole;
use risingwave_pb::connector_service::sink_metadata::Metadata::Serialized;
use risingwave_pb::stream_plan::PbSinkSchemaChange;

use super::common::{
    IcebergConfig, IcebergV3CommitPayload, fast_append_with_retry, is_snapshot_committed,
};
use crate::sink::catalog::SinkId;
use crate::sink::{Result, SinkError, TwoPhaseCommitCoordinator};

/// Pre-commit data serialized between `pre_commit` and `commit_data`.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct V3PreCommitData {
    snapshot_id: i64,
    schema_id: Option<i32>,
    partition_spec_id: Option<i32>,
    /// Serialized `DataFile` JSON values from `WriterExecutors`.
    data_files: Vec<serde_json::Value>,
    /// Serialized `DataFile` JSON values for DV Puffin files from `DvMergerExecutors`.
    dv_data_files: Vec<serde_json::Value>,
}

/// `TwoPhaseCommitCoordinator` for Iceberg V3 sinks.
///
/// This coordinator receives `IcebergV3CommitPayload` from both `WriterExecutors`
/// (data files) and `DvMergerExecutors` (DV files), merges them, and commits
/// a single Iceberg snapshot using `fast_append_with_retry`.
pub struct IcebergWithPkIndexCommitter {
    catalog: Arc<dyn Catalog>,
    table: Table,
    config: IcebergConfig,
    sink_id: SinkId,
    commit_retry_num: u32,
}

impl IcebergWithPkIndexCommitter {
    pub fn new(
        catalog: Arc<dyn Catalog>,
        table: Table,
        config: IcebergConfig,
        sink_id: SinkId,
        commit_retry_num: u32,
    ) -> Self {
        Self {
            catalog,
            table,
            config,
            sink_id,
            commit_retry_num,
        }
    }
}

#[async_trait]
impl TwoPhaseCommitCoordinator for IcebergWithPkIndexCommitter {
    async fn init(&mut self) -> Result<()> {
        Ok(())
    }

    async fn pre_commit(
        &mut self,
        epoch: u64,
        metadata: Vec<SinkMetadata>,
        _schema_change: Option<PbSinkSchemaChange>,
    ) -> Result<Option<Vec<u8>>> {
        let mut all_data_files: Vec<serde_json::Value> = Vec::new();
        let mut all_dv_data_files: Vec<serde_json::Value> = Vec::new();
        let mut schema_id: Option<i32> = None;
        let mut partition_spec_id: Option<i32> = None;

        for m in &metadata {
            let bytes = match &m.metadata {
                Some(Serialized(s)) => &s.metadata,
                _ => {
                    return Err(SinkError::Coordinator(anyhow!(
                        "expected serialized metadata for Iceberg V3 commit, but got non-serialized metadata"
                    )));
                }
            };
            if bytes.is_empty() {
                continue;
            }

            let payload: IcebergV3CommitPayload = serde_json::from_slice(bytes)
                .context("failed to deserialize IcebergV3CommitPayload from metadata")
                .map_err(SinkError::Coordinator)?;

            match payload {
                IcebergV3CommitPayload::DataFiles(data_meta) => {
                    schema_id.get_or_insert(data_meta.schema_id);
                    partition_spec_id.get_or_insert(data_meta.partition_spec_id);
                    all_data_files.extend(data_meta.data_files);
                }
                IcebergV3CommitPayload::DvFiles(dv_meta) => {
                    all_dv_data_files.extend(dv_meta.serialized_dv_data_files);
                }
            }
        }

        if all_data_files.is_empty() && all_dv_data_files.is_empty() {
            tracing::debug!(epoch, sink_id = %self.sink_id, "V3 pre_commit: no data");
            return Ok(None);
        }

        let snapshot_id = FastAppendAction::generate_snapshot_id(&self.table);

        let pre_commit = V3PreCommitData {
            snapshot_id,
            schema_id,
            partition_spec_id,
            data_files: all_data_files,
            dv_data_files: all_dv_data_files,
        };

        let bytes = serde_json::to_vec(&pre_commit)
            .context("failed to serialize pre-commit data")
            .map_err(SinkError::Coordinator)?;

        Ok(Some(bytes))
    }

    async fn commit_data(&mut self, epoch: u64, commit_metadata: Vec<u8>) -> Result<()> {
        tracing::debug!(epoch, sink_id = %self.sink_id, "V3 commit_data: start");

        if commit_metadata.is_empty() {
            tracing::debug!(epoch, sink_id = %self.sink_id, "V3 commit_data: empty metadata");
            return Ok(());
        }

        let pre_commit: V3PreCommitData = serde_json::from_slice(&commit_metadata)
            .context("failed to deserialize pre-commit data")
            .map_err(SinkError::Coordinator)?;

        // Idempotency check: if snapshot already committed, skip.
        let snapshot_committed =
            is_snapshot_committed(&self.config, pre_commit.snapshot_id).await?;
        if snapshot_committed {
            tracing::info!(
                sink_id = %self.sink_id,
                snapshot_id = pre_commit.snapshot_id,
                "V3 commit_data: snapshot already committed, skipping"
            );
            return Ok(());
        }

        // Reload table to get latest metadata.
        self.table = self
            .catalog
            .load_table(self.table.identifier())
            .await
            .context("failed to reload table in commit_data")
            .map_err(SinkError::Coordinator)?;

        let partition_type = self
            .table
            .metadata()
            .default_partition_spec()
            .partition_type(self.table.metadata().current_schema().as_ref())
            .context("failed to get partition type in commit_data")
            .map_err(SinkError::Coordinator)?;
        let schema = self.table.metadata().current_schema();
        let default_partition_spec_id = self.table.metadata().default_partition_spec_id();

        // Convert data file JSON to DataFile.
        let mut all_files: Vec<iceberg::spec::DataFile> = pre_commit
            .data_files
            .into_iter()
            .map(|json_value| {
                let serialized: SerializedDataFile = serde_json::from_value(json_value)
                    .context("failed to deserialize SerializedDataFile from JSON")
                    .map_err(SinkError::Coordinator)?;

                serialized
                    .try_into(default_partition_spec_id, &partition_type, schema.as_ref())
                    .context("failed to convert SerializedDataFile to DataFile")
                    .map_err(SinkError::Coordinator)
            })
            .collect::<Result<Vec<_>>>()?;

        // Convert DV data file JSON to DataFile.
        if !pre_commit.dv_data_files.is_empty() {
            let dv_files: Vec<iceberg::spec::DataFile> = pre_commit
                .dv_data_files
                .into_iter()
                .map(|json_value| {
                    let serialized: SerializedDataFile = serde_json::from_value(json_value)
                        .context("failed to deserialize DV SerializedDataFile from JSON")
                        .map_err(SinkError::Coordinator)?;
                    serialized
                        .try_into(default_partition_spec_id, &partition_type, schema.as_ref())
                        .context("failed to convert DV SerializedDataFile to DataFile")
                        .map_err(SinkError::Coordinator)
                })
                .try_collect()?;
            all_files.extend(dv_files);
        }

        if all_files.is_empty() {
            tracing::debug!(epoch, sink_id = %self.sink_id, "V3 commit_data: no files to append");
            return Ok(());
        }

        tracing::info!(
            sink_id = %self.sink_id,
            epoch,
            snapshot_id = pre_commit.snapshot_id,
            all_files = ?all_files.iter().map(|f| f.file_path()).collect::<Vec<_>>(),
            "V3 commit_data: committing snapshot with fast append"
        );

        fast_append_with_retry(
            self.catalog.clone(),
            self.table.identifier(),
            all_files,
            pre_commit.snapshot_id,
            MAIN_BRANCH,
            None, // V3 path: no schema/partition validation needed
            None,
            self.commit_retry_num,
        )
        .await
        .context("V3 commit_data: fast append failed")
        .map_err(SinkError::Coordinator)?;

        tracing::info!(
            sink_id = %self.sink_id,
            epoch,
            snapshot_id = pre_commit.snapshot_id,
            "V3 commit_data: snapshot committed successfully"
        );

        Ok(())
    }

    async fn commit_schema_change(
        &mut self,
        _epoch: u64,
        _schema_change: PbSinkSchemaChange,
    ) -> Result<()> {
        // V3 does not support schema change yet; no-op.
        tracing::warn!(
            sink_id = %self.sink_id,
            "V3 commit_schema_change: no-op (schema change not supported in V3)"
        );
        Ok(())
    }

    async fn abort(&mut self, _epoch: u64, _commit_metadata: Vec<u8>) {
        // No-op: snapshot_id idempotency handles recovery.
        tracing::debug!(
            sink_id = %self.sink_id,
            "V3 abort: no-op (idempotent via snapshot_id)"
        );
    }

    fn roles(&self) -> Arc<[CoordinationRole]> {
        Arc::from([CoordinationRole::Unspecified, CoordinationRole::DvMerger])
    }
}
