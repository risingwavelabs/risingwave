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
use std::time::Duration;

use anyhow::{Context, anyhow};
use async_trait::async_trait;
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::spec::{DataFile, Operation, SerializedDataFile};
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, FastAppendAction, Transaction};
use iceberg::{Catalog, TableIdent};
use itertools::Itertools;
use risingwave_common::array::arrow::arrow_schema_iceberg::Field as ArrowField;
use risingwave_common::array::arrow::{IcebergArrowConvert, IcebergCreateTableArrowConvert};
use risingwave_common::bail;
use risingwave_common::catalog::Field;
use risingwave_common::error::IcebergError;
use risingwave_pb::connector_service::SinkMetadata;
use risingwave_pb::connector_service::sink_metadata::Metadata::Serialized;
use risingwave_pb::connector_service::sink_metadata::SerializedMetadata;
use risingwave_pb::stream_plan::PbSinkSchemaChange;
use serde_json::from_value;
use thiserror_ext::AsReport;
use tokio::sync::mpsc::UnboundedSender;
use tokio_retry::RetryIf;
use tokio_retry::strategy::{ExponentialBackoff, jitter};
use tracing::warn;

use super::{GLOBAL_SINK_METRICS, IcebergConfig, SinkError, commit_branch};
use crate::connector_common::IcebergSinkCompactionUpdate;
use crate::sink::catalog::SinkId;
use crate::sink::{Result, SinglePhaseCommitCoordinator, SinkParam, TwoPhaseCommitCoordinator};

const SCHEMA_ID: &str = "schema_id";
const PARTITION_SPEC_ID: &str = "partition_spec_id";
const DATA_FILES: &str = "data_files";

#[derive(Default, Clone)]
pub struct IcebergCommitResult {
    pub schema_id: i32,
    pub partition_spec_id: i32,
    pub data_files: Vec<SerializedDataFile>,
}

impl IcebergCommitResult {
    fn try_from(value: &SinkMetadata) -> Result<Self> {
        if let Some(Serialized(v)) = &value.metadata {
            let mut values = if let serde_json::Value::Object(v) =
                serde_json::from_slice::<serde_json::Value>(&v.metadata)
                    .context("Can't parse iceberg sink metadata")?
            {
                v
            } else {
                bail!("iceberg sink metadata should be an object");
            };

            let schema_id;
            if let Some(serde_json::Value::Number(value)) = values.remove(SCHEMA_ID) {
                schema_id = value
                    .as_u64()
                    .ok_or_else(|| anyhow!("schema_id should be a u64"))?;
            } else {
                bail!("iceberg sink metadata should have schema_id");
            }

            let partition_spec_id;
            if let Some(serde_json::Value::Number(value)) = values.remove(PARTITION_SPEC_ID) {
                partition_spec_id = value
                    .as_u64()
                    .ok_or_else(|| anyhow!("partition_spec_id should be a u64"))?;
            } else {
                bail!("iceberg sink metadata should have partition_spec_id");
            }

            let data_files: Vec<SerializedDataFile>;
            if let serde_json::Value::Array(values) = values
                .remove(DATA_FILES)
                .ok_or_else(|| anyhow!("iceberg sink metadata should have data_files object"))?
            {
                data_files = values
                    .into_iter()
                    .map(from_value::<SerializedDataFile>)
                    .collect::<std::result::Result<_, _>>()
                    .unwrap();
            } else {
                bail!("iceberg sink metadata should have data_files object");
            }

            Ok(Self {
                schema_id: schema_id as i32,
                partition_spec_id: partition_spec_id as i32,
                data_files,
            })
        } else {
            bail!("Can't create iceberg sink write result from empty data!")
        }
    }

    fn try_from_serialized_bytes(value: Vec<u8>) -> Result<Self> {
        let mut values = if let serde_json::Value::Object(value) =
            serde_json::from_slice::<serde_json::Value>(&value)
                .context("Can't parse iceberg sink metadata")?
        {
            value
        } else {
            bail!("iceberg sink metadata should be an object");
        };

        let schema_id;
        if let Some(serde_json::Value::Number(value)) = values.remove(SCHEMA_ID) {
            schema_id = value
                .as_u64()
                .ok_or_else(|| anyhow!("schema_id should be a u64"))?;
        } else {
            bail!("iceberg sink metadata should have schema_id");
        }

        let partition_spec_id;
        if let Some(serde_json::Value::Number(value)) = values.remove(PARTITION_SPEC_ID) {
            partition_spec_id = value
                .as_u64()
                .ok_or_else(|| anyhow!("partition_spec_id should be a u64"))?;
        } else {
            bail!("iceberg sink metadata should have partition_spec_id");
        }

        let data_files: Vec<SerializedDataFile>;
        if let serde_json::Value::Array(values) = values
            .remove(DATA_FILES)
            .ok_or_else(|| anyhow!("iceberg sink metadata should have data_files object"))?
        {
            data_files = values
                .into_iter()
                .map(from_value::<SerializedDataFile>)
                .collect::<std::result::Result<_, _>>()
                .unwrap();
        } else {
            bail!("iceberg sink metadata should have data_files object");
        }

        Ok(Self {
            schema_id: schema_id as i32,
            partition_spec_id: partition_spec_id as i32,
            data_files,
        })
    }
}

impl<'a> TryFrom<&'a IcebergCommitResult> for SinkMetadata {
    type Error = SinkError;

    fn try_from(value: &'a IcebergCommitResult) -> std::result::Result<SinkMetadata, Self::Error> {
        let json_data_files = serde_json::Value::Array(
            value
                .data_files
                .iter()
                .map(serde_json::to_value)
                .collect::<std::result::Result<Vec<serde_json::Value>, _>>()
                .context("Can't serialize data files to json")?,
        );
        let json_value = serde_json::Value::Object(
            vec![
                (
                    SCHEMA_ID.to_owned(),
                    serde_json::Value::Number(value.schema_id.into()),
                ),
                (
                    PARTITION_SPEC_ID.to_owned(),
                    serde_json::Value::Number(value.partition_spec_id.into()),
                ),
                (DATA_FILES.to_owned(), json_data_files),
            ]
            .into_iter()
            .collect(),
        );
        Ok(SinkMetadata {
            metadata: Some(Serialized(SerializedMetadata {
                metadata: serde_json::to_vec(&json_value)
                    .context("Can't serialize iceberg sink metadata")?,
            })),
        })
    }
}

impl TryFrom<IcebergCommitResult> for Vec<u8> {
    type Error = SinkError;

    fn try_from(value: IcebergCommitResult) -> std::result::Result<Vec<u8>, Self::Error> {
        let json_data_files = serde_json::Value::Array(
            value
                .data_files
                .iter()
                .map(serde_json::to_value)
                .collect::<std::result::Result<Vec<serde_json::Value>, _>>()
                .context("Can't serialize data files to json")?,
        );
        let json_value = serde_json::Value::Object(
            vec![
                (
                    SCHEMA_ID.to_owned(),
                    serde_json::Value::Number(value.schema_id.into()),
                ),
                (
                    PARTITION_SPEC_ID.to_owned(),
                    serde_json::Value::Number(value.partition_spec_id.into()),
                ),
                (DATA_FILES.to_owned(), json_data_files),
            ]
            .into_iter()
            .collect(),
        );
        Ok(serde_json::to_vec(&json_value).context("Can't serialize iceberg sink metadata")?)
    }
}
pub struct IcebergSinkCommitter {
    pub(super) catalog: Arc<dyn Catalog>,
    pub(super) table: Table,
    pub last_commit_epoch: u64,
    pub(crate) sink_id: SinkId,
    pub(crate) config: IcebergConfig,
    pub(crate) param: SinkParam,
    pub(super) commit_retry_num: u32,
    pub(crate) iceberg_compact_stat_sender: Option<UnboundedSender<IcebergSinkCompactionUpdate>>,
}

impl IcebergSinkCommitter {
    // Reload table and guarantee current schema_id and partition_spec_id matches
    // given `schema_id` and `partition_spec_id`
    async fn reload_table(
        catalog: &dyn Catalog,
        table_ident: &TableIdent,
        schema_id: i32,
        partition_spec_id: i32,
    ) -> Result<Table> {
        let table = catalog
            .load_table(table_ident)
            .await
            .map_err(|err| SinkError::Iceberg(anyhow!(err)))?;
        if table.metadata().current_schema_id() != schema_id {
            return Err(SinkError::Iceberg(anyhow!(
                "Schema evolution not supported, expect schema id {}, but got {}",
                schema_id,
                table.metadata().current_schema_id()
            )));
        }
        if table.metadata().default_partition_spec_id() != partition_spec_id {
            return Err(SinkError::Iceberg(anyhow!(
                "Partition evolution not supported, expect partition spec id {}, but got {}",
                partition_spec_id,
                table.metadata().default_partition_spec_id()
            )));
        }
        Ok(table)
    }
}

#[async_trait]
impl SinglePhaseCommitCoordinator for IcebergSinkCommitter {
    async fn init(&mut self) -> Result<()> {
        tracing::info!(
            sink_id = %self.param.sink_id,
            "Iceberg sink coordinator initialized",
        );

        Ok(())
    }

    async fn commit_data(&mut self, epoch: u64, metadata: Vec<SinkMetadata>) -> Result<()> {
        tracing::debug!("Starting iceberg direct commit in epoch {epoch}");

        if metadata.is_empty() {
            tracing::debug!(?epoch, "No datafile to commit");
            return Ok(());
        }

        // Commit data if present
        if let Some((write_results, snapshot_id)) = self.pre_commit_inner(epoch, metadata)? {
            self.commit_data_impl(epoch, write_results, snapshot_id)
                .await?;
        }

        Ok(())
    }

    async fn commit_schema_change(
        &mut self,
        epoch: u64,
        schema_change: PbSinkSchemaChange,
    ) -> Result<()> {
        tracing::info!(
            "Committing schema change {:?} in epoch {}",
            schema_change,
            epoch
        );
        self.commit_schema_change_impl(schema_change).await?;
        tracing::info!("Successfully committed schema change in epoch {}", epoch);

        Ok(())
    }
}

#[async_trait]
impl TwoPhaseCommitCoordinator for IcebergSinkCommitter {
    async fn init(&mut self) -> Result<()> {
        tracing::info!(
            sink_id = %self.param.sink_id,
            "Iceberg sink coordinator initialized",
        );

        Ok(())
    }

    async fn pre_commit(
        &mut self,
        epoch: u64,
        metadata: Vec<SinkMetadata>,
        _schema_change: Option<PbSinkSchemaChange>,
    ) -> Result<Option<Vec<u8>>> {
        tracing::debug!("Starting iceberg pre commit in epoch {epoch}");

        let (write_results, snapshot_id) = match self.pre_commit_inner(epoch, metadata)? {
            Some((write_results, snapshot_id)) => (write_results, snapshot_id),
            None => {
                tracing::debug!(?epoch, "no data to pre commit");
                return Ok(None);
            }
        };

        let mut write_results_bytes = Vec::new();
        for each_parallelism_write_result in write_results {
            let each_parallelism_write_result_bytes: Vec<u8> =
                each_parallelism_write_result.try_into()?;
            write_results_bytes.push(each_parallelism_write_result_bytes);
        }

        let snapshot_id_bytes: Vec<u8> = snapshot_id.to_le_bytes().to_vec();
        write_results_bytes.push(snapshot_id_bytes);

        let pre_commit_metadata_bytes: Vec<u8> = serialize_metadata(write_results_bytes);
        Ok(Some(pre_commit_metadata_bytes))
    }

    async fn commit_data(&mut self, epoch: u64, commit_metadata: Vec<u8>) -> Result<()> {
        tracing::debug!("Starting iceberg commit in epoch {epoch}");

        if commit_metadata.is_empty() {
            tracing::debug!(?epoch, "No datafile to commit");
            return Ok(());
        }

        // Deserialize commit metadata
        let mut payload = deserialize_metadata(commit_metadata);
        if payload.is_empty() {
            return Err(SinkError::Iceberg(anyhow!(
                "Invalid commit metadata: empty payload"
            )));
        }

        // Last element is snapshot_id
        let snapshot_id_bytes = payload.pop().ok_or_else(|| {
            SinkError::Iceberg(anyhow!("Invalid commit metadata: missing snapshot_id"))
        })?;
        let snapshot_id = i64::from_le_bytes(
            snapshot_id_bytes
                .try_into()
                .map_err(|_| SinkError::Iceberg(anyhow!("Invalid snapshot id bytes")))?,
        );

        // Remaining elements are write_results
        let write_results = payload
            .into_iter()
            .map(IcebergCommitResult::try_from_serialized_bytes)
            .collect::<Result<Vec<_>>>()?;

        let snapshot_committed = self
            .is_snapshot_id_in_iceberg(&self.config, snapshot_id)
            .await?;

        if snapshot_committed {
            tracing::info!(
                "Snapshot id {} already committed in iceberg table, skip committing again.",
                snapshot_id
            );
            return Ok(());
        }

        self.commit_data_impl(epoch, write_results, snapshot_id)
            .await
    }

    async fn commit_schema_change(
        &mut self,
        epoch: u64,
        schema_change: PbSinkSchemaChange,
    ) -> Result<()> {
        let schema_updated = self.check_schema_change_applied(&schema_change)?;
        if schema_updated {
            tracing::info!("Schema change already committed in epoch {}, skip", epoch);
            return Ok(());
        }

        tracing::info!(
            "Committing schema change {:?} in epoch {}",
            schema_change,
            epoch
        );
        self.commit_schema_change_impl(schema_change).await?;
        tracing::info!("Successfully committed schema change in epoch {epoch}");

        Ok(())
    }

    async fn abort(&mut self, _epoch: u64, _commit_metadata: Vec<u8>) {
        // TODO: Files that have been written but not committed should be deleted.
        tracing::debug!("Abort not implemented yet");
    }
}

/// Methods Required to Achieve Exactly Once Semantics
impl IcebergSinkCommitter {
    fn pre_commit_inner(
        &mut self,
        _epoch: u64,
        metadata: Vec<SinkMetadata>,
    ) -> Result<Option<(Vec<IcebergCommitResult>, i64)>> {
        let write_results: Vec<IcebergCommitResult> = metadata
            .iter()
            .map(IcebergCommitResult::try_from)
            .collect::<Result<Vec<IcebergCommitResult>>>()?;

        // Skip if no data to commit
        if write_results.is_empty() || write_results.iter().all(|r| r.data_files.is_empty()) {
            return Ok(None);
        }

        let expect_schema_id = write_results[0].schema_id;
        let expect_partition_spec_id = write_results[0].partition_spec_id;

        // guarantee that all write results has same schema_id and partition_spec_id
        if write_results
            .iter()
            .any(|r| r.schema_id != expect_schema_id)
            || write_results
                .iter()
                .any(|r| r.partition_spec_id != expect_partition_spec_id)
        {
            return Err(SinkError::Iceberg(anyhow!(
                "schema_id and partition_spec_id should be the same in all write results"
            )));
        }

        let snapshot_id = FastAppendAction::generate_snapshot_id(&self.table);

        Ok(Some((write_results, snapshot_id)))
    }

    async fn commit_data_impl(
        &mut self,
        epoch: u64,
        write_results: Vec<IcebergCommitResult>,
        snapshot_id: i64,
    ) -> Result<()> {
        // Empty write results should be handled before calling this function.
        assert!(
            !write_results.is_empty() && !write_results.iter().all(|r| r.data_files.is_empty())
        );

        // Check snapshot limit before proceeding with commit
        self.wait_for_snapshot_limit().await?;

        let expect_schema_id = write_results[0].schema_id;
        let expect_partition_spec_id = write_results[0].partition_spec_id;

        // Load the latest table to avoid concurrent modification with the best effort.
        self.table = Self::reload_table(
            self.catalog.as_ref(),
            self.table.identifier(),
            expect_schema_id,
            expect_partition_spec_id,
        )
        .await?;

        let Some(schema) = self.table.metadata().schema_by_id(expect_schema_id) else {
            return Err(SinkError::Iceberg(anyhow!(
                "Can't find schema by id {}",
                expect_schema_id
            )));
        };
        let Some(partition_spec) = self
            .table
            .metadata()
            .partition_spec_by_id(expect_partition_spec_id)
        else {
            return Err(SinkError::Iceberg(anyhow!(
                "Can't find partition spec by id {}",
                expect_partition_spec_id
            )));
        };
        let partition_type = partition_spec
            .as_ref()
            .clone()
            .partition_type(schema)
            .map_err(|err| SinkError::Iceberg(anyhow!(err)))?;

        let data_files = write_results
            .into_iter()
            .flat_map(|r| {
                r.data_files.into_iter().map(|f| {
                    f.try_into(expect_partition_spec_id, &partition_type, schema)
                        .map_err(|err| SinkError::Iceberg(anyhow!(err)))
                })
            })
            .collect::<Result<Vec<DataFile>>>()?;
        // # TODO:
        // This retry behavior should be revert and do in iceberg-rust when it supports retry(Track in: https://github.com/apache/iceberg-rust/issues/964)
        // because retry logic involved reapply the commit metadata.
        // For now, we just retry the commit operation.
        let retry_strategy = ExponentialBackoff::from_millis(10)
            .max_delay(Duration::from_secs(60))
            .map(jitter)
            .take(self.commit_retry_num as usize);
        let catalog = self.catalog.clone();
        let table_ident = self.table.identifier().clone();

        // Custom retry logic that:
        // 1. Calls reload_table before each commit attempt to get the latest metadata
        // 2. If reload_table fails (table not exists/schema/partition mismatch), stops retrying immediately
        // 3. If commit fails, retries with backoff
        enum CommitError {
            ReloadTable(SinkError), // Non-retriable: schema/partition mismatch
            Commit(SinkError),      // Retriable: commit conflicts, network errors
        }

        let table = RetryIf::spawn(
            retry_strategy,
            || async {
                // Reload table before each commit attempt to get the latest metadata
                let table = Self::reload_table(
                    catalog.as_ref(),
                    &table_ident,
                    expect_schema_id,
                    expect_partition_spec_id,
                )
                .await
                .map_err(|e| {
                    tracing::error!(error = %e.as_report(), "Failed to reload iceberg table");
                    CommitError::ReloadTable(e)
                })?;

                let txn = Transaction::new(&table);
                let append_action = txn
                    .fast_append()
                    .set_snapshot_id(snapshot_id)
                    .set_target_branch(commit_branch(
                        self.config.r#type.as_str(),
                        self.config.write_mode,
                    ))
                    .add_data_files(data_files.clone());

                let tx = append_action.apply(txn).map_err(|err| {
                    let err: IcebergError = err.into();
                    tracing::error!(error = %err.as_report(), "Failed to apply iceberg table");
                    CommitError::Commit(SinkError::Iceberg(anyhow!(err)))
                })?;

                tx.commit(catalog.as_ref()).await.map_err(|err| {
                    let err: IcebergError = err.into();
                    tracing::error!(error = %err.as_report(), "Failed to commit iceberg table");
                    CommitError::Commit(SinkError::Iceberg(anyhow!(err)))
                })
            },
            |err: &CommitError| {
                // Only retry on commit errors, not on reload_table errors
                match err {
                    CommitError::Commit(_) => {
                        tracing::warn!("Commit failed, will retry");
                        true
                    }
                    CommitError::ReloadTable(_) => {
                        tracing::error!(
                            "reload_table failed with non-retriable error, will not retry"
                        );
                        false
                    }
                }
            },
        )
        .await
        .map_err(|e| match e {
            CommitError::ReloadTable(e) | CommitError::Commit(e) => e,
        })?;
        self.table = table;

        let snapshot_num = self.table.metadata().snapshots().count();
        let catalog_name = self.config.common.catalog_name();
        let table_name = self.table.identifier().to_string();
        let metrics_labels = [&self.param.sink_name, &catalog_name, &table_name];
        GLOBAL_SINK_METRICS
            .iceberg_snapshot_num
            .with_guarded_label_values(&metrics_labels)
            .set(snapshot_num as i64);

        tracing::debug!("Succeeded to commit to iceberg table in epoch {epoch}.");

        if let Some(iceberg_compact_stat_sender) = &self.iceberg_compact_stat_sender
            && self.config.enable_compaction
            && iceberg_compact_stat_sender
                .send(IcebergSinkCompactionUpdate {
                    sink_id: self.sink_id,
                    compaction_interval: self.config.compaction_interval_sec(),
                    force_compaction: false,
                })
                .is_err()
        {
            warn!("failed to send iceberg compaction stats");
        }

        Ok(())
    }

    /// During pre-commit metadata, we record the `snapshot_id` corresponding to each batch of files.
    /// Therefore, the logic for checking whether all files in this batch are present in Iceberg
    /// has been changed to verifying if their corresponding `snapshot_id` exists in Iceberg.
    async fn is_snapshot_id_in_iceberg(
        &self,
        iceberg_config: &IcebergConfig,
        snapshot_id: i64,
    ) -> Result<bool> {
        let table = iceberg_config.load_table().await?;
        if table.metadata().snapshot_by_id(snapshot_id).is_some() {
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Check if the specified columns already exist in the iceberg table's current schema.
    /// This is used to determine if schema change has already been applied.
    fn check_schema_change_applied(&self, schema_change: &PbSinkSchemaChange) -> Result<bool> {
        let current_schema = self.table.metadata().current_schema();
        let current_arrow_schema = schema_to_arrow_schema(current_schema.as_ref())
            .context("Failed to convert schema")
            .map_err(SinkError::Iceberg)?;

        let iceberg_arrow_convert = IcebergArrowConvert;

        let schema_matches = |expected: &[ArrowField]| {
            if current_arrow_schema.fields().len() != expected.len() {
                return false;
            }

            expected.iter().all(|expected_field| {
                current_arrow_schema.fields().iter().any(|current_field| {
                    current_field.name() == expected_field.name()
                        && current_field.data_type() == expected_field.data_type()
                })
            })
        };

        let original_arrow_fields: Vec<ArrowField> = schema_change
            .original_schema
            .iter()
            .map(|pb_field| {
                let field = Field::from(pb_field);
                iceberg_arrow_convert
                    .to_arrow_field(&field.name, &field.data_type)
                    .context("Failed to convert field to arrow")
                    .map_err(SinkError::Iceberg)
            })
            .collect::<Result<_>>()?;

        // If current schema equals original_schema, then schema change is NOT applied.
        if schema_matches(&original_arrow_fields) {
            tracing::debug!(
                "Current iceberg schema matches original_schema ({} columns); schema change not applied",
                original_arrow_fields.len()
            );
            return Ok(false);
        }

        // We only support add_columns for now.
        let Some(risingwave_pb::stream_plan::sink_schema_change::Op::AddColumns(add_columns_op)) =
            schema_change.op.as_ref()
        else {
            return Err(SinkError::Iceberg(anyhow!(
                "Unsupported sink schema change op in iceberg sink: {:?}",
                schema_change.op
            )));
        };

        let add_arrow_fields: Vec<ArrowField> = add_columns_op
            .fields
            .iter()
            .map(|pb_field| {
                let field = Field::from(pb_field);
                iceberg_arrow_convert
                    .to_arrow_field(&field.name, &field.data_type)
                    .context("Failed to convert field to arrow")
                    .map_err(SinkError::Iceberg)
            })
            .collect::<Result<_>>()?;

        let mut expected_after_change = original_arrow_fields;
        expected_after_change.extend(add_arrow_fields);

        // If current schema equals original_schema + add_columns, then schema change is applied.
        if schema_matches(&expected_after_change) {
            tracing::debug!(
                "Current iceberg schema matches original_schema + add_columns ({} columns); schema change already applied",
                expected_after_change.len()
            );
            return Ok(true);
        }

        Err(SinkError::Iceberg(anyhow!(
            "Current iceberg schema does not match either original_schema ({} cols) or original_schema + add_columns; cannot determine whether schema change is applied",
            schema_change.original_schema.len()
        )))
    }

    /// Commit schema changes (e.g., add columns) to the iceberg table.
    /// This function uses Transaction API to atomically update the table schema
    /// with optimistic locking to prevent concurrent conflicts.
    async fn commit_schema_change_impl(&mut self, schema_change: PbSinkSchemaChange) -> Result<()> {
        use iceberg::spec::NestedField;

        let Some(risingwave_pb::stream_plan::sink_schema_change::Op::AddColumns(add_columns_op)) =
            schema_change.op.as_ref()
        else {
            return Err(SinkError::Iceberg(anyhow!(
                "Unsupported sink schema change op in iceberg sink: {:?}",
                schema_change.op
            )));
        };

        let add_columns = add_columns_op.fields.iter().map(Field::from).collect_vec();

        // Step 1: Get current table metadata
        let metadata = self.table.metadata();
        let mut next_field_id = metadata.last_column_id() + 1;
        tracing::debug!("Starting schema change, next_field_id: {}", next_field_id);

        // Step 2: Build new fields to add
        let iceberg_create_table_arrow_convert = IcebergCreateTableArrowConvert::default();
        let mut new_fields = Vec::new();

        for field in &add_columns {
            // Convert RisingWave Field to Arrow Field using IcebergCreateTableArrowConvert
            let arrow_field = iceberg_create_table_arrow_convert
                .to_arrow_field(&field.name, &field.data_type)
                .with_context(|| format!("Failed to convert field '{}' to arrow", field.name))
                .map_err(SinkError::Iceberg)?;

            // Convert Arrow DataType to Iceberg Type
            let iceberg_type = iceberg::arrow::arrow_type_to_type(arrow_field.data_type())
                .map_err(|err| {
                    SinkError::Iceberg(
                        anyhow!(err).context("Failed to convert Arrow type to Iceberg type"),
                    )
                })?;

            // Create NestedField with the next available field ID
            let nested_field = Arc::new(NestedField::optional(
                next_field_id,
                &field.name,
                iceberg_type,
            ));

            new_fields.push(nested_field);
            tracing::info!("Prepared field '{}' with ID {}", field.name, next_field_id);
            next_field_id += 1;
        }

        // Step 3: Create Transaction with UpdateSchemaAction
        tracing::info!(
            "Committing schema change to catalog for table {}",
            self.table.identifier()
        );

        let txn = Transaction::new(&self.table);
        let action = txn.update_schema().add_fields(new_fields);

        let updated_table = action
            .apply(txn)
            .context("Failed to apply schema update action")
            .map_err(SinkError::Iceberg)?
            .commit(self.catalog.as_ref())
            .await
            .context("Failed to commit table schema change")
            .map_err(SinkError::Iceberg)?;

        self.table = updated_table;

        tracing::info!(
            "Successfully committed schema change, added {} columns to iceberg table",
            add_columns.len()
        );

        Ok(())
    }

    /// Check if the number of snapshots since the last rewrite/overwrite operation exceeds the limit
    /// Returns the number of snapshots since the last rewrite/overwrite
    fn count_snapshots_since_rewrite(&self) -> usize {
        let mut snapshots: Vec<_> = self.table.metadata().snapshots().collect();
        snapshots.sort_by_key(|b| std::cmp::Reverse(b.timestamp_ms()));

        // Iterate through snapshots in reverse order (newest first) to find the last rewrite/overwrite
        let mut count = 0;
        for snapshot in snapshots {
            // Check if this snapshot represents a rewrite or overwrite operation
            let summary = snapshot.summary();
            match &summary.operation {
                Operation::Replace => {
                    // Found a rewrite/overwrite operation, stop counting
                    break;
                }

                _ => {
                    // Increment count for each snapshot that is not a rewrite/overwrite
                    count += 1;
                }
            }
        }

        count
    }

    /// Wait until snapshot count since last rewrite is below the limit
    async fn wait_for_snapshot_limit(&mut self) -> Result<()> {
        if !self.config.enable_compaction {
            return Ok(());
        }

        if let Some(max_snapshots) = self.config.max_snapshots_num_before_compaction {
            loop {
                let current_count = self.count_snapshots_since_rewrite();

                if current_count < max_snapshots {
                    tracing::info!(
                        "Snapshot count check passed: {} < {}",
                        current_count,
                        max_snapshots
                    );
                    break;
                }

                tracing::info!(
                    "Snapshot count {} exceeds limit {}, waiting...",
                    current_count,
                    max_snapshots
                );

                if let Some(iceberg_compact_stat_sender) = &self.iceberg_compact_stat_sender
                    && iceberg_compact_stat_sender
                        .send(IcebergSinkCompactionUpdate {
                            sink_id: self.sink_id,
                            compaction_interval: self.config.compaction_interval_sec(),
                            force_compaction: true,
                        })
                        .is_err()
                {
                    tracing::warn!("failed to send iceberg compaction stats");
                }

                // Wait for 30 seconds before checking again
                tokio::time::sleep(Duration::from_secs(30)).await;

                // Reload table to get latest snapshots
                self.table = self.config.load_table().await?;
            }
        }
        Ok(())
    }
}

fn serialize_metadata(metadata: Vec<Vec<u8>>) -> Vec<u8> {
    serde_json::to_vec(&metadata).unwrap()
}

fn deserialize_metadata(bytes: Vec<u8>) -> Vec<Vec<u8>> {
    serde_json::from_slice(&bytes).unwrap()
}
