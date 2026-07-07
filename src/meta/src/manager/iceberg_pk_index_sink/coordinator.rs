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

//! Per-sink iceberg pk-index commit coordinator. This is a plain struct (no background task / mpsc): the
//! [`crate::manager::iceberg_pk_index_sink::IcebergPkIndexSinkManager`] holds one per registered sink behind a
//! per-sink async mutex and calls [`IcebergPkIndexSinkCoordinator::pre_commit`] / [`IcebergPkIndexSinkCoordinator::commit`]
//! directly from the barrier-completion path. The barrier path already serializes pre-commit/commit per
//! epoch, so the coordinator never needs its own request queue.
//!
//! [`IcebergPkIndexSinkCoordinator::init`] is synchronous with respect to registration: it loads the iceberg
//! catalog/table, reads `pending_sink_state`, and drains any recovered pending epoch via an iceberg
//! `overwrite_files` transaction BEFORE returning a ready coordinator. Only once init completes does the
//! sink start accepting live pre-commit/commit calls.
//!
//! The two phases dispatched from `complete_barrier`:
//!
//! 1. `pre_commit` — aggregate the reports, generate a `snapshot_id`, persist the merged file list under
//!    `pending_sink_state`, return. No iceberg I/O.
//! 2. `commit` — run an iceberg `overwrite_files` transaction (keyed on the pre-generated `snapshot_id`
//!    for idempotency), then mark the row Committed and prune the prior epoch's row.
//!
//! On retry-exhausted commit failure the error propagates to the caller; the barrier then fails and the
//! meta-recovery path drops the coordinator, re-registers it (re-running `init`), and retries from the
//! persisted `pending_sink_state` rows.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use iceberg::Catalog;
use iceberg::spec::{DataFile, SerializedDataFile};
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, FastAppendAction, Transaction};
use prost::Message;
use risingwave_connector::sink::catalog::SinkId;
use risingwave_connector::sink::iceberg::commit_retry::{self, CommitError};
use risingwave_connector::sink::iceberg::{
    IcebergCommitResult, IcebergConfig, IcebergPositionDeleteMergerCommitResult, commit_branch,
    resolve_partition_type,
};
use risingwave_meta_model::pending_sink_state::SinkState;
use risingwave_pb::connector_service::PbIcebergPkIndexPreCommitState;
use risingwave_pb::stream_service::PbIcebergPkIndexSinkRole;
use risingwave_pb::stream_service::barrier_complete_response::PbIcebergPkIndexSinkMetadata;
use sea_orm::DatabaseConnection;
use serde::{Deserialize, Serialize};
use thiserror_ext::AsReport;
use tokio::time::timeout;

use super::backfill::backfill_delete_file_partitions;
use crate::manager::exactly_once_util::{
    clean_aborted_records, commit_and_prune_epoch, list_sink_states_ordered_by_epoch,
    persist_pre_commit_metadata,
};

/// Bound the init phase so a register call can't hang forever if the iceberg endpoint is unreachable;
/// on timeout `init` returns an error and registration fails (the caller surfaces it / retries).
const INIT_TIMEOUT: Duration = Duration::from_secs(60);

/// One epoch's worth of pre-committed state queued inside the coordinator. Holds the decoded merged file
/// list and the pre-generated `snapshot_id`. The blob form ([`PbIcebergPkIndexPreCommitState`]) is only
/// materialized when persisting to `pending_sink_state`; in-memory we keep the structured form.
#[derive(Clone)]
struct EpochCommit {
    epoch: u64,
    merged: Arc<IcebergPkIndexSinkAggResult>,
    snapshot_id: i64,
    /// Data + delete files already materialized during pre-commit backfill (partitioned tables
    /// only), in the `add_data_files` order expected by the commit. Flows straight into the commit
    /// so the files are not deserialized again. `None` on the recovery path (and for unpartitioned
    /// tables that skip backfill), where the commit materializes them from `merged` once.
    materialized_add_files: Option<Vec<DataFile>>,
}

/// Per-sink iceberg pk-index commit coordinator. Owns the loaded iceberg catalog/table (reused across commits)
/// and the meta SQL connection (used for `pending_sink_state` exactly-once persistence).
pub struct IcebergPkIndexSinkCoordinator {
    sink_id: SinkId,
    db: DatabaseConnection,
    catalog: Arc<dyn Catalog>,
    table: Table,
    target_branch: String,
    retry_num: usize,
    /// The epoch pre-committed but not yet committed, carried from `pre_commit` to the next `commit`.
    waiting_commit: Option<EpochCommit>,
    prev_committed_epoch: Option<u64>,
}

impl IcebergPkIndexSinkCoordinator {
    /// Build a ready-to-serve coordinator: load the iceberg catalog/table, recover any persisted pending
    /// state, and drain recovered pending epochs to iceberg. Returns only once recovery is complete, so a
    /// successful return means the sink is ready to accept live pre-commit/commit calls.
    pub async fn init(
        sink_id: SinkId,
        iceberg_config: IcebergConfig,
        db: DatabaseConnection,
    ) -> Result<Self> {
        let (catalog, table) = timeout(INIT_TIMEOUT, load_catalog_and_table(&iceberg_config))
            .await
            .map_err(|_| {
                anyhow!(
                    "iceberg pk-index sink coordinator for sink {} timed out after {}s loading iceberg catalog/table",
                    sink_id,
                    INIT_TIMEOUT.as_secs()
                )
            })?
            .with_context(|| format!("init iceberg pk-index sink coordinator for sink {}", sink_id))?;

        let (prev_committed_epoch, recovered) =
            recovery(&db, sink_id).await.with_context(|| {
                format!(
                    "recover pending state for iceberg pk-index sink {}",
                    sink_id
                )
            })?;

        let target_branch =
            commit_branch(iceberg_config.r#type.as_str(), iceberg_config.write_mode);

        let mut coordinator = Self {
            sink_id,
            db,
            catalog,
            table,
            target_branch,
            retry_num: iceberg_config.commit_retry_num as usize,
            waiting_commit: None,
            prev_committed_epoch,
        };

        for commit in recovered {
            coordinator.waiting_commit = Some(commit);
            coordinator
                .commit()
                .await
                .with_context(|| format!("drain recovered pending epoch for sink {}", sink_id))?;
        }

        Ok(coordinator)
    }

    pub async fn pre_commit(
        &mut self,
        prev_epoch: u64,
        reports: Vec<PbIcebergPkIndexSinkMetadata>,
    ) -> Result<()> {
        if reports.iter().all(|r| r.metadata.is_none()) {
            return Ok(());
        }

        let merged = aggregate_reports(&reports)?;
        if merged.data_files.is_empty() && merged.delete_files.is_empty() {
            bail!(
                "pk-index sink epoch {} has no data files to commit",
                prev_epoch
            );
        }
        let (merged, materialized_add_files) = self.backfill_delete_file_partitions(merged)?;
        let merged = Arc::new(merged);

        let snapshot_id = FastAppendAction::generate_snapshot_id(&self.table);
        let blob = encode_pre_commit_state(&merged, snapshot_id)?;
        persist_pre_commit_metadata(&self.db, self.sink_id, prev_epoch, Some(blob), None).await?;

        self.waiting_commit = Some(EpochCommit {
            epoch: prev_epoch,
            merged,
            snapshot_id,
            materialized_add_files,
        });
        Ok(())
    }

    pub async fn commit(&mut self) -> Result<()> {
        let Some(commit) = self.waiting_commit.take() else {
            return Ok(());
        };

        let refreshed_table = commit_one_epoch(
            self.catalog.clone(),
            self.table.identifier().clone(),
            self.target_branch.clone(),
            &commit,
            self.retry_num,
        )
        .await
        .map_err(|err| {
            let err_report = match err {
                CommitError::Commit(e) | CommitError::ReloadTable(e) => e,
            };
            anyhow!(err_report).context(format!(
                "iceberg pk-index sink commit failed for sink {} epoch {}",
                self.sink_id, commit.epoch
            ))
        })?;
        self.table = refreshed_table;

        commit_and_prune_epoch(
            &self.db,
            self.sink_id,
            commit.epoch,
            self.prev_committed_epoch,
        )
        .await
        .with_context(|| {
            format!(
                "iceberg pk-index sink mark_committed failed for sink {} epoch {}",
                self.sink_id, commit.epoch
            )
        })?;

        self.prev_committed_epoch = Some(commit.epoch);
        Ok(())
    }

    /// Commit a pk-index **compaction** overwrite: replace `input_file_paths` with `output_files`
    /// via a single iceberg `overwrite_files` transaction. Reached only through
    /// [`crate::manager::iceberg_pk_index_sink::IcebergPkIndexSinkManager::commit_compaction_overwrite`],
    /// which takes the same per-sink `Mutex` as [`Self::pre_commit`]/[`Self::commit`] — this commit can
    /// therefore never race the sink's own barrier-driven commits.
    ///
    /// `read_snapshot_id` is the snapshot the compaction plan was computed against; it is not used to
    /// gate the commit (this coordinator's table keeps advancing via ordinary streaming commits, so an
    /// exact snapshot match would almost never hold). Instead, on every attempt this reloads the table
    /// and requires every path in `input_file_paths` to still be present as a live file in the
    /// CURRENT branch snapshot; if any are missing (already removed by a racing commit) the whole
    /// commit is aborted rather than silently adding the output files without removing all inputs,
    /// which would otherwise duplicate rows.
    ///
    /// TODO(B3/B4): this assumes no concurrent position/equality deletes landed on the input files
    /// between `read_snapshot_id` and commit time (no dead-row masking / index remap yet); it only
    /// guards against the input files themselves having disappeared, not against their rows having
    /// been independently marked deleted in the meantime.
    pub async fn commit_compaction_overwrite(
        &mut self,
        output_files: Vec<SerializedDataFile>,
        input_file_paths: Vec<String>,
        read_snapshot_id: i64,
    ) -> Result<()> {
        let schema_id = self.table.metadata().current_schema_id();
        let partition_spec_id = self.table.metadata().default_partition_spec_id();
        let table_ident = self.table.identifier().clone();
        let target_branch = self.target_branch.clone();
        let catalog = self.catalog.clone();

        let refreshed_table = commit_retry::run_with_retry(
            catalog.clone(),
            table_ident,
            schema_id,
            partition_spec_id,
            self.retry_num,
            |table| {
                let output_files = output_files.clone();
                let input_file_paths = input_file_paths.clone();
                let target_branch = target_branch.clone();
                let catalog = catalog.clone();
                async move {
                    commit_compaction_overwrite_once(
                        table,
                        catalog,
                        target_branch,
                        output_files,
                        input_file_paths,
                    )
                    .await
                }
            },
        )
        .await
        .with_context(|| {
            format!(
                "iceberg pk-index compaction overwrite failed for sink {} (read snapshot {})",
                self.sink_id, read_snapshot_id
            )
        })?;
        self.table = refreshed_table;
        Ok(())
    }

    /// Backfills missing partition values on the delete files (partitioned tables only) from the
    /// referenced data files. Returns the updated aggregate (with the backfilled delete files
    /// re-serialized for persistence) and, when backfill ran, the data + delete files already
    /// materialized as `DataFile`s so the commit can reuse them instead of deserializing again.
    fn backfill_delete_file_partitions(
        &self,
        merged: IcebergPkIndexSinkAggResult,
    ) -> Result<(IcebergPkIndexSinkAggResult, Option<Vec<DataFile>>)> {
        let partition_spec = self
            .table
            .metadata()
            .partition_spec_by_id(merged.partition_spec_id)
            .context("find partition spec for pk-index sink commit")?;
        if partition_spec.is_unpartitioned() {
            // No backfill needed; the commit materializes the files from serialized form once.
            return Ok((merged, None));
        }

        let schema = self.table.metadata().current_schema();
        let partition_type = partition_spec.partition_type(schema)?;
        let data_files = merged
            .data_files
            .clone()
            .into_iter()
            .map(|f| f.try_into(merged.partition_spec_id, &partition_type, schema))
            .try_collect::<Vec<DataFile>>()?;
        let mut delete_files = merged
            .delete_files
            .into_iter()
            .map(|f| f.try_into(merged.partition_spec_id, &partition_type, schema))
            .try_collect::<Vec<DataFile>>()?;
        backfill_delete_file_partitions(&data_files, &mut delete_files)?;

        let format_version = self.table.metadata().format_version();
        let serialized_delete_files = delete_files
            .iter()
            .cloned()
            .map(|f| SerializedDataFile::try_from(f, &partition_type, format_version))
            .try_collect()?;

        let merged = IcebergPkIndexSinkAggResult {
            schema_id: merged.schema_id,
            partition_spec_id: merged.partition_spec_id,
            data_files: merged.data_files,
            delete_files: serialized_delete_files,
            overwrite_files: merged.overwrite_files,
        };

        // `add_data_files` order in `commit_one_epoch` is data files followed by delete files.
        let mut materialized_add_files = data_files;
        materialized_add_files.extend(delete_files);

        Ok((merged, Some(materialized_add_files)))
    }
}

async fn load_catalog_and_table(
    iceberg_config: &IcebergConfig,
) -> Result<(Arc<dyn Catalog>, Table)> {
    let catalog = iceberg_config
        .create_catalog()
        .await
        .map_err(|e| anyhow!(e).context("create iceberg catalog for pk-index sink"))?;
    let table = iceberg_config
        .load_table()
        .await
        .map_err(|e| anyhow!(e).context("load iceberg table for pk-index sink"))?;
    Ok((catalog, table))
}

/// Read every persisted row for this sink, recovering `prev_committed_epoch` and pending commits.
async fn recovery(
    db: &DatabaseConnection,
    sink_id: SinkId,
) -> Result<(Option<u64>, Vec<EpochCommit>)> {
    fail::fail_point!("iceberg_v3_recovery_fail", |_| Err(anyhow::anyhow!(
        "injected: iceberg_v3_recovery_fail"
    )));
    let rows = list_sink_states_ordered_by_epoch(db, sink_id)
        .await
        .context("list pending sink states for pk-index sink recovery")?;

    let mut prev_committed_epoch = None;
    let mut pending = Vec::new();
    let mut aborted_epochs = Vec::new();
    for (epoch, state, metadata, _schema_change) in rows {
        match state {
            SinkState::Committed => {
                prev_committed_epoch = Some(epoch);
            }
            SinkState::Pending => {
                let blob = metadata.ok_or_else(|| {
                    anyhow!(
                        "pk-index sink pending row at epoch {} missing metadata blob",
                        epoch
                    )
                })?;
                let (merged, snapshot_id) = decode_pre_commit_state(&blob).with_context(|| {
                    format!("decode pk-index sink pre-commit state at epoch {}", epoch)
                })?;
                pending.push(EpochCommit {
                    epoch,
                    merged,
                    snapshot_id,
                    // Recovered from the persisted blob; the commit materializes files from
                    // `merged` once.
                    materialized_add_files: None,
                });
            }
            SinkState::Aborted => {
                // V3 doesn't produce Aborted rows; tolerate them defensively and drop them so they don't
                // accumulate across restarts.
                tracing::warn!(
                    sink_id = %sink_id,
                    epoch,
                    "unexpected Aborted state in pk-index sink recovery; cleaning up",
                );
                aborted_epochs.push(epoch);
            }
        }
    }
    if !aborted_epochs.is_empty()
        && let Err(e) = clean_aborted_records(db, sink_id, aborted_epochs).await
    {
        // Best-effort cleanup; defer to next recovery if the DB rejects.
        tracing::warn!(
            error = %e.as_report(),
            sink_id = %sink_id,
            "failed to clean unexpected Aborted rows during pk-index sink recovery",
        );
    }
    Ok((prev_committed_epoch, pending))
}

async fn commit_one_epoch(
    catalog: Arc<dyn Catalog>,
    table_ident: iceberg::TableIdent,
    target_branch: String,
    commit: &EpochCommit,
    retry_num: usize,
) -> Result<Table, CommitError> {
    let merged = commit.merged.clone();
    let snapshot_id = commit.snapshot_id;
    let materialized_add_files = commit.materialized_add_files.clone();

    commit_retry::run_with_retry(
        catalog.clone(),
        table_ident,
        merged.schema_id,
        merged.partition_spec_id,
        retry_num,
        |table| {
            let merged = merged.clone();
            let catalog = catalog.clone();
            let target_branch = target_branch.clone();
            let materialized_add_files = materialized_add_files.clone();
            async move {
                // Idempotency: if iceberg already saw this `snapshot_id`, skip the overwrite_files transaction.
                if table
                    .metadata()
                    .snapshots()
                    .any(|s| s.snapshot_id() == snapshot_id)
                {
                    return Ok(table);
                }

                let schema = table.metadata().current_schema();
                let partition_type =
                    resolve_partition_type(&table, merged.partition_spec_id, schema)
                        .map_err(|e| CommitError::Commit(anyhow!(e)))?;

                let materialize =
                    |serialized: &SerializedDataFile| -> Result<DataFile, CommitError> {
                        serialized
                            .clone()
                            .try_into(merged.partition_spec_id, &partition_type, schema)
                            .map_err(|err| {
                                CommitError::Commit(
                                    anyhow!(err)
                                        .context("materialize pk-index sink SerializedDataFile"),
                                )
                            })
                    };

                // Reuse the files already materialized during pre-commit backfill when available;
                // otherwise (recovery / unpartitioned) materialize from the persisted form once.
                let add_files: Vec<DataFile> = match materialized_add_files {
                    Some(add_files) => add_files,
                    None => merged
                        .data_files
                        .iter()
                        .chain(merged.delete_files.iter())
                        .map(&materialize)
                        .collect::<Result<Vec<_>, _>>()?,
                };
                let overwrite_files: Vec<DataFile> = merged
                    .overwrite_files
                    .iter()
                    .map(&materialize)
                    .collect::<Result<Vec<_>, _>>()?;

                let txn = Transaction::new(&table);
                let action = txn
                    .overwrite_files()
                    .set_snapshot_id(snapshot_id)
                    .set_target_branch(target_branch)
                    .add_data_files(add_files)
                    .delete_files(overwrite_files);
                let txn = action.apply(txn).map_err(|err| {
                    CommitError::Commit(
                        anyhow!(err).context("apply iceberg pk-index sink overwrite_files action"),
                    )
                })?;
                let table = txn.commit(catalog.as_ref()).await.map_err(|err| {
                    CommitError::Commit(
                        anyhow!(err).context("commit iceberg pk-index sink transaction"),
                    )
                })?;
                Ok(table)
            }
        },
    )
    .await
    .map_err(CommitError::Commit)
}

/// Run one attempt of the pk-index compaction overwrite against the freshly-reloaded `table`.
/// Walks the target branch's CURRENT manifest list to find the live [`DataFile`] entries matching
/// `input_file_paths` (matched by path, regardless of content type — data compaction can also
/// replace delete files); fails if any requested path is no longer present (see
/// [`IcebergPkIndexSinkCoordinator::commit_compaction_overwrite`] for the rationale).
async fn commit_compaction_overwrite_once(
    table: Table,
    catalog: Arc<dyn Catalog>,
    target_branch: String,
    output_files: Vec<SerializedDataFile>,
    input_file_paths: Vec<String>,
) -> std::result::Result<Table, CommitError> {
    let mut remaining: HashSet<&str> = input_file_paths.iter().map(String::as_str).collect();
    let mut removed: Vec<DataFile> = Vec::with_capacity(input_file_paths.len());

    if let Some(snapshot) = table.metadata().snapshot_for_ref(&target_branch) {
        let manifest_list = snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .map_err(|e| CommitError::Commit(anyhow!(e).context("load manifest list")))?;

        'outer: for manifest_file in manifest_list.entries() {
            let manifest = manifest_file
                .load_manifest(table.file_io())
                .await
                .map_err(|e| CommitError::Commit(anyhow!(e).context("load manifest")))?;
            for entry in manifest.entries() {
                if remaining.remove(entry.file_path()) {
                    removed.push(entry.data_file().clone());
                }
                if remaining.is_empty() {
                    break 'outer;
                }
            }
        }
    }

    if !remaining.is_empty() {
        // Some requested input files are no longer live on the target branch: a racing commit
        // (e.g. a second compaction of the same sink) must already have replaced them. Committing
        // the output files without removing all inputs would duplicate rows, so abort instead;
        // `run_with_retry` will retry with a fresh reload, and if the mismatch persists the caller
        // surfaces the error so the compaction task is retried from a fresh plan.
        return Err(CommitError::Commit(anyhow!(
            "pk-index compaction overwrite: {} of {} input files are no longer present on branch {}",
            remaining.len(),
            input_file_paths.len(),
            target_branch,
        )));
    }

    let schema = table.metadata().current_schema();
    let partition_spec_id = table.metadata().default_partition_spec_id();
    let partition_type = resolve_partition_type(&table, partition_spec_id, schema)
        .map_err(|e| CommitError::Commit(anyhow!(e)))?;
    let add_files: Vec<DataFile> = output_files
        .into_iter()
        .map(|f| {
            f.try_into(partition_spec_id, &partition_type, schema)
                .map_err(|e| {
                    CommitError::Commit(
                        anyhow!(e).context("materialize pk-index compaction output file"),
                    )
                })
        })
        .collect::<std::result::Result<_, _>>()?;

    let txn = Transaction::new(&table);
    let action = txn
        .overwrite_files()
        .set_target_branch(target_branch)
        .add_data_files(add_files)
        .delete_files(removed);
    let txn = action.apply(txn).map_err(|err| {
        CommitError::Commit(
            anyhow!(err).context("apply iceberg pk-index compaction overwrite action"),
        )
    })?;
    txn.commit(catalog.as_ref()).await.map_err(|err| {
        CommitError::Commit(
            anyhow!(err).context("commit iceberg pk-index compaction overwrite transaction"),
        )
    })
}

#[derive(Clone, Serialize, Deserialize)]
struct IcebergPkIndexSinkAggResult {
    schema_id: i32,
    partition_spec_id: i32,
    data_files: Vec<SerializedDataFile>,
    delete_files: Vec<SerializedDataFile>,
    overwrite_files: Vec<SerializedDataFile>,
}

fn aggregate_reports(
    reports: &[PbIcebergPkIndexSinkMetadata],
) -> Result<IcebergPkIndexSinkAggResult> {
    let mut shared_schema_id: Option<i32> = None;
    let mut shared_partition_spec_id: Option<i32> = None;

    let mut data_files: Vec<SerializedDataFile> = Vec::new();
    let mut delete_files: Vec<SerializedDataFile> = Vec::new();
    let mut overwrite_files: Vec<SerializedDataFile> = Vec::new();

    if reports.is_empty() {
        bail!("no reports to aggregate for iceberg pk-index sink coordinator");
    }

    for r in reports {
        let Some(meta) = &r.metadata else {
            bail!("iceberg pk-index sink report missing metadata in aggregate_reports");
        };

        // Validate role: explicitly-Unspecified is a wire-format bug.
        let role = PbIcebergPkIndexSinkRole::try_from(r.role)
            .ok()
            .filter(|r| !matches!(r, PbIcebergPkIndexSinkRole::Unspecified))
            .ok_or_else(|| anyhow!("iceberg pk-index sink report has invalid role: {}", r.role))?;

        match role {
            PbIcebergPkIndexSinkRole::Writer => {
                let commit_result = IcebergCommitResult::try_from(meta)?;
                align_report_id(
                    commit_result.schema_id,
                    commit_result.partition_spec_id,
                    &mut shared_schema_id,
                    &mut shared_partition_spec_id,
                )?;
                data_files.extend(commit_result.data_files);
            }
            PbIcebergPkIndexSinkRole::PositionDeleteMerger => {
                let commit_result = IcebergPositionDeleteMergerCommitResult::try_from(meta)
                    .map_err(|e| {
                        anyhow!(e).context("decode pk-index sink position-delete merger metadata")
                    })?;
                align_report_id(
                    commit_result.schema_id,
                    commit_result.partition_spec_id,
                    &mut shared_schema_id,
                    &mut shared_partition_spec_id,
                )?;
                delete_files.extend(commit_result.delete_files);
                overwrite_files.extend(commit_result.overwrite_files);
            }
            _ => unreachable!(),
        }
    }

    Ok(IcebergPkIndexSinkAggResult {
        schema_id: shared_schema_id.unwrap(),
        partition_spec_id: shared_partition_spec_id.unwrap(),
        data_files,
        delete_files,
        overwrite_files,
    })
}

fn align_report_id(
    schema_id: i32,
    partition_spec_id: i32,
    shared_schema_id: &mut Option<i32>,
    shared_partition_spec_id: &mut Option<i32>,
) -> Result<()> {
    match shared_schema_id {
        Some(prev) if *prev != schema_id => {
            bail!(
                "iceberg pk-index sink reports disagree on schema_id: {} vs {}",
                prev,
                schema_id
            );
        }
        None => *shared_schema_id = Some(schema_id),
        _ => {}
    }
    match shared_partition_spec_id {
        Some(prev) if *prev != partition_spec_id => {
            bail!(
                "iceberg pk-index sink reports disagree on partition_spec_id: {} vs {}",
                prev,
                partition_spec_id
            );
        }
        None => *shared_partition_spec_id = Some(partition_spec_id),
        _ => {}
    }
    Ok(())
}

fn encode_pre_commit_state(
    agg_result: &IcebergPkIndexSinkAggResult,
    snapshot_id: i64,
) -> Result<Vec<u8>> {
    let agg_result = serde_json::to_vec(agg_result)?;
    Ok(PbIcebergPkIndexPreCommitState {
        agg_result,
        snapshot_id,
    }
    .encode_to_vec())
}

fn decode_pre_commit_state(blob: &[u8]) -> Result<(Arc<IcebergPkIndexSinkAggResult>, i64)> {
    let state = PbIcebergPkIndexPreCommitState::decode(blob)?;
    let agg_result = Arc::new(serde_json::from_slice(&state.agg_result)?);
    Ok((agg_result, state.snapshot_id))
}
