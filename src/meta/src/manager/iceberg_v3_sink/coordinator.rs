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

//! Per-sink Iceberg V3 commit coordinator. This is a plain struct (no background task / mpsc): the
//! [`crate::manager::iceberg_v3_sink::IcebergV3SinkManager`] holds one per registered sink behind a
//! per-sink async mutex and calls [`IcebergV3Coordinator::pre_commit`] / [`IcebergV3Coordinator::commit`]
//! directly from the barrier-completion path. The barrier path already serializes pre-commit/commit per
//! epoch, so the coordinator never needs its own request queue.
//!
//! [`IcebergV3Coordinator::init`] is synchronous with respect to registration: it loads the iceberg
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

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use iceberg::Catalog;
use iceberg::spec::{DataFile, FormatVersion, SerializedDataFile};
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, FastAppendAction, Transaction};
use prost::Message;
use risingwave_connector::sink::catalog::SinkId;
use risingwave_connector::sink::iceberg::commit_retry::{self, CommitError};
use risingwave_connector::sink::iceberg::{
    IcebergCommitResult, IcebergConfig, IcebergDvMergerCommitResult, commit_branch,
};
use risingwave_meta_model::pending_sink_state::SinkState;
use risingwave_pb::connector_service::PbIcebergV3PreCommitState;
use risingwave_pb::stream_service::PbIcebergV3SinkRole;
use risingwave_pb::stream_service::barrier_complete_response::PbIcebergV3SinkMetadata;
use sea_orm::DatabaseConnection;
use serde::{Deserialize, Serialize};
use thiserror_ext::AsReport;
use tokio::time::timeout;

use super::backfill::backfill_dv_partitions;
use super::compaction_resolver::collect_compaction_delete_files;
use crate::manager::exactly_once_util::{
    clean_aborted_records, commit_and_prune_epoch, list_sink_states_ordered_by_epoch,
    persist_pre_commit_metadata,
};

/// Bound the init phase so a register call can't hang forever if the iceberg endpoint is unreachable;
/// on timeout `init` returns an error and registration fails (the caller surfaces it / retries).
const INIT_TIMEOUT: Duration = Duration::from_secs(60);

/// One epoch's worth of pre-committed state queued inside the coordinator. Holds the decoded merged file
/// list and the pre-generated `snapshot_id`. The blob form ([`PbIcebergV3PreCommitState`]) is only
/// materialized when persisting to `pending_sink_state`; in-memory we keep the structured form.
#[derive(Clone)]
struct EpochCommit {
    epoch: u64,
    merged: Arc<IcebergV3AggResult>,
    snapshot_id: i64,
}

/// State for an in-flight compaction-resolve window, armed at ATTACH time and consumed when the
/// bracketing checkpoint's `Resolver`-done report arrives. Threaded from the compaction manager's
/// `route_v3_compaction_report` (which holds these from the compaction report) so the coordinator can
/// turn the bracketing epoch's commit into an OVERWRITE without re-deriving anything from the report.
#[derive(Clone)]
struct PendingCompaction {
    /// Compaction OUTPUT data files (decoded from the report's `v3_output_files`). Added by the
    /// overwrite, alongside the bracketing epoch's DvMerger DVs.
    output_files: Vec<SerializedDataFile>,
    /// Input data-file paths consumed by this compaction; the overwrite's delete set
    /// (input data files + their `N` Puffin DVs) is re-derived from these by path.
    input_file_paths: Vec<String>,
}

/// Per-sink Iceberg V3 commit coordinator. Owns the loaded iceberg catalog/table (reused across commits)
/// and the meta SQL connection (used for `pending_sink_state` exactly-once persistence).
pub struct IcebergV3Coordinator {
    sink_id: SinkId,
    db: DatabaseConnection,
    catalog: Arc<dyn Catalog>,
    table: Table,
    target_branch: String,
    retry_num: usize,
    /// The epoch pre-committed but not yet committed, carried from `pre_commit` to the next `commit`.
    waiting_commit: Option<EpochCommit>,
    prev_committed_epoch: Option<u64>,
    /// Armed at compaction ATTACH; `Some` while a resolve window is open. Consumed when the
    /// bracketing epoch's `Resolver`-done report turns that epoch's commit into an overwrite.
    pending_compaction: Option<PendingCompaction>,
}

impl IcebergV3Coordinator {
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
                    "iceberg v3 coordinator for sink {} timed out after {}s loading iceberg catalog/table",
                    sink_id,
                    INIT_TIMEOUT.as_secs()
                )
            })?
            .with_context(|| format!("init iceberg v3 coordinator for sink {}", sink_id))?;

        let (prev_committed_epoch, recovered) = recovery(&db, sink_id)
            .await
            .with_context(|| format!("recover pending state for iceberg v3 sink {}", sink_id))?;

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
            pending_compaction: None,
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
        reports: Vec<PbIcebergV3SinkMetadata>,
    ) -> Result<()> {
        // The transient resolve pipeline's leaf reports `Resolver` (role only, no metadata) at the
        // bracketing checkpoint once its scan is exhausted. Its presence — together with an armed
        // `pending_compaction` — marks THIS epoch as the one whose commit must become the coordinated
        // overwrite (inputs out, compaction outputs in). Resolver reports carry no file metadata, and
        // `aggregate_reports` requires every report to have metadata, so strip both the Resolver report
        // and any other metadata-less report before aggregation.
        let resolver_done = reports.iter().any(|r| {
            PbIcebergV3SinkRole::try_from(r.role)
                .is_ok_and(|role| matches!(role, PbIcebergV3SinkRole::Resolver))
        });
        let reports: Vec<PbIcebergV3SinkMetadata> = reports
            .into_iter()
            .filter(|r| {
                r.metadata.is_some()
                    && !PbIcebergV3SinkRole::try_from(r.role)
                        .is_ok_and(|role| matches!(role, PbIcebergV3SinkRole::Resolver))
            })
            .collect();

        // Whether THIS epoch is the one that folds the armed compaction overwrite into its commit.
        let should_fold = resolver_done && self.pending_compaction.is_some();

        // Nothing to commit only when there is neither fresh per-epoch metadata NOR an armed
        // compaction to fold. The fold path must NOT be short-circuited by an empty epoch: an
        // all-survivors resolve repairs only the (Hummock) pk-index and emits no new iceberg files,
        // so the writer's bracketing-epoch report is empty — yet the overwrite (outputs in, inputs
        // out) still has to commit, otherwise the resolve window never closes and the writer wedges.
        if reports.is_empty() && !should_fold {
            return Ok(());
        }

        // Build the per-epoch merged result. When the bracketing epoch carries no metadata of its own
        // (the all-survivors case above), start from an empty result keyed to the table's current
        // schema and the default (unpartitioned) spec — the resolver only runs on unpartitioned
        // tables — so `fold_compaction_into_merged` can supply the overwrite.
        let merged = if reports.is_empty() {
            IcebergV3AggResult {
                schema_id: self.table.metadata().current_schema_id(),
                partition_spec_id: self.table.metadata().default_partition_spec_id(),
                data_files: vec![],
                delete_files: vec![],
                overwrite_files: vec![],
            }
        } else {
            let merged = aggregate_reports(&reports)?;
            self.backfill_dv_partitions(merged)?
        };

        // At the bracketing checkpoint, fold the held compaction state into this epoch's commit so
        // the single overwrite atomically (a) adds the compaction outputs + this epoch's fresh DVs and
        // (b) removes the inputs being compacted away. Done in the SAME commit slot as the per-epoch
        // append so there is no extra snapshot and exactly-once persistence covers it unchanged.
        let merged = if should_fold {
            self.fold_compaction_into_merged(merged).await?
        } else {
            merged
        };

        if merged.data_files.is_empty()
            && merged.delete_files.is_empty()
            && merged.overwrite_files.is_empty()
        {
            bail!("v3 sink epoch {} has no data files to commit", prev_epoch);
        }
        let merged = Arc::new(merged);

        let snapshot_id = FastAppendAction::generate_snapshot_id(&self.table);
        let blob = encode_pre_commit_state(&merged, snapshot_id)?;
        persist_pre_commit_metadata(&self.db, self.sink_id, prev_epoch, Some(blob), None).await?;

        self.waiting_commit = Some(EpochCommit {
            epoch: prev_epoch,
            merged,
            snapshot_id,
        });
        Ok(())
    }

    /// Fold the armed [`PendingCompaction`] into an epoch's merged report so the per-epoch commit slot
    /// performs the coordinated overwrite. `add` gains the compaction output data files; `overwrite`
    /// (delete) gains the input data files + their `N` Puffin DVs, re-derived from the held input paths
    /// by reading only manifest METADATA (never data bodies). The bracketing epoch's own DvMerger DVs
    /// stay in `merged.delete_files` (added DVs) untouched. Consumes `pending_compaction` on success.
    async fn fold_compaction_into_merged(
        &mut self,
        mut merged: IcebergV3AggResult,
    ) -> Result<IcebergV3AggResult> {
        let pending = self
            .pending_compaction
            .as_ref()
            .expect("caller checks pending_compaction is_some")
            .clone();

        // Serialize the manifest-derived delete DataFiles back to `SerializedDataFile` so they ride the
        // same pre-commit persistence path as every other file; they are re-materialized at commit time
        // against the (idempotency-pinned) schema/partition spec in `commit_one_epoch`.
        let delete_data_files =
            collect_compaction_delete_files(&self.table, &pending.input_file_paths)
                .await
                .context("derive overwrite delete-set for v3 compaction")?;
        let partition_type = self
            .table
            .metadata()
            .partition_spec_by_id(merged.partition_spec_id)
            .context("find partition spec for v3 compaction overwrite")?
            .partition_type(self.table.metadata().current_schema())?;
        let serialized_deletes = delete_data_files
            .into_iter()
            .map(|f| SerializedDataFile::try_from(f, &partition_type, FormatVersion::V3))
            .try_collect::<Vec<_>>()
            .context("serialize v3 compaction delete files")?;

        merged.data_files.extend(pending.output_files);
        merged.overwrite_files.extend(serialized_deletes);

        // Resolution succeeded for this window; clear it so a later spurious Resolver report (e.g. on a
        // recovery replay) does not re-fold an already-applied compaction.
        self.pending_compaction = None;

        tracing::info!(
            sink_id = %self.sink_id,
            "V3 compaction folded into epoch commit as overwrite"
        );
        Ok(merged)
    }

    /// Arm a compaction-resolve window at ATTACH time. The next bracketing-checkpoint `Resolver`-done
    /// report folds the held outputs/inputs into that epoch's commit as an overwrite.
    pub fn arm_compaction(
        &mut self,
        output_files: Vec<SerializedDataFile>,
        input_file_paths: Vec<String>,
    ) {
        self.pending_compaction = Some(PendingCompaction {
            output_files,
            input_file_paths,
        });
    }

    /// Whether a compaction-resolve window is currently armed but not yet folded into a commit.
    pub fn has_pending_compaction(&self) -> bool {
        self.pending_compaction.is_some()
    }

    pub async fn commit(&mut self) -> Result<()> {
        let Some(commit) = self.waiting_commit.take() else {
            return Ok(());
        };

        let refreshed_table = commit_one_epoch(
            &self.catalog,
            &self.table,
            &self.target_branch,
            &commit,
            self.retry_num,
        )
        .await
        .map_err(|err| {
            let err_report = match err {
                CommitError::Commit(e) | CommitError::ReloadTable(e) => e,
            };
            anyhow!(err_report).context(format!(
                "iceberg v3 commit failed for sink {} epoch {}",
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
                "iceberg v3 mark_committed failed for sink {} epoch {}",
                self.sink_id, commit.epoch
            )
        })?;

        self.prev_committed_epoch = Some(commit.epoch);
        Ok(())
    }

    fn backfill_dv_partitions(&self, merged: IcebergV3AggResult) -> Result<IcebergV3AggResult> {
        let partition_spec = self
            .table
            .metadata()
            .partition_spec_by_id(merged.partition_spec_id)
            .context("find partition spec for v3 commit")?;
        if partition_spec.is_unpartitioned() {
            return Ok(merged);
        }

        let schema = self.table.metadata().current_schema();
        let partition_type = partition_spec.partition_type(schema)?;
        let data_files = merged
            .data_files
            .clone()
            .into_iter()
            .map(|f| f.try_into(merged.partition_spec_id, &partition_type, schema))
            .try_collect::<Vec<_>>()?;
        let mut delete_files = merged
            .delete_files
            .into_iter()
            .map(|f| f.try_into(merged.partition_spec_id, &partition_type, schema))
            .try_collect::<Vec<_>>()?;
        backfill_dv_partitions(&data_files, &mut delete_files)?;
        let delete_files = delete_files
            .into_iter()
            .map(|f| SerializedDataFile::try_from(f, &partition_type, FormatVersion::V3))
            .try_collect()?;

        Ok(IcebergV3AggResult {
            schema_id: merged.schema_id,
            partition_spec_id: merged.partition_spec_id,
            data_files: merged.data_files,
            delete_files,
            overwrite_files: merged.overwrite_files,
        })
    }
}

async fn load_catalog_and_table(
    iceberg_config: &IcebergConfig,
) -> Result<(Arc<dyn Catalog>, Table)> {
    let catalog = iceberg_config
        .create_catalog()
        .await
        .map_err(|e| anyhow!(e).context("create iceberg catalog for v3 sink"))?;
    let table = iceberg_config
        .load_table()
        .await
        .map_err(|e| anyhow!(e).context("load iceberg table for v3 sink"))?;
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
        .context("list pending sink states for v3 recovery")?;

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
                    anyhow!("v3 pending row at epoch {} missing metadata blob", epoch)
                })?;
                let (merged, snapshot_id) = decode_pre_commit_state(&blob)
                    .with_context(|| format!("decode v3 pre-commit state at epoch {}", epoch))?;
                pending.push(EpochCommit {
                    epoch,
                    merged,
                    snapshot_id,
                });
            }
            SinkState::Aborted => {
                // V3 doesn't produce Aborted rows; tolerate them defensively and drop them so they don't
                // accumulate across restarts.
                tracing::warn!(
                    sink_id = %sink_id,
                    epoch,
                    "unexpected Aborted state in v3 recovery; cleaning up",
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
            "failed to clean unexpected Aborted rows during v3 recovery",
        );
    }
    Ok((prev_committed_epoch, pending))
}

async fn commit_one_epoch(
    catalog: &Arc<dyn Catalog>,
    table: &Table,
    target_branch: &str,
    commit: &EpochCommit,
    retry_num: usize,
) -> Result<Table, CommitError> {
    let merged = commit.merged.clone();

    // Materialize the pre-committed `SerializedDataFile`s once against the current schema/partition
    // type. `run_with_retry` (inside `commit_overwrite`) asserts the reloaded table keeps the same
    // `schema_id`/`partition_spec_id` across attempts, so the materialization is stable across retries
    // and matches what the previous in-loop materialization produced.
    let schema = table.metadata().current_schema();
    let partition_spec = table
        .metadata()
        .partition_spec_by_id(merged.partition_spec_id)
        .ok_or_else(|| CommitError::Commit(anyhow!("partition spec not found")))?;
    let partition_type = partition_spec
        .partition_type(schema)
        .map_err(|e| CommitError::Commit(anyhow!(e)))?;

    let mut add_files: Vec<DataFile> = Vec::new();
    let mut overwrite_files: Vec<DataFile> = Vec::new();
    for serialized in merged.data_files.iter().chain(merged.delete_files.iter()) {
        let f = serialized
            .clone()
            .try_into(merged.partition_spec_id, &partition_type, schema)
            .map_err(|err| {
                CommitError::Commit(anyhow!(err).context("materialize v3 SerializedDataFile"))
            })?;
        add_files.push(f);
    }
    for serialized in &merged.overwrite_files {
        let f = serialized
            .clone()
            .try_into(merged.partition_spec_id, &partition_type, schema)
            .map_err(|err| {
                CommitError::Commit(anyhow!(err).context("materialize v3 SerializedDataFile"))
            })?;
        overwrite_files.push(f);
    }

    commit_overwrite(
        catalog,
        table.identifier().clone(),
        merged.schema_id,
        merged.partition_spec_id,
        target_branch,
        commit.snapshot_id,
        add_files,
        overwrite_files,
        retry_num,
    )
    .await
}

/// Run an idempotent iceberg `overwrite_files` transaction for the given pre-materialized add/delete
/// files, keyed on `snapshot_id`, with reload+retry. Used by `commit_one_epoch` (the per-epoch commit
/// path, including the fold-compaction overwrite) so the idempotency check and the transaction
/// shape live in one place. Returns the committed table, or the (unchanged) reloaded table when
/// `snapshot_id` was already applied.
async fn commit_overwrite(
    catalog: &Arc<dyn Catalog>,
    table_ident: iceberg::TableIdent,
    schema_id: i32,
    partition_spec_id: i32,
    target_branch: &str,
    snapshot_id: i64,
    add_files: Vec<DataFile>,
    delete_files: Vec<DataFile>,
    retry_num: usize,
) -> Result<Table, CommitError> {
    let target_branch = target_branch.to_owned();

    commit_retry::run_with_retry(
        catalog.clone(),
        table_ident,
        schema_id,
        partition_spec_id,
        retry_num,
        |table| {
            let add_files = add_files.clone();
            let delete_files = delete_files.clone();
            let catalog = catalog.clone();
            let target_branch = target_branch.clone();
            async move {
                // Idempotency: if iceberg already saw this `snapshot_id`, skip the overwrite_files transaction.
                if table
                    .metadata()
                    .snapshots()
                    .any(|s| s.snapshot_id() == snapshot_id)
                {
                    return Ok(table);
                }

                let txn = Transaction::new(&table);
                let action = txn
                    .overwrite_files()
                    .set_snapshot_id(snapshot_id)
                    .set_target_branch(target_branch)
                    .add_data_files(add_files)
                    .delete_files(delete_files);
                let txn = action.apply(txn).map_err(|err| {
                    CommitError::Commit(
                        anyhow!(err).context("apply iceberg v3 overwrite_files action"),
                    )
                })?;
                let table = txn.commit(catalog.as_ref()).await.map_err(|err| {
                    CommitError::Commit(anyhow!(err).context("commit iceberg v3 transaction"))
                })?;
                Ok(table)
            }
        },
    )
    .await
    .map_err(CommitError::Commit)
}

#[derive(Clone, Serialize, Deserialize)]
struct IcebergV3AggResult {
    schema_id: i32,
    partition_spec_id: i32,
    data_files: Vec<SerializedDataFile>,
    delete_files: Vec<SerializedDataFile>,
    overwrite_files: Vec<SerializedDataFile>,
}

fn aggregate_reports(reports: &[PbIcebergV3SinkMetadata]) -> Result<IcebergV3AggResult> {
    let mut shared_schema_id: Option<i32> = None;
    let mut shared_partition_spec_id: Option<i32> = None;

    let mut data_files: Vec<SerializedDataFile> = Vec::new();
    let mut delete_files: Vec<SerializedDataFile> = Vec::new();
    let mut overwrite_files: Vec<SerializedDataFile> = Vec::new();

    if reports.is_empty() {
        bail!("no reports to aggregate for iceberg v3 coordinator");
    }

    for r in reports {
        let Some(meta) = &r.metadata else {
            bail!("iceberg v3 sink report missing metadata in aggregate_reports");
        };

        // Validate role: explicitly-Unspecified is a wire-format bug.
        let role = PbIcebergV3SinkRole::try_from(r.role)
            .ok()
            .filter(|r| !matches!(r, PbIcebergV3SinkRole::Unspecified))
            .ok_or_else(|| anyhow!("iceberg v3 sink report has invalid role: {}", r.role))?;

        match role {
            PbIcebergV3SinkRole::Writer => {
                let commit_result = IcebergCommitResult::try_from(meta)?;
                align_report_id(
                    commit_result.schema_id,
                    commit_result.partition_spec_id,
                    &mut shared_schema_id,
                    &mut shared_partition_spec_id,
                )?;
                data_files.extend(commit_result.data_files);
            }
            PbIcebergV3SinkRole::DvMerger => {
                let commit_result = IcebergDvMergerCommitResult::try_from(meta)
                    .map_err(|e| anyhow!(e).context("decode v3 dv merger metadata"))?;
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

    Ok(IcebergV3AggResult {
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
                "iceberg v3 sink reports disagree on schema_id: {} vs {}",
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
                "iceberg v3 sink reports disagree on partition_spec_id: {} vs {}",
                prev,
                partition_spec_id
            );
        }
        None => *shared_partition_spec_id = Some(partition_spec_id),
        _ => {}
    }
    Ok(())
}

fn encode_pre_commit_state(agg_result: &IcebergV3AggResult, snapshot_id: i64) -> Result<Vec<u8>> {
    let agg_result = serde_json::to_vec(agg_result)?;
    Ok(PbIcebergV3PreCommitState {
        agg_result,
        snapshot_id,
    }
    .encode_to_vec())
}

fn decode_pre_commit_state(blob: &[u8]) -> Result<(Arc<IcebergV3AggResult>, i64)> {
    let state = PbIcebergV3PreCommitState::decode(blob)?;
    let agg_result = Arc::new(serde_json::from_slice(&state.agg_result)?);
    Ok((agg_result, state.snapshot_id))
}
