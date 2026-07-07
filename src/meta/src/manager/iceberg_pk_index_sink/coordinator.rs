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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use iceberg::Catalog;
use iceberg::delete_vector::DeleteVector;
use iceberg::spec::{
    DataContentType, DataFile, DataFileFormat, ManifestContentType, PartitionKey,
    SerializedDataFile,
};
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, FastAppendAction, Transaction};
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use prost::Message;
use risingwave_connector::sink::catalog::SinkId;
use risingwave_connector::sink::iceberg::commit_retry::{self, CommitError};
use risingwave_connector::sink::iceberg::{
    IcebergCommitResult, IcebergConfig, IcebergPositionDeleteMergerCommitResult,
    RowProvenanceEntry, commit_branch, read_dv_positions_from_data_file, resolve_partition_spec,
    resolve_partition_type, write_dv_puffin_file,
};
use risingwave_meta_model::pending_sink_state::SinkState;
use risingwave_pb::connector_service::PbIcebergPkIndexPreCommitState;
use risingwave_pb::stream_service::PbIcebergPkIndexSinkRole;
use risingwave_pb::stream_service::barrier_complete_response::PbIcebergPkIndexSinkMetadata;
use sea_orm::DatabaseConnection;
use serde::{Deserialize, Serialize};
use thiserror_ext::AsReport;
use tokio::time::timeout;
use uuid::Uuid;

use super::backfill::backfill_delete_file_partitions;
use crate::manager::exactly_once_util::{
    clean_aborted_records, clear_pending_remap, commit_and_prune_epoch, list_pending_remaps,
    list_sink_states_ordered_by_epoch, persist_pending_remap, persist_pre_commit_metadata,
};

/// A durable pending remap that must be re-injected as an `IcebergPkIndexRemap` barrier mutation on
/// meta startup. Returned from [`IcebergPkIndexSinkCoordinator::init`] up to the recovery caller,
/// which owns the barrier scheduler; the coordinator itself never touches the scheduler.
pub struct PendingRemapReTrigger {
    pub remap_id: i64,
    pub mapping_paths: Vec<String>,
}

/// In-memory mirror of a durable pending-remap row. A record is cleared (here and in the durable
/// `iceberg_pk_index_pending_remap` table) ONLY when the writer reports `REMAP_DONE` for its
/// `remap_id` — i.e. when the writer has durably applied the remap. It is never cleared by
/// inspecting the iceberg snapshot: the overwrite removes the input files immediately, long before
/// the writer remaps, so a snapshot-based clear would drop the record while the index is still stale.
///
/// While the record is present the steady-state commit path uses `input_files` + `mapping_paths` to
/// remap in-window stray position-delete DVs (see [`remap_in_window_position_deletes`]): a delete of
/// a just-compacted PK before the writer remaps still resolves the pk-index to a REMOVED input file,
/// so the writer emits a DV against that removed file. Left as-is it masks nothing and the deleted
/// row resurrects; the fix-up rewrites it into an OUTPUT DV.
struct PendingRemapState {
    remap_id: i64,
    /// Removed input file paths of this remap (DATA + R-era DV paths; only DATA paths ever match a
    /// stray DV's `referenced_data_file`). Used to detect stray in-window DVs.
    input_files: HashSet<String>,
    /// Row-provenance NDJSON paths, read lazily by the fix-up only when a stray DV is found.
    mapping_paths: Vec<String>,
}

/// The subset of a [`PendingRemapState`] the steady-state commit needs to remap in-window stray
/// position-delete DVs. Built from `pending_remaps` and passed into [`commit_one_epoch`]; cloned per
/// retry attempt so each attempt runs against its own freshly-reloaded snapshot.
#[derive(Clone)]
struct PendingRemapFixup {
    input_files: HashSet<String>,
    mapping_paths: Vec<String>,
}

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
    /// Durable pending compaction remaps not yet acknowledged by a writer `REMAP_DONE`. Populated
    /// at `init` (from the `iceberg_pk_index_pending_remap` table) and by
    /// `commit_compaction_overwrite`; drained only by `clear_pending_remap`, driven by the writer's
    /// `REMAP_DONE` report routed through meta.
    pending_remaps: Vec<PendingRemapState>,
}

impl IcebergPkIndexSinkCoordinator {
    /// Build a ready-to-serve coordinator: load the iceberg catalog/table, recover any persisted pending
    /// state, and drain recovered pending epochs to iceberg. Returns only once recovery is complete, so a
    /// successful return means the sink is ready to accept live pre-commit/commit calls.
    pub async fn init(
        sink_id: SinkId,
        iceberg_config: IcebergConfig,
        db: DatabaseConnection,
    ) -> Result<(Self, Vec<PendingRemapReTrigger>)> {
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
            pending_remaps: Vec::new(),
        };

        for commit in recovered {
            coordinator.waiting_commit = Some(commit);
            coordinator
                .commit()
                .await
                .with_context(|| format!("drain recovered pending epoch for sink {}", sink_id))?;
        }

        // Reload durable pending compaction remaps. A record persists until the writer reports
        // `REMAP_DONE`, so every still-present record is re-injected as a barrier mutation by the
        // recovery caller (which owns the barrier scheduler). The writer remap is idempotent, so
        // re-running an already-applied remap is a no-op, and its `REMAP_DONE` then clears the record.
        let re_trigger = coordinator
            .reload_pending_remaps()
            .await
            .with_context(|| format!("reload pending pk-index remaps for sink {}", sink_id))?;

        Ok((coordinator, re_trigger))
    }

    /// Load every durable pending remap for this sink, keep them all in memory, and return the full
    /// list to be re-injected as barrier mutations. Records are cleared ONLY by a writer `REMAP_DONE`
    /// (see [`Self::clear_pending_remap`]); recovery never infers completion from the snapshot, so it
    /// re-triggers all of them unconditionally (idempotent on the writer side).
    async fn reload_pending_remaps(&mut self) -> Result<Vec<PendingRemapReTrigger>> {
        let records = list_pending_remaps(&self.db, self.sink_id).await?;
        let mut re_trigger = Vec::with_capacity(records.len());
        for record in records {
            re_trigger.push(PendingRemapReTrigger {
                remap_id: record.remap_id,
                mapping_paths: record.mapping_paths.clone(),
            });
            self.pending_remaps.push(PendingRemapState {
                remap_id: record.remap_id,
                input_files: record.input_files.into_iter().collect(),
                mapping_paths: record.mapping_paths,
            });
        }
        Ok(re_trigger)
    }

    /// Clear a durable pending remap once its writer has reported `REMAP_DONE` (the remap's rewrites
    /// are durably applied). This is the ONLY place a pending remap is cleared. Idempotent: clearing
    /// a missing `(sink_id, remap_id)` is a harmless no-op, so a duplicate `REMAP_DONE` — or one for
    /// a record already cleared on a prior run — costs nothing.
    pub async fn clear_pending_remap(&mut self, remap_id: i64) -> Result<()> {
        clear_pending_remap(&self.db, self.sink_id, remap_id).await?;
        self.pending_remaps.retain(|p| p.remap_id != remap_id);
        Ok(())
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

        // Snapshot the still-pending compaction remaps for the in-window fix-up. Empty in the
        // common steady state, in which case `commit_one_epoch` short-circuits with zero overhead.
        let pending_remaps: Vec<PendingRemapFixup> = self
            .pending_remaps
            .iter()
            .map(|p| PendingRemapFixup {
                input_files: p.input_files.clone(),
                mapping_paths: p.mapping_paths.clone(),
            })
            .collect();

        let refreshed_table = commit_one_epoch(
            self.catalog.clone(),
            self.table.identifier().clone(),
            self.target_branch.clone(),
            &commit,
            self.retry_num,
            pending_remaps,
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
    /// `mapping_paths` are the per-plan spilled row-provenance NDJSON files (each line an
    /// `{input_file, input_pos, output_file, output_pos}` mapping). They drive B3 dead-row masking:
    /// any row deleted on an input file between `read_snapshot_id` and commit time is remapped to its
    /// output position and masked with an OUTPUT position-delete (DV) file folded into the same
    /// overwrite commit. See [`commit_compaction_overwrite_once`].
    pub async fn commit_compaction_overwrite(
        &mut self,
        remap_id: i64,
        output_files: Vec<SerializedDataFile>,
        input_file_paths: Vec<String>,
        mapping_paths: Vec<String>,
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
                let mapping_paths = mapping_paths.clone();
                let target_branch = target_branch.clone();
                let catalog = catalog.clone();
                async move {
                    commit_compaction_overwrite_once(
                        table,
                        catalog,
                        target_branch,
                        output_files,
                        input_file_paths,
                        mapping_paths,
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

        // Durably record the pending remap BEFORE returning to the caller (which then enqueues the
        // `IcebergPkIndexRemap` barrier mutation). This persist-before-enqueue ordering makes a crash
        // in the window between the overwrite commit and the mutation delivery recoverable: on meta
        // startup `reload_pending_remaps` re-injects the mutation. The record is cleared only when the
        // writer reports `REMAP_DONE`; the writer remap is idempotent, so a re-injected mutation that
        // already applied is a no-op and still produces the `REMAP_DONE` that clears the record.
        persist_pending_remap(
            &self.db,
            self.sink_id,
            remap_id,
            &mapping_paths,
            &input_file_paths,
        )
        .await
        .with_context(|| {
            format!(
                "persist pending pk-index remap {} for sink {}",
                remap_id, self.sink_id
            )
        })?;
        self.pending_remaps.push(PendingRemapState {
            remap_id,
            input_files: input_file_paths.iter().cloned().collect(),
            mapping_paths,
        });
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
    pending_remaps: Vec<PendingRemapFixup>,
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
            let pending_remaps = pending_remaps.clone();
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
                let mut overwrite_files: Vec<DataFile> = merged
                    .overwrite_files
                    .iter()
                    .map(&materialize)
                    .collect::<Result<Vec<_>, _>>()?;

                // In-window fix-up: while a compaction remap is pending, a delete of a
                // just-compacted PK still resolves the pk-index to a REMOVED input file, so the
                // writer emits a position-delete DV against that removed file. Committed as-is it
                // masks nothing (the file is gone) and the deleted row resurrects. Rewrite each such
                // stray DV into an OUTPUT DV masking the remapped positions on the live output data
                // file. Returns the fixed `add_files` plus any pre-existing output DVs to supersede
                // (merged into the new DV so a data file never ends up with two DVs). Fast path:
                // with no pending remap this returns immediately, adding zero steady-state overhead.
                let (add_files, superseded) = remap_in_window_position_deletes(
                    &table,
                    &target_branch,
                    add_files,
                    &pending_remaps,
                )
                .await?;
                overwrite_files.extend(superseded);

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

/// Run one attempt of the pk-index compaction overwrite against the freshly-reloaded `table`
/// (snapshot `N`). Everything below runs against this single reloaded snapshot so the input-file
/// resolution, the DV read at `N`, the dead-row masking, and the overwrite are all consistent per
/// attempt.
///
/// Steps:
/// 1. Walk `N`'s manifest list for the live [`DataFile`] entries matching `input_file_paths` and
///    add them to the overwrite's remove set. `input_file_paths` mixes input DATA files with the
///    R-era input delete/DV file paths. Only the input **DATA** files are required to still be
///    live at `N`: if any input data file was already removed by a racing commit we abort (adding
///    the outputs on top of rewritten rows would duplicate them). Input **delete/DV** paths do NOT
///    gate the overwrite — a concurrent `(R, N]` delete can supersede an R-era DV, leaving its
///    path `Deleted` at `N`; requiring it live would thrash forever on repeatedly-deleted rows.
///    Their removal is handled by step 1 (if still live) and the `DV_N` walk in step 3.
/// 2. **B3 dead-row masking:** collect the Puffin position-delete (DV) files at `N` that reference
///    the input data files, read their deleted positions, and remap each to its output position via
///    the spilled row-provenance `mapping_paths`. A deleted input position with NO mapping entry was
///    already dead when the rewrite read snapshot `R` (its row was never written to any output), so
///    it is skipped; a deleted input position WITH a mapping entry is a row that was alive at `R`
///    (hence written to output) but deleted during `(R, N]`, so it must be masked. Author one output
///    DV Puffin file per output data file with dead rows and fold them into the same overwrite.
/// 3. Also remove those input DV files (they reference the removed input data files and must not
///    dangle), deduped against the input files already resolved in step 1.
async fn commit_compaction_overwrite_once(
    table: Table,
    catalog: Arc<dyn Catalog>,
    target_branch: String,
    output_files: Vec<SerializedDataFile>,
    input_file_paths: Vec<String>,
    mapping_paths: Vec<String>,
) -> std::result::Result<Table, CommitError> {
    // Input DATA files that must still be live at N. Starts as every input path; entries that turn
    // out to be delete/DV files (position/equality deletes) are dropped from this set regardless of
    // their liveness, so only the input data files remain required-live after the walk. Anything
    // still here at the end is an input data file a racing commit removed -> abort.
    let mut required_live_data: HashSet<&str> =
        input_file_paths.iter().map(String::as_str).collect();
    let mut removed: Vec<DataFile> = Vec::with_capacity(input_file_paths.len());
    // Puffin DV files at N that reference one of the input data files. Both read for masking and
    // removed from the overwrite so they don't dangle after their referenced data files go away.
    let mut input_dv_files: Vec<DataFile> = Vec::new();

    let input_paths: HashSet<&str> = input_file_paths.iter().map(String::as_str).collect();

    if let Some(snapshot) = table.metadata().snapshot_for_ref(&target_branch) {
        let manifest_list = snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .map_err(|e| CommitError::Commit(anyhow!(e).context("load manifest list")))?;

        for manifest_file in manifest_list.entries() {
            let manifest = manifest_file
                .load_manifest(table.file_io())
                .await
                .map_err(|e| CommitError::Commit(anyhow!(e).context("load manifest")))?;
            for entry in manifest.entries() {
                let data_file = entry.data_file();
                let path = data_file.file_path();

                // Liveness relaxation: an input path whose manifest entry is a delete/DV file
                // (position or equality delete) must NOT gate the overwrite, whether it is still
                // live or already `Deleted` at N. A concurrent (R, N] delete supersedes a prior
                // R-era DV, leaving that R-era path `Deleted` at N; requiring it live would abort
                // and thrash forever on repeatedly-deleted rows. Drop such input paths from the
                // required-live set here (classified from any entry, alive or not); their removal
                // is handled by step 1 below (if still live) or the DV_N walk in step 2/3.
                let is_delete_content = matches!(
                    data_file.content_type(),
                    DataContentType::PositionDeletes | DataContentType::EqualityDeletes,
                );
                if is_delete_content && input_paths.contains(path) {
                    required_live_data.remove(path);
                }

                if !entry.is_alive() {
                    // A `Deleted` entry keeps the same `file_path()` as when it was live; the
                    // `is_alive()` guard prevents a racing commit's stale entry from being treated
                    // as a still-live input (which would otherwise let us add the compaction
                    // outputs on top of rewritten rows). Only used above for delete classification.
                    continue;
                }

                // Step 1: remove every live input file (data files, and still-live input delete
                // files) in the overwrite. Data files also clear the required-live set.
                if input_paths.contains(path) {
                    required_live_data.remove(path);
                    removed.push(data_file.clone());
                }

                // Step 2/3: collect the live Puffin DV files (at N) referencing an input data file.
                // These include DVs created during (R, N] that are NOT in `input_file_paths` (the
                // plan only saw R-era files), so this is a separate collection from step 1.
                if manifest_file.content == ManifestContentType::Deletes
                    && data_file.content_type() == DataContentType::PositionDeletes
                    && let Some(referenced) = data_file.referenced_data_file()
                    && input_paths.contains(referenced.as_str())
                {
                    // Coordinated pk-index compaction assumes V3/Puffin deletion vectors. A live
                    // non-Puffin position delete (e.g. a Parquet V2 positional delete) referencing
                    // an input data file would be silently ignored here and dangle/miss a delete;
                    // fail explicitly instead.
                    if data_file.file_format() != DataFileFormat::Puffin {
                        return Err(CommitError::Commit(anyhow!(
                            "pk-index compaction masking: position-delete file {} referencing \
                             input data file {} is {:?}, not Puffin; coordinated pk-index \
                             compaction requires Puffin deletion vectors",
                            path,
                            referenced,
                            data_file.file_format(),
                        )));
                    }
                    input_dv_files.push(data_file.clone());
                }
            }
        }
    }

    if !required_live_data.is_empty() {
        // Some input DATA files are no longer live on the target branch: a racing commit (e.g. a
        // second compaction of the same sink) must already have replaced them. Committing the
        // output files without removing all input data files would duplicate rows, so abort
        // instead; `run_with_retry` retries with a fresh reload, and if the mismatch persists the
        // caller surfaces the error so the compaction task is retried from a fresh plan.
        return Err(CommitError::Commit(anyhow!(
            "pk-index compaction overwrite: {} of {} input files (data files) are no longer \
             present on branch {}",
            required_live_data.len(),
            input_file_paths.len(),
            target_branch,
        )));
    }

    let schema = table.metadata().current_schema();
    let partition_spec_id = table.metadata().default_partition_spec_id();
    let partition_type = resolve_partition_type(&table, partition_spec_id, schema)
        .map_err(|e| CommitError::Commit(anyhow!(e)))?;
    let mut add_files: Vec<DataFile> = output_files
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

    // --- B3: author output DV files masking rows deleted on the inputs during (R, N]. ---
    let mapping = read_row_provenance_mapping(&table, &mapping_paths).await?;
    // Masking depends on a non-empty row-provenance mapping. An empty mapping (no `mapping_paths`,
    // or every spilled file decoded to zero entries) while we are committing output data files
    // means masking is silently disabled: any row deleted on an input during (R, N] would resurface
    // in the compaction output. Fail the commit rather than commit unmasked.
    if !add_files.is_empty() && mapping.is_empty() {
        return Err(CommitError::Commit(anyhow!(
            "pk-index compaction masking: row-provenance mapping is empty (from {} mapping file(s)) \
             but the overwrite has {} output data file(s) to commit; refusing to commit without \
             masking",
            mapping_paths.len(),
            add_files.len(),
        )));
    }
    let deleted_inputs = read_input_dv_positions(&table, &input_dv_files).await?;
    let dead_by_output = compute_dead_out_positions(&mapping, deleted_inputs);
    if !dead_by_output.is_empty() {
        let output_dv_files = author_output_dv_files(&table, &add_files, dead_by_output).await?;
        add_files.extend(output_dv_files);
    }

    // Remove the input DV files too (they reference the now-removed input data files). Some may
    // already be in `removed` if they were R-era DVs listed in `input_file_paths`; dedup by path.
    let already_removed: HashSet<String> =
        removed.iter().map(|f| f.file_path().to_owned()).collect();
    for dv in input_dv_files {
        if !already_removed.contains(dv.file_path()) {
            removed.push(dv);
        }
    }

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

/// Reads and merges the per-plan spilled row-provenance NDJSON files into a lookup keyed by
/// `(input_file, input_pos)` → `(output_file, output_pos)`. Read through the coordinator's own
/// `FileIO` (same catalog/table the compactor spilled through).
async fn read_row_provenance_mapping(
    table: &Table,
    mapping_paths: &[String],
) -> std::result::Result<HashMap<(String, u64), (String, u64)>, CommitError> {
    let mut mapping: HashMap<(String, u64), (String, u64)> = HashMap::new();
    for path in mapping_paths {
        let input = table.file_io().new_input(path).map_err(|e| {
            CommitError::Commit(anyhow!(e).context(format!("open row-provenance file {}", path)))
        })?;
        let bytes = input.read().await.map_err(|e| {
            CommitError::Commit(anyhow!(e).context(format!("read row-provenance file {}", path)))
        })?;
        for line in bytes.split(|b| *b == b'\n') {
            if line.is_empty() {
                continue;
            }
            let entry: RowProvenanceEntry = serde_json::from_slice(line).map_err(|e| {
                CommitError::Commit(
                    anyhow!(e).context(format!("parse row-provenance entry in {}", path)),
                )
            })?;
            mapping.insert(
                (entry.input_file, entry.input_pos),
                (entry.output_file, entry.output_pos),
            );
        }
    }
    Ok(mapping)
}

/// Reads the deleted positions from the given input DV Puffin files, returning `(input_file, pos)`
/// pairs (`DV_N` restricted to the input data files). Each file's `referenced_data_file` is the input
/// data file the positions belong to (guaranteed set by the collection in
/// [`commit_compaction_overwrite_once`]).
async fn read_input_dv_positions(
    table: &Table,
    input_dv_files: &[DataFile],
) -> std::result::Result<Vec<(String, u64)>, CommitError> {
    let mut deleted_inputs = Vec::new();
    for dv in input_dv_files {
        let referenced = dv
            .referenced_data_file()
            .expect("collected input DV file must carry referenced_data_file");
        let positions = read_dv_positions_from_data_file(table.file_io(), dv)
            .await
            .map_err(|e| {
                CommitError::Commit(anyhow!(e).context("read input DV positions for masking"))
            })?;
        for pos in positions.iter() {
            deleted_inputs.push((referenced.clone(), pos));
        }
    }
    Ok(deleted_inputs)
}

/// Pure remap: group the deleted input `(file, pos)` positions by the output file/position they map
/// to. An input position with no mapping entry was already dead at read snapshot `R` (pre-`R`
/// delete) — its row was never written to any output file — and is skipped.
fn compute_dead_out_positions(
    mapping: &HashMap<(String, u64), (String, u64)>,
    deleted_inputs: impl IntoIterator<Item = (String, u64)>,
) -> HashMap<String, DeleteVector> {
    let mut dead_by_output: HashMap<String, DeleteVector> = HashMap::new();
    for (input_file, input_pos) in deleted_inputs {
        if let Some((output_file, output_pos)) = mapping.get(&(input_file, input_pos)) {
            dead_by_output
                .entry(output_file.clone())
                .or_default()
                .insert(*output_pos);
        }
    }
    dead_by_output
}

/// A still-pending compaction remap's removed input file set + its parsed row-provenance mapping,
/// as consumed by the pure in-window fix-up core [`plan_in_window_dv_remap`].
struct PendingRemapMapping {
    input_files: HashSet<String>,
    mapping: HashMap<(String, u64), (String, u64)>,
}

/// If `data_file` is a position-delete DV whose referenced data file was removed by one of the
/// pending remaps, return that remap's index. Returns `None` for data files, equality deletes, and
/// DVs referencing a still-live (non-input) data file — those pass through the fix-up untouched.
fn stray_dv_owner(data_file: &DataFile, input_sets: &[&HashSet<String>]) -> Option<usize> {
    if data_file.content_type() != DataContentType::PositionDeletes {
        return None;
    }
    let referenced = data_file.referenced_data_file()?;
    input_sets
        .iter()
        .position(|s| s.contains(referenced.as_str()))
}

/// Pure core of the in-window position-delete fix-up. Splits `add_files` into the files to keep and
/// the stray position-delete DVs (those referencing a REMOVED compaction input data file), and
/// remaps each stray DV's deleted positions — supplied by `dv_positions`, keyed by the DV's file
/// path — to OUTPUT positions via the owning remap's mapping. Returns the surviving `add_files`
/// (stray DVs dropped) and the `output_file -> DeleteVector` mask to author as replacement DVs.
///
/// A stray position with no mapping entry was already dead at read snapshot `R` (its row was never
/// written to any output) and is skipped — but its stray DV is still dropped: it must not be
/// committed against a removed file. A DV referencing a still-live (non-input) data file, or any
/// data file, is kept untouched. With no pending remap `stray_dv_owner` never matches, so every file
/// is kept and the mask is empty (the async caller guards this even earlier).
fn plan_in_window_dv_remap(
    add_files: Vec<DataFile>,
    pending: &[PendingRemapMapping],
    dv_positions: &HashMap<String, Vec<u64>>,
) -> (Vec<DataFile>, HashMap<String, DeleteVector>) {
    let input_sets: Vec<&HashSet<String>> = pending.iter().map(|p| &p.input_files).collect();
    let mut kept = Vec::with_capacity(add_files.len());
    let mut dead_by_output: HashMap<String, DeleteVector> = HashMap::new();
    for f in add_files {
        let Some(idx) = stray_dv_owner(&f, &input_sets) else {
            kept.push(f);
            continue;
        };
        let referenced = f
            .referenced_data_file()
            .expect("stray_dv_owner guarantees referenced_data_file");
        let positions = dv_positions.get(f.file_path()).cloned().unwrap_or_default();
        let deleted_inputs = positions.into_iter().map(|pos| (referenced.clone(), pos));
        for (out_file, dv) in compute_dead_out_positions(&pending[idx].mapping, deleted_inputs) {
            let entry = dead_by_output.entry(out_file).or_default();
            for pos in dv.iter() {
                entry.insert(pos);
            }
        }
        // Stray DV dropped (not pushed to `kept`).
    }
    (kept, dead_by_output)
}

/// Steady-state in-window fix-up. Rewrites any position-delete DV in `add_files` that still
/// references a REMOVED compaction input data file (a delete that landed before the writer applied
/// its pk-index remap) into an OUTPUT DV masking the remapped positions on the live output data
/// file. Without this the stray DV commits against a file absent from the snapshot, masks nothing,
/// and the deleted row resurrects.
///
/// Returns `(add_files, superseded)`: the rewritten add set (stray DVs dropped, replacement output
/// DVs appended) and any pre-existing live output DVs that were merged into a replacement and must
/// be superseded (added to the overwrite's delete set) so no output data file ends up referenced by
/// two DVs.
///
/// Fast path: with no pending remap (the overwhelmingly common steady state) this returns after a
/// single `is_empty` check — zero overhead on the hot path.
async fn remap_in_window_position_deletes(
    table: &Table,
    target_branch: &str,
    add_files: Vec<DataFile>,
    pending: &[PendingRemapFixup],
) -> std::result::Result<(Vec<DataFile>, Vec<DataFile>), CommitError> {
    if pending.is_empty() {
        return Ok((add_files, Vec::new()));
    }

    let input_sets: Vec<&HashSet<String>> = pending.iter().map(|p| &p.input_files).collect();

    // Identify stray DVs and read their deleted positions; record which pending remaps own a stray
    // so only their provenance mappings are read.
    let mut dv_positions: HashMap<String, Vec<u64>> = HashMap::new();
    let mut owning: HashSet<usize> = HashSet::new();
    for f in &add_files {
        if let Some(idx) = stray_dv_owner(f, &input_sets) {
            owning.insert(idx);
            let positions = read_dv_positions_from_data_file(table.file_io(), f)
                .await
                .map_err(|e| {
                    CommitError::Commit(anyhow!(e).context("read in-window stray DV positions"))
                })?;
            dv_positions.insert(f.file_path().to_owned(), positions.iter().collect());
        }
    }

    if owning.is_empty() {
        // A remap is pending but no in-flight DV references its removed inputs — nothing to do.
        return Ok((add_files, Vec::new()));
    }

    // Read the row-provenance mapping only for the pending remaps that actually own a stray DV.
    let mut parsed: Vec<PendingRemapMapping> = Vec::with_capacity(pending.len());
    for (i, p) in pending.iter().enumerate() {
        let mapping = if owning.contains(&i) {
            read_row_provenance_mapping(table, &p.mapping_paths).await?
        } else {
            HashMap::new()
        };
        parsed.push(PendingRemapMapping {
            input_files: p.input_files.clone(),
            mapping,
        });
    }

    let (mut kept, mut dead_by_output) = plan_in_window_dv_remap(add_files, &parsed, &dv_positions);

    if dead_by_output.is_empty() {
        // Every stray position was already dead at R (no mapping entry): the strays are dropped and
        // there is nothing to mask.
        return Ok((kept, Vec::new()));
    }

    // BIGGEST RISK resolution: the replacement DV must carry the OUTPUT data file's real
    // partition/spec as it exists in the LIVE snapshot. The output files are NOT in this commit's
    // `add_files` — they were committed by the earlier compaction overwrite — so resolve each from
    // the current branch manifests (cloning the live manifest entry preserves its exact partition
    // value + spec id, so this is correct for partitioned tables too, not just unpartitioned). Also
    // collect any existing live DV already referencing an output file (a compaction-time B3 mask or
    // an earlier in-window fix-up) so its positions are merged in and it is superseded — otherwise
    // two DVs would reference one data file and a delete could be lost.
    let needed: HashSet<String> = dead_by_output.keys().cloned().collect();
    let (output_data_files, existing_dvs) =
        resolve_output_files_and_existing_dvs(table, target_branch, &needed).await?;

    for dv in &existing_dvs {
        let referenced = dv
            .referenced_data_file()
            .expect("collected existing output DV must carry referenced_data_file");
        let positions = read_dv_positions_from_data_file(table.file_io(), dv)
            .await
            .map_err(|e| {
                CommitError::Commit(
                    anyhow!(e).context("read existing output DV positions for in-window merge"),
                )
            })?;
        let entry = dead_by_output.entry(referenced).or_default();
        for pos in positions.iter() {
            entry.insert(pos);
        }
    }

    let resolved: Vec<DataFile> = output_data_files.into_values().collect();
    let output_dv_files = author_output_dv_files(table, &resolved, dead_by_output).await?;
    kept.extend(output_dv_files);

    Ok((kept, existing_dvs))
}

/// Walk the CURRENT branch snapshot's manifests to resolve, for each path in `needed`, the live
/// output data file's materialized [`DataFile`] (for the replacement DV's partition/spec) and any
/// live position-delete DV already referencing it (to merge + supersede). Errors if a needed output
/// data file is not live in the snapshot — it must be, since the compaction overwrite committed it.
async fn resolve_output_files_and_existing_dvs(
    table: &Table,
    target_branch: &str,
    needed: &HashSet<String>,
) -> std::result::Result<(HashMap<String, DataFile>, Vec<DataFile>), CommitError> {
    let mut out_files: HashMap<String, DataFile> = HashMap::new();
    let mut existing_dvs: Vec<DataFile> = Vec::new();

    if let Some(snapshot) = table.metadata().snapshot_for_ref(target_branch) {
        let manifest_list = snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .map_err(|e| {
                CommitError::Commit(
                    anyhow!(e).context("load manifest list for output DV resolution"),
                )
            })?;
        for manifest_file in manifest_list.entries() {
            let manifest = manifest_file
                .load_manifest(table.file_io())
                .await
                .map_err(|e| {
                    CommitError::Commit(
                        anyhow!(e).context("load manifest for output DV resolution"),
                    )
                })?;
            for entry in manifest.entries() {
                if !entry.is_alive() {
                    continue;
                }
                let data_file = entry.data_file();
                match data_file.content_type() {
                    DataContentType::Data => {
                        if needed.contains(data_file.file_path()) {
                            out_files.insert(data_file.file_path().to_owned(), data_file.clone());
                        }
                    }
                    DataContentType::PositionDeletes => {
                        if let Some(referenced) = data_file.referenced_data_file()
                            && needed.contains(referenced.as_str())
                        {
                            existing_dvs.push(data_file.clone());
                        }
                    }
                    DataContentType::EqualityDeletes => {}
                }
            }
        }
    }

    for path in needed {
        if !out_files.contains_key(path) {
            return Err(CommitError::Commit(anyhow!(
                "pk-index in-window fix-up: output data file {} (target of a remapped in-flight \
                 delete) is not live in the current snapshot on branch {}",
                path,
                target_branch,
            )));
        }
    }

    Ok((out_files, existing_dvs))
}

/// Authors one output DV Puffin file per output data file that has dead rows, referencing the
/// output data file so the masked positions are excluded on read. The output data files are fresh
/// (just written by the rewrite), so there is no existing DV to merge.
async fn author_output_dv_files(
    table: &Table,
    output_data_files: &[DataFile],
    dead_by_output: HashMap<String, DeleteVector>,
) -> std::result::Result<Vec<DataFile>, CommitError> {
    let location_generator =
        DefaultLocationGenerator::new(table.metadata().clone()).map_err(|e| {
            CommitError::Commit(anyhow!(e).context("build location generator for output DV"))
        })?;
    let file_name_generator = DefaultFileNameGenerator::new(
        "pk-index-compaction".to_owned(),
        Some(format!("delvec-{}", Uuid::now_v7())),
        DataFileFormat::Puffin,
    );
    let partitioned = !table.metadata().default_partition_spec().is_unpartitioned();

    // Map output data file path -> its materialized DataFile (for the partition key of the DV).
    let by_path: HashMap<&str, &DataFile> = output_data_files
        .iter()
        .map(|f| (f.file_path(), f))
        .collect();

    let mut output_dv_files = Vec::with_capacity(dead_by_output.len());
    for (output_file, delete_vector) in dead_by_output {
        let data_file = by_path.get(output_file.as_str()).ok_or_else(|| {
            // Every mapping output_file must be one of the rewrite's output data files.
            CommitError::Commit(anyhow!(
                "pk-index compaction masking: output file {} referenced by row-provenance mapping \
                 is not among the rewrite output files",
                output_file
            ))
        })?;
        let partition_key = if partitioned {
            Some(build_partition_key(table, data_file)?)
        } else {
            None
        };
        let dv_file = write_dv_puffin_file(
            table,
            &location_generator,
            &file_name_generator,
            output_file,
            delete_vector,
            partition_key.as_ref(),
        )
        .await
        .map_err(|e| CommitError::Commit(anyhow!(e).context("author output DV puffin file")))?;
        output_dv_files.push(dv_file);
    }
    Ok(output_dv_files)
}

fn build_partition_key(
    table: &Table,
    data_file: &DataFile,
) -> std::result::Result<PartitionKey, CommitError> {
    let partition_spec = resolve_partition_spec(table, data_file.partition_spec_id())
        .map_err(|e| CommitError::Commit(anyhow!(e).context("resolve partition spec for DV")))?;
    Ok(PartitionKey::new(
        partition_spec.as_ref().clone(),
        table.metadata().current_schema().clone(),
        data_file.partition().clone(),
    ))
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
            PbIcebergPkIndexSinkRole::RemapDone => {
                // `REMAP_DONE` reports are stripped at the meta routing layer
                // (`pre_commit_iceberg_pk_index_sink_metadata`) and must never reach the aggregate.
                bail!(
                    "unexpected RemapDone report in aggregate_reports; it should be routed to \
                     clear_pending_remap before reaching pre-commit"
                );
            }
            PbIcebergPkIndexSinkRole::Unspecified => unreachable!("filtered above"),
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

#[cfg(test)]
mod tests {
    use iceberg::spec::{DataFileBuilder, Struct};

    use super::*;

    fn data_file(path: &str) -> DataFile {
        DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(path.to_owned())
            .file_format(DataFileFormat::Parquet)
            .partition(Struct::empty())
            .partition_spec_id(0)
            .record_count(1)
            .file_size_in_bytes(1)
            .build()
            .expect("data file build")
    }

    fn dv_file(path: &str, referenced: &str) -> DataFile {
        DataFileBuilder::default()
            .content(DataContentType::PositionDeletes)
            .file_path(path.to_owned())
            .file_format(DataFileFormat::Puffin)
            .partition(Struct::empty())
            .partition_spec_id(0)
            .record_count(1)
            .file_size_in_bytes(1)
            .referenced_data_file(Some(referenced.to_owned()))
            .build()
            .expect("dv file build")
    }

    fn pending_remap(
        input_files: &[&str],
        mapping_entries: &[(&str, u64, &str, u64)],
    ) -> PendingRemapMapping {
        PendingRemapMapping {
            input_files: input_files.iter().map(|s| s.to_string()).collect(),
            mapping: mapping(mapping_entries),
        }
    }

    fn mapping(entries: &[(&str, u64, &str, u64)]) -> HashMap<(String, u64), (String, u64)> {
        entries
            .iter()
            .map(|(in_file, in_pos, out_file, out_pos)| {
                (
                    (in_file.to_string(), *in_pos),
                    (out_file.to_string(), *out_pos),
                )
            })
            .collect()
    }

    fn positions(dv: &DeleteVector) -> Vec<u64> {
        dv.iter().collect()
    }

    /// A deleted input position that has a provenance mapping entry (a row alive at R, hence written
    /// to output, then deleted during (R, N]) is remapped to its output position; positions are
    /// grouped by output file.
    #[test]
    fn dead_out_remaps_and_groups_by_output_file() {
        let mapping = mapping(&[
            ("in-a.parquet", 0, "out-0.parquet", 5),
            ("in-a.parquet", 1, "out-0.parquet", 6),
            ("in-b.parquet", 0, "out-1.parquet", 2),
        ]);
        let deleted_inputs = vec![
            ("in-a.parquet".to_owned(), 0),
            ("in-a.parquet".to_owned(), 1),
            ("in-b.parquet".to_owned(), 0),
        ];

        let dead = compute_dead_out_positions(&mapping, deleted_inputs);

        assert_eq!(dead.len(), 2);
        assert_eq!(positions(dead.get("out-0.parquet").unwrap()), vec![5, 6]);
        assert_eq!(positions(dead.get("out-1.parquet").unwrap()), vec![2]);
    }

    /// A deleted input position with NO mapping entry was already dead at read snapshot R (pre-R
    /// delete); its row was never written to any output, so it must be skipped (not masked).
    #[test]
    fn dead_out_skips_deleted_input_without_mapping_entry() {
        let mapping = mapping(&[("in-a.parquet", 0, "out-0.parquet", 5)]);
        let deleted_inputs = vec![
            ("in-a.parquet".to_owned(), 0), // mapped -> masked
            ("in-a.parquet".to_owned(), 7), // pre-R delete, no mapping entry -> skipped
            ("in-c.parquet".to_owned(), 3), // unrelated input file, no mapping entry -> skipped
        ];

        let dead = compute_dead_out_positions(&mapping, deleted_inputs);

        assert_eq!(dead.len(), 1);
        assert_eq!(positions(dead.get("out-0.parquet").unwrap()), vec![5]);
    }

    /// Two distinct input files can each contribute a dead row to the SAME output file (compaction
    /// merges many inputs into one output). Both remapped positions must land in that output file's
    /// delete vector.
    #[test]
    fn dead_out_merges_two_inputs_into_same_output_file() {
        let mapping = mapping(&[
            ("in-a.parquet", 3, "out-0.parquet", 10),
            ("in-b.parquet", 7, "out-0.parquet", 20),
        ]);
        let deleted_inputs = vec![
            ("in-a.parquet".to_owned(), 3),
            ("in-b.parquet".to_owned(), 7),
        ];

        let dead = compute_dead_out_positions(&mapping, deleted_inputs);

        assert_eq!(dead.len(), 1);
        assert_eq!(positions(dead.get("out-0.parquet").unwrap()), vec![10, 20]);
    }

    /// No concurrent deletes -> nothing to mask.
    #[test]
    fn dead_out_empty_when_no_deleted_inputs() {
        let mapping = mapping(&[("in-a.parquet", 0, "out-0.parquet", 5)]);
        let dead = compute_dead_out_positions(&mapping, Vec::new());
        assert!(dead.is_empty());
    }

    fn kept_paths(kept: &[DataFile]) -> HashSet<&str> {
        kept.iter().map(|f| f.file_path()).collect()
    }

    /// Core of the in-window fix-up: a stray position-delete DV referencing a REMOVED input data
    /// file is dropped from `add_files` and its deleted position is remapped to the output file's
    /// position to mask; a DV referencing a still-live (non-input) data file and a plain data file
    /// are both kept untouched.
    #[test]
    fn in_window_remap_replaces_stray_dv_and_keeps_others() {
        let add_files = vec![
            data_file("out-0.parquet"),
            dv_file("dv-stray.puff", "in-a.parquet"),
            dv_file("dv-live.puff", "live-x.parquet"),
        ];
        let pending = vec![pending_remap(
            &["in-a.parquet"],
            &[("in-a.parquet", 3, "out-0.parquet", 9)],
        )];
        let dv_positions = HashMap::from([
            ("dv-stray.puff".to_owned(), vec![3]),
            ("dv-live.puff".to_owned(), vec![1]),
        ]);

        let (kept, dead) = plan_in_window_dv_remap(add_files, &pending, &dv_positions);

        let paths = kept_paths(&kept);
        assert_eq!(paths.len(), 2);
        assert!(paths.contains("out-0.parquet"));
        assert!(paths.contains("dv-live.puff"));
        assert!(!paths.contains("dv-stray.puff"));

        assert_eq!(dead.len(), 1);
        assert_eq!(positions(dead.get("out-0.parquet").unwrap()), vec![9]);
    }

    /// Fast path: with no pending remap the fix-up core is a no-op — every file is kept and there is
    /// nothing to mask (the async caller short-circuits even earlier on the same emptiness).
    #[test]
    fn in_window_remap_fast_path_no_pending_is_noop() {
        let add_files = vec![
            data_file("out-0.parquet"),
            dv_file("dv-a.puff", "in-a.parquet"),
        ];
        let dv_positions = HashMap::new();

        let (kept, dead) = plan_in_window_dv_remap(add_files, &[], &dv_positions);

        assert_eq!(kept_paths(&kept).len(), 2);
        assert!(dead.is_empty());
    }

    /// A stray DV whose only deleted position has NO mapping entry (a delete of a row already dead
    /// at R) is still dropped — it must not commit against a removed file — but contributes no mask.
    #[test]
    fn in_window_remap_drops_stray_with_only_pre_r_positions() {
        let add_files = vec![dv_file("dv-stray.puff", "in-a.parquet")];
        let pending = vec![pending_remap(
            &["in-a.parquet"],
            &[("in-a.parquet", 3, "out-0.parquet", 9)],
        )];
        let dv_positions = HashMap::from([("dv-stray.puff".to_owned(), vec![7])]);

        let (kept, dead) = plan_in_window_dv_remap(add_files, &pending, &dv_positions);

        assert!(kept.is_empty());
        assert!(dead.is_empty());
    }

    /// Two disjoint pending remaps compose: each stray DV is remapped against whichever remap owns
    /// its referenced input file.
    #[test]
    fn in_window_remap_composes_disjoint_pending_remaps() {
        let add_files = vec![
            dv_file("dv-a.puff", "in-a.parquet"),
            dv_file("dv-b.puff", "in-b.parquet"),
        ];
        let pending = vec![
            pending_remap(
                &["in-a.parquet"],
                &[("in-a.parquet", 0, "out-0.parquet", 5)],
            ),
            pending_remap(
                &["in-b.parquet"],
                &[("in-b.parquet", 1, "out-1.parquet", 8)],
            ),
        ];
        let dv_positions = HashMap::from([
            ("dv-a.puff".to_owned(), vec![0]),
            ("dv-b.puff".to_owned(), vec![1]),
        ]);

        let (kept, dead) = plan_in_window_dv_remap(add_files, &pending, &dv_positions);

        assert!(kept.is_empty());
        assert_eq!(dead.len(), 2);
        assert_eq!(positions(dead.get("out-0.parquet").unwrap()), vec![5]);
        assert_eq!(positions(dead.get("out-1.parquet").unwrap()), vec![8]);
    }
}
