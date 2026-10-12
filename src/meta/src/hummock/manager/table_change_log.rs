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

use std::collections::{HashMap, HashSet};
use std::time::Instant;

use anyhow::{Context, anyhow};
use futures::{StreamExt, TryStreamExt};
use risingwave_common::array::DataChunk;
use risingwave_common::bitmap::Bitmap;
use risingwave_common::catalog::{TableDesc, TableId};
use risingwave_common::hash::VnodeCountCompat;
use risingwave_common::row::Row;
use risingwave_common::util::epoch::Epoch;
use risingwave_hummock_sdk::change_log::TableChangeLog;
use risingwave_hummock_sdk::version::HummockVersion;
use risingwave_pb::batch_plan::exchange_info::DistributionMode;
use risingwave_pb::batch_plan::plan_node::NodeBody;
use risingwave_pb::batch_plan::{ExchangeInfo, PlanFragment, PlanNode, RowSeqScanNode, TaskId};
use risingwave_pb::common::{
    BatchQueryCommittedEpoch, BatchQueryEpoch, WorkerNode as PbWorkerNode, batch_query_epoch,
};
use risingwave_pb::plan_common::ExprContext;
use risingwave_pb::task_service::ExecuteRequest;
use sea_orm::{
    ColumnTrait, Condition, ConnectionTrait, DbBackend, EntityTrait, FromQueryResult, QueryFilter,
    QueryTrait, TransactionTrait,
};

use crate::controller::streaming_job::{CrossDbBackfillChangeLogInfo, TableChangeLogTruncateInfo};
use crate::hummock::HummockManager;
use crate::hummock::error::{Error, Result};
use crate::hummock::model::ext::to_table_change_log;

fn update_truncate_epoch(
    truncate_epochs: &mut HashMap<TableId, u64>,
    table_id: TableId,
    truncate_epoch: u64,
) {
    truncate_epochs
        .entry(table_id)
        .and_modify(|epoch| *epoch = (*epoch).min(truncate_epoch))
        .or_insert(truncate_epoch);
}

fn resolve_table_change_log_truncate_epochs(
    info: &TableChangeLogTruncateInfo,
    version: &HummockVersion,
    current_time_epoch: Epoch,
) -> anyhow::Result<HashMap<TableId, u64>> {
    let mut truncate_epochs = HashMap::new();
    let mut untruncatable_table_ids = HashSet::new();
    for (table_id, retention_seconds) in &info.subscription_retention_seconds {
        if version.table_committed_epoch(*table_id).is_none() {
            // A concurrently dropped table is cleaned up by its commit-epoch transaction.
            tracing::warn!(
                %table_id,
                "cannot get committed epoch for subscribed table, skip table change log truncation"
            );
            continue;
        }
        let truncate_epoch = current_time_epoch
            .subtract_ms(retention_seconds.saturating_mul(1000))
            .0;
        update_truncate_epoch(&mut truncate_epochs, *table_id, truncate_epoch);
    }

    for job in &info.independent_jobs {
        let mut all_snapshot_epochs_none = true;
        for (upstream_table_id, snapshot_epoch) in &job.upstream_table_snapshot_epochs {
            match snapshot_epoch {
                Some(_) => all_snapshot_epochs_none = false,
                None => {
                    // The independent job has not fixed a safe snapshot epoch yet. This vetoes
                    // truncation even when another consumer provides a concrete cutoff.
                    untruncatable_table_ids.insert(*upstream_table_id);
                }
            }
        }
        if all_snapshot_epochs_none {
            continue;
        }

        let mut state_table_ids = job.state_table_ids.iter();
        let first_table_id = state_table_ids
            .next()
            .ok_or_else(|| anyhow!("independent job {} has no state table", job.job_id))?;
        let committed_epoch = version
            .table_committed_epoch(*first_table_id)
            .ok_or_else(|| {
                anyhow!(
                    "cannot get committed epoch of state table {} in independent job {}",
                    first_table_id,
                    job.job_id
                )
            })?;
        for table_id in state_table_ids {
            let table_committed_epoch =
                version.table_committed_epoch(*table_id).ok_or_else(|| {
                    anyhow!(
                        "cannot get committed epoch of state table {} in independent job {}",
                        table_id,
                        job.job_id
                    )
                })?;
            if table_committed_epoch != committed_epoch {
                return Err(anyhow!(
                    "state tables {} and {} in independent job {} have different committed epochs {} and {}",
                    first_table_id,
                    table_id,
                    job.job_id,
                    committed_epoch,
                    table_committed_epoch
                ));
            }
        }

        for (upstream_table_id, snapshot_epoch) in &job.upstream_table_snapshot_epochs {
            if let Some(snapshot_epoch) = snapshot_epoch {
                let pinned_epoch = committed_epoch.max(*snapshot_epoch);
                update_truncate_epoch(&mut truncate_epochs, *upstream_table_id, pinned_epoch);
            }
        }
    }
    truncate_epochs.retain(|table_id, _| !untruncatable_table_ids.contains(table_id));
    Ok(truncate_epochs)
}

impl HummockManager {
    /// Returns the exclusive change-log truncation epoch for this cross-database backfill.
    ///
    /// `Some(epoch)` means every vnode has fully consumed all epochs below `epoch`. A vnode that is
    /// still consuming epoch `e` contributes `e`; one that finished `e` contributes `e + 1`, which
    /// also permits truncating `e`. `None` means at least one vnode has no usable progress yet.
    async fn query_cross_db_backfill_progress(
        &self,
        info: &CrossDbBackfillChangeLogInfo,
        version: &HummockVersion,
        worker: &PbWorkerNode,
    ) -> anyhow::Result<Option<u64>> {
        let committed_epoch = version
            .table_committed_epoch(info.progress_table.id)
            .ok_or_else(|| {
                anyhow!(
                    "cannot get committed epoch of cross-database progress table {} for downstream job {}",
                    info.progress_table.id,
                    info.downstream_job_id
                )
            })?;
        let table_desc = TableDesc::from_pb_table(&info.progress_table).try_to_protobuf()?;
        let epoch_column_id = table_desc
            .columns
            .iter()
            .find(|column| column.name == "epoch")
            .ok_or_else(|| {
                anyhow!(
                    "cross-database progress table {} has no epoch column",
                    info.progress_table.id
                )
            })?
            .column_id;
        let is_finished_column_id = table_desc
            .columns
            .iter()
            .find(|column| column.name == "is_finished")
            .ok_or_else(|| {
                anyhow!(
                    "cross-database progress table {} has no is_finished column",
                    info.progress_table.id
                )
            })?
            .column_id;
        let client = self.env.compute_client_pool().get(worker).await?;
        let task_id = TaskId {
            query_id: format!(
                "meta-cross-db-progress-{}-{}-{}",
                info.downstream_job_id,
                info.progress_table.id,
                uuid::Uuid::new_v4()
            ),
            stage_id: 0,
            task_id: 0,
        };
        let request = ExecuteRequest {
            task_id: Some(task_id),
            plan: Some(PlanFragment {
                root: Some(PlanNode {
                    children: vec![],
                    identity: "CrossDbBackfillProgressScan".to_owned(),
                    node_body: Some(NodeBody::RowSeqScan(RowSeqScanNode {
                        table_desc: Some(table_desc),
                        column_ids: vec![epoch_column_id, is_finished_column_id],
                        scan_ranges: vec![],
                        vnode_bitmap: Some(
                            Bitmap::ones(info.progress_table.vnode_count()).to_protobuf(),
                        ),
                        ordered: false,
                        limit: None,
                        query_epoch: Some(BatchQueryEpoch {
                            epoch: Some(batch_query_epoch::Epoch::Committed(
                                BatchQueryCommittedEpoch {
                                    epoch: committed_epoch,
                                    hummock_version_id: version.id,
                                },
                            )),
                        }),
                    })),
                }),
                exchange_info: Some(ExchangeInfo {
                    mode: DistributionMode::Single as i32,
                    ..Default::default()
                }),
            }),
            tracing_context: Default::default(),
            expr_context: Some(ExprContext {
                time_zone: "UTC".to_owned(),
                strict_mode: true,
            }),
        };
        let mut stream = client.execute(request).await?;
        let mut row_count = 0;
        let mut min_truncate_epoch = None;
        while let Some(response) = stream.try_next().await? {
            let chunk = DataChunk::from_protobuf(response.get_record_batch()?)?;
            row_count += chunk.cardinality();
            for row in chunk.rows() {
                let Some(epoch) = row.datum_at(0) else {
                    return Ok(None);
                };
                let Some(is_finished) = row.datum_at(1) else {
                    return Ok(None);
                };
                let epoch = u64::try_from(epoch.into_int64())?;
                let truncate_epoch = epoch.saturating_add(u64::from(is_finished.into_bool()));
                min_truncate_epoch =
                    Some(min_truncate_epoch.map_or(truncate_epoch, |min_epoch: u64| {
                        min_epoch.min(truncate_epoch)
                    }));
            }
        }
        if row_count < info.progress_table.vnode_count() {
            return Ok(None);
        }
        Ok(min_truncate_epoch)
    }
}

impl HummockManager {
    pub async fn truncate_table_change_log(&self, info: TableChangeLogTruncateInfo) -> Result<()> {
        let _timer = self.metrics.table_change_log_truncate_latency.start_timer();
        let resolution_started_at = Instant::now();
        let version = self.versioning.read().await.current_version.clone();
        let mut cross_db_truncate_epochs = HashMap::new();
        let mut cross_db_untruncatable_table_ids = HashSet::new();
        const MAX_PROGRESS_QUERY_CONCURRENCY: usize = 16;
        let workers = if info.cross_db_backfills.is_empty() {
            Vec::new()
        } else {
            let workers = self
                .metadata_manager
                .list_active_streaming_compute_nodes()
                .await
                .map_err(|err| Error::Internal(err.into()))?;
            if workers.is_empty() {
                return Err(Error::Internal(anyhow!(
                    "no active compute node for querying cross-database backfill progress"
                )));
            }
            workers
        };
        let cross_db_progress = futures::stream::iter(
            info.cross_db_backfills
                .iter()
                .enumerate()
                .map(|(index, cross_db_backfill)| {
                    (cross_db_backfill, &workers[index % workers.len()])
                }),
        )
        .map(|(cross_db_backfill, worker)| {
            let version = version.clone();
            async move {
                let progress_epoch = self
                    .query_cross_db_backfill_progress(cross_db_backfill, version.as_ref(), worker)
                    .await
                    .with_context(|| {
                        format!(
                            "query progress table {} for cross-database downstream job {}",
                            cross_db_backfill.progress_table.id,
                            cross_db_backfill.downstream_job_id
                        )
                    })?;
                Ok::<_, anyhow::Error>((cross_db_backfill.upstream_table_id, progress_epoch))
            }
        })
        .buffer_unordered(MAX_PROGRESS_QUERY_CONCURRENCY)
        .try_collect::<Vec<_>>()
        .await
        .map_err(Error::Internal)?;
        for (upstream_table_id, progress_epoch) in cross_db_progress {
            if let Some(progress_epoch) = progress_epoch {
                update_truncate_epoch(
                    &mut cross_db_truncate_epochs,
                    upstream_table_id,
                    progress_epoch,
                );
            } else {
                cross_db_untruncatable_table_ids.insert(upstream_table_id);
            }
        }
        let mut versioning = self
            .versioning
            .write_with_process_name("truncate_table_change_log")
            .await;
        let current_time_epoch = Epoch::now();
        let truncate_epochs = resolve_table_change_log_truncate_epochs(
            &info,
            versioning.current_version.as_ref(),
            current_time_epoch,
        )
        .map_err(Error::Internal)?;
        let mut truncate_epochs = truncate_epochs;
        for (table_id, truncate_epoch) in cross_db_truncate_epochs {
            update_truncate_epoch(&mut truncate_epochs, table_id, truncate_epoch);
        }
        truncate_epochs.retain(|table_id, _| !cross_db_untruncatable_table_ids.contains(table_id));
        let truncate_epochs: Vec<_> = truncate_epochs
            .into_iter()
            .filter(|(table_id, _)| versioning.table_change_log.contains_key(table_id))
            .collect();
        let resolution_time = resolution_started_at.elapsed();
        let table_count = truncate_epochs.len();
        if truncate_epochs.is_empty() {
            tracing::info!(
                resolution_time = ?resolution_time,
                truncation_time = ?std::time::Duration::ZERO,
                table_count,
                rows_affected = 0,
                may_delete_object_count = 0,
                "table change log truncation finished"
            );
            return Ok(());
        }

        let truncation_started_at = Instant::now();
        let sql_store = self.env.meta_store_ref();
        let txn = sql_store.conn.begin().await?;
        let batch_size = self.env.opts.table_change_log_delete_batch_size as usize;
        let mut rows_affected = 0;
        let mut may_delete_object_ids = HashSet::new();
        for batch in truncate_epochs.chunks(batch_size) {
            let mut condition = Condition::any();
            for (table_id, truncate_epoch) in batch {
                let truncate_epoch = risingwave_meta_model::Epoch::try_from(*truncate_epoch)
                    .context("table change log truncate epoch exceeds meta store range")
                    .map_err(Error::Internal)?;
                condition = condition.add(
                    Condition::all()
                        .add(
                            risingwave_meta_model::hummock_table_change_log::Column::TableId
                                .eq(*table_id),
                        )
                        .add(
                            risingwave_meta_model::hummock_table_change_log::Column::CheckpointEpoch
                                .lt(truncate_epoch),
                        ),
                );
            }
            let (change_logs_to_delete, deleted_count) = match txn.get_database_backend() {
                DbBackend::Postgres => {
                    let mut delete =
                        risingwave_meta_model::hummock_table_change_log::Entity::delete_many()
                            .filter(condition)
                            .into_query();
                    delete.returning_all();
                    let statement = DbBackend::Postgres.build(&delete);
                    let change_logs_to_delete = txn
                        .query_all(statement)
                        .await?
                        .iter()
                        .map(|row| {
                            risingwave_meta_model::hummock_table_change_log::Model::from_query_result(
                                row, "",
                            )
                        })
                        .collect::<std::result::Result<Vec<_>, _>>()?;
                    let deleted_count = change_logs_to_delete.len() as u64;
                    (change_logs_to_delete, deleted_count)
                }
                DbBackend::MySql | DbBackend::Sqlite => {
                    // MySQL does not support DELETE RETURNING, and SQLite returning support is not
                    // enabled in SeaORM, so select the rows in the same transaction before deleting.
                    let change_logs_to_delete =
                        risingwave_meta_model::hummock_table_change_log::Entity::find()
                            .filter(condition.clone())
                            .all(&txn)
                            .await?;
                    let deleted_count =
                        risingwave_meta_model::hummock_table_change_log::Entity::delete_many()
                            .filter(condition)
                            .exec(&txn)
                            .await?
                            .rows_affected;
                    (change_logs_to_delete, deleted_count)
                }
            };
            for change_log_to_delete in change_logs_to_delete {
                let deleted_change_log =
                    TableChangeLog::new([to_table_change_log(change_log_to_delete)]);
                may_delete_object_ids.extend(deleted_change_log.get_object_ids());
            }
            rows_affected += deleted_count;
        }
        txn.commit().await?;

        for (table_id, truncate_epoch) in truncate_epochs {
            if let Some(change_log) = versioning.table_change_log.get_mut(&table_id) {
                change_log.truncate(truncate_epoch);
            }
        }
        drop(versioning);
        let may_delete_object_count = may_delete_object_ids.len();
        self.gc_manager
            .add_may_delete_object_ids(may_delete_object_ids.into_iter());
        let truncation_time = truncation_started_at.elapsed();
        tracing::info!(
            resolution_time = ?resolution_time,
            truncation_time = ?truncation_time,
            table_count,
            rows_affected,
            may_delete_object_count,
            "truncated table change logs"
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};

    use risingwave_common::id::JobId;
    use risingwave_pb::hummock::StateTableInfoDelta;

    use super::*;
    use crate::controller::streaming_job::IndependentJobChangeLogInfo;

    fn version_with_committed_epochs(
        committed_epochs: impl IntoIterator<Item = (TableId, u64)>,
    ) -> HummockVersion {
        let mut version = HummockVersion::default();
        let mut delta = version.version_delta_after();
        for (table_id, committed_epoch) in committed_epochs {
            delta.state_table_info_delta.insert(
                table_id,
                StateTableInfoDelta {
                    committed_epoch,
                    compaction_group_id: 1.into(),
                },
            );
        }
        version.apply_version_delta(&delta);
        version
    }

    #[test]
    fn test_resolve_table_change_log_truncate_epochs() {
        let upstream_table_id = TableId::new(1);
        let job_state_table_id = TableId::new(2);
        let current_time_epoch = Epoch::from_physical_time(100_000);
        let subscription_epoch = Epoch::from_physical_time(70_000).0;
        let job_committed_epoch = Epoch::from_physical_time(80_000).0;
        let snapshot_epoch = Epoch::from_physical_time(85_000).0;
        let version = version_with_committed_epochs([
            (upstream_table_id, subscription_epoch),
            (job_state_table_id, job_committed_epoch),
        ]);
        let info = TableChangeLogTruncateInfo {
            subscription_retention_seconds: HashMap::from([(upstream_table_id, 10)]),
            cross_db_backfills: vec![],
            independent_jobs: vec![IndependentJobChangeLogInfo {
                job_id: JobId::new(3),
                state_table_ids: HashSet::from([job_state_table_id]),
                upstream_table_snapshot_epochs: HashMap::from([(
                    upstream_table_id,
                    Some(snapshot_epoch),
                )]),
            }],
        };

        let truncate_epochs =
            resolve_table_change_log_truncate_epochs(&info, &version, current_time_epoch).unwrap();
        assert_eq!(truncate_epochs[&upstream_table_id], snapshot_epoch);
    }

    #[test]
    fn test_missing_snapshot_epoch_prevents_truncation() {
        let upstream_table_id = TableId::new(1);
        let job_state_table_id = TableId::new(2);
        let upstream_committed_epoch = Epoch::from_physical_time(100_000).0;
        let job_committed_epoch = Epoch::from_physical_time(80_000).0;
        let version = version_with_committed_epochs([
            (upstream_table_id, upstream_committed_epoch),
            (job_state_table_id, job_committed_epoch),
        ]);
        let info = TableChangeLogTruncateInfo {
            subscription_retention_seconds: HashMap::from([(upstream_table_id, 10)]),
            cross_db_backfills: vec![],
            independent_jobs: vec![IndependentJobChangeLogInfo {
                job_id: JobId::new(3),
                state_table_ids: HashSet::from([job_state_table_id]),
                upstream_table_snapshot_epochs: HashMap::from([(upstream_table_id, None)]),
            }],
        };

        let truncate_epochs = resolve_table_change_log_truncate_epochs(
            &info,
            &version,
            Epoch::from_physical_time(100_000),
        )
        .unwrap();
        assert!(!truncate_epochs.contains_key(&upstream_table_id));
    }

    #[test]
    fn test_inconsistent_job_committed_epoch_fails() {
        let state_table_id_1 = TableId::new(1);
        let state_table_id_2 = TableId::new(2);
        let version = version_with_committed_epochs([
            (state_table_id_1, Epoch::from_physical_time(1).0),
            (state_table_id_2, Epoch::from_physical_time(2).0),
        ]);
        let info = TableChangeLogTruncateInfo {
            subscription_retention_seconds: HashMap::new(),
            cross_db_backfills: vec![],
            independent_jobs: vec![IndependentJobChangeLogInfo {
                job_id: JobId::new(3),
                state_table_ids: HashSet::from([state_table_id_1, state_table_id_2]),
                upstream_table_snapshot_epochs: HashMap::from([(
                    TableId::new(4),
                    Some(Epoch::from_physical_time(1).0),
                )]),
            }],
        };

        assert!(
            resolve_table_change_log_truncate_epochs(
                &info,
                &version,
                Epoch::from_physical_time(100_000),
            )
            .is_err()
        );
    }
}
