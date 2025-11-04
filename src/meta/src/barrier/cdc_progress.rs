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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use anyhow::anyhow;
use parking_lot::Mutex;
use risingwave_common::catalog::FragmentTypeFlag;
use risingwave_common::id::JobId;
use risingwave_common::row::{OwnedRow, Row};
use risingwave_connector::source::cdc::external::CDC_TABLE_SPLIT_ID_START;
use risingwave_connector::source::cdc::{
    INITIAL_CDC_SPLIT_ASSIGNMENT_GENERATION_ID, INVALID_CDC_SPLIT_ASSIGNMENT_GENERATION_ID,
};
use risingwave_meta_model::{FragmentId, cdc_table_snapshot_split, fragment};
use risingwave_pb::stream_service::PbBarrierCompleteResponse;
use risingwave_pb::stream_service::barrier_complete_response::PbCdcTableBackfillProgress;
use sea_orm::prelude::Expr;
use sea_orm::{ColumnTrait, EntityTrait, QueryFilter, QuerySelect, TransactionTrait};

use crate::MetaResult;
use crate::controller::SqlMetaStore;
use crate::controller::fragment::FragmentTypeMaskExt;

pub type CdcTableBackfillTrackerRef = Arc<CdcTableBackfillTracker>;

pub struct CdcTableBackfillTracker {
    meta_store: SqlMetaStore,
    inner: Mutex<CdcTableBackfillTrackerInner>,
}

#[derive(Clone)]
pub struct CdcProgress {
    /// The total number of splits, immutable.
    pub split_total_count: u64,
    /// The number of splits that has completed backfill.
    pub split_backfilled_count: u64,
    /// The number of splits that has completed backfill and synchronized CDC offset.
    pub split_completed_count: u64,
    /// The generation of split assignment.
    split_assignment_generation: u64,
    is_completed: bool,
}

impl CdcProgress {
    fn restore(split_total_count: u64, generation: u64, is_completed: bool) -> Self {
        Self {
            split_total_count,
            split_backfilled_count: 0,
            split_completed_count: 0,
            split_assignment_generation: generation,
            is_completed,
        }
    }

    /// A generation ID must be assigned before it can be used.
    fn new_partial(split_total_count: u64) -> Self {
        Self {
            split_total_count,
            split_backfilled_count: 0,
            split_completed_count: 0,
            split_assignment_generation: INVALID_CDC_SPLIT_ASSIGNMENT_GENERATION_ID,
            is_completed: false,
        }
    }
}

impl CdcTableBackfillTracker {
    pub async fn new(meta_store: SqlMetaStore) -> MetaResult<Self> {
        let inner = CdcTableBackfillTrackerInner::new(meta_store.clone()).await?;
        let inst = Self {
            meta_store,
            inner: Mutex::new(inner),
        };
        Ok(inst)
    }

    pub fn apply_collected_command(
        &self,
        resps: impl IntoIterator<Item = &PbBarrierCompleteResponse>,
    ) -> Vec<JobId> {
        let mut inner = self.inner.lock();
        let mut pre_completed_jobs = vec![];
        for resp in resps {
            for progress in &resp.cdc_table_backfill_progress {
                pre_completed_jobs.extend(inner.update_split_progress(progress));
            }
        }
        pre_completed_jobs
    }

    pub async fn complete_job(&self, job_id: JobId) -> MetaResult<()> {
        let txn = self.meta_store.conn.begin().await?;
        // Rewrite the first split as [inf, inf].
        let bound = OwnedRow::new(vec![None]).value_serialize();
        cdc_table_snapshot_split::Entity::update_many()
            .col_expr(
                cdc_table_snapshot_split::Column::IsBackfillFinished,
                Expr::value(1),
            )
            .col_expr(
                cdc_table_snapshot_split::Column::Left,
                Expr::value(bound.clone()),
            )
            .col_expr(cdc_table_snapshot_split::Column::Right, Expr::value(bound))
            .filter(cdc_table_snapshot_split::Column::TableId.eq(job_id))
            .filter(cdc_table_snapshot_split::Column::SplitId.eq(CDC_TABLE_SPLIT_ID_START))
            .exec(&txn)
            .await?;
        // Keep only the first split.
        cdc_table_snapshot_split::Entity::delete_many()
            .filter(cdc_table_snapshot_split::Column::TableId.eq(job_id))
            .filter(cdc_table_snapshot_split::Column::SplitId.gt(CDC_TABLE_SPLIT_ID_START))
            .exec(&txn)
            .await?;
        txn.commit().await?;
        self.inner.lock().complete_job(job_id);
        Ok(())
    }

    /// Starts to track the progress for the `job_id`.
    /// A generation ID must be assigned via `next_generation` before it can be used.
    pub fn track_new_job(&self, job_id: JobId, split_count: u64) {
        self.inner.lock().track_new_job(job_id, split_count);
    }

    pub fn add_fragment_table_mapping(
        &self,
        fragment_ids: impl Iterator<Item = u32>,
        job_id: JobId,
    ) {
        self.inner
            .lock()
            .add_fragment_job_mapping(fragment_ids, job_id);
    }

    pub fn next_generation(&self, job_ids: impl Iterator<Item = JobId>) -> u64 {
        let mut inner = self.inner.lock();
        let generation = inner.next_generation;
        inner.next_generation += 1;
        for job_id in job_ids {
            inner.update_split_assignment_generation(job_id, generation);
        }
        generation
    }

    pub fn list_cdc_progress(&self) -> HashMap<JobId, CdcProgress> {
        self.inner.lock().list_cdc_progress()
    }

    pub fn completed_job_ids(&self) -> HashSet<JobId> {
        self.inner.lock().completed_job_ids()
    }
}

struct CdcTableBackfillTrackerInner {
    cdc_progress: HashMap<JobId, CdcProgress>,
    next_generation: u64,
    fragment_id_to_job_id: HashMap<u32, JobId>,
}

impl CdcTableBackfillTrackerInner {
    async fn new(meta_store: SqlMetaStore) -> MetaResult<Self> {
        // The generation only resets after meta node is restarted.
        // The barrier carrying expired generation will be rejected by the restarted meta node.
        // Thus the invalid progress won't be applied to the tracker.
        let init_generation = INITIAL_CDC_SPLIT_ASSIGNMENT_GENERATION_ID;
        let restored = restore_progress(&meta_store).await?;
        let cdc_progress = restored
            .into_iter()
            .map(|(job_id, (split_total_count, is_completed))| {
                (
                    job_id,
                    CdcProgress::restore(split_total_count, init_generation, is_completed),
                )
            })
            .collect();
        let fragment_id_to_job_id = load_cdc_fragment_table_mapping(&meta_store).await?;
        let inst = Self {
            cdc_progress,
            next_generation: init_generation + 1,
            fragment_id_to_job_id,
        };
        Ok(inst)
    }

    fn track_new_job(&mut self, job_id: JobId, split_count: u64) {
        self.cdc_progress
            .insert(job_id, CdcProgress::new_partial(split_count));
    }

    fn update_split_progress(&mut self, progress: &PbCdcTableBackfillProgress) -> Vec<JobId> {
        let Some(job_id) = self.fragment_id_to_job_id.get(&progress.fragment_id) else {
            tracing::warn!(
                fragment_id = progress.fragment_id,
                "CDC table mapping not found."
            );
            return vec![];
        };
        tracing::debug!(?progress, "Complete split.");
        let Some(current_progress) = self.cdc_progress.get_mut(job_id) else {
            tracing::warn!(%job_id, "CDC table current progress not found.");
            return vec![];
        };
        if current_progress.is_completed {
            return vec![];
        }
        let mut pre_completed_jobs = vec![];
        assert_ne!(
            progress.generation,
            INVALID_CDC_SPLIT_ASSIGNMENT_GENERATION_ID
        );
        if current_progress.split_assignment_generation == progress.generation {
            if progress.done {
                current_progress.split_completed_count += (1 + progress.split_id_end_inclusive
                    - progress.split_id_start_inclusive)
                    as u64;
                if current_progress.split_completed_count == current_progress.split_total_count {
                    pre_completed_jobs.push(*job_id);
                }
            }
            current_progress.split_backfilled_count +=
                (1 + progress.split_id_end_inclusive - progress.split_id_start_inclusive) as u64;
        }
        pre_completed_jobs
    }

    fn complete_job(&mut self, job_id: JobId) {
        if let Some(p) = self.cdc_progress.get_mut(&job_id) {
            p.is_completed = true;
        } else {
            tracing::warn!(%job_id, "CDC table current progress not found.");
        }
    }

    fn update_split_assignment_generation(&mut self, job_id: JobId, generation: u64) {
        if let Some(p) = self.cdc_progress.get_mut(&job_id) {
            p.split_assignment_generation = generation;
            p.split_backfilled_count = 0;
            p.split_completed_count = 0;
        } else {
            tracing::warn!(%job_id, generation, "CDC table current progress not found.");
        }
    }

    fn add_fragment_job_mapping(&mut self, fragment_ids: impl Iterator<Item = u32>, job_id: JobId) {
        for fragment_id in fragment_ids {
            self.fragment_id_to_job_id.insert(fragment_id, job_id);
        }
    }

    fn list_cdc_progress(&self) -> HashMap<JobId, CdcProgress> {
        self.cdc_progress
            .iter()
            .map(|(job_id, progress)| {
                if progress.is_completed {
                    // The progress of a completed job won't be set by try_complete_split correctly.
                    // Instead, assign it an arbitrary value.
                    (
                        *job_id,
                        CdcProgress {
                            split_total_count: progress.split_total_count,
                            split_backfilled_count: progress.split_total_count,
                            split_completed_count: progress.split_total_count,
                            split_assignment_generation: progress.split_assignment_generation,
                            is_completed: true,
                        },
                    )
                } else {
                    (*job_id, progress.clone())
                }
            })
            .collect()
    }

    fn completed_job_ids(&self) -> HashSet<JobId> {
        self.cdc_progress
            .iter()
            .filter_map(
                |(job_id, p)| {
                    if p.is_completed { Some(*job_id) } else { None }
                },
            )
            .collect()
    }
}

async fn restore_progress(meta_store: &SqlMetaStore) -> MetaResult<HashMap<JobId, (u64, bool)>> {
    let split_progress: Vec<(JobId, i64, i16)> = cdc_table_snapshot_split::Entity::find()
        .select_only()
        .column(cdc_table_snapshot_split::Column::TableId)
        .column_as(
            cdc_table_snapshot_split::Column::TableId.count(),
            "split_total_count",
        )
        .column_as(
            // The sum of PG and MySQL behaves differently in terms of returning type and casting.
            // Should use sum here but currently use max instead to work around compatibility issue.
            cdc_table_snapshot_split::Column::IsBackfillFinished.max(),
            "split_completed_count",
        )
        .group_by(cdc_table_snapshot_split::Column::TableId)
        .into_tuple()
        .all(&meta_store.conn)
        .await?;
    split_progress
        .into_iter()
        .map(|(job_id, split_total_count, split_completed_count)| {
            assert!(
                split_completed_count <= 1,
                "split_completed_count = {}",
                split_completed_count
            );
            let is_backfill_finished = split_completed_count == 1;
            if is_backfill_finished && split_total_count != 1 {
                // CdcTableBackfillTracker::complete_job rewrites splits in a transaction.
                // This error should only happen when the meta store reads uncommitted data.
                tracing::error!(
                    %job_id,
                    split_total_count,
                    split_completed_count,
                    "unexpected split count"
                );
                Err(anyhow!(format!("unexpected split count:job_id={job_id}, split_total_count={split_total_count}, split_completed_count={split_completed_count}")).into())
            } else {
                Ok((
                    job_id,
                    (
                        u64::try_from(split_total_count).unwrap(),
                        is_backfill_finished,
                    ),
                ))
            }
        })
        .try_collect()
}

async fn load_cdc_fragment_table_mapping(
    meta_store: &SqlMetaStore,
) -> MetaResult<HashMap<u32, JobId>> {
    use risingwave_common::catalog::FragmentTypeMask;
    use risingwave_meta_model::prelude::Fragment as FragmentModel;
    let fragment_jobs: Vec<(FragmentId, JobId)> = FragmentModel::find()
        .select_only()
        .columns([fragment::Column::FragmentId, fragment::Column::JobId])
        .filter(FragmentTypeMask::intersects(
            FragmentTypeFlag::StreamCdcScan,
        ))
        .into_tuple()
        .all(&meta_store.conn)
        .await?;
    Ok(fragment_jobs
        .into_iter()
        .map(|(k, v)| (u32::try_from(k).unwrap(), v))
        .collect())
}

#[cfg(test)]
mod test {
    use std::iter;

    use risingwave_common::id::JobId;
    use risingwave_pb::stream_service::BarrierCompleteResponse;
    use risingwave_pb::stream_service::barrier_complete_response::CdcTableBackfillProgress;

    use crate::barrier::cdc_progress::CdcTableBackfillTracker;
    use crate::manager::MetaSrvEnv;

    #[tokio::test]
    async fn test_generation() {
        let env = MetaSrvEnv::for_test().await;
        let meta_store = env.meta_store();
        let tracker = CdcTableBackfillTracker::new(meta_store).await.unwrap();
        assert_eq!(tracker.inner.lock().next_generation, 2);
        let table_id = 123.into();
        let split_count = 10;
        tracker.track_new_job(table_id, split_count);
        tracker.add_fragment_table_mapping(vec![11, 12, 13].into_iter(), table_id);
        let generation = tracker.next_generation(vec![table_id].into_iter());
        assert_eq!(generation, 2);
        assert_init_state(&tracker, table_id, generation, split_count);
        let barrier_complete = BarrierCompleteResponse {
            cdc_table_backfill_progress: vec![
                CdcTableBackfillProgress {
                    done: true,
                    split_id_start_inclusive: 1,
                    split_id_end_inclusive: 2,
                    generation,
                    fragment_id: 12,
                    ..Default::default()
                },
                CdcTableBackfillProgress {
                    done: true,
                    split_id_start_inclusive: 5,
                    split_id_end_inclusive: 10,
                    generation,
                    fragment_id: 11,
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        let completed = tracker.apply_collected_command(iter::once(&barrier_complete));
        assert!(completed.is_empty());
        assert_eq!(
            tracker
                .inner
                .lock()
                .cdc_progress
                .get(&table_id)
                .unwrap()
                .split_completed_count,
            8
        );

        // Reset generation.
        let generation = tracker.next_generation(vec![table_id].into_iter());
        assert_eq!(generation, 3);
        assert_init_state(&tracker, table_id, generation, split_count);
        let barrier_complete = BarrierCompleteResponse {
            cdc_table_backfill_progress: vec![CdcTableBackfillProgress {
                done: true,
                split_id_start_inclusive: 3,
                split_id_end_inclusive: 4,
                // Expired generation.
                generation: generation - 1,
                fragment_id: 13,
                ..Default::default()
            }],
            ..Default::default()
        };
        let completed = tracker.apply_collected_command(iter::once(&barrier_complete));
        assert!(completed.is_empty());
        assert_init_state(&tracker, table_id, generation, split_count);
        assert_eq!(
            tracker
                .inner
                .lock()
                .cdc_progress
                .get(&table_id)
                .unwrap()
                .split_completed_count,
            0
        );

        let barrier_complete = BarrierCompleteResponse {
            cdc_table_backfill_progress: vec![
                CdcTableBackfillProgress {
                    done: true,
                    split_id_start_inclusive: 1,
                    split_id_end_inclusive: 2,
                    generation,
                    fragment_id: 12,
                    ..Default::default()
                },
                CdcTableBackfillProgress {
                    done: true,
                    split_id_start_inclusive: 5,
                    split_id_end_inclusive: 10,
                    generation,
                    fragment_id: 11,
                    ..Default::default()
                },
                CdcTableBackfillProgress {
                    done: true,
                    split_id_start_inclusive: 3,
                    split_id_end_inclusive: 4,
                    generation,
                    fragment_id: 13,
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        let completed = tracker.apply_collected_command(iter::once(&barrier_complete));
        assert_eq!(completed, vec![table_id]);
        assert_eq!(
            tracker
                .inner
                .lock()
                .cdc_progress
                .get(&table_id)
                .unwrap()
                .split_completed_count,
            10
        );
    }

    fn assert_init_state(
        tracker: &CdcTableBackfillTracker,
        table_id: JobId,
        generation: u64,
        split_count: u64,
    ) {
        assert_eq!(
            tracker
                .inner
                .lock()
                .cdc_progress
                .get(&table_id)
                .unwrap()
                .split_assignment_generation,
            generation
        );
        assert_eq!(
            tracker
                .inner
                .lock()
                .cdc_progress
                .get(&table_id)
                .unwrap()
                .split_total_count,
            split_count
        );
        assert_eq!(
            tracker
                .inner
                .lock()
                .cdc_progress
                .get(&table_id)
                .cloned()
                .unwrap()
                .split_completed_count,
            0
        );
    }
}
