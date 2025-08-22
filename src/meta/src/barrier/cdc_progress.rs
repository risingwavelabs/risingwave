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

use std::collections::HashMap;
use std::sync::Arc;

use num_traits::ToPrimitive;
use parking_lot::Mutex;
use risingwave_common::catalog::{FragmentTypeFlag, TableId};
use risingwave_meta_model::{FragmentId, ObjectId, cdc_table_snapshot_split, fragment};
use risingwave_pb::stream_service::PbBarrierCompleteResponse;
use risingwave_pb::stream_service::barrier_complete_response::PbCdcTableBackfillProgress;
use sea_orm::prelude::Expr;
use sea_orm::{ColumnTrait, EntityTrait, QueryFilter, QuerySelect};

use crate::MetaResult;
use crate::controller::SqlMetaStore;

pub type CdcTableBackfillTrackerRef = Arc<CdcTableBackfillTracker>;

pub struct CdcTableBackfillTracker {
    meta_store: SqlMetaStore,
    inner: Mutex<CdcTableBackfillTrackerInner>,
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
    ) -> Vec<TableId> {
        let mut inner = self.inner.lock();
        let mut completed_jobs = vec![];
        for resp in resps {
            for progress in &resp.cdc_table_backfill_progress {
                completed_jobs.extend(inner.try_complete_split(progress));
            }
        }
        completed_jobs.into_iter().map(Into::into).collect()
    }

    pub async fn finish_backfill(&self, job_id: TableId) -> MetaResult<()> {
        cdc_table_snapshot_split::Entity::update_many()
            .col_expr(
                cdc_table_snapshot_split::Column::IsBackfillFinished,
                Expr::value(true),
            )
            .filter(
                cdc_table_snapshot_split::Column::TableId
                    .eq(job_id.table_id as risingwave_meta_model::TableId),
            )
            .exec(&self.meta_store.conn)
            .await?;
        Ok(())
    }

    /// Tracks the split count for the `table_id`.
    pub fn add_split_count(&self, table_id: u32, split_count: u64) {
        self.inner.lock().add_split_count(table_id, split_count);
    }

    pub fn add_fragment_table_mapping(
        &self,
        fragment_ids: impl Iterator<Item = u32>,
        table_id: u32,
    ) {
        self.inner
            .lock()
            .add_fragment_table_mapping(fragment_ids, table_id);
    }

    pub fn next_generation(&self, table_ids: impl Iterator<Item = u32>) -> u64 {
        let mut inner = self.inner.lock();
        let generation = inner.next_generation;
        inner.next_generation += 1;
        for table_id in table_ids {
            inner.update_split_assignment_generation(table_id, generation);
        }
        generation
    }
}

struct CdcTableBackfillTrackerInner {
    table_split_total_counts: HashMap<u32, u64>,
    table_split_completed_counts: HashMap<u32, u64>,
    table_split_assignment_generations: HashMap<u32, u64>,
    next_generation: u64,
    fragment_id_to_table_id: HashMap<u32, u32>,
}

impl CdcTableBackfillTrackerInner {
    async fn new(meta_store: SqlMetaStore) -> MetaResult<Self> {
        // TODO(zw): improve init generation.
        let init_generation = 1;
        let table_split_total_counts = load_split_counts(&meta_store).await?;
        let fragment_id_to_table_id = load_cdc_fragment_table_mapping(&meta_store).await?;
        let table_split_assignment_generations = table_split_total_counts
            .keys()
            .map(|table_id| (*table_id, init_generation))
            .collect();
        let table_split_completed_counts = HashMap::default();
        let inst = Self {
            table_split_total_counts,
            table_split_completed_counts,
            table_split_assignment_generations,
            next_generation: init_generation + 1,
            fragment_id_to_table_id,
        };
        Ok(inst)
    }

    fn add_split_count(&mut self, table_id: u32, split_count: u64) {
        self.table_split_total_counts.insert(table_id, split_count);
    }

    fn try_complete_split(&mut self, progress: &PbCdcTableBackfillProgress) -> Vec<u32> {
        let Some(table_id) = self.fragment_id_to_table_id.get(&progress.fragment_id) else {
            tracing::warn!(
                fragment_id = progress.fragment_id,
                "CDC table mapping not found."
            );
            return vec![];
        };
        tracing::debug!(?progress, "Complete split.");
        let Some(completed_count) = self.table_split_completed_counts.get_mut(table_id) else {
            tracing::warn!(
                table_id,
                "CDC table progress state (split_completed_counts) not found."
            );
            return vec![];
        };
        let Some(expected_count) = self.table_split_total_counts.get(table_id) else {
            tracing::warn!(
                table_id,
                "CDC table progress state (split_total_counts) not found."
            );
            return vec![];
        };
        let Some(generation) = self.table_split_assignment_generations.get(table_id) else {
            tracing::warn!(
                table_id,
                "CDC table progress state (split_assignment_generations) not found."
            );
            return vec![];
        };
        let mut completed_jobs = vec![];
        if *generation == progress.generation && progress.done {
            *completed_count +=
                (1 + progress.split_id_end_inclusive - progress.split_id_start_inclusive) as u64;
            if *completed_count == *expected_count {
                completed_jobs.push(*table_id);
            }
        }
        completed_jobs
    }

    fn update_split_assignment_generation(&mut self, table_id: u32, generation: u64) {
        self.table_split_assignment_generations
            .insert(table_id, generation);
        self.table_split_completed_counts.insert(table_id, 0);
    }

    fn add_fragment_table_mapping(
        &mut self,
        fragment_ids: impl Iterator<Item = u32>,
        table_id: u32,
    ) {
        for fragment_id in fragment_ids {
            self.fragment_id_to_table_id.insert(fragment_id, table_id);
        }
    }
}

/// Tracks the split counts of existing tables.
async fn load_split_counts(meta_store: &SqlMetaStore) -> MetaResult<HashMap<u32, u64>> {
    let split_counts: Vec<(i32, i64)> = cdc_table_snapshot_split::Entity::find()
        .select_only()
        .column(cdc_table_snapshot_split::Column::TableId)
        .column_as(cdc_table_snapshot_split::Column::TableId.count(), "count")
        .group_by(cdc_table_snapshot_split::Column::TableId)
        .into_tuple()
        .all(&meta_store.conn)
        .await?;
    Ok(split_counts
        .into_iter()
        .map(|(k, v)| (k.to_u32().unwrap(), v.to_u64().unwrap()))
        .collect())
}

async fn load_cdc_fragment_table_mapping(
    meta_store: &SqlMetaStore,
) -> MetaResult<HashMap<u32, u32>> {
    use risingwave_meta_model::prelude::Fragment as FragmentModel;
    let stream_cdc_scan_flag = FragmentTypeFlag::StreamCdcScan as i32;
    let fragment_type_mask = stream_cdc_scan_flag;
    let fragment_jobs: Vec<(FragmentId, ObjectId)> = FragmentModel::find()
        .select_only()
        .columns([fragment::Column::FragmentId, fragment::Column::JobId])
        .filter(fragment::Column::FragmentTypeMask.eq(fragment_type_mask))
        .into_tuple()
        .all(&meta_store.conn)
        .await?;
    Ok(fragment_jobs
        .into_iter()
        .map(|(k, v)| (k.to_u32().unwrap(), v.to_u32().unwrap()))
        .collect())
}

#[cfg(test)]
mod test {
    use std::iter;

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
        let table_id = 123;
        let split_count = 10;
        tracker.add_split_count(table_id, split_count);
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
                .table_split_completed_counts
                .get(&table_id)
                .cloned()
                .unwrap(),
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
                .table_split_completed_counts
                .get(&table_id)
                .cloned()
                .unwrap(),
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
        assert_eq!(completed, vec![table_id.into()]);
        assert_eq!(
            tracker
                .inner
                .lock()
                .table_split_completed_counts
                .get(&table_id)
                .cloned()
                .unwrap(),
            10
        );
    }

    fn assert_init_state(
        tracker: &CdcTableBackfillTracker,
        table_id: u32,
        generation: u64,
        split_count: u64,
    ) {
        assert_eq!(
            tracker
                .inner
                .lock()
                .table_split_assignment_generations
                .get(&table_id)
                .cloned()
                .unwrap(),
            generation
        );
        assert_eq!(
            tracker
                .inner
                .lock()
                .table_split_total_counts
                .get(&table_id)
                .cloned()
                .unwrap(),
            split_count
        );
        assert_eq!(
            tracker
                .inner
                .lock()
                .table_split_completed_counts
                .get(&table_id)
                .cloned()
                .unwrap(),
            0
        );
    }
}
