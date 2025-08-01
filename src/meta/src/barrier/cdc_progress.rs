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
use std::sync::atomic::Ordering;

use parking_lot::Mutex;
use risingwave_common::catalog::TableId;
use risingwave_meta_model::cdc_table_snapshot_split;
use risingwave_pb::stream_service::PbBarrierCompleteResponse;
use risingwave_pb::stream_service::barrier_complete_response::PbCdcTableBackfillProgress;
use sea_orm::{ColumnTrait, EntityTrait, QuerySelect};

use crate::MetaResult;
use crate::controller::SqlMetaStore;

pub type CdcTableBackfillTrackerRef = Arc<CdcTableBackfillTracker>;

pub struct CdcTableBackfillTracker {
    inner: Mutex<CdcTableBackfillTrackerInner>,
}

impl CdcTableBackfillTracker {
    pub async fn new(meta_store: SqlMetaStore) -> MetaResult<Self> {
        let inst = Self {
            inner: Mutex::new(CdcTableBackfillTrackerInner::new(meta_store).await?),
        };
        Ok(inst)
    }

    pub fn apply_collected_command(
        &self,
        resps: impl IntoIterator<Item = &PbBarrierCompleteResponse>,
    ) -> Vec<TableId> {
        for resp in resps.into_iter() {
            let progress = &resp.cdc_table_backfill_progress;
            // TODO(zw): !!!
        }
        vec![]
    }

    pub async fn finish_backfill(&self, job_id: TableId) -> MetaResult<()> {
        // TODO(zw): !!!
        Ok(())
    }

    /// Tracks the split count for the `table_id`.
    pub fn add_split_count(&self, table_id: u32, split_count: u64) {
        self.inner.lock().add_split_count(table_id, split_count);
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
    meta_store: SqlMetaStore,
    table_split_total_counts: HashMap<u32, u64>,
    table_split_completed_counts: HashMap<u32, u64>,
    table_split_assignment_generations: HashMap<u32, u64>,
    next_generation: u64,
}

impl CdcTableBackfillTrackerInner {
    async fn new(meta_store: SqlMetaStore) -> MetaResult<Self> {
        // TODO(zw): improve init generation.
        let init_generation = 1;
        let table_split_total_counts = load_split_counts(&meta_store).await?;
        let table_split_assignment_generations = table_split_total_counts
            .keys()
            .map(|table_id| (*table_id, init_generation))
            .collect();
        let table_split_completed_counts = HashMap::default();
        let inst = Self {
            meta_store,
            table_split_total_counts,
            table_split_completed_counts,
            table_split_assignment_generations,
            next_generation: init_generation + 1,
        };
        Ok(inst)
    }

    fn add_split_count(&mut self, table_id: u32, split_count: u64) {
        self.table_split_total_counts.insert(table_id, split_count);
        self.table_split_completed_counts.insert(table_id, 0);
    }

    fn try_complete_split(&mut self, table_id: u32, progress: PbCdcTableBackfillProgress) {
        todo!("!!!")
    }

    fn update_split_assignment_generation(&mut self, table_id: u32, generation: u64) {
        self.table_split_assignment_generations
            .insert(table_id, generation);
    }
}

/// Tracks the split counts of existing tables.
async fn load_split_counts(meta_store: &SqlMetaStore) -> MetaResult<HashMap<u32, u64>> {
    let split_counts: Vec<(u32, u64)> = cdc_table_snapshot_split::Entity::find()
        .select_only()
        .column(cdc_table_snapshot_split::Column::TableId)
        .column_as(cdc_table_snapshot_split::Column::TableId.count(), "count")
        .group_by(cdc_table_snapshot_split::Column::TableId)
        .into_tuple()
        .all(&meta_store.conn)
        .await?;
    Ok(split_counts.into_iter().collect())
}
