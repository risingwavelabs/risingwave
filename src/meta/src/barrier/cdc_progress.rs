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
use std::sync::LazyLock;

use risingwave_common::id::JobId;
use risingwave_common::row::{OwnedRow, Row};
use risingwave_connector::source::CdcTableSnapshotSplitRaw;
use risingwave_connector::source::cdc::external::CDC_TABLE_SPLIT_ID_START;
use risingwave_connector::source::cdc::{
    INITIAL_CDC_SPLIT_ASSIGNMENT_GENERATION_ID, INVALID_CDC_SPLIT_ASSIGNMENT_GENERATION_ID,
};
use risingwave_meta_model::{FragmentId, cdc_table_snapshot_split};
use risingwave_pb::id::ActorId;
use risingwave_pb::source::PbCdcTableSnapshotSplits;
use risingwave_pb::stream_service::barrier_complete_response::PbCdcTableBackfillProgress;
use sea_orm::prelude::Expr;
use sea_orm::{ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter};

use crate::MetaResult;
use crate::stream::cdc::{
    CdcTableSnapshotSplits, assign_cdc_table_snapshot_splits, single_merged_split,
};

#[derive(Debug)]
enum CdcBackfillStatus {
    Backfilling(CdcBackfillProgress),
    PreCompleted,
    Completed,
}

#[derive(Debug)]
struct CdcBackfillProgress {
    splits: Vec<CdcTableSnapshotSplitRaw>,
    /// The number of splits that has completed backfill.
    split_backfilled_count: u64,
    /// The number of splits that has completed backfill and synchronized CDC offset.
    split_completed_count: u64,
    /// The generation of split assignment.
    split_assignment_generation: u64,
}

#[derive(Debug)]
pub struct CdcProgress {
    /// The total number of splits, immutable.
    pub split_total_count: u64,
    /// The number of splits that has completed backfill.
    pub split_backfilled_count: u64,
    /// The number of splits that has completed backfill and synchronized CDC offset.
    pub split_completed_count: u64,
}

impl CdcTableBackfillTracker {
    pub async fn mark_complete_job(txn: &impl ConnectionTrait, job_id: JobId) -> MetaResult<()> {
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
            .exec(txn)
            .await?;
        // Keep only the first split.
        cdc_table_snapshot_split::Entity::delete_many()
            .filter(cdc_table_snapshot_split::Column::TableId.eq(job_id))
            .filter(cdc_table_snapshot_split::Column::SplitId.gt(CDC_TABLE_SPLIT_ID_START))
            .exec(txn)
            .await?;
        Ok(())
    }
}

#[derive(Debug)]
pub(super) struct CdcTableBackfillTracker {
    status: CdcBackfillStatus,
    cdc_scan_fragment_id: FragmentId,
    next_generation: u64,
}

impl CdcTableBackfillTracker {
    fn new_inner(cdc_scan_fragment_id: FragmentId, splits: CdcTableSnapshotSplits) -> Self {
        let status = match splits {
            CdcTableSnapshotSplits::Backfilling(splits) => {
                CdcBackfillStatus::Backfilling(CdcBackfillProgress {
                    splits,
                    split_backfilled_count: 0,
                    split_completed_count: 0,
                    split_assignment_generation: INITIAL_CDC_SPLIT_ASSIGNMENT_GENERATION_ID,
                })
            }
            CdcTableSnapshotSplits::Finished => CdcBackfillStatus::Completed,
        };
        Self {
            status,
            cdc_scan_fragment_id,
            next_generation: INITIAL_CDC_SPLIT_ASSIGNMENT_GENERATION_ID + 1,
        }
    }

    pub fn restore(cdc_scan_fragment_id: FragmentId, splits: CdcTableSnapshotSplits) -> Self {
        Self::new_inner(cdc_scan_fragment_id, splits)
    }

    pub fn new(cdc_scan_fragment_id: FragmentId, splits: Vec<CdcTableSnapshotSplitRaw>) -> Self {
        Self::new_inner(
            cdc_scan_fragment_id,
            CdcTableSnapshotSplits::Backfilling(splits),
        )
    }

    pub fn cdc_scan_fragment_id(&self) -> FragmentId {
        self.cdc_scan_fragment_id
    }

    pub fn update_split_progress(&mut self, progress: &PbCdcTableBackfillProgress) {
        tracing::debug!(?progress, "Complete split.");
        let current_progress = match &mut self.status {
            CdcBackfillStatus::Backfilling(progress) => progress,
            CdcBackfillStatus::PreCompleted | CdcBackfillStatus::Completed => {
                return;
            }
        };
        assert_ne!(
            progress.generation,
            INVALID_CDC_SPLIT_ASSIGNMENT_GENERATION_ID
        );
        if current_progress.split_assignment_generation == progress.generation {
            current_progress.split_backfilled_count +=
                (1 + progress.split_id_end_inclusive - progress.split_id_start_inclusive) as u64;
            if progress.done {
                current_progress.split_completed_count += (1 + progress.split_id_end_inclusive
                    - progress.split_id_start_inclusive)
                    as u64;
                if current_progress.split_completed_count == current_progress.splits.len() as u64 {
                    self.status = CdcBackfillStatus::PreCompleted;
                }
            }
        }
    }

    pub fn reassign_splits(
        &mut self,
        actor_ids: HashSet<ActorId>,
    ) -> MetaResult<HashMap<ActorId, PbCdcTableSnapshotSplits>> {
        let generation = self.next_generation;
        self.next_generation += 1;
        let splits = match &mut self.status {
            CdcBackfillStatus::Backfilling(progress) => {
                progress.split_assignment_generation = generation;
                progress.splits.as_slice()
            }
            CdcBackfillStatus::PreCompleted | CdcBackfillStatus::Completed => {
                static SINGLE_SPLIT: LazyLock<CdcTableSnapshotSplitRaw> =
                    LazyLock::new(single_merged_split);
                core::slice::from_ref(&*SINGLE_SPLIT)
            }
        };
        assign_cdc_table_snapshot_splits(actor_ids, splits, generation)
    }

    pub fn gen_cdc_progress(&self) -> CdcProgress {
        match &self.status {
            CdcBackfillStatus::Backfilling(progress) => CdcProgress {
                split_total_count: progress.splits.len() as _,
                split_backfilled_count: progress.split_backfilled_count,
                split_completed_count: progress.split_completed_count,
            },
            CdcBackfillStatus::PreCompleted | CdcBackfillStatus::Completed => CdcProgress {
                split_total_count: 1,
                split_backfilled_count: 1,
                split_completed_count: 1,
            },
        }
    }

    pub fn take_pre_completed(&mut self) -> bool {
        if let CdcBackfillStatus::PreCompleted = &self.status {
            self.status = CdcBackfillStatus::Completed;
            true
        } else {
            false
        }
    }
}

#[cfg(test)]
mod test {

    use risingwave_connector::source::CdcTableSnapshotSplitRaw;
    use risingwave_pb::stream_service::barrier_complete_response::CdcTableBackfillProgress;

    use crate::barrier::cdc_progress::{
        CdcBackfillProgress, CdcBackfillStatus, CdcTableBackfillTracker,
    };

    impl CdcTableBackfillTracker {
        fn progress(&self) -> &CdcBackfillProgress {
            if let CdcBackfillStatus::Backfilling(progress) = &self.status {
                progress
            } else {
                unreachable!()
            }
        }
    }

    #[tokio::test]
    async fn test_generation() {
        let split_count = 10u64;
        let mut tracker = CdcTableBackfillTracker::new(
            233.into(),
            (0..split_count)
                .map(|split_id| CdcTableSnapshotSplitRaw {
                    split_id: split_id as _,
                    left_bound_inclusive: vec![],
                    right_bound_exclusive: vec![],
                })
                .collect(),
        );
        assert_eq!(tracker.next_generation, 2);
        tracker
            .reassign_splits([1.into()].into_iter().collect())
            .unwrap();
        let generation = tracker.progress().split_assignment_generation;
        assert_eq!(generation, 2);
        assert_init_state(&tracker, split_count);
        let cdc_table_backfill_progress = vec![
            CdcTableBackfillProgress {
                done: true,
                split_id_start_inclusive: 1,
                split_id_end_inclusive: 2,
                generation,
                fragment_id: 12.into(),
                ..Default::default()
            },
            CdcTableBackfillProgress {
                done: true,
                split_id_start_inclusive: 5,
                split_id_end_inclusive: 10,
                generation,
                fragment_id: 11.into(),
                ..Default::default()
            },
        ];
        for progress in &cdc_table_backfill_progress {
            tracker.update_split_progress(progress);
        }
        assert_eq!(tracker.progress().split_completed_count, 8);

        // Reset generation.
        tracker
            .reassign_splits([1.into()].into_iter().collect())
            .unwrap();
        let generation = tracker.progress().split_assignment_generation;
        assert_eq!(generation, 3);
        assert_init_state(&tracker, split_count);
        let cdc_table_backfill_progress = CdcTableBackfillProgress {
            done: true,
            split_id_start_inclusive: 3,
            split_id_end_inclusive: 4,
            // Expired generation.
            generation: generation - 1,
            fragment_id: 13.into(),
            ..Default::default()
        };
        tracker.update_split_progress(&cdc_table_backfill_progress);
        assert_init_state(&tracker, split_count);
        assert_eq!(tracker.progress().split_completed_count, 0);

        let cdc_table_backfill_progress = [
            CdcTableBackfillProgress {
                done: true,
                split_id_start_inclusive: 1,
                split_id_end_inclusive: 2,
                generation,
                fragment_id: 12.into(),
                ..Default::default()
            },
            CdcTableBackfillProgress {
                done: true,
                split_id_start_inclusive: 5,
                split_id_end_inclusive: 10,
                generation,
                fragment_id: 11.into(),
                ..Default::default()
            },
            CdcTableBackfillProgress {
                done: true,
                split_id_start_inclusive: 3,
                split_id_end_inclusive: 4,
                generation,
                fragment_id: 13.into(),
                ..Default::default()
            },
        ];
        for progress in &cdc_table_backfill_progress {
            tracker.update_split_progress(progress);
        }
        assert!(tracker.take_pre_completed());
    }

    fn assert_init_state(tracker: &CdcTableBackfillTracker, split_count: u64) {
        let CdcBackfillStatus::Backfilling(progress) = &tracker.status else {
            unreachable!()
        };
        assert_eq!(progress.splits.len() as u64, split_count);
        assert_eq!(progress.split_completed_count, 0);
    }
}
