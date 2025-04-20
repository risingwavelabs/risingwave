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

use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::mem::take;
use std::time::Duration;

use risingwave_common::hash::ActorId;
use risingwave_common::util::epoch::Epoch;
use risingwave_pb::hummock::HummockVersionStats;
use risingwave_pb::stream_plan::barrier::PbBarrierKind;
use risingwave_pb::stream_service::barrier_complete_response::{
    CreateMviewProgress, PbCreateMviewProgress,
};
use tracing::warn;

use crate::barrier::progress::CreateMviewProgressTracker;
use crate::barrier::{BarrierInfo, BarrierKind, TracedEpoch};

#[derive(Debug)]
pub(super) struct CreateMviewLogStoreProgressTracker {
    /// `actor_id` -> `pending_epoch_lag`
    ongoing_actors: HashMap<ActorId, u64>,
    finished_actors: HashSet<ActorId>,
}

impl CreateMviewLogStoreProgressTracker {
    pub(super) fn new(actors: impl Iterator<Item = ActorId>, pending_barrier_lag: u64) -> Self {
        Self {
            ongoing_actors: HashMap::from_iter(actors.map(|actor| (actor, pending_barrier_lag))),
            finished_actors: HashSet::new(),
        }
    }

    pub(super) fn gen_ddl_progress(&self) -> String {
        let sum = self.ongoing_actors.values().sum::<u64>() as f64;
        let count = if self.ongoing_actors.is_empty() {
            1
        } else {
            self.ongoing_actors.len()
        } as f64;
        let avg = sum / count;
        let avg_lag_time = Duration::from_millis(Epoch(avg as _).physical_time());
        format!(
            "actor: {}/{}, avg lag {:?}",
            self.finished_actors.len(),
            self.ongoing_actors.len() + self.finished_actors.len(),
            avg_lag_time
        )
    }

    fn update(&mut self, progress: impl IntoIterator<Item = &PbCreateMviewProgress>) {
        for progress in progress {
            match self.ongoing_actors.entry(progress.backfill_actor_id) {
                Entry::Occupied(mut entry) => {
                    if progress.done {
                        entry.remove_entry();
                        assert!(
                            self.finished_actors.insert(progress.backfill_actor_id),
                            "non-duplicate"
                        );
                    } else {
                        *entry.get_mut() = progress.pending_epoch_lag as _;
                    }
                }
                Entry::Vacant(_) => {
                    if cfg!(debug_assertions) {
                        panic!(
                            "reporting progress on non-inflight actor: {:?} {:?}",
                            progress, self
                        );
                    } else {
                        warn!(?progress, progress_tracker = ?self, "reporting progress on non-inflight actor");
                    }
                }
            }
        }
    }

    pub(super) fn is_finished(&self) -> bool {
        self.ongoing_actors.is_empty()
    }
}

#[derive(Debug)]
pub(super) enum CreatingStreamingJobStatus {
    /// The creating job is consuming upstream snapshot.
    /// Will transit to `ConsumingLogStore` on `update_progress` when
    /// the snapshot has been fully consumed after `update_progress`.
    ConsumingSnapshot {
        prev_epoch_fake_physical_time: u64,
        pending_upstream_barriers: Vec<BarrierInfo>,
        version_stats: HummockVersionStats,
        create_mview_tracker: CreateMviewProgressTracker,
        snapshot_backfill_actors: HashSet<ActorId>,
        backfill_epoch: u64,
        /// The `prev_epoch` of pending non checkpoint barriers
        pending_non_checkpoint_barriers: Vec<u64>,
    },
    /// The creating job is consuming log store.
    ///
    /// Will transit to `Finishing` on `on_new_upstream_epoch` when `start_consume_upstream` is `true`.
    ConsumingLogStore {
        log_store_progress_tracker: CreateMviewLogStoreProgressTracker,
        barriers_to_inject: Option<Vec<BarrierInfo>>,
    },
    /// All backfill actors have started consuming upstream, and the job
    /// will be finished when all previously injected barriers have been collected
    /// Store the `prev_epoch` that will finish at.
    Finishing(u64),
}

impl CreatingStreamingJobStatus {
    pub(super) fn update_progress(
        &mut self,
        create_mview_progress: impl IntoIterator<Item = &CreateMviewProgress>,
    ) {
        match self {
            &mut Self::ConsumingSnapshot {
                ref mut create_mview_tracker,
                ref version_stats,
                ref mut prev_epoch_fake_physical_time,
                ref mut pending_upstream_barriers,
                ref mut pending_non_checkpoint_barriers,
                ref backfill_epoch,
                ref snapshot_backfill_actors,
                ..
            } => {
                create_mview_tracker.update_tracking_jobs(
                    None,
                    create_mview_progress,
                    version_stats,
                );
                if create_mview_tracker.has_pending_finished_jobs() {
                    pending_non_checkpoint_barriers.push(*backfill_epoch);

                    let prev_epoch = Epoch::from_physical_time(*prev_epoch_fake_physical_time);
                    let barriers_to_inject: Vec<_> = [BarrierInfo {
                        curr_epoch: TracedEpoch::new(Epoch(*backfill_epoch)),
                        prev_epoch: TracedEpoch::new(prev_epoch),
                        kind: BarrierKind::Checkpoint(take(pending_non_checkpoint_barriers)),
                    }]
                    .into_iter()
                    .chain(pending_upstream_barriers.drain(..))
                    .collect();

                    *self = CreatingStreamingJobStatus::ConsumingLogStore {
                        log_store_progress_tracker: CreateMviewLogStoreProgressTracker::new(
                            snapshot_backfill_actors.iter().cloned(),
                            barriers_to_inject
                                .last()
                                .map(|barrier_info| {
                                    barrier_info.prev_epoch().saturating_sub(*backfill_epoch)
                                })
                                .unwrap_or(0),
                        ),
                        barriers_to_inject: Some(barriers_to_inject),
                    };
                }
            }
            CreatingStreamingJobStatus::ConsumingLogStore {
                log_store_progress_tracker,
                ..
            } => {
                log_store_progress_tracker.update(create_mview_progress);
            }
            CreatingStreamingJobStatus::Finishing(_) => {}
        }
    }

    pub(super) fn start_consume_upstream(&mut self, barrier_info: &BarrierInfo) {
        match self {
            CreatingStreamingJobStatus::ConsumingSnapshot { .. } => {
                unreachable!(
                    "should not start consuming upstream for a job that are consuming snapshot"
                )
            }
            CreatingStreamingJobStatus::ConsumingLogStore { .. } => {
                let prev_epoch = barrier_info.prev_epoch();
                {
                    assert!(barrier_info.kind.is_checkpoint());
                    *self = CreatingStreamingJobStatus::Finishing(prev_epoch);
                }
            }
            CreatingStreamingJobStatus::Finishing { .. } => {
                unreachable!("should not start consuming upstream for a job again")
            }
        }
    }

    pub(super) fn on_new_upstream_epoch(&mut self, barrier_info: &BarrierInfo) -> Vec<BarrierInfo> {
        match self {
            CreatingStreamingJobStatus::ConsumingSnapshot {
                pending_upstream_barriers,
                prev_epoch_fake_physical_time,
                pending_non_checkpoint_barriers,
                ..
            } => {
                pending_upstream_barriers.push(barrier_info.clone());
                vec![CreatingStreamingJobStatus::new_fake_barrier(
                    prev_epoch_fake_physical_time,
                    pending_non_checkpoint_barriers,
                    match barrier_info.kind {
                        BarrierKind::Barrier => PbBarrierKind::Barrier,
                        BarrierKind::Checkpoint(_) => PbBarrierKind::Checkpoint,
                        BarrierKind::Initial => {
                            unreachable!("upstream new epoch should not be initial")
                        }
                    },
                )]
            }
            CreatingStreamingJobStatus::ConsumingLogStore {
                barriers_to_inject, ..
            } => barriers_to_inject
                .take()
                .into_iter()
                .flatten()
                .chain([barrier_info.clone()])
                .collect(),
            CreatingStreamingJobStatus::Finishing { .. } => {
                vec![]
            }
        }
    }

    pub(super) fn new_fake_barrier(
        prev_epoch_fake_physical_time: &mut u64,
        pending_non_checkpoint_barriers: &mut Vec<u64>,
        kind: PbBarrierKind,
    ) -> BarrierInfo {
        {
            {
                let prev_epoch =
                    TracedEpoch::new(Epoch::from_physical_time(*prev_epoch_fake_physical_time));
                *prev_epoch_fake_physical_time += 1;
                let curr_epoch =
                    TracedEpoch::new(Epoch::from_physical_time(*prev_epoch_fake_physical_time));
                pending_non_checkpoint_barriers.push(prev_epoch.value().0);
                let kind = match kind {
                    PbBarrierKind::Unspecified => {
                        unreachable!()
                    }
                    PbBarrierKind::Initial => {
                        pending_non_checkpoint_barriers.clear();
                        BarrierKind::Initial
                    }
                    PbBarrierKind::Barrier => BarrierKind::Barrier,
                    PbBarrierKind::Checkpoint => {
                        BarrierKind::Checkpoint(take(pending_non_checkpoint_barriers))
                    }
                };
                BarrierInfo {
                    prev_epoch,
                    curr_epoch,
                    kind,
                }
            }
        }
    }

    pub(super) fn is_finishing(&self) -> bool {
        matches!(self, Self::Finishing(_))
    }
}
