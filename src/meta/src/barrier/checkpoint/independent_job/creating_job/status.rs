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

use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet, VecDeque};
use std::mem::replace;
use std::time::Duration;

use risingwave_common::hash::ActorId;
use risingwave_common::util::epoch::Epoch;
use risingwave_pb::id::{FragmentId, PartialGraphId};
use risingwave_pb::stream_plan::barrier_mutation::Mutation;
use risingwave_pb::stream_service::barrier_complete_response::{
    CreateMviewProgress, PbCreateMviewProgress,
};
use tracing::warn;

use crate::barrier::BarrierInfo;
use crate::barrier::checkpoint::independent_job::SnapshotPhaseControl;
use crate::barrier::checkpoint::independent_job::creating_job::CreatingJobInfo;
use crate::barrier::command::{ThrottleConfigMap, extract_throttle_config};
use crate::barrier::partial_graph::PartialGraphManager;
use crate::barrier::progress::TrackingJob;
use crate::controller::fragment::InflightFragmentInfo;

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

    pub(super) fn gen_backfill_progress(&self) -> String {
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
        snapshot: SnapshotPhaseControl,
        pending_upstream_barriers: Vec<BarrierInfo>,
        snapshot_backfill_actors: HashSet<ActorId>,
        info: CreatingJobInfo,
    },
    /// The creating job is consuming log store.
    ///
    /// Will transit to `Finishing` on `on_new_upstream_epoch` when `start_consume_upstream` is `true`.
    ConsumingLogStore {
        tracking_job: TrackingJob,
        info: CreatingJobInfo,
        log_store_progress_tracker: CreateMviewLogStoreProgressTracker,
        pending_barriers: VecDeque<BarrierInfo>,
    },
    /// All backfill actors have started consuming upstream, and the job
    /// will be finished when all previously injected barriers have been collected
    /// Store the `prev_epoch` that will finish at.
    Finishing(u64, TrackingJob),
    PlaceHolder,
}

impl CreatingStreamingJobStatus {
    pub(super) fn update_progress(
        &mut self,
        create_mview_progress: impl IntoIterator<Item = &CreateMviewProgress>,
    ) {
        match self {
            &mut Self::ConsumingSnapshot {
                ref mut snapshot,
                ref mut pending_upstream_barriers,
                ..
            } => {
                if snapshot.apply_progress(create_mview_progress) {
                    let pending_barriers: VecDeque<_> = [snapshot.finish_snapshot_barrier()]
                        .into_iter()
                        .chain(pending_upstream_barriers.drain(..))
                        .collect();

                    let CreatingStreamingJobStatus::ConsumingSnapshot {
                        snapshot,
                        info,
                        snapshot_backfill_actors,
                        ..
                    } = replace(self, CreatingStreamingJobStatus::PlaceHolder)
                    else {
                        unreachable!()
                    };

                    let tracking_job = snapshot.create_mview_tracker.into_tracking_job();

                    *self = CreatingStreamingJobStatus::ConsumingLogStore {
                        tracking_job,
                        info,
                        log_store_progress_tracker: CreateMviewLogStoreProgressTracker::new(
                            snapshot_backfill_actors.iter().cloned(),
                            pending_barriers
                                .back()
                                .map(|barrier_info| {
                                    barrier_info
                                        .prev_epoch()
                                        .saturating_sub(snapshot.snapshot_epoch)
                                })
                                .unwrap_or(0),
                        ),
                        pending_barriers,
                    };
                }
            }
            CreatingStreamingJobStatus::ConsumingLogStore {
                log_store_progress_tracker,
                ..
            } => {
                log_store_progress_tracker.update(create_mview_progress);
            }
            CreatingStreamingJobStatus::Finishing(..) => {}
            CreatingStreamingJobStatus::PlaceHolder => {
                unreachable!()
            }
        }
    }

    pub(super) fn start_consume_upstream(&mut self, barrier_info: &BarrierInfo) -> CreatingJobInfo {
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
                    let CreatingStreamingJobStatus::ConsumingLogStore {
                        info, tracking_job, ..
                    } = replace(self, CreatingStreamingJobStatus::PlaceHolder)
                    else {
                        unreachable!()
                    };
                    *self = CreatingStreamingJobStatus::Finishing(prev_epoch, tracking_job);
                    info
                }
            }
            CreatingStreamingJobStatus::Finishing { .. } => {
                unreachable!("should not start consuming upstream for a job again")
            }
            CreatingStreamingJobStatus::PlaceHolder => {
                unreachable!()
            }
        }
    }

    pub(super) fn on_new_upstream_epoch(
        &mut self,
        partial_graph_manager: &PartialGraphManager,
        partial_graph_id: PartialGraphId,
        max_pending_barrier_num: usize,
        barrier_info: &BarrierInfo,
        mutation: Option<Mutation>, // mutation to be set for the first barrier to inject
    ) -> Vec<(BarrierInfo, Option<Mutation>)> {
        let resolve_initial_barrier_num_to_inject = || {
            max_pending_barrier_num
                .saturating_sub(partial_graph_manager.pending_barrier_num(partial_graph_id))
        };
        match self {
            CreatingStreamingJobStatus::ConsumingSnapshot {
                pending_upstream_barriers,
                snapshot,
                ..
            } => {
                let mutation = mutation.or_else(|| snapshot.take_start_backfill_mutation());
                let barrier_num_to_inject = resolve_initial_barrier_num_to_inject();
                pending_upstream_barriers.push(barrier_info.clone());
                // Mutation barriers must be forwarded even when the partial graph has reached the
                // configured pending-barrier limit.
                if barrier_num_to_inject == 0 && mutation.is_none() {
                    return vec![];
                }
                vec![(snapshot.next_fake_barrier(&barrier_info.kind), mutation)]
            }
            CreatingStreamingJobStatus::ConsumingLogStore {
                pending_barriers, ..
            } => {
                // Throttle has no effect on the snapshot executor after it starts consuming the
                // log store. The updated fragment plan is kept for the actors created on merge,
                // so the mutation does not need to be forwarded in this phase.
                drain_pending_barriers(
                    pending_barriers,
                    barrier_info.clone(),
                    resolve_initial_barrier_num_to_inject(),
                )
                .into_iter()
                .map(|barrier_info| (barrier_info, None))
                .collect()
            }
            CreatingStreamingJobStatus::Finishing { .. } => vec![],
            CreatingStreamingJobStatus::PlaceHolder => {
                unreachable!()
            }
        }
    }

    pub(super) fn fragment_infos(&self) -> Option<&HashMap<FragmentId, InflightFragmentInfo>> {
        match self {
            CreatingStreamingJobStatus::ConsumingSnapshot { info, .. }
            | CreatingStreamingJobStatus::ConsumingLogStore { info, .. } => {
                Some(&info.fragment_infos)
            }
            CreatingStreamingJobStatus::Finishing(..) => None,
            CreatingStreamingJobStatus::PlaceHolder => {
                unreachable!()
            }
        }
    }

    pub(super) fn pre_apply_throttle(
        &mut self,
        config: &mut ThrottleConfigMap,
    ) -> Option<Mutation> {
        let fragment_infos = match self {
            CreatingStreamingJobStatus::ConsumingSnapshot { info, .. }
            | CreatingStreamingJobStatus::ConsumingLogStore { info, .. } => {
                &mut info.fragment_infos
            }
            CreatingStreamingJobStatus::Finishing(..) => return None,
            CreatingStreamingJobStatus::PlaceHolder => {
                unreachable!()
            }
        };

        extract_throttle_config(config, |fragment_id, stream_node| {
            if let Some(fragment_info) = fragment_infos.get_mut(&fragment_id) {
                fragment_info.nodes = stream_node.clone();
                true
            } else {
                false
            }
        })
    }
}

fn drain_pending_barriers(
    pending_barriers: &mut VecDeque<BarrierInfo>,
    new_upstream_barrier: BarrierInfo,
    barrier_num_to_inject: usize,
) -> Vec<BarrierInfo> {
    pending_barriers.push_back(new_upstream_barrier);
    let barrier_count = pending_barriers.len().min(barrier_num_to_inject);
    pending_barriers.drain(..barrier_count).collect()
}

#[cfg(test)]
mod tests {
    use risingwave_pb::stream_plan::PbStreamNode;

    use super::*;
    use crate::barrier::{BarrierKind, TracedEpoch};

    fn barrier(prev_epoch: u64, curr_epoch: u64) -> BarrierInfo {
        BarrierInfo {
            prev_epoch: TracedEpoch::new(Epoch(prev_epoch)),
            curr_epoch: TracedEpoch::new(Epoch(curr_epoch)),
            kind: BarrierKind::Barrier,
        }
    }

    fn epochs(barriers: &[BarrierInfo]) -> Vec<(u64, u64)> {
        barriers
            .iter()
            .map(|barrier| (barrier.prev_epoch(), barrier.curr_epoch()))
            .collect()
    }

    #[test]
    fn test_drain_pending_barriers_with_available_capacity() {
        let mut pending_barriers = VecDeque::from([barrier(1, 2), barrier(2, 3), barrier(3, 4)]);

        let injected = drain_pending_barriers(&mut pending_barriers, barrier(4, 5), 0);
        assert!(injected.is_empty());
        assert_eq!(
            epochs(pending_barriers.make_contiguous()),
            vec![(1, 2), (2, 3), (3, 4), (4, 5)]
        );

        let injected = drain_pending_barriers(&mut pending_barriers, barrier(5, 6), 2);
        assert_eq!(epochs(&injected), vec![(1, 2), (2, 3)]);
        assert_eq!(
            epochs(pending_barriers.make_contiguous()),
            vec![(3, 4), (4, 5), (5, 6)]
        );

        let injected = drain_pending_barriers(&mut pending_barriers, barrier(6, 7), 2);
        assert_eq!(epochs(&injected), vec![(3, 4), (4, 5)]);
        assert_eq!(
            epochs(pending_barriers.make_contiguous()),
            vec![(5, 6), (6, 7)]
        );

        let injected = drain_pending_barriers(&mut pending_barriers, barrier(7, 8), 2);
        assert_eq!(epochs(&injected), vec![(5, 6), (6, 7)]);
        assert_eq!(epochs(pending_barriers.make_contiguous()), vec![(7, 8)]);
    }

    #[test]
    fn test_drain_pending_barriers_without_backlog() {
        let mut pending_barriers = VecDeque::new();

        let injected = drain_pending_barriers(&mut pending_barriers, barrier(1, 2), 100);

        assert_eq!(epochs(&injected), vec![(1, 2)]);
        assert!(pending_barriers.is_empty());
    }

    #[test]
    fn test_pre_apply_throttle_before_merge() {
        let job_id = risingwave_common::id::JobId::new(1);
        let fragment_id = FragmentId::new(1);
        let old_node = PbStreamNode {
            identity: "old".to_owned(),
            ..Default::default()
        };
        let new_node = PbStreamNode {
            identity: "new".to_owned(),
            ..Default::default()
        };
        let fragment_infos = HashMap::from([(
            fragment_id,
            InflightFragmentInfo {
                fragment_id,
                distribution_type: risingwave_meta_model::fragment::DistributionType::Single,
                fragment_type_mask: Default::default(),
                vnode_count: 1,
                nodes: old_node,
                actors: Default::default(),
                state_table_ids: Default::default(),
            },
        )]);
        let mut status = CreatingStreamingJobStatus::ConsumingLogStore {
            tracking_job: TrackingJob::recovered(job_id, &fragment_infos),
            info: CreatingJobInfo {
                fragment_infos,
                upstream_fragment_downstreams: Default::default(),
                downstreams: Default::default(),
                snapshot_backfill_upstream_tables: Default::default(),
                stream_actors: Default::default(),
            },
            log_store_progress_tracker: CreateMviewLogStoreProgressTracker::new(
                std::iter::empty(),
                0,
            ),
            pending_barriers: Default::default(),
        };
        let mut config = HashMap::from([(
            fragment_id,
            (
                risingwave_pb::stream_plan::throttle_mutation::ThrottleConfig {
                    rate_limit: Some(1_000),
                    throttle_type: Default::default(),
                },
                new_node.clone(),
            ),
        )]);

        assert!(status.pre_apply_throttle(&mut config).is_some());
        assert!(config.is_empty());

        let info = status.start_consume_upstream(&BarrierInfo {
            prev_epoch: TracedEpoch::new(Epoch(1)),
            curr_epoch: TracedEpoch::new(Epoch(2)),
            kind: BarrierKind::Checkpoint(vec![1]),
        });
        assert_eq!(info.fragment_infos[&fragment_id].nodes, new_node);
    }
}
