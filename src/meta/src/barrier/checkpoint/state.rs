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

use std::assert_matches::assert_matches;
use std::collections::{HashMap, HashSet};
use std::mem::take;

use risingwave_common::catalog::TableId;
use risingwave_common::id::JobId;
use risingwave_common::util::epoch::Epoch;
use risingwave_pb::hummock::HummockVersionStats;
use risingwave_pb::stream_plan::barrier_mutation::PbMutation;
use risingwave_pb::stream_plan::{
    PbDropSubscriptionsMutation, PbStartFragmentBackfillMutation, PbSubscriptionUpstreamInfo,
};
use tracing::warn;

use crate::MetaResult;
use crate::barrier::checkpoint::{CreatingStreamingJobControl, DatabaseCheckpointControl};
use crate::barrier::info::{
    BarrierInfo, CreateStreamingJobStatus, InflightStreamingJobInfo, SubscriberType,
};
use crate::barrier::rpc::ControlStreamManager;
use crate::barrier::utils::NodeToCollect;
use crate::barrier::{BarrierKind, Command, CreateStreamingJobType, TracedEpoch};
use crate::controller::fragment::InflightFragmentInfo;
use crate::stream::fill_snapshot_backfill_epoch;

/// The latest state of `GlobalBarrierWorker` after injecting the latest barrier.
pub(in crate::barrier) struct BarrierWorkerState {
    /// The last sent `prev_epoch`
    ///
    /// There's no need to persist this field. On recovery, we will restore this from the latest
    /// committed snapshot in `HummockManager`.
    in_flight_prev_epoch: TracedEpoch,

    /// The `prev_epoch` of pending non checkpoint barriers
    pending_non_checkpoint_barriers: Vec<u64>,

    /// Whether the cluster is paused.
    is_paused: bool,
}

impl BarrierWorkerState {
    pub(super) fn new() -> Self {
        Self {
            in_flight_prev_epoch: TracedEpoch::new(Epoch::now()),
            pending_non_checkpoint_barriers: vec![],
            is_paused: false,
        }
    }

    pub fn recovery(in_flight_prev_epoch: TracedEpoch, is_paused: bool) -> Self {
        Self {
            in_flight_prev_epoch,
            pending_non_checkpoint_barriers: vec![],
            is_paused,
        }
    }

    pub fn is_paused(&self) -> bool {
        self.is_paused
    }

    fn set_is_paused(&mut self, is_paused: bool) {
        if self.is_paused != is_paused {
            tracing::info!(
                currently_paused = self.is_paused,
                newly_paused = is_paused,
                "update paused state"
            );
            self.is_paused = is_paused;
        }
    }

    pub fn in_flight_prev_epoch(&self) -> &TracedEpoch {
        &self.in_flight_prev_epoch
    }

    /// Returns the `BarrierInfo` for the next barrier, and updates the state.
    pub fn next_barrier_info(
        &mut self,
        is_checkpoint: bool,
        curr_epoch: TracedEpoch,
    ) -> BarrierInfo {
        assert!(
            self.in_flight_prev_epoch.value() < curr_epoch.value(),
            "curr epoch regress. {} > {}",
            self.in_flight_prev_epoch.value(),
            curr_epoch.value()
        );
        let prev_epoch = self.in_flight_prev_epoch.clone();
        self.in_flight_prev_epoch = curr_epoch.clone();
        self.pending_non_checkpoint_barriers
            .push(prev_epoch.value().0);
        let kind = if is_checkpoint {
            let epochs = take(&mut self.pending_non_checkpoint_barriers);
            BarrierKind::Checkpoint(epochs)
        } else {
            BarrierKind::Barrier
        };
        BarrierInfo {
            prev_epoch,
            curr_epoch,
            kind,
        }
    }
}

pub(super) struct ApplyCommandInfo {
    pub mv_subscription_max_retention: HashMap<TableId, u64>,
    pub table_ids_to_commit: HashSet<TableId>,
    pub jobs_to_wait: HashSet<JobId>,
    pub node_to_collect: NodeToCollect,
    pub command: Option<Command>,
}

impl DatabaseCheckpointControl {
    /// Returns the inflight actor infos that have included the newly added actors in the given command. The dropped actors
    /// will be removed from the state after the info get resolved.
    pub(super) fn apply_command(
        &mut self,
        mut command: Option<Command>,
        barrier_info: &BarrierInfo,
        control_stream_manager: &mut ControlStreamManager,
        hummock_version_stats: &HummockVersionStats,
    ) -> MetaResult<ApplyCommandInfo> {
        let mut edges = self
            .database_info
            .build_edge(command.as_ref(), &*control_stream_manager);

        // Insert newly added snapshot backfill job
        if let &mut Some(Command::CreateStreamingJob {
            ref mut job_type,
            ref mut info,
            ref cross_db_snapshot_backfill_info,
        }) = &mut command
        {
            match job_type {
                CreateStreamingJobType::Normal | CreateStreamingJobType::SinkIntoTable(_) => {
                    for fragment in info.stream_job_fragments.inner.fragments.values_mut() {
                        fill_snapshot_backfill_epoch(
                            &mut fragment.nodes,
                            None,
                            cross_db_snapshot_backfill_info,
                        )?;
                    }
                }
                CreateStreamingJobType::SnapshotBackfill(snapshot_backfill_info) => {
                    assert!(!self.state.is_paused());
                    let snapshot_epoch = barrier_info.prev_epoch();
                    // set snapshot epoch of upstream table for snapshot backfill
                    for snapshot_backfill_epoch in snapshot_backfill_info
                        .upstream_mv_table_id_to_backfill_epoch
                        .values_mut()
                    {
                        assert_eq!(
                            snapshot_backfill_epoch.replace(snapshot_epoch),
                            None,
                            "must not set previously"
                        );
                    }
                    for fragment in info.stream_job_fragments.inner.fragments.values_mut() {
                        fill_snapshot_backfill_epoch(
                            &mut fragment.nodes,
                            Some(snapshot_backfill_info),
                            cross_db_snapshot_backfill_info,
                        )?;
                    }
                    let job_id = info.stream_job_fragments.stream_job_id();
                    let snapshot_backfill_upstream_tables = snapshot_backfill_info
                        .upstream_mv_table_id_to_backfill_epoch
                        .keys()
                        .cloned()
                        .collect();

                    let job = CreatingStreamingJobControl::new(
                        info,
                        snapshot_backfill_upstream_tables,
                        snapshot_epoch,
                        hummock_version_stats,
                        control_stream_manager,
                        edges.as_mut().expect("should exist"),
                    )?;

                    self.database_info
                        .shared_actor_infos
                        .upsert(self.database_id, job.fragment_infos_with_job_id());

                    self.creating_streaming_job_controls.insert(job_id, job);
                }
            }
        }

        // update the fragment_infos outside pre_apply
        let post_apply_changes = if let Some(Command::CreateStreamingJob {
            job_type: CreateStreamingJobType::SnapshotBackfill(_),
            ..
        }) = command
        {
            None
        } else if let Some((new_job, fragment_changes)) =
            command.as_ref().and_then(Command::fragment_changes)
        {
            Some(self.database_info.pre_apply(new_job, fragment_changes))
        } else {
            None
        };

        match &command {
            Some(Command::CreateSubscription {
                subscription_id,
                upstream_mv_table_id,
                retention_second,
            }) => {
                self.database_info.register_subscriber(
                    upstream_mv_table_id.as_job_id(),
                    subscription_id.as_subscriber_id(),
                    SubscriberType::Subscription(*retention_second),
                );
            }
            Some(Command::CreateStreamingJob {
                info,
                job_type: CreateStreamingJobType::SnapshotBackfill(snapshot_backfill_info),
                ..
            }) => {
                for upstream_mv_table_id in snapshot_backfill_info
                    .upstream_mv_table_id_to_backfill_epoch
                    .keys()
                {
                    self.database_info.register_subscriber(
                        upstream_mv_table_id.as_job_id(),
                        info.streaming_job.id().as_subscriber_id(),
                        SubscriberType::SnapshotBackfill,
                    );
                }
            }
            _ => {}
        };

        let mut table_ids_to_commit: HashSet<_> = self.database_info.existing_table_ids().collect();
        let actors_to_create = command.as_ref().and_then(|command| {
            command.actors_to_create(&self.database_info, &mut edges, control_stream_manager)
        });
        let node_actors =
            InflightFragmentInfo::actor_ids_to_collect(self.database_info.fragment_infos());

        if let Some(post_apply_changes) = post_apply_changes {
            self.database_info.post_apply(post_apply_changes);
        }

        let prev_is_paused = self.state.is_paused();
        let curr_is_paused = match command {
            Some(Command::Pause) => true,
            Some(Command::Resume) => false,
            _ => prev_is_paused,
        };
        self.state.set_is_paused(curr_is_paused);

        let mutation = if let Some(c) = &command {
            c.to_mutation(
                prev_is_paused,
                &mut edges,
                control_stream_manager,
                &mut self.database_info,
            )?
        } else {
            None
        };

        let mut finished_snapshot_backfill_jobs = HashSet::new();
        let mutation = match mutation {
            Some(mutation) => Some(mutation),
            None => {
                let mut subscription_info_to_drop = vec![];
                if barrier_info.kind.is_checkpoint() {
                    for (&job_id, creating_job) in &mut self.creating_streaming_job_controls {
                        if let Some(upstream_table_ids) = creating_job.should_merge_to_upstream() {
                            subscription_info_to_drop.extend(upstream_table_ids.iter().map(
                                |upstream_table_id| PbSubscriptionUpstreamInfo {
                                    subscriber_id: job_id.as_subscriber_id(),
                                    upstream_mv_table_id: *upstream_table_id,
                                },
                            ));
                            for upstream_mv_table_id in upstream_table_ids {
                                assert_matches!(
                                    self.database_info.unregister_subscriber(
                                        upstream_mv_table_id.as_job_id(),
                                        job_id.as_subscriber_id()
                                    ),
                                    Some(SubscriberType::SnapshotBackfill)
                                );
                            }
                            let job_fragments = creating_job
                                .start_consume_upstream(control_stream_manager, barrier_info)?;
                            finished_snapshot_backfill_jobs.insert(job_id);
                            table_ids_to_commit.extend(InflightFragmentInfo::existing_table_ids(
                                job_fragments.values(),
                            ));
                            self.database_info.add_existing(InflightStreamingJobInfo {
                                job_id,
                                fragment_infos: job_fragments,
                                subscribers: Default::default(), // no initial subscribers for newly created snapshot backfill
                                status: CreateStreamingJobStatus::Created,
                                cdc_table_backfill_tracker: None, // no cdc table backfill for snapshot backfill
                            });
                        }
                    }
                }
                if !finished_snapshot_backfill_jobs.is_empty() {
                    Some(PbMutation::DropSubscriptions(PbDropSubscriptionsMutation {
                        info: subscription_info_to_drop,
                    }))
                } else {
                    let fragment_ids = self.database_info.take_pending_backfill_nodes();
                    if fragment_ids.is_empty() {
                        None
                    } else {
                        Some(PbMutation::StartFragmentBackfill(
                            PbStartFragmentBackfillMutation { fragment_ids },
                        ))
                    }
                }
            }
        };

        #[expect(clippy::collapsible_if)]
        if let Some(Command::DropSubscription {
            subscription_id,
            upstream_mv_table_id,
        }) = command
        {
            if self
                .database_info
                .unregister_subscriber(
                    upstream_mv_table_id.as_job_id(),
                    subscription_id.as_subscriber_id(),
                )
                .is_none()
            {
                warn!(%subscription_id, %upstream_mv_table_id, "no subscription to drop");
            }
        }

        for (job_id, creating_job) in &mut self.creating_streaming_job_controls {
            if !finished_snapshot_backfill_jobs.contains(job_id) {
                creating_job.on_new_upstream_barrier(control_stream_manager, barrier_info)?;
            }
        }

        let node_to_collect = control_stream_manager.inject_barrier(
            self.database_id,
            None,
            mutation,
            barrier_info,
            &node_actors,
            InflightFragmentInfo::existing_table_ids(self.database_info.fragment_infos()),
            actors_to_create,
        )?;

        Ok(ApplyCommandInfo {
            mv_subscription_max_retention: self.database_info.max_subscription_retention(),
            table_ids_to_commit,
            jobs_to_wait: finished_snapshot_backfill_jobs,
            node_to_collect,
            command,
        })
    }
}
