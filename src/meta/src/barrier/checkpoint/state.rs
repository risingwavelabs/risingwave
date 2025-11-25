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

use risingwave_common::catalog::{DatabaseId, TableId};
use risingwave_common::id::JobId;
use risingwave_common::util::epoch::Epoch;
use risingwave_pb::id::{ActorId, FragmentId, WorkerId};
use risingwave_pb::stream_plan::barrier_mutation::Mutation;
use tracing::warn;

use crate::barrier::edge_builder::FragmentEdgeBuildResult;
use crate::barrier::info::{
    BarrierInfo, CreateStreamingJobStatus, InflightDatabaseInfo, InflightStreamingJobInfo,
    SharedActorInfos, SubscriberType,
};
use crate::barrier::rpc::ControlStreamManager;
use crate::barrier::{BarrierKind, Command, CreateStreamingJobType, TracedEpoch};
use crate::controller::fragment::InflightFragmentInfo;
use crate::model::StreamJobActorsToCreate;

/// The latest state of `GlobalBarrierWorker` after injecting the latest barrier.
pub(in crate::barrier) struct BarrierWorkerState {
    /// The last sent `prev_epoch`
    ///
    /// There's no need to persist this field. On recovery, we will restore this from the latest
    /// committed snapshot in `HummockManager`.
    in_flight_prev_epoch: TracedEpoch,

    /// The `prev_epoch` of pending non checkpoint barriers
    pending_non_checkpoint_barriers: Vec<u64>,

    /// Running info of database.
    pub(super) database_info: InflightDatabaseInfo,

    /// Whether the cluster is paused.
    is_paused: bool,
}

impl BarrierWorkerState {
    pub(super) fn new(database_id: DatabaseId, shared_actor_infos: SharedActorInfos) -> Self {
        Self {
            in_flight_prev_epoch: TracedEpoch::new(Epoch::now()),
            pending_non_checkpoint_barriers: vec![],
            database_info: InflightDatabaseInfo::empty(database_id, shared_actor_infos),
            is_paused: false,
        }
    }

    pub fn recovery(
        database_id: DatabaseId,
        shared_actor_infos: SharedActorInfos,
        in_flight_prev_epoch: TracedEpoch,
        jobs: impl Iterator<Item = InflightStreamingJobInfo>,
        is_paused: bool,
    ) -> Self {
        Self {
            in_flight_prev_epoch,
            pending_non_checkpoint_barriers: vec![],
            database_info: InflightDatabaseInfo::recover(database_id, jobs, shared_actor_infos),
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
        command: Option<&Command>,
        is_checkpoint: bool,
        curr_epoch: TracedEpoch,
    ) -> Option<BarrierInfo> {
        if self.database_info.is_empty()
            && !matches!(&command, Some(Command::CreateStreamingJob { .. }))
        {
            return None;
        };
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
        Some(BarrierInfo {
            prev_epoch,
            curr_epoch,
            kind,
        })
    }
}

pub(super) struct ApplyCommandInfo {
    pub node_actors: HashMap<WorkerId, HashSet<ActorId>>,
    pub actors_to_create: Option<StreamJobActorsToCreate>,
    pub mv_subscription_max_retention: HashMap<TableId, u64>,
    pub table_ids_to_commit: HashSet<TableId>,
    pub table_ids_to_sync: HashSet<TableId>,
    pub jobs_to_wait: HashSet<JobId>,
    pub mutation: Option<Mutation>,
}

impl BarrierWorkerState {
    /// Returns the inflight actor infos that have included the newly added actors in the given command. The dropped actors
    /// will be removed from the state after the info get resolved.
    pub(super) fn apply_command(
        &mut self,
        command: Option<&Command>,
        finished_snapshot_backfill_job_fragments: HashMap<
            JobId,
            HashMap<FragmentId, InflightFragmentInfo>,
        >,
        edges: &mut Option<FragmentEdgeBuildResult>,
        control_stream_manager: &ControlStreamManager,
    ) -> ApplyCommandInfo {
        // update the fragment_infos outside pre_apply
        let post_apply_changes = if let Some(Command::CreateStreamingJob {
            job_type: CreateStreamingJobType::SnapshotBackfill(_),
            ..
        }) = command
        {
            None
        } else if let Some((new_job_id, fragment_changes)) =
            command.and_then(Command::fragment_changes)
        {
            Some(self.database_info.pre_apply(new_job_id, fragment_changes))
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
                    subscription_id.as_raw_id(),
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
                        info.streaming_job.id().as_raw_id(),
                        SubscriberType::SnapshotBackfill,
                    );
                }
            }
            _ => {}
        };

        let mut table_ids_to_commit: HashSet<_> = self.database_info.existing_table_ids().collect();
        let actors_to_create = command.as_ref().and_then(|command| {
            command.actors_to_create(&self.database_info, edges, control_stream_manager)
        });
        let node_actors =
            InflightFragmentInfo::actor_ids_to_collect(self.database_info.fragment_infos());

        if let Some(post_apply_changes) = post_apply_changes {
            self.database_info.post_apply(post_apply_changes);
        }

        let mut jobs_to_wait = HashSet::new();
        if let Some(Command::MergeSnapshotBackfillStreamingJobs(jobs_to_merge)) = command {
            assert!(
                jobs_to_merge
                    .keys()
                    .all(|job_id| finished_snapshot_backfill_job_fragments.contains_key(job_id))
            );
            for (job_id, job_fragments) in finished_snapshot_backfill_job_fragments {
                assert!(jobs_to_merge.contains_key(&job_id));
                jobs_to_wait.insert(job_id);
                table_ids_to_commit.extend(InflightFragmentInfo::existing_table_ids(
                    job_fragments.values(),
                ));
                self.database_info.add_existing(InflightStreamingJobInfo {
                    job_id,
                    fragment_infos: job_fragments,
                    subscribers: Default::default(), // no initial subscribers for newly created snapshot backfill
                    status: CreateStreamingJobStatus::Created,
                });
            }
        } else {
            assert!(finished_snapshot_backfill_job_fragments.is_empty());
        }

        match &command {
            Some(Command::DropSubscription {
                subscription_id,
                upstream_mv_table_id,
            }) => {
                if self
                    .database_info
                    .unregister_subscriber(
                        upstream_mv_table_id.as_job_id(),
                        subscription_id.as_raw_id(),
                    )
                    .is_none()
                {
                    warn!(%subscription_id, %upstream_mv_table_id, "no subscription to drop");
                }
            }
            Some(Command::MergeSnapshotBackfillStreamingJobs(snapshot_backfill_jobs)) => {
                for (snapshot_backfill_job_id, upstream_tables) in snapshot_backfill_jobs {
                    for upstream_mv_table_id in upstream_tables {
                        assert_matches!(
                            self.database_info.unregister_subscriber(
                                upstream_mv_table_id.as_job_id(),
                                snapshot_backfill_job_id.as_raw_id()
                            ),
                            Some(SubscriberType::SnapshotBackfill)
                        );
                    }
                }
            }
            _ => {}
        }

        let prev_is_paused = self.is_paused();
        let curr_is_paused = match command {
            Some(Command::Pause) => true,
            Some(Command::Resume) => false,
            _ => prev_is_paused,
        };
        self.set_is_paused(curr_is_paused);

        let table_ids_to_sync =
            InflightFragmentInfo::existing_table_ids(self.database_info.fragment_infos()).collect();

        let mutation = command
            .as_ref()
            .and_then(|c| c.to_mutation(prev_is_paused, edges, control_stream_manager));

        ApplyCommandInfo {
            node_actors,
            actors_to_create,
            mv_subscription_max_retention: self.database_info.max_subscription_retention(),
            table_ids_to_commit,
            table_ids_to_sync,
            jobs_to_wait,
            mutation,
        }
    }
}
