// Copyright 2024 RisingWave Labs
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
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::mem::take;

use risingwave_common::bail;
use risingwave_common::catalog::TableId;
use risingwave_common::id::JobId;
use risingwave_common::util::epoch::Epoch;
use risingwave_pb::hummock::HummockVersionStats;
use risingwave_pb::source::{ConnectorSplit, ConnectorSplits};
use risingwave_pb::stream_plan::barrier_mutation::{Mutation, PbMutation};
use risingwave_pb::stream_plan::update_mutation::PbDispatcherUpdate;
use risingwave_pb::stream_plan::{
    PbStartFragmentBackfillMutation, PbSubscriptionUpstreamInfo, PbUpdateMutation, ThrottleMutation,
};
use tracing::warn;

use crate::MetaResult;
use crate::barrier::checkpoint::{CreatingStreamingJobControl, DatabaseCheckpointControl};
use crate::barrier::command::PostCollectCommand;
use crate::barrier::context::CreateSnapshotBackfillJobCommandInfo;
use crate::barrier::edge_builder::FragmentEdgeBuilder;
use crate::barrier::info::{
    BarrierInfo, CreateStreamingJobStatus, InflightStreamingJobInfo, SubscriberType,
};
use crate::barrier::notifier::Notifier;
use crate::barrier::partial_graph::PartialGraphManager;
use crate::barrier::rpc::to_partial_graph_id;
use crate::barrier::{BarrierKind, Command, CreateStreamingJobType, TracedEpoch};
use crate::controller::fragment::InflightFragmentInfo;
use crate::stream::{GlobalActorIdGen, fill_snapshot_backfill_epoch};

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
    pub command: PostCollectCommand,
}

impl DatabaseCheckpointControl {
    /// Returns the inflight actor infos that have included the newly added actors in the given command. The dropped actors
    /// will be removed from the state after the info get resolved.
    pub(super) fn apply_command(
        &mut self,
        mut command: Option<Command>,
        notifiers: &mut Vec<Notifier>,
        barrier_info: &BarrierInfo,
        partial_graph_manager: &mut PartialGraphManager,
        hummock_version_stats: &HummockVersionStats,
    ) -> MetaResult<ApplyCommandInfo> {
        debug_assert!(
            !matches!(
                command,
                Some(Command::RescheduleIntent {
                    reschedule_plan: None,
                    ..
                })
            ),
            "reschedule intent must be resolved before apply"
        );
        if matches!(
            command,
            Some(Command::RescheduleIntent {
                reschedule_plan: None,
                ..
            })
        ) {
            bail!("reschedule intent must be resolved before apply");
        }
        let mut edges = self.database_info.build_edge(
            command.as_ref(),
            partial_graph_manager.control_stream_manager(),
        );

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

                    let Entry::Vacant(entry) = self.creating_streaming_job_controls.entry(job_id)
                    else {
                        panic!("duplicated creating snapshot backfill job {job_id}");
                    };

                    let job = CreatingStreamingJobControl::new(
                        entry,
                        CreateSnapshotBackfillJobCommandInfo {
                            info: info.clone(),
                            snapshot_backfill_info: snapshot_backfill_info.clone(),
                            cross_db_snapshot_backfill_info: cross_db_snapshot_backfill_info
                                .clone(),
                        },
                        take(notifiers),
                        snapshot_backfill_upstream_tables,
                        snapshot_epoch,
                        hummock_version_stats,
                        partial_graph_manager,
                        edges.as_mut().expect("should exist"),
                    )?;

                    self.database_info
                        .shared_actor_infos
                        .upsert(self.database_id, job.fragment_infos_with_job_id());
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
            Some(Command::AlterSubscriptionRetention {
                subscription_id,
                upstream_mv_table_id,
                retention_second,
            }) => {
                self.database_info.update_subscription_retention(
                    upstream_mv_table_id.as_job_id(),
                    subscription_id.as_subscriber_id(),
                    *retention_second,
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
        let mut actors_to_create = command.as_ref().and_then(|command| {
            command.actors_to_create(
                &self.database_info,
                &mut edges,
                partial_graph_manager.control_stream_manager(),
            )
        });
        let mut node_actors =
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
                partial_graph_manager.control_stream_manager(),
                &mut self.database_info,
            )?
        } else {
            None
        };

        let mut finished_snapshot_backfill_jobs = HashSet::new();
        let mutation = match mutation {
            Some(mutation) => Some(mutation),
            None => {
                let mut finished_snapshot_backfill_job_info = HashMap::new();
                if barrier_info.kind.is_checkpoint() {
                    for (&job_id, creating_job) in &mut self.creating_streaming_job_controls {
                        if creating_job.should_merge_to_upstream() {
                            let info = creating_job
                                .start_consume_upstream(partial_graph_manager, barrier_info)?;
                            finished_snapshot_backfill_job_info
                                .try_insert(job_id, info)
                                .expect("non-duplicated");
                        }
                    }
                }

                if !finished_snapshot_backfill_job_info.is_empty() {
                    let actors_to_create = actors_to_create.get_or_insert_default();
                    let mut subscriptions_to_drop = vec![];
                    let mut dispatcher_update = vec![];
                    let mut actor_splits = HashMap::new();
                    for (job_id, info) in finished_snapshot_backfill_job_info {
                        finished_snapshot_backfill_jobs.insert(job_id);
                        subscriptions_to_drop.extend(
                            info.snapshot_backfill_upstream_tables.iter().map(
                                |upstream_table_id| PbSubscriptionUpstreamInfo {
                                    subscriber_id: job_id.as_subscriber_id(),
                                    upstream_mv_table_id: *upstream_table_id,
                                },
                            ),
                        );
                        for upstream_mv_table_id in &info.snapshot_backfill_upstream_tables {
                            assert_matches!(
                                self.database_info.unregister_subscriber(
                                    upstream_mv_table_id.as_job_id(),
                                    job_id.as_subscriber_id()
                                ),
                                Some(SubscriberType::SnapshotBackfill)
                            );
                        }

                        table_ids_to_commit.extend(
                            info.fragment_infos
                                .values()
                                .flat_map(|fragment| fragment.state_table_ids.iter())
                                .copied(),
                        );

                        let actor_len = info
                            .fragment_infos
                            .values()
                            .map(|fragment| fragment.actors.len() as u64)
                            .sum();
                        let id_gen = GlobalActorIdGen::new(
                            partial_graph_manager
                                .control_stream_manager()
                                .env
                                .actor_id_generator(),
                            actor_len,
                        );
                        let mut next_local_actor_id = 0;
                        // mapping from old_actor_id to new_actor_id
                        let actor_mapping: HashMap<_, _> = info
                            .fragment_infos
                            .values()
                            .flat_map(|fragment| fragment.actors.keys())
                            .map(|old_actor_id| {
                                let new_actor_id = id_gen.to_global_id(next_local_actor_id);
                                next_local_actor_id += 1;
                                (*old_actor_id, new_actor_id.as_global_id())
                            })
                            .collect();
                        let actor_mapping = &actor_mapping;
                        let new_stream_actors: HashMap<_, _> = info
                            .stream_actors
                            .into_iter()
                            .map(|(old_actor_id, mut actor)| {
                                let new_actor_id = actor_mapping[&old_actor_id];
                                actor.actor_id = new_actor_id;
                                (new_actor_id, actor)
                            })
                            .collect();
                        let new_fragment_info: HashMap<_, _> = info
                            .fragment_infos
                            .into_iter()
                            .map(|(fragment_id, mut fragment)| {
                                let actors = take(&mut fragment.actors);
                                fragment.actors = actors
                                    .into_iter()
                                    .map(|(old_actor_id, actor)| {
                                        let new_actor_id = actor_mapping[&old_actor_id];
                                        (new_actor_id, actor)
                                    })
                                    .collect();
                                (fragment_id, fragment)
                            })
                            .collect();
                        actor_splits.extend(
                            new_fragment_info
                                .values()
                                .flat_map(|fragment| &fragment.actors)
                                .map(|(actor_id, actor)| {
                                    (
                                        *actor_id,
                                        ConnectorSplits {
                                            splits: actor
                                                .splits
                                                .iter()
                                                .map(ConnectorSplit::from)
                                                .collect(),
                                        },
                                    )
                                }),
                        );
                        // new actors belong to the database partial graph
                        let partial_graph_id = to_partial_graph_id(self.database_id, None);
                        let mut edge_builder = FragmentEdgeBuilder::new(
                            info.upstream_fragment_downstreams
                                .keys()
                                .map(|upstream_fragment_id| {
                                    self.database_info.fragment(*upstream_fragment_id)
                                })
                                .chain(new_fragment_info.values())
                                .map(|info| (info, partial_graph_id)),
                            partial_graph_manager.control_stream_manager(),
                        );
                        edge_builder.add_relations(&info.upstream_fragment_downstreams);
                        edge_builder.add_relations(&info.downstreams);
                        let mut edges = edge_builder.build();
                        let new_actors_to_create = edges.collect_actors_to_create(
                            new_fragment_info.values().map(|fragment| {
                                (
                                    fragment.fragment_id,
                                    &fragment.nodes,
                                    fragment.actors.iter().map(|(actor_id, actor)| {
                                        (&new_stream_actors[actor_id], actor.worker_id)
                                    }),
                                    [], // no initial subscriber for backfilling job
                                )
                            }),
                        );
                        dispatcher_update.extend(
                            info.upstream_fragment_downstreams.keys().flat_map(
                                |upstream_fragment_id| {
                                    let new_actor_dispatchers = edges
                                        .dispatchers
                                        .remove(upstream_fragment_id)
                                        .expect("should exist");
                                    new_actor_dispatchers.into_iter().flat_map(
                                        |(upstream_actor_id, dispatchers)| {
                                            dispatchers.into_iter().map(move |dispatcher| {
                                                PbDispatcherUpdate {
                                                    actor_id: upstream_actor_id,
                                                    dispatcher_id: dispatcher.dispatcher_id,
                                                    hash_mapping: dispatcher.hash_mapping,
                                                    removed_downstream_actor_id: dispatcher
                                                        .downstream_actor_id
                                                        .iter()
                                                        .map(|new_downstream_actor_id| {
                                                            actor_mapping
                                                            .iter()
                                                            .find_map(
                                                                |(old_actor_id, new_actor_id)| {
                                                                    (new_downstream_actor_id
                                                                        == new_actor_id)
                                                                        .then_some(*old_actor_id)
                                                                },
                                                            )
                                                            .expect("should exist")
                                                        })
                                                        .collect(),
                                                    added_downstream_actor_id: dispatcher
                                                        .downstream_actor_id,
                                                }
                                            })
                                        },
                                    )
                                },
                            ),
                        );
                        assert!(edges.is_empty(), "remaining edges: {:?}", edges);
                        for (worker_id, worker_actors) in new_actors_to_create {
                            node_actors.entry(worker_id).or_default().extend(
                                worker_actors.values().flat_map(|(_, actors, _)| {
                                    actors.iter().map(|(actor, _, _)| actor.actor_id)
                                }),
                            );
                            actors_to_create
                                .entry(worker_id)
                                .or_default()
                                .extend(worker_actors);
                        }
                        self.database_info.add_existing(InflightStreamingJobInfo {
                            job_id,
                            fragment_infos: new_fragment_info,
                            subscribers: Default::default(), // no initial subscribers for newly created snapshot backfill
                            status: CreateStreamingJobStatus::Created,
                            cdc_table_backfill_tracker: None, // no cdc table backfill for snapshot backfill
                        });
                    }

                    Some(PbMutation::Update(PbUpdateMutation {
                        dispatcher_update,
                        merge_update: vec![], // no upstream update on existing actors
                        actor_vnode_bitmap_update: Default::default(), /* no in place update vnode bitmap happened */
                        dropped_actors: vec![], /* no actors to drop in the partial graph of database */
                        actor_splits,
                        actor_new_dispatchers: Default::default(), // no new dispatcher
                        actor_cdc_table_snapshot_splits: None, /* no cdc table backfill in snapshot backfill */
                        sink_schema_change: Default::default(), /* no sink auto schema change happened here */
                        subscriptions_to_drop,
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
                let throttle_mutation = if let Some(Command::Throttle { jobs, config }) = &command
                    && jobs.contains(job_id)
                {
                    assert_eq!(
                        jobs.len(),
                        1,
                        "should not alter rate limit of snapshot backfill job with other jobs"
                    );
                    Some((
                        Mutation::Throttle(ThrottleMutation {
                            fragment_throttle: config
                                .iter()
                                .map(|(fragment_id, config)| (*fragment_id, *config))
                                .collect(),
                        }),
                        take(notifiers),
                    ))
                } else {
                    None
                };
                creating_job.on_new_upstream_barrier(
                    partial_graph_manager,
                    barrier_info,
                    throttle_mutation,
                )?;
            }
        }

        partial_graph_manager.inject_barrier(
            to_partial_graph_id(self.database_id, None),
            mutation,
            barrier_info,
            &node_actors,
            InflightFragmentInfo::existing_table_ids(self.database_info.fragment_infos()),
            InflightFragmentInfo::workers(self.database_info.fragment_infos()),
            actors_to_create,
        )?;

        Ok(ApplyCommandInfo {
            mv_subscription_max_retention: self.database_info.max_subscription_retention(),
            table_ids_to_commit,
            jobs_to_wait: finished_snapshot_backfill_jobs,
            command: command
                .map(Command::into_post_collect)
                .unwrap_or(PostCollectCommand::barrier()),
        })
    }
}
