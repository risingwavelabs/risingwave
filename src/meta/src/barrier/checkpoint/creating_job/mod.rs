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

mod barrier_control;
mod status;

use std::cmp::max;
use std::collections::{HashMap, HashSet};
use std::mem::take;
use std::ops::Bound::{Excluded, Unbounded};

use barrier_control::CreatingStreamingJobBarrierControl;
use risingwave_common::catalog::{DatabaseId, TableId};
use risingwave_common::id::JobId;
use risingwave_common::metrics::LabelGuardedIntGauge;
use risingwave_common::util::epoch::Epoch;
use risingwave_meta_model::WorkerId;
use risingwave_pb::ddl_service::PbBackfillType;
use risingwave_pb::hummock::HummockVersionStats;
use risingwave_pb::id::{ActorId, FragmentId};
use risingwave_pb::stream_plan::barrier::PbBarrierKind;
use risingwave_pb::stream_plan::barrier_mutation::Mutation;
use risingwave_pb::stream_plan::{AddMutation, StopMutation};
use risingwave_pb::stream_service::BarrierCompleteResponse;
use risingwave_pb::stream_service::streaming_control_stream_response::ResetPartialGraphResponse;
use status::CreatingStreamingJobStatus;
use tracing::{debug, info};

use crate::MetaResult;
use crate::barrier::backfill_order_control::get_nodes_with_backfill_dependencies;
use crate::barrier::checkpoint::creating_job::status::CreateMviewLogStoreProgressTracker;
use crate::barrier::checkpoint::recovery::ResetPartialGraphCollector;
use crate::barrier::context::CreateSnapshotBackfillJobCommandInfo;
use crate::barrier::edge_builder::FragmentEdgeBuildResult;
use crate::barrier::info::{BarrierInfo, InflightStreamingJobInfo};
use crate::barrier::notifier::Notifier;
use crate::barrier::progress::{CreateMviewProgressTracker, TrackingJob};
use crate::barrier::rpc::{
    ControlStreamManager, build_locality_fragment_state_table_mapping, to_partial_graph_id,
};
use crate::barrier::{BackfillOrderState, BackfillProgress, BarrierKind, TracedEpoch};
use crate::controller::fragment::InflightFragmentInfo;
use crate::model::{FragmentDownstreamRelation, StreamActor, StreamJobActorsToCreate};
use crate::rpc::metrics::GLOBAL_META_METRICS;
use crate::stream::{ExtendedFragmentBackfillOrder, build_actor_connector_splits};

#[derive(Debug)]
pub(crate) struct CreatingJobInfo {
    pub fragment_infos: HashMap<FragmentId, InflightFragmentInfo>,
    pub upstream_fragment_downstreams: FragmentDownstreamRelation,
    pub downstreams: FragmentDownstreamRelation,
    pub snapshot_backfill_upstream_tables: HashSet<TableId>,
    pub stream_actors: HashMap<ActorId, StreamActor>,
}

#[derive(Debug)]
pub(crate) struct CreatingStreamingJobControl {
    database_id: DatabaseId,
    pub(super) job_id: JobId,
    pub(super) snapshot_backfill_upstream_tables: HashSet<TableId>,
    snapshot_epoch: u64,

    node_actors: HashMap<WorkerId, HashSet<ActorId>>,
    state_table_ids: HashSet<TableId>,

    barrier_control: CreatingStreamingJobBarrierControl,
    status: CreatingStreamingJobStatus,

    upstream_lag: LabelGuardedIntGauge,
}

impl CreatingStreamingJobControl {
    pub(super) fn new(
        create_info: CreateSnapshotBackfillJobCommandInfo,
        notifiers: Vec<Notifier>,
        snapshot_backfill_upstream_tables: HashSet<TableId>,
        snapshot_epoch: u64,
        version_stat: &HummockVersionStats,
        control_stream_manager: &mut ControlStreamManager,
        edges: &mut FragmentEdgeBuildResult,
    ) -> MetaResult<Self> {
        let info = create_info.info.clone();
        let job_id = info.stream_job_fragments.stream_job_id();
        let database_id = info.streaming_job.database_id();
        debug!(
            %job_id,
            definition = info.definition,
            "new creating job"
        );
        let fragment_infos = info
            .stream_job_fragments
            .new_fragment_info(&info.init_split_assignment)
            .collect();
        let snapshot_backfill_actors =
            InflightStreamingJobInfo::snapshot_backfill_actor_ids(&fragment_infos).collect();
        let backfill_nodes_to_pause =
            get_nodes_with_backfill_dependencies(&info.fragment_backfill_ordering)
                .into_iter()
                .collect();
        let backfill_order_state = BackfillOrderState::new(
            &info.fragment_backfill_ordering,
            &info.stream_job_fragments,
            info.locality_fragment_state_table_mapping.clone(),
        );
        let create_mview_tracker = CreateMviewProgressTracker::recover(
            job_id,
            &fragment_infos,
            backfill_order_state,
            version_stat,
        );

        let actors_to_create =
            edges.collect_actors_to_create(info.stream_job_fragments.actors_to_create().map(
                |(fragment_id, node, actors)| {
                    (
                        fragment_id,
                        node,
                        actors,
                        [], // no subscribers for newly creating job
                    )
                },
            ));

        let mut barrier_control =
            CreatingStreamingJobBarrierControl::new(job_id, snapshot_epoch, None);

        let mut prev_epoch_fake_physical_time = 0;
        let mut pending_non_checkpoint_barriers = vec![];

        let initial_barrier_info = CreatingStreamingJobStatus::new_fake_barrier(
            &mut prev_epoch_fake_physical_time,
            &mut pending_non_checkpoint_barriers,
            PbBarrierKind::Checkpoint,
        );

        let added_actors = info.stream_job_fragments.actor_ids().collect();
        let actor_splits = info
            .init_split_assignment
            .values()
            .flat_map(build_actor_connector_splits)
            .collect();

        assert!(
            info.cdc_table_snapshot_splits.is_none(),
            "should not have cdc backfill for snapshot backfill job"
        );

        let initial_mutation = Mutation::Add(AddMutation {
            // for mutation of snapshot backfill job, we won't include changes to dispatchers of upstream actors.
            actor_dispatchers: Default::default(),
            added_actors,
            actor_splits,
            // we assume that when handling snapshot backfill, the cluster must not be paused
            pause: false,
            subscriptions_to_add: Default::default(),
            backfill_nodes_to_pause,
            actor_cdc_table_snapshot_splits: None,
            new_upstream_sinks: Default::default(),
        });

        let node_actors = InflightFragmentInfo::actor_ids_to_collect(fragment_infos.values());
        let state_table_ids =
            InflightFragmentInfo::existing_table_ids(fragment_infos.values()).collect();

        control_stream_manager.add_partial_graph(database_id, Some(job_id));
        Self::inject_barrier(
            database_id,
            job_id,
            control_stream_manager,
            &mut barrier_control,
            &node_actors,
            Some(&state_table_ids),
            initial_barrier_info,
            Some(actors_to_create),
            Some(initial_mutation),
            notifiers,
            Some(create_info),
        )?;

        assert!(pending_non_checkpoint_barriers.is_empty());

        let job_info = CreatingJobInfo {
            fragment_infos,
            upstream_fragment_downstreams: info.upstream_fragment_downstreams.clone(),
            downstreams: info.stream_job_fragments.downstreams.clone(),
            snapshot_backfill_upstream_tables: snapshot_backfill_upstream_tables.clone(),
            stream_actors: info
                .stream_job_fragments
                .fragments
                .values()
                .flat_map(|fragment| {
                    fragment
                        .actors
                        .iter()
                        .map(|actor| (actor.actor_id, actor.clone()))
                })
                .collect(),
        };

        Ok(Self {
            database_id,
            job_id,
            snapshot_backfill_upstream_tables,
            barrier_control,
            snapshot_epoch,
            status: CreatingStreamingJobStatus::ConsumingSnapshot {
                prev_epoch_fake_physical_time,
                pending_upstream_barriers: vec![],
                version_stats: version_stat.clone(),
                create_mview_tracker,
                snapshot_backfill_actors,
                snapshot_epoch,
                info: job_info,
                pending_non_checkpoint_barriers,
            },
            upstream_lag: GLOBAL_META_METRICS
                .snapshot_backfill_lag
                .with_guarded_label_values(&[&format!("{}", job_id)]),
            node_actors,
            state_table_ids,
        })
    }

    fn resolve_upstream_log_epochs(
        snapshot_backfill_upstream_tables: &HashSet<TableId>,
        upstream_table_log_epochs: &HashMap<TableId, Vec<(Vec<u64>, u64)>>,
        exclusive_start_log_epoch: u64,
        upstream_barrier_info: &BarrierInfo,
    ) -> MetaResult<Vec<BarrierInfo>> {
        let table_id = snapshot_backfill_upstream_tables
            .iter()
            .next()
            .expect("snapshot backfill job should have upstream");
        let epochs_iter = if let Some(epochs) = upstream_table_log_epochs.get(table_id) {
            let mut epochs_iter = epochs.iter();
            loop {
                let (_, checkpoint_epoch) =
                    epochs_iter.next().expect("not reach committed epoch yet");
                if *checkpoint_epoch < exclusive_start_log_epoch {
                    continue;
                }
                assert_eq!(*checkpoint_epoch, exclusive_start_log_epoch);
                break;
            }
            epochs_iter
        } else {
            // snapshot backfill job has been marked as creating, but upstream table has not committed a new epoch yet, so no table change log
            assert_eq!(
                upstream_barrier_info.prev_epoch(),
                exclusive_start_log_epoch
            );
            static EMPTY_VEC: Vec<(Vec<u64>, u64)> = Vec::new();
            EMPTY_VEC.iter()
        };

        let mut ret = vec![];
        let mut prev_epoch = exclusive_start_log_epoch;
        let mut pending_non_checkpoint_barriers = vec![];
        for (non_checkpoint_epochs, checkpoint_epoch) in epochs_iter {
            for (i, epoch) in non_checkpoint_epochs
                .iter()
                .chain([checkpoint_epoch])
                .enumerate()
            {
                assert!(*epoch > prev_epoch);
                pending_non_checkpoint_barriers.push(prev_epoch);
                ret.push(BarrierInfo {
                    prev_epoch: TracedEpoch::new(Epoch(prev_epoch)),
                    curr_epoch: TracedEpoch::new(Epoch(*epoch)),
                    kind: if i == 0 {
                        BarrierKind::Checkpoint(take(&mut pending_non_checkpoint_barriers))
                    } else {
                        BarrierKind::Barrier
                    },
                });
                prev_epoch = *epoch;
            }
        }
        ret.push(BarrierInfo {
            prev_epoch: TracedEpoch::new(Epoch(prev_epoch)),
            curr_epoch: TracedEpoch::new(Epoch(upstream_barrier_info.curr_epoch())),
            kind: BarrierKind::Checkpoint(pending_non_checkpoint_barriers),
        });
        Ok(ret)
    }

    fn recover_consuming_snapshot(
        job_id: JobId,
        upstream_table_log_epochs: &HashMap<TableId, Vec<(Vec<u64>, u64)>>,
        snapshot_epoch: u64,
        committed_epoch: u64,
        upstream_barrier_info: &BarrierInfo,
        info: CreatingJobInfo,
        backfill_order_state: BackfillOrderState,
        version_stat: &HummockVersionStats,
    ) -> MetaResult<(CreatingStreamingJobStatus, BarrierInfo)> {
        let mut prev_epoch_fake_physical_time = Epoch(committed_epoch).physical_time();
        let mut pending_non_checkpoint_barriers = vec![];
        let create_mview_tracker = CreateMviewProgressTracker::recover(
            job_id,
            &info.fragment_infos,
            backfill_order_state,
            version_stat,
        );
        let barrier_info = CreatingStreamingJobStatus::new_fake_barrier(
            &mut prev_epoch_fake_physical_time,
            &mut pending_non_checkpoint_barriers,
            PbBarrierKind::Initial,
        );

        Ok((
            CreatingStreamingJobStatus::ConsumingSnapshot {
                prev_epoch_fake_physical_time,
                pending_upstream_barriers: Self::resolve_upstream_log_epochs(
                    &info.snapshot_backfill_upstream_tables,
                    upstream_table_log_epochs,
                    snapshot_epoch,
                    upstream_barrier_info,
                )?,
                version_stats: version_stat.clone(),
                create_mview_tracker,
                snapshot_backfill_actors: InflightStreamingJobInfo::snapshot_backfill_actor_ids(
                    &info.fragment_infos,
                )
                .collect(),
                info,
                snapshot_epoch,
                pending_non_checkpoint_barriers,
            },
            barrier_info,
        ))
    }

    fn recover_consuming_log_store(
        job_id: JobId,
        upstream_table_log_epochs: &HashMap<TableId, Vec<(Vec<u64>, u64)>>,
        committed_epoch: u64,
        upstream_barrier_info: &BarrierInfo,
        info: CreatingJobInfo,
    ) -> MetaResult<(CreatingStreamingJobStatus, BarrierInfo)> {
        let mut barriers_to_inject = Self::resolve_upstream_log_epochs(
            &info.snapshot_backfill_upstream_tables,
            upstream_table_log_epochs,
            committed_epoch,
            upstream_barrier_info,
        )?;
        let mut first_barrier = barriers_to_inject.remove(0);
        assert!(first_barrier.kind.is_checkpoint());
        first_barrier.kind = BarrierKind::Initial;

        Ok((
            CreatingStreamingJobStatus::ConsumingLogStore {
                tracking_job: TrackingJob::recovered(job_id, &info.fragment_infos),
                log_store_progress_tracker: CreateMviewLogStoreProgressTracker::new(
                    InflightStreamingJobInfo::snapshot_backfill_actor_ids(&info.fragment_infos),
                    barriers_to_inject
                        .last()
                        .map(|info| info.prev_epoch() - committed_epoch)
                        .unwrap_or(0),
                ),
                barriers_to_inject: Some(barriers_to_inject),
                info,
            },
            first_barrier,
        ))
    }

    #[expect(clippy::too_many_arguments)]
    pub(crate) fn recover(
        database_id: DatabaseId,
        job_id: JobId,
        snapshot_backfill_upstream_tables: HashSet<TableId>,
        upstream_table_log_epochs: &HashMap<TableId, Vec<(Vec<u64>, u64)>>,
        snapshot_epoch: u64,
        committed_epoch: u64,
        upstream_barrier_info: &BarrierInfo,
        fragment_infos: HashMap<FragmentId, InflightFragmentInfo>,
        backfill_order: ExtendedFragmentBackfillOrder,
        fragment_relations: &FragmentDownstreamRelation,
        version_stat: &HummockVersionStats,
        new_actors: StreamJobActorsToCreate,
        initial_mutation: Mutation,
        control_stream_manager: &mut ControlStreamManager,
    ) -> MetaResult<Self> {
        info!(
            %job_id,
            "recovered creating snapshot backfill job"
        );
        let mut barrier_control =
            CreatingStreamingJobBarrierControl::new(job_id, snapshot_epoch, Some(committed_epoch));

        let node_actors = InflightFragmentInfo::actor_ids_to_collect(fragment_infos.values());
        let state_table_ids =
            InflightFragmentInfo::existing_table_ids(fragment_infos.values()).collect();

        let mut upstream_fragment_downstreams: FragmentDownstreamRelation = Default::default();
        for (upstream_fragment_id, downstreams) in fragment_relations {
            if fragment_infos.contains_key(upstream_fragment_id) {
                continue;
            }
            for downstream in downstreams {
                if fragment_infos.contains_key(&downstream.downstream_fragment_id) {
                    upstream_fragment_downstreams
                        .entry(*upstream_fragment_id)
                        .or_default()
                        .push(downstream.clone());
                }
            }
        }
        let downstreams = fragment_infos
            .keys()
            .filter_map(|fragment_id| {
                fragment_relations
                    .get(fragment_id)
                    .map(|relation| (*fragment_id, relation.clone()))
            })
            .collect();

        let info = CreatingJobInfo {
            fragment_infos,
            upstream_fragment_downstreams,
            downstreams,
            snapshot_backfill_upstream_tables: snapshot_backfill_upstream_tables.clone(),
            stream_actors: new_actors
                .values()
                .flat_map(|fragments| {
                    fragments.values().flat_map(|(_, actors, _)| {
                        actors
                            .iter()
                            .map(|(actor, _, _)| (actor.actor_id, actor.clone()))
                    })
                })
                .collect(),
        };

        let (status, first_barrier_info) = if committed_epoch < snapshot_epoch {
            let locality_fragment_state_table_mapping =
                build_locality_fragment_state_table_mapping(&info.fragment_infos);
            let backfill_order_state = BackfillOrderState::recover_from_fragment_infos(
                &backfill_order,
                &info.fragment_infos,
                locality_fragment_state_table_mapping,
            );
            Self::recover_consuming_snapshot(
                job_id,
                upstream_table_log_epochs,
                snapshot_epoch,
                committed_epoch,
                upstream_barrier_info,
                info,
                backfill_order_state,
                version_stat,
            )?
        } else {
            Self::recover_consuming_log_store(
                job_id,
                upstream_table_log_epochs,
                committed_epoch,
                upstream_barrier_info,
                info,
            )?
        };
        control_stream_manager.add_partial_graph(database_id, Some(job_id));

        Self::inject_barrier(
            database_id,
            job_id,
            control_stream_manager,
            &mut barrier_control,
            &node_actors,
            Some(&state_table_ids),
            first_barrier_info,
            Some(new_actors),
            Some(initial_mutation),
            vec![], // no notifiers in recovery
            None,
        )?;
        Ok(Self {
            database_id,
            job_id,
            snapshot_backfill_upstream_tables,
            snapshot_epoch,
            node_actors,
            state_table_ids,
            barrier_control,
            status,
            upstream_lag: GLOBAL_META_METRICS
                .snapshot_backfill_lag
                .with_guarded_label_values(&[&format!("{}", job_id)]),
        })
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.barrier_control.is_empty()
    }

    pub(crate) fn is_valid_after_worker_err(&self, worker_id: WorkerId) -> bool {
        self.barrier_control.is_valid_after_worker_err(worker_id)
            && self
                .status
                .fragment_infos()
                .map(|fragment_infos| {
                    !InflightFragmentInfo::contains_worker(fragment_infos.values(), worker_id)
                })
                .unwrap_or(true)
    }

    pub(crate) fn gen_backfill_progress(&self) -> BackfillProgress {
        let progress = match &self.status {
            CreatingStreamingJobStatus::ConsumingSnapshot {
                create_mview_tracker,
                ..
            } => {
                if create_mview_tracker.is_finished() {
                    "Snapshot finished".to_owned()
                } else {
                    let progress = create_mview_tracker.gen_backfill_progress();
                    format!("Snapshot [{}]", progress)
                }
            }
            CreatingStreamingJobStatus::ConsumingLogStore {
                log_store_progress_tracker,
                ..
            } => {
                format!(
                    "LogStore [{}]",
                    log_store_progress_tracker.gen_backfill_progress()
                )
            }
            CreatingStreamingJobStatus::Finishing(..) => {
                format!(
                    "Finishing [epoch count: {}]",
                    self.barrier_control.inflight_barrier_count()
                )
            }
            CreatingStreamingJobStatus::Resetting(_, _) => "Resetting".to_owned(),
            CreatingStreamingJobStatus::PlaceHolder => {
                unreachable!()
            }
        };
        BackfillProgress {
            progress,
            backfill_type: PbBackfillType::SnapshotBackfill,
        }
    }

    pub(super) fn pinned_upstream_log_epoch(&self) -> u64 {
        max(
            self.barrier_control.max_committed_epoch().unwrap_or(0),
            self.snapshot_epoch,
        )
    }

    #[expect(clippy::too_many_arguments)]
    fn inject_barrier(
        database_id: DatabaseId,
        job_id: JobId,
        control_stream_manager: &mut ControlStreamManager,
        barrier_control: &mut CreatingStreamingJobBarrierControl,
        node_actors: &HashMap<WorkerId, HashSet<ActorId>>,
        state_table_ids: Option<&HashSet<TableId>>,
        barrier_info: BarrierInfo,
        new_actors: Option<StreamJobActorsToCreate>,
        mutation: Option<Mutation>,
        mut notifiers: Vec<Notifier>,
        first_create_info: Option<CreateSnapshotBackfillJobCommandInfo>,
    ) -> MetaResult<()> {
        let (state_table_ids, nodes_to_sync_table) = if let Some(state_table_ids) = state_table_ids
        {
            (Some(state_table_ids), Some(node_actors.keys().copied()))
        } else {
            (None, None)
        };
        let node_to_collect = control_stream_manager.inject_barrier(
            database_id,
            Some(job_id),
            mutation,
            &barrier_info,
            node_actors,
            state_table_ids.into_iter().flatten().copied(),
            nodes_to_sync_table.into_iter().flatten(),
            new_actors,
        )?;
        notifiers.iter_mut().for_each(|n| n.notify_started());
        barrier_control.enqueue_epoch(
            barrier_info.prev_epoch(),
            node_to_collect,
            barrier_info.kind.clone(),
            notifiers,
            first_create_info,
        );
        Ok(())
    }

    pub(super) fn start_consume_upstream(
        &mut self,
        control_stream_manager: &mut ControlStreamManager,
        barrier_info: &BarrierInfo,
    ) -> MetaResult<CreatingJobInfo> {
        info!(
            job_id = %self.job_id,
            prev_epoch = barrier_info.prev_epoch(),
            "start consuming upstream"
        );
        let info = self.status.start_consume_upstream(barrier_info);
        Self::inject_barrier(
            self.database_id,
            self.job_id,
            control_stream_manager,
            &mut self.barrier_control,
            &self.node_actors,
            None,
            barrier_info.clone(),
            None,
            Some(Mutation::Stop(StopMutation {
                // stop all actors
                actors: info
                    .fragment_infos
                    .values()
                    .flat_map(|info| info.actors.keys().copied())
                    .collect(),
                dropped_sink_fragments: vec![], // not related to sink-into-table
            })),
            vec![], // no notifiers when start consuming upstream
            None,
        )?;
        Ok(info)
    }

    pub(super) fn on_new_upstream_barrier(
        &mut self,
        control_stream_manager: &mut ControlStreamManager,
        barrier_info: &BarrierInfo,
        mutation: Option<(Mutation, Vec<Notifier>)>,
    ) -> MetaResult<()> {
        let progress_epoch =
            if let Some(max_committed_epoch) = self.barrier_control.max_committed_epoch() {
                max(max_committed_epoch, self.snapshot_epoch)
            } else {
                self.snapshot_epoch
            };
        self.upstream_lag.set(
            barrier_info
                .prev_epoch
                .value()
                .0
                .saturating_sub(progress_epoch) as _,
        );
        let (mut mutation, mut notifiers) = match mutation {
            Some((mutation, notifiers)) => (Some(mutation), notifiers),
            None => (None, vec![]),
        };
        {
            for (barrier_to_inject, mutation) in self
                .status
                .on_new_upstream_epoch(barrier_info, mutation.take())
            {
                Self::inject_barrier(
                    self.database_id,
                    self.job_id,
                    control_stream_manager,
                    &mut self.barrier_control,
                    &self.node_actors,
                    Some(&self.state_table_ids),
                    barrier_to_inject,
                    None,
                    mutation,
                    take(&mut notifiers),
                    None,
                )?;
            }
            assert!(mutation.is_none(), "must have consumed mutation");
            assert!(notifiers.is_empty(), "must consumed notifiers");
        }
        Ok(())
    }

    pub(crate) fn collect(&mut self, resp: BarrierCompleteResponse) -> bool {
        self.status.update_progress(&resp.create_mview_progress);
        self.barrier_control.collect(resp);
        self.should_merge_to_upstream()
    }

    pub(super) fn should_merge_to_upstream(&self) -> bool {
        if let CreatingStreamingJobStatus::ConsumingLogStore {
            log_store_progress_tracker,
            barriers_to_inject,
            ..
        } = &self.status
            && barriers_to_inject.is_none()
            && log_store_progress_tracker.is_finished()
        {
            true
        } else {
            false
        }
    }
}

pub(super) enum CompleteJobType {
    /// The first barrier
    First(CreateSnapshotBackfillJobCommandInfo),
    Normal,
    /// The last barrier to complete
    Finished,
}

impl CreatingStreamingJobControl {
    pub(super) fn start_completing(
        &mut self,
        min_upstream_inflight_epoch: Option<u64>,
        upstream_committed_epoch: u64,
    ) -> Option<(u64, Vec<BarrierCompleteResponse>, CompleteJobType)> {
        // do not commit snapshot backfill job until upstream has committed the snapshot epoch
        if upstream_committed_epoch < self.snapshot_epoch {
            return None;
        }
        let (finished_at_epoch, epoch_end_bound) = match &self.status {
            CreatingStreamingJobStatus::Finishing(finish_at_epoch, _) => {
                let epoch_end_bound = min_upstream_inflight_epoch
                    .map(|upstream_epoch| {
                        if upstream_epoch < *finish_at_epoch {
                            Excluded(upstream_epoch)
                        } else {
                            Unbounded
                        }
                    })
                    .unwrap_or(Unbounded);
                (Some(*finish_at_epoch), epoch_end_bound)
            }
            CreatingStreamingJobStatus::ConsumingSnapshot { .. }
            | CreatingStreamingJobStatus::ConsumingLogStore { .. } => (
                None,
                min_upstream_inflight_epoch
                    .map(Excluded)
                    .unwrap_or(Unbounded),
            ),
            CreatingStreamingJobStatus::Resetting(_, _) => {
                return None;
            }
            CreatingStreamingJobStatus::PlaceHolder => {
                unreachable!()
            }
        };
        self.barrier_control.start_completing(epoch_end_bound).map(
            |(epoch, resps, create_job_info)| {
                let status = if let Some(finish_at_epoch) = finished_at_epoch {
                    assert!(create_job_info.is_none());
                    if epoch == finish_at_epoch {
                        self.barrier_control.ack_completed(epoch);
                        assert!(self.barrier_control.is_empty());
                        CompleteJobType::Finished
                    } else {
                        CompleteJobType::Normal
                    }
                } else if let Some(info) = create_job_info {
                    CompleteJobType::First(info)
                } else {
                    CompleteJobType::Normal
                };
                (epoch, resps, status)
            },
        )
    }

    pub(super) fn ack_completed(&mut self, completed_epoch: u64) {
        self.barrier_control.ack_completed(completed_epoch);
    }

    pub fn is_consuming(&self) -> bool {
        match &self.status {
            CreatingStreamingJobStatus::ConsumingSnapshot { .. }
            | CreatingStreamingJobStatus::ConsumingLogStore { .. } => true,
            CreatingStreamingJobStatus::Finishing(..)
            | CreatingStreamingJobStatus::Resetting(_, _) => false,
            CreatingStreamingJobStatus::PlaceHolder => {
                unreachable!()
            }
        }
    }

    pub fn state_table_ids(&self) -> &HashSet<TableId> {
        &self.state_table_ids
    }

    pub fn fragment_infos_with_job_id(
        &self,
    ) -> impl Iterator<Item = (&InflightFragmentInfo, JobId)> + '_ {
        self.status
            .fragment_infos()
            .into_iter()
            .flat_map(|fragments| fragments.values().map(|fragment| (fragment, self.job_id)))
    }

    pub fn into_tracking_job(self) -> TrackingJob {
        assert!(self.barrier_control.is_empty());
        match self.status {
            CreatingStreamingJobStatus::ConsumingSnapshot { .. }
            | CreatingStreamingJobStatus::ConsumingLogStore { .. }
            | CreatingStreamingJobStatus::Resetting(_, _)
            | CreatingStreamingJobStatus::PlaceHolder => {
                unreachable!("expect finish")
            }
            CreatingStreamingJobStatus::Finishing(_, tracking_job) => tracking_job,
        }
    }

    pub(super) fn on_reset_partial_graph_resp(
        &mut self,
        worker_id: WorkerId,
        resp: ResetPartialGraphResponse,
    ) -> bool {
        match &mut self.status {
            CreatingStreamingJobStatus::Resetting(collector, notifiers) => {
                collector.collect(worker_id, resp);
                if collector.remaining_workers.is_empty() {
                    for notifier in notifiers.drain(..) {
                        notifier.notify_collected();
                    }
                    true
                } else {
                    false
                }
            }
            CreatingStreamingJobStatus::ConsumingSnapshot { .. }
            | CreatingStreamingJobStatus::ConsumingLogStore { .. }
            | CreatingStreamingJobStatus::Finishing(_, _) => {
                panic!(
                    "should be resetting when receiving reset partial graph resp, but at {:?}",
                    self.status
                )
            }
            CreatingStreamingJobStatus::PlaceHolder => {
                unreachable!()
            }
        }
    }

    /// Drop a creating snapshot backfill job by directly resetting the partial graph
    /// Return `false` if the partial graph has been merged to upstream database, and `true` otherwise
    /// to mean that the job has been dropped.
    pub(super) fn drop(
        &mut self,
        notifiers: &mut Vec<Notifier>,
        control_stream_manager: &mut ControlStreamManager,
    ) -> bool {
        match &mut self.status {
            CreatingStreamingJobStatus::Resetting(_, existing_notifiers) => {
                for notifier in &mut *notifiers {
                    notifier.notify_started();
                }
                existing_notifiers.append(notifiers);
                true
            }
            CreatingStreamingJobStatus::ConsumingSnapshot { .. }
            | CreatingStreamingJobStatus::ConsumingLogStore { .. } => {
                for notifier in &mut *notifiers {
                    notifier.notify_started();
                }
                let remaining_workers =
                    control_stream_manager.reset_partial_graphs(vec![to_partial_graph_id(
                        self.database_id,
                        Some(self.job_id),
                    )]);
                let collector = ResetPartialGraphCollector {
                    remaining_workers,
                    reset_resps: Default::default(),
                };
                self.status = CreatingStreamingJobStatus::Resetting(collector, take(notifiers));
                true
            }
            CreatingStreamingJobStatus::Finishing(_, _) => false,
            CreatingStreamingJobStatus::PlaceHolder => {
                unreachable!()
            }
        }
    }

    pub(super) fn reset(self) -> Option<ResetPartialGraphCollector> {
        match self.status {
            CreatingStreamingJobStatus::Resetting(collector, notifiers) => {
                for notifier in notifiers {
                    notifier.notify_collected();
                }
                Some(collector)
            }
            CreatingStreamingJobStatus::ConsumingSnapshot { .. }
            | CreatingStreamingJobStatus::ConsumingLogStore { .. }
            | CreatingStreamingJobStatus::Finishing(_, _) => None,
            CreatingStreamingJobStatus::PlaceHolder => {
                unreachable!()
            }
        }
    }
}
