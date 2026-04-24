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

//! Batch refresh job checkpoint control for snapshot-only materialized views.
//!
//! It lives permanently in `DatabaseCheckpointControl.independent_checkpoint_job_controls`
//! as an `IndependentCheckpointJobControl::BatchRefresh` variant for its entire lifetime.
//!
//! Lifecycle for first run (snapshot only):
//!   DDL → `ConsumingSnapshot` → stop actors committed → `Idle`

use std::collections::{HashMap, HashSet};
use std::mem::{replace, take};
use std::sync::atomic::AtomicU32;

use anyhow::anyhow;
use itertools::Itertools;
use risingwave_common::catalog::{DatabaseId, TableId};
use risingwave_common::id::JobId;
use risingwave_common::metrics::{LabelGuardedHistogram, LabelGuardedIntGauge};
use risingwave_common::system_param::AdaptiveParallelismStrategy;
use risingwave_common::util::epoch::{Epoch, EpochPair};
use risingwave_meta_model::{DispatcherType, WorkerId, streaming_job};
use risingwave_pb::common::WorkerNode;
use risingwave_pb::ddl_service::PbBackfillType;
use risingwave_pb::hummock::HummockVersionStats;
use risingwave_pb::id::{ActorId, FragmentId, PartialGraphId};
use risingwave_pb::stream_plan::barrier::PbBarrierKind;
use risingwave_pb::stream_plan::barrier_mutation::Mutation;
use risingwave_pb::stream_plan::{AddMutation, StartFragmentBackfillMutation, StopMutation};
use risingwave_pb::stream_service::BarrierCompleteResponse;
use tracing::{debug, info};

use crate::MetaResult;
use crate::barrier::backfill_order_control::get_nodes_with_backfill_dependencies;
use crate::barrier::command::PostCollectCommand;
use crate::barrier::context::CreateSnapshotBackfillJobCommandInfo;
use crate::barrier::edge_builder::{EdgeBuilderFragmentInfo, FragmentEdgeBuilder};
use crate::barrier::info::BarrierInfo;
use crate::barrier::notifier::Notifier;
use crate::barrier::partial_graph::{
    CollectedBarrier, PartialGraphBarrierInfo, PartialGraphManager, PartialGraphStat,
};
use crate::barrier::progress::{CreateMviewProgressTracker, TrackingJob, collect_done_fragments};
use crate::barrier::rpc::to_partial_graph_id;
use crate::barrier::{
    BackfillOrderState, BackfillProgress, BarrierKind, FragmentBackfillProgress, TracedEpoch,
};
use crate::controller::fragment::InflightFragmentInfo;
use crate::controller::scale::{
    ComponentFragmentAligner, EnsembleActorTemplate, LoadedFragment, NoShuffleEnsemble,
    build_no_shuffle_fragment_graph_edges, find_no_shuffle_graphs,
};
use crate::model::{
    FragmentDownstreamRelation, StreamActor, StreamJobActorsToCreate, StreamingJobModelContextExt,
};
use crate::rpc::metrics::GLOBAL_META_METRICS;
use crate::stream::ExtendedFragmentBackfillOrder;

// ── Public types ──────────────────────────────────────────────────────────────

/// Logical fragment metadata for a batch refresh job.
///
/// Contains only catalog-level information: fragment structure, stream plan nodes,
/// distribution, and downstream relations. No actor IDs, no worker placement.
///
/// Used as the uniform input for `render_actors_and_build_job_info()`, which performs
/// actor rendering (ID allocation, worker placement, vnode assignment) internally.
/// No-shuffle ensembles are derived from `downstreams` internally.
#[derive(Debug)]
pub(crate) struct BatchRefreshLogicalFragments {
    /// Logical fragments of this job. Keyed by `fragment_id`.
    pub fragments: HashMap<FragmentId, LoadedFragment>,
    /// Internal downstream relations (intra-job only; no upstream edges).
    pub downstreams: FragmentDownstreamRelation,
}

/// Result of the unified actor rendering for a batch refresh job.
///
/// Produced by `render_actors_and_build_job_info()` and consumed by both
/// `new()` (create) and `recover()`.
#[derive(Debug)]
pub(crate) struct BatchRefreshRenderResult {
    pub fragment_infos: HashMap<FragmentId, InflightFragmentInfo>,
    pub node_actors: HashMap<WorkerId, HashSet<ActorId>>,
    pub state_table_ids: HashSet<TableId>,
    pub actors_to_create: StreamJobActorsToCreate,
}

// ── Status ────────────────────────────────────────────────────────────────────

#[derive(Debug)]
enum BatchRefreshJobStatus {
    /// The job is consuming upstream snapshot.
    ///
    /// Once snapshot consumption finishes, the final checkpoint + stop barriers are injected
    /// and the status transitions to `FinishingSnapshot`.
    ConsumingSnapshot {
        prev_epoch_fake_physical_time: u64,
        version_stats: HummockVersionStats,
        create_mview_tracker: CreateMviewProgressTracker,
        snapshot_epoch: u64,
        fragment_infos: HashMap<FragmentId, InflightFragmentInfo>,
        pending_non_checkpoint_barriers: Vec<u64>,
        node_actors: HashMap<WorkerId, HashSet<ActorId>>,
        state_table_ids: HashSet<TableId>,
    },
    /// The job has finished consuming the snapshot.
    ///
    /// The final checkpoint barrier (at `snapshot_epoch`) and the stop barrier have been
    /// injected. Once the stop epoch is committed the job transitions to `Idle`.
    /// The committed epoch is expected to be the snapshot epoch when the snapshot
    /// consumption finishes.
    FinishingSnapshot {
        tracking_job: Option<TrackingJob>,
        fragment_infos: HashMap<FragmentId, InflightFragmentInfo>,
    },
    /// The job is idle, waiting for the next trigger. No partial graph is held.
    Idle,
    /// The partial graph is being reset (only for drop).
    Resetting { notifiers: Vec<Notifier> },
}

// ── Complete type ─────────────────────────────────────────────────────────────

// ── Main checkpoint control ───────────────────────────────────────────────────

/// Self-contained checkpoint control for a batch refresh MV.
///
/// Unlike `CreatingStreamingJobControl`, this struct handles the full lifecycle
/// (snapshot → idle → re-run → idle → ...). Both types are stored together in
/// `DatabaseCheckpointControl.independent_checkpoint_job_controls` as
/// `IndependentCheckpointJobControl` variants.
#[derive(Debug)]
pub(crate) struct BatchRefreshJobCheckpointControl {
    job_id: JobId,
    partial_graph_id: PartialGraphId,
    snapshot_backfill_upstream_tables: HashSet<TableId>,
    snapshot_epoch: u64,

    max_committed_epoch: Option<u64>,
    status: BatchRefreshJobStatus,
}

// ── Unified actor rendering ───────────────────────────────────────────────────

impl BatchRefreshJobCheckpointControl {
    /// Render actors for a batch refresh job from logical metadata only.
    ///
    /// Performs the full pipeline:
    /// 1. Derive no-shuffle ensembles from `downstreams`
    /// 2. Render actor assignments (ID allocation, worker placement, vnode bitmap)
    /// 3. Build `StreamActor` structs
    /// 4. Build internal-only edges (no upstream dispatcher edges)
    /// 5. Produce `fragment_infos`, `node_actors`, `state_table_ids`, `actors_to_create`
    ///
    /// Shared by both the DDL create path and the recovery path.
    pub(crate) fn render_actors_and_build_job_info(
        fragments: &HashMap<FragmentId, LoadedFragment>,
        downstreams: &FragmentDownstreamRelation,
        definition: &str,
        // Actor rendering context:
        actor_id_generator: &AtomicU32,
        worker_nodes: &HashMap<WorkerId, WorkerNode>,
        adaptive_parallelism_strategy: AdaptiveParallelismStrategy,
        database_resource_group: &str,
        streaming_job_model: &streaming_job::Model,
        // Edge building context:
        partial_graph_id: PartialGraphId,
    ) -> MetaResult<BatchRefreshRenderResult> {
        // Step 1: Derive no-shuffle ensembles from downstreams.
        let ensembles = Self::resolve_ensembles(fragments, downstreams)?;

        // Step 2: Render actor assignments for each ensemble.
        let mut actor_assignments: HashMap<
            FragmentId,
            HashMap<ActorId, (WorkerId, Option<risingwave_common::bitmap::Bitmap>)>,
        > = HashMap::new();

        for ensemble in &ensembles {
            // All fragments are new (batch refresh has no existing upstream fragments).
            let first_component = ensemble
                .component_fragments()
                .next()
                .expect("ensemble must have at least one component");
            let fragment = &fragments[&first_component];
            let distribution_type = fragment.distribution_type;
            let vnode_count = fragment.vnode_count;

            // Assert all component fragments share the same vnode count.
            for fid in ensemble.component_fragments() {
                let f = &fragments[&fid];
                assert_eq!(
                    vnode_count, f.vnode_count,
                    "fragments {} and {} in same ensemble have different vnode counts",
                    first_component, fid,
                );
            }

            let entry_fragment_parallelism = ensemble
                .entry_fragments()
                .map(|fid| fragments[&fid].parallelism.clone())
                .dedup()
                .exactly_one()
                .map_err(|_| {
                    anyhow!(
                        "entry fragments have inconsistent parallelism settings in batch refresh job"
                    )
                })?;

            let actor_template = EnsembleActorTemplate::render_new(
                streaming_job_model,
                worker_nodes,
                adaptive_parallelism_strategy,
                entry_fragment_parallelism,
                database_resource_group.to_owned(),
                distribution_type,
                vnode_count,
            )?;

            for fid in ensemble.component_fragments() {
                let f = &fragments[&fid];
                let aligner =
                    ComponentFragmentAligner::new_persistent(&actor_template, actor_id_generator);
                let assignments = aligner.align_component_actor(f.distribution_type);
                actor_assignments.insert(fid, assignments);
            }
        }

        // Step 3: Expand assignments into StreamActor + actor_location + InflightFragmentInfo.
        let mut stream_actors: HashMap<FragmentId, Vec<StreamActor>> = HashMap::new();
        let mut actor_location: HashMap<ActorId, WorkerId> = HashMap::new();

        for (fragment_id, assignments) in &actor_assignments {
            let mut actors = Vec::with_capacity(assignments.len());
            for (&actor_id, (worker_id, vnode_bitmap)) in assignments {
                actor_location.insert(actor_id, *worker_id);
                let stream_context = streaming_job_model.stream_context();
                actors.push(StreamActor {
                    actor_id,
                    fragment_id: *fragment_id,
                    vnode_bitmap: vnode_bitmap.clone(),
                    mview_definition: definition.to_owned(),
                    expr_context: Some(stream_context.to_expr_context()),
                    config_override: stream_context.config_override.clone(),
                });
            }
            stream_actors.insert(*fragment_id, actors);
        }

        // Build InflightFragmentInfo from logical fragments + rendered actors.
        let fragment_infos: HashMap<FragmentId, InflightFragmentInfo> = fragments
            .iter()
            .map(|(fragment_id, loaded)| {
                let actors = stream_actors
                    .get(fragment_id)
                    .into_iter()
                    .flatten()
                    .map(|actor| {
                        (
                            actor.actor_id,
                            crate::controller::fragment::InflightActorInfo {
                                worker_id: actor_location[&actor.actor_id],
                                vnode_bitmap: actor.vnode_bitmap.clone(),
                                splits: vec![], // batch refresh has no source splits
                            },
                        )
                    })
                    .collect();
                (
                    *fragment_id,
                    InflightFragmentInfo {
                        fragment_id: *fragment_id,
                        distribution_type: loaded.distribution_type,
                        fragment_type_mask: loaded.fragment_type_mask,
                        vnode_count: loaded.vnode_count,
                        nodes: loaded.nodes.clone(),
                        actors,
                        state_table_ids: loaded.state_table_ids.clone(),
                    },
                )
            })
            .collect();

        // Step 4: Build edges (internal-only, no upstream).
        let mut builder = FragmentEdgeBuilder::new(fragment_infos.values().map(|f| {
            (
                f.fragment_id,
                EdgeBuilderFragmentInfo::from_inflight_with_worker_nodes(
                    f,
                    partial_graph_id,
                    worker_nodes,
                ),
            )
        }));
        builder.add_relations(downstreams);
        let mut edges = builder.build();

        let actors_to_create = edges.collect_actors_to_create(fragment_infos.values().map(|f| {
            (
                f.fragment_id,
                &f.nodes,
                f.actors.iter().map(|(actor_id, actor)| {
                    let sa = stream_actors[&f.fragment_id]
                        .iter()
                        .find(|a| a.actor_id == *actor_id)
                        .expect("should exist");
                    (sa, actor.worker_id)
                }),
                vec![], // no subscribers for batch refresh jobs
            )
        }));

        // Step 5: Build node_actors, state_table_ids.
        let node_actors = InflightFragmentInfo::actor_ids_to_collect(fragment_infos.values());
        let state_table_ids =
            InflightFragmentInfo::existing_table_ids(fragment_infos.values()).collect();

        Ok(BatchRefreshRenderResult {
            fragment_infos,
            node_actors,
            state_table_ids,
            actors_to_create,
        })
    }

    /// Build the initial `Add` mutation for the partial graph's first barrier.
    ///
    /// The rendered actors come from a prior `render_actors_and_build_job_info()` call;
    /// `backfill_nodes_to_pause` is derived from the job's backfill ordering.
    pub(crate) fn build_initial_partial_graph_mutation(
        render_result: &BatchRefreshRenderResult,
        backfill_ordering: &ExtendedFragmentBackfillOrder,
    ) -> Mutation {
        let added_actors: Vec<ActorId> = render_result
            .fragment_infos
            .values()
            .flat_map(|f| f.actors.keys().copied())
            .collect();
        let backfill_nodes_to_pause = get_nodes_with_backfill_dependencies(backfill_ordering)
            .into_iter()
            .collect();
        Mutation::Add(AddMutation {
            actor_dispatchers: Default::default(),
            added_actors,
            actor_splits: Default::default(),
            pause: false,
            subscriptions_to_add: Default::default(),
            backfill_nodes_to_pause,
            actor_cdc_table_snapshot_splits: None,
            new_upstream_sinks: Default::default(),
        })
    }

    /// Derive no-shuffle ensembles from fragment downstreams.
    fn resolve_ensembles(
        fragments: &HashMap<FragmentId, LoadedFragment>,
        downstreams: &FragmentDownstreamRelation,
    ) -> MetaResult<Vec<NoShuffleEnsemble>> {
        let mut new_no_shuffle: HashMap<_, HashSet<_>> = HashMap::new();
        for (upstream_fid, relations) in downstreams {
            for rel in relations {
                if rel.dispatcher_type == DispatcherType::NoShuffle {
                    new_no_shuffle
                        .entry(*upstream_fid)
                        .or_default()
                        .insert(rel.downstream_fragment_id);
                }
            }
        }

        let mut ensembles = if new_no_shuffle.is_empty() {
            Vec::new()
        } else {
            let no_shuffle_edges: Vec<(FragmentId, FragmentId)> = new_no_shuffle
                .iter()
                .flat_map(|(u, ds)| ds.iter().map(move |d| (*u, *d)))
                .collect();
            let all_fragment_ids: Vec<FragmentId> = no_shuffle_edges
                .iter()
                .flat_map(|(u, d)| [*u, *d])
                .collect::<HashSet<_>>()
                .into_iter()
                .collect();
            let (fwd, bwd) = build_no_shuffle_fragment_graph_edges(no_shuffle_edges);
            find_no_shuffle_graphs(&all_fragment_ids, &fwd, &bwd)?
        };

        // Add standalone fragments as single-fragment ensembles.
        let covered: HashSet<FragmentId> = ensembles
            .iter()
            .flat_map(|e| e.component_fragments())
            .collect();
        for fragment_id in fragments.keys() {
            if !covered.contains(fragment_id) {
                ensembles.push(NoShuffleEnsemble::singleton(*fragment_id));
            }
        }

        Ok(ensembles)
    }
}

// ── Construction ──────────────────────────────────────────────────────────────

impl BatchRefreshJobCheckpointControl {
    /// Create from DDL command. Starts in `ConsumingSnapshot`.
    ///
    /// Internally calls `render_actors_and_build_job_info()` and injects the
    /// partial-graph initial barrier.
    #[expect(clippy::too_many_arguments)]
    pub(crate) fn new(
        database_id: DatabaseId,
        job_id: JobId,
        create_info: CreateSnapshotBackfillJobCommandInfo,
        notifiers: Vec<Notifier>,
        snapshot_backfill_upstream_tables: HashSet<TableId>,
        snapshot_epoch: u64,
        version_stat: &HummockVersionStats,
        partial_graph_manager: &mut PartialGraphManager,
        logical: &BatchRefreshLogicalFragments,
        worker_nodes: &HashMap<WorkerId, WorkerNode>,
        adaptive_parallelism_strategy: AdaptiveParallelismStrategy,
    ) -> MetaResult<Self> {
        debug!(
            %job_id,
            "new batch refresh job"
        );

        let partial_graph_id = to_partial_graph_id(database_id, Some(job_id));
        let backfill_ordering = &create_info.info.fragment_backfill_ordering;
        let actor_id_generator = partial_graph_manager
            .control_stream_manager()
            .env
            .actor_id_generator();

        let render_result = Self::render_actors_and_build_job_info(
            &logical.fragments,
            &logical.downstreams,
            &create_info.info.definition,
            actor_id_generator,
            worker_nodes,
            adaptive_parallelism_strategy,
            &create_info.info.database_resource_group,
            &create_info.info.streaming_job_model,
            partial_graph_id,
        )?;
        let initial_partial_graph_mutation =
            Self::build_initial_partial_graph_mutation(&render_result, backfill_ordering);

        let backfill_order_state = BackfillOrderState::new(
            backfill_ordering,
            &render_result.fragment_infos,
            create_info
                .info
                .locality_fragment_state_table_mapping
                .clone(),
        );
        let create_mview_tracker = CreateMviewProgressTracker::recover(
            job_id,
            &render_result.fragment_infos,
            backfill_order_state,
            version_stat,
        );

        let mut prev_epoch_fake_physical_time = 0;
        let mut pending_non_checkpoint_barriers = vec![];

        let initial_barrier_info = super::new_fake_barrier(
            &mut prev_epoch_fake_physical_time,
            &mut pending_non_checkpoint_barriers,
            PbBarrierKind::Checkpoint,
        );

        let mut graph_adder = partial_graph_manager.add_partial_graph(
            partial_graph_id,
            BatchRefreshBarrierStats::new(job_id, snapshot_epoch),
        );

        if let Err(e) = Self::inject_barrier(
            partial_graph_id,
            graph_adder.manager(),
            &render_result.node_actors,
            &render_result.state_table_ids,
            initial_barrier_info,
            Some(render_result.actors_to_create),
            Some(initial_partial_graph_mutation),
            notifiers,
            Some(create_info),
            false,
        ) {
            graph_adder.failed();
            return Err(e);
        }

        graph_adder.added();
        assert!(pending_non_checkpoint_barriers.is_empty());
        let this = Self {
            partial_graph_id,
            job_id,
            snapshot_backfill_upstream_tables,
            snapshot_epoch,
            max_committed_epoch: None,
            status: BatchRefreshJobStatus::ConsumingSnapshot {
                prev_epoch_fake_physical_time,
                version_stats: version_stat.clone(),
                create_mview_tracker,
                snapshot_epoch,
                fragment_infos: render_result.fragment_infos,
                pending_non_checkpoint_barriers,
                node_actors: render_result.node_actors,
                state_table_ids: render_result.state_table_ids,
            },
        };
        Ok(this)
    }

    /// Recover from a persistent state during recovery.
    ///
    /// - If `committed_epoch >= snapshot_epoch` → Idle (snapshot completed before crash).
    /// - If `committed_epoch < snapshot_epoch` → `ConsumingSnapshot` using pre-rendered actors.
    pub(crate) fn recover(
        database_id: DatabaseId,
        job_id: JobId,
        snapshot_backfill_upstream_tables: HashSet<TableId>,
        snapshot_epoch: u64,
        committed_epoch: u64,
        backfill_order: ExtendedFragmentBackfillOrder,
        version_stat: &HummockVersionStats,
        initial_mutation: Mutation,
        render_result: BatchRefreshRenderResult,
        partial_graph_recoverer: &mut crate::barrier::partial_graph::PartialGraphRecoverer<'_>,
    ) -> MetaResult<Self> {
        let partial_graph_id = to_partial_graph_id(database_id, Some(job_id));

        if committed_epoch >= snapshot_epoch {
            // Snapshot completed; recover to Idle.
            info!(
                %job_id,
                committed_epoch,
                snapshot_epoch,
                "recovered idle batch refresh job (no partial graph)"
            );
            return Ok(Self {
                job_id,
                partial_graph_id,
                snapshot_backfill_upstream_tables,
                snapshot_epoch,
                max_committed_epoch: Some(committed_epoch),
                status: BatchRefreshJobStatus::Idle,
            });
        }

        // Snapshot still in-progress; recover to ConsumingSnapshot.
        info!(
            %job_id,
            committed_epoch,
            snapshot_epoch,
            "recovered batch refresh job to consuming snapshot"
        );

        let mut prev_epoch_fake_physical_time = Epoch(committed_epoch).physical_time();
        let mut pending_non_checkpoint_barriers = vec![];

        let locality_fragment_state_table_mapping =
            crate::barrier::rpc::build_locality_fragment_state_table_mapping(
                &render_result.fragment_infos,
            );
        let backfill_order_state = BackfillOrderState::recover_from_fragment_infos(
            &backfill_order,
            &render_result.fragment_infos,
            locality_fragment_state_table_mapping,
        );

        let create_mview_tracker = CreateMviewProgressTracker::recover(
            job_id,
            &render_result.fragment_infos,
            backfill_order_state,
            version_stat,
        );

        let first_barrier_info = super::new_fake_barrier(
            &mut prev_epoch_fake_physical_time,
            &mut pending_non_checkpoint_barriers,
            PbBarrierKind::Initial,
        );

        partial_graph_recoverer.recover_graph(
            partial_graph_id,
            initial_mutation,
            &first_barrier_info,
            &render_result.node_actors,
            render_result.state_table_ids.iter().copied(),
            render_result.actors_to_create,
            BatchRefreshBarrierStats::new(job_id, snapshot_epoch),
        )?;

        Ok(Self {
            job_id,
            partial_graph_id,
            snapshot_backfill_upstream_tables,
            snapshot_epoch,
            max_committed_epoch: Some(committed_epoch),
            status: BatchRefreshJobStatus::ConsumingSnapshot {
                prev_epoch_fake_physical_time,
                version_stats: version_stat.clone(),
                create_mview_tracker,
                fragment_infos: render_result.fragment_infos,
                snapshot_epoch,
                pending_non_checkpoint_barriers,
                node_actors: render_result.node_actors,
                state_table_ids: render_result.state_table_ids,
            },
        })
    }
}

// ── Barrier injection ─────────────────────────────────────────────────────────

impl BatchRefreshJobCheckpointControl {
    fn inject_barrier(
        partial_graph_id: PartialGraphId,
        partial_graph_manager: &mut PartialGraphManager,
        node_actors: &HashMap<WorkerId, HashSet<ActorId>>,
        state_table_ids: &HashSet<TableId>,
        barrier_info: BarrierInfo,
        new_actors: Option<StreamJobActorsToCreate>,
        mutation: Option<Mutation>,
        notifiers: Vec<Notifier>,
        first_create_info: Option<CreateSnapshotBackfillJobCommandInfo>,
        is_stop: bool,
    ) -> MetaResult<()> {
        if is_stop {
            assert!(
                matches!(&mutation, Some(Mutation::Stop(_))),
                "stop barrier must carry a Stop mutation"
            );
        }
        partial_graph_manager.inject_barrier(
            partial_graph_id,
            mutation,
            node_actors,
            state_table_ids.iter().copied(),
            if is_stop {
                // Stop barrier: data already synced by the prior checkpoint.
                itertools::Either::Left(std::iter::empty())
            } else {
                itertools::Either::Right(node_actors.keys().copied())
            },
            new_actors,
            PartialGraphBarrierInfo::new(
                first_create_info.map_or_else(
                    PostCollectCommand::barrier,
                    CreateSnapshotBackfillJobCommandInfo::into_post_collect,
                ),
                barrier_info,
                notifiers,
                state_table_ids.clone(),
            ),
        )?;
        Ok(())
    }
}

// ── Barrier forwarding and collection ─────────────────────────────────────────

impl BatchRefreshJobCheckpointControl {
    pub(crate) fn on_new_upstream_barrier(
        &mut self,
        partial_graph_manager: &mut PartialGraphManager,
        barrier_info: &BarrierInfo,
        mutation: Option<(Mutation, Vec<Notifier>)>,
    ) -> MetaResult<()> {
        if !matches!(self.status, BatchRefreshJobStatus::ConsumingSnapshot { .. }) {
            return Ok(());
        }
        let (mut mutation, mut notifiers) = match mutation {
            Some((mutation, notifiers)) => (Some(mutation), notifiers),
            None => (None, vec![]),
        };

        // Check if snapshot consumption is finished and we need to inject stop barriers.
        let is_finished = matches!(
            &self.status,
            BatchRefreshJobStatus::ConsumingSnapshot { create_mview_tracker, .. }
            if create_mview_tracker.is_finished()
        );

        if is_finished {
            // Discard the upstream mutation — not needed for stop barriers.
            mutation.take();

            // Take the status out to destructure and transition to `FinishingSnapshot`.
            let old_status = replace(&mut self.status, BatchRefreshJobStatus::Idle);
            let BatchRefreshJobStatus::ConsumingSnapshot {
                prev_epoch_fake_physical_time,
                mut pending_non_checkpoint_barriers,
                snapshot_epoch,
                fragment_infos,
                create_mview_tracker,
                node_actors,
                state_table_ids,
                ..
            } = old_status
            else {
                unreachable!()
            };

            let tracking_job = create_mview_tracker.into_tracking_job();

            // Inject final checkpoint at snapshot epoch.
            pending_non_checkpoint_barriers.push(snapshot_epoch);
            let prev_epoch = Epoch::from_physical_time(prev_epoch_fake_physical_time);
            let final_checkpoint = BarrierInfo {
                curr_epoch: TracedEpoch::new(Epoch(snapshot_epoch)),
                prev_epoch: TracedEpoch::new(prev_epoch),
                kind: BarrierKind::Checkpoint(take(&mut pending_non_checkpoint_barriers)),
            };

            // Inject stop barrier with u64::MAX as curr_epoch and empty nodes_to_sync_table.
            let stop_barrier = BarrierInfo {
                prev_epoch: TracedEpoch::new(Epoch(snapshot_epoch)),
                curr_epoch: TracedEpoch::new(Epoch(u64::MAX)),
                kind: BarrierKind::Checkpoint(vec![snapshot_epoch]),
            };

            let stop_actors: Vec<ActorId> = fragment_infos
                .values()
                .flat_map(|f| f.actors.keys().copied())
                .collect();

            Self::inject_barrier(
                self.partial_graph_id,
                partial_graph_manager,
                &node_actors,
                &state_table_ids,
                final_checkpoint,
                None,
                None,
                take(&mut notifiers),
                None,
                false,
            )?;
            Self::inject_barrier(
                self.partial_graph_id,
                partial_graph_manager,
                &node_actors,
                &state_table_ids,
                stop_barrier,
                None,
                Some(Mutation::Stop(StopMutation {
                    actors: stop_actors,
                    dropped_sink_fragments: vec![],
                })),
                vec![],
                None,
                true,
            )?;

            self.status = BatchRefreshJobStatus::FinishingSnapshot {
                tracking_job: Some(tracking_job),
                fragment_infos,
            };
        } else {
            // Normal barrier — still consuming snapshot.
            let BatchRefreshJobStatus::ConsumingSnapshot {
                prev_epoch_fake_physical_time,
                pending_non_checkpoint_barriers,
                create_mview_tracker,
                node_actors,
                state_table_ids,
                ..
            } = &mut self.status
            else {
                unreachable!("is_finished was false, status must be ConsumingSnapshot")
            };

            // Forward a fake barrier to the partial graph.
            let mutation = mutation.take().or_else(|| {
                let pending_backfill_nodes = create_mview_tracker
                    .take_pending_backfill_nodes()
                    .collect_vec();
                if pending_backfill_nodes.is_empty() {
                    None
                } else {
                    Some(Mutation::StartFragmentBackfill(
                        StartFragmentBackfillMutation {
                            fragment_ids: pending_backfill_nodes,
                        },
                    ))
                }
            });
            let barrier_to_inject = super::new_fake_barrier(
                prev_epoch_fake_physical_time,
                pending_non_checkpoint_barriers,
                match barrier_info.kind {
                    BarrierKind::Barrier => PbBarrierKind::Barrier,
                    BarrierKind::Checkpoint(_) => PbBarrierKind::Checkpoint,
                    BarrierKind::Initial => {
                        unreachable!("upstream new epoch should not be initial")
                    }
                },
            );
            Self::inject_barrier(
                self.partial_graph_id,
                partial_graph_manager,
                node_actors,
                state_table_ids,
                barrier_to_inject,
                None,
                mutation,
                take(&mut notifiers),
                None,
                false,
            )?;
        }
        assert!(mutation.is_none(), "must have consumed mutation");
        assert!(notifiers.is_empty(), "must consumed notifiers");
        Ok(())
    }

    pub(crate) fn collect(&mut self, collected_barrier: CollectedBarrier<'_>) -> bool {
        match &mut self.status {
            BatchRefreshJobStatus::ConsumingSnapshot {
                create_mview_tracker,
                version_stats,
                ..
            } => {
                for progress in collected_barrier
                    .resps
                    .values()
                    .flat_map(|resp| &resp.create_mview_progress)
                {
                    create_mview_tracker.apply_progress(progress, version_stats);
                }
                create_mview_tracker.is_finished()
            }
            _ => false,
        }
    }
}

// ── Completing ────────────────────────────────────────────────────────────────

impl BatchRefreshJobCheckpointControl {
    #[expect(clippy::type_complexity)]
    pub(crate) fn start_completing(
        &mut self,
        partial_graph_manager: &mut PartialGraphManager,
    ) -> Option<(
        u64,
        HashMap<WorkerId, BarrierCompleteResponse>,
        PartialGraphBarrierInfo,
        Option<TrackingJob>,
    )> {
        match &self.status {
            BatchRefreshJobStatus::ConsumingSnapshot { .. }
            | BatchRefreshJobStatus::FinishingSnapshot { .. } => {}
            BatchRefreshJobStatus::Idle | BatchRefreshJobStatus::Resetting { .. } => {
                return None;
            }
        };

        partial_graph_manager
            .start_completing(
                self.partial_graph_id,
                std::ops::Bound::Unbounded,
                |_non_checkpoint_epoch, _resps, _| {
                    // Progress already applied in `collect()`.
                },
            )
            .map(|(epoch, resps, info)| {
                // Take tracking job only when completing the snapshot epoch
                // (the stop barrier). By this point the final checkpoint has
                // already been committed in a prior task.
                let tracking_job = if epoch == self.snapshot_epoch {
                    match &mut self.status {
                        BatchRefreshJobStatus::FinishingSnapshot { tracking_job, .. } => Some(
                            tracking_job
                                .take()
                                .expect("tracking job should not have been taken yet"),
                        ),
                        _ => panic!(
                            "batch refresh job {}: expected FinishingSnapshot at snapshot epoch",
                            self.job_id
                        ),
                    }
                } else {
                    None
                };
                (epoch, resps, info, tracking_job)
            })
    }

    pub(super) fn ack_completed(
        &mut self,
        partial_graph_manager: &mut PartialGraphManager,
        completed_epoch: u64,
    ) {
        partial_graph_manager.ack_completed(self.partial_graph_id, completed_epoch);
        if let Some(prev_max_committed_epoch) = self.max_committed_epoch.replace(completed_epoch) {
            assert!(completed_epoch > prev_max_committed_epoch);
        }

        if completed_epoch == self.snapshot_epoch {
            // The stop barrier (prev_epoch = snapshot_epoch) has been committed.
            // Assert expected state and transition to idle.
            match &self.status {
                BatchRefreshJobStatus::FinishingSnapshot { tracking_job, .. } => {
                    assert!(
                        tracking_job.is_none(),
                        "tracking job should have been taken at start_completing"
                    );
                }
                _ => panic!(
                    "batch refresh job {}: expected FinishingSnapshot when completing snapshot epoch",
                    self.job_id
                ),
            }
            info!(
                job_id = %self.job_id,
                completed_epoch,
                "batch refresh job: transitioned to idle, removing partial graph"
            );
            partial_graph_manager.remove_partial_graphs(vec![self.partial_graph_id]);
            self.status = BatchRefreshJobStatus::Idle;
        }
    }

    /// Called when the partial graph reset is confirmed (drop only).
    pub(super) fn on_partial_graph_reset(mut self) {
        match &mut self.status {
            BatchRefreshJobStatus::Resetting { notifiers } => {
                for notifier in notifiers.drain(..) {
                    notifier.notify_collected();
                }
            }
            _ => {
                panic!(
                    "batch refresh job {}: on_partial_graph_reset in unexpected state {:?}",
                    self.job_id, self.status
                );
            }
        }
    }
}

// ── Query methods ─────────────────────────────────────────────────────────────

impl BatchRefreshJobCheckpointControl {
    pub(crate) fn gen_backfill_progress(&self) -> Option<BackfillProgress> {
        match &self.status {
            BatchRefreshJobStatus::ConsumingSnapshot {
                create_mview_tracker,
                ..
            } => {
                let progress = if create_mview_tracker.is_finished() {
                    "Snapshot finished".to_owned()
                } else {
                    let progress = create_mview_tracker.gen_backfill_progress();
                    format!("BatchRefresh Snapshot [{}]", progress)
                };
                Some(BackfillProgress {
                    progress,
                    backfill_type: PbBackfillType::SnapshotBackfill,
                })
            }
            BatchRefreshJobStatus::FinishingSnapshot { .. } => Some(BackfillProgress {
                progress: "BatchRefresh Stopping".to_owned(),
                backfill_type: PbBackfillType::SnapshotBackfill,
            }),
            BatchRefreshJobStatus::Idle | BatchRefreshJobStatus::Resetting { .. } => None,
        }
    }

    pub(super) fn gen_fragment_backfill_progress(&self) -> Vec<FragmentBackfillProgress> {
        match &self.status {
            BatchRefreshJobStatus::ConsumingSnapshot {
                create_mview_tracker,
                fragment_infos,
                ..
            } => create_mview_tracker.collect_fragment_progress(fragment_infos, true),
            BatchRefreshJobStatus::FinishingSnapshot { fragment_infos, .. } => {
                collect_done_fragments(self.job_id, fragment_infos)
            }
            _ => vec![],
        }
    }

    /// Returns the pinned upstream log epoch and upstream table IDs.
    pub(super) fn pinned_upstream_log_epoch(&self) -> (u64, HashSet<TableId>) {
        (
            self.snapshot_epoch,
            self.snapshot_backfill_upstream_tables.clone(),
        )
    }

    pub(crate) fn fragment_infos(&self) -> Option<&HashMap<FragmentId, InflightFragmentInfo>> {
        match &self.status {
            BatchRefreshJobStatus::ConsumingSnapshot { fragment_infos, .. } => Some(fragment_infos),
            BatchRefreshJobStatus::FinishingSnapshot { .. }
            | BatchRefreshJobStatus::Idle
            | BatchRefreshJobStatus::Resetting { .. } => None,
        }
    }

    pub(crate) fn is_snapshot_backfilling(&self) -> bool {
        matches!(
            self.status,
            BatchRefreshJobStatus::ConsumingSnapshot { .. }
                | BatchRefreshJobStatus::FinishingSnapshot { .. }
        )
    }
}

// ── Drop handling ─────────────────────────────────────────────────────────────

impl BatchRefreshJobCheckpointControl {
    /// Drop this batch refresh job.
    pub(super) fn drop(
        &mut self,
        notifiers: &mut Vec<Notifier>,
        partial_graph_manager: &mut PartialGraphManager,
    ) -> bool {
        match &mut self.status {
            BatchRefreshJobStatus::Resetting {
                notifiers: existing_notifiers,
            } => {
                for notifier in &mut *notifiers {
                    notifier.notify_started();
                }
                existing_notifiers.append(notifiers);
                true
            }
            BatchRefreshJobStatus::ConsumingSnapshot { .. }
            | BatchRefreshJobStatus::FinishingSnapshot { .. } => {
                for notifier in &mut *notifiers {
                    notifier.notify_started();
                }
                partial_graph_manager.reset_partial_graphs([self.partial_graph_id]);
                self.status = BatchRefreshJobStatus::Resetting {
                    notifiers: take(notifiers),
                };
                true
            }
            BatchRefreshJobStatus::Idle => {
                // Idle has no running partial graph, but we still go through
                // the reset flow so the cleanup path is uniform.
                for notifier in &mut *notifiers {
                    notifier.notify_started();
                }
                partial_graph_manager.reset_partial_graphs([self.partial_graph_id]);
                self.status = BatchRefreshJobStatus::Resetting {
                    notifiers: take(notifiers),
                };
                true
            }
        }
    }

    /// Reset during database recovery.
    ///
    /// Returns `true` if the partial graph was already resetting (from a prior drop),
    /// meaning we should not issue a new reset request.
    pub(crate) fn reset(self) -> bool {
        match self.status {
            BatchRefreshJobStatus::ConsumingSnapshot { .. }
            | BatchRefreshJobStatus::FinishingSnapshot { .. }
            | BatchRefreshJobStatus::Idle => false,
            BatchRefreshJobStatus::Resetting { notifiers } => {
                for notifier in notifiers {
                    notifier.notify_collected();
                }
                true
            }
        }
    }
}

// ── Barrier stats ─────────────────────────────────────────────────────────────

struct BatchRefreshBarrierStats {
    barrier_latency: LabelGuardedHistogram,
    inflight_barrier_num: LabelGuardedIntGauge,
}

impl BatchRefreshBarrierStats {
    fn new(job_id: JobId, _snapshot_epoch: u64) -> Self {
        let table_id_str = format!("{}", job_id);
        Self {
            barrier_latency: GLOBAL_META_METRICS
                .snapshot_backfill_barrier_latency
                .with_guarded_label_values(&[table_id_str.as_str(), "batch_refresh_snapshot"]),
            inflight_barrier_num: GLOBAL_META_METRICS
                .snapshot_backfill_inflight_barrier_num
                .with_guarded_label_values(&[&table_id_str]),
        }
    }
}

impl PartialGraphStat for BatchRefreshBarrierStats {
    fn observe_barrier_latency(&self, _epoch: EpochPair, barrier_latency_secs: f64) {
        self.barrier_latency.observe(barrier_latency_secs);
    }

    fn observe_barrier_num(&self, inflight_barrier_num: usize, _collected_barrier_num: usize) {
        self.inflight_barrier_num.set(inflight_barrier_num as _);
    }
}
