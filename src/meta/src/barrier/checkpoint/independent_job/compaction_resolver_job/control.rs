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

use std::collections::{HashMap, HashSet};
use std::mem::take;
use std::sync::Arc;

use fail::fail_point;
use iceberg::spec::SerializedDataFile;
use risingwave_common::catalog::{DatabaseId, TableId};
use risingwave_common::id::JobId;
use risingwave_common::util::epoch::Epoch;
use risingwave_meta_model::WorkerId;
use risingwave_pb::id::{ActorId, FragmentId, IcebergCompactionTaskId, PartialGraphId, SinkId};
use risingwave_pb::stream_plan::barrier_mutation::Mutation;
use risingwave_pb::stream_plan::{AddMutation, StopMutation};
use risingwave_pb::stream_service::PbIcebergPkIndexSinkRole;
use risingwave_pb::stream_service::barrier_complete_response::PbIcebergPkIndexSinkMetadata;
use tracing::info;

use super::render::{CompactionResolveBarrierStats, CompactionResolverRenderResult};
use crate::MetaResult;
use crate::barrier::command::PostCollectCommand;
use crate::barrier::complete_task::CompactionOverwrite;
use crate::barrier::info::BarrierInfo;
use crate::barrier::notifier::Notifier;
use crate::barrier::partial_graph::{
    CollectedBarrier, PartialGraphBarrierInfo, PartialGraphManager,
};
use crate::barrier::rpc::to_partial_graph_id;
use crate::barrier::{BarrierKind, TracedEpoch};
use crate::controller::fragment::InflightFragmentInfo;
use crate::manager::iceberg_compaction::CompactionResolveCompletion;
use crate::model::StreamJobActorsToCreate;

/// The compaction overwrite a resolver job co-commits at its single checkpoint: replace
/// `input_file_paths` with `output_files` ∪ the masking delete files authored by
/// the `CompactionResolver`. Held on the job (in `Pausing`/`Resolving`) until the resolve is done,
/// then contributed to the seal checkpoint's complete-barrier task alongside the delete files.
#[derive(educe::Educe)]
#[educe(Debug)]
pub(crate) struct OverwriteInput {
    pub sink_id: SinkId,
    #[educe(Debug(ignore))]
    pub output_files: Vec<SerializedDataFile>,
    pub input_file_paths: Vec<String>,
    pub read_snapshot_id: i64,
}

#[derive(Debug)]
enum Phase {
    /// The dedicated Pause mutation has been injected on the main graph. Once that checkpoint is
    /// durably committed, the resolver graph is started without waiting for another main barrier.
    Pausing {
        render_result: CompactionResolverRenderResult,
        pause_barrier: BarrierInfo,
        overwrite: OverwriteInput,
    },
    /// Resolver partial graph running in exactly one streaming epoch.
    Resolving {
        render_result: CompactionResolverRenderResult,
        resolve_epoch: u64,
        overwrite: OverwriteInput,
        /// Masking delete-file metadata reported by the `CompactionResolver`, accumulated from the
        /// resolver Stop response and folded into the overwrite's add-set at commit time.
        delete_reports: Vec<PbIcebergPkIndexSinkMetadata>,
    },
    /// Resolver Stop is collected and drained locally. The writer has been told to release B2.
    AwaitingMainCommit {
        render_result: CompactionResolverRenderResult,
        seal_epoch: u64,
        overwrite: OverwriteInput,
        delete_reports: Vec<PbIcebergPkIndexSinkMetadata>,
    },
    /// The overwrite has been attached to the main B2 completion task.
    Committing {
        render_result: CompactionResolverRenderResult,
        seal_epoch: u64,
    },
    /// Terminal — the job is to be removed from the map.
    Done,
    /// Being reset (drop / recovery).
    Resetting { notifiers: Vec<Notifier> },
}

/// Checkpoint control for the transient pk-index resolver job. Stored in
/// `DatabaseCheckpointControl.independent_checkpoint_job_controls` as
/// `IndependentCheckpointJobControl::CompactionResolve`.
#[derive(Debug)]
pub(crate) struct CompactionResolveJobControl {
    database_id: DatabaseId,
    job_id: JobId,
    partial_graph_id: PartialGraphId,
    sink_id: SinkId,
    task_id: IcebergCompactionTaskId,
    completion: Arc<CompactionResolveCompletion>,
    writer_node_actors: HashMap<WorkerId, Vec<ActorId>>,
    phase: Phase,
}

impl CompactionResolveJobControl {
    /// The synthetic `job_id` for a resolve of `sink_id`. There is at most one live resolver job per
    /// sink at a time (disjoint compaction inputs are serialized), so the sink id is a stable,
    /// collision-free key within a database's independent-job map / partial-graph id space.
    pub(crate) fn job_id_for_sink(sink_id: SinkId) -> JobId {
        JobId::new(sink_id.as_raw_id())
    }

    /// Create the job. The writer relinquishes `T` via the pause half of the target-scoped barrier
    /// pair; the resolver partial graph is added once that barrier is durably committed.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        database_id: DatabaseId,
        sink_id: SinkId,
        task_id: IcebergCompactionTaskId,
        completion: Arc<CompactionResolveCompletion>,
        writer_node_actors: HashMap<WorkerId, Vec<ActorId>>,
        pause_barrier: BarrierInfo,
        overwrite: OverwriteInput,
        job_state_table_ids: HashSet<TableId>,
        render_result: CompactionResolverRenderResult,
    ) -> Self {
        assert!(
            render_result
                .state_table_ids
                .is_subset(&job_state_table_ids),
            "resolver state tables must belong to the target job"
        );
        let job_id = Self::job_id_for_sink(sink_id);
        let partial_graph_id = to_partial_graph_id(database_id, Some(job_id));
        info!(
            %sink_id, %task_id,
            "created pk-index resolver job"
        );
        Self {
            database_id,
            job_id,
            partial_graph_id,
            sink_id,
            task_id,
            completion,
            writer_node_actors,
            phase: Phase::Pausing {
                render_result,
                pause_barrier,
                overwrite,
            },
        }
    }

    pub(crate) fn fragment_infos(&self) -> Option<&HashMap<FragmentId, InflightFragmentInfo>> {
        match &self.phase {
            Phase::Pausing { render_result, .. }
            | Phase::Resolving { render_result, .. }
            | Phase::AwaitingMainCommit { render_result, .. }
            | Phase::Committing { render_result, .. } => Some(&render_result.fragment_infos),
            Phase::Done | Phase::Resetting { .. } => None,
        }
    }

    /// Start the resolver as soon as the pause checkpoint is durable. The Initial and checkpoint
    /// Stop barriers are injected back-to-back, so the entire resolve runs in one streaming epoch
    /// without waiting for or mirroring another main-graph barrier.
    pub(crate) fn on_main_graph_committed(
        &mut self,
        partial_graph_manager: &mut PartialGraphManager,
        main_graph_committed_epoch: u64,
    ) -> MetaResult<bool> {
        match &self.phase {
            Phase::Pausing { pause_barrier, .. }
                if main_graph_committed_epoch >= pause_barrier.prev_epoch() =>
            {
                fail_point!("iceberg_pk_index_resolver_start_fail", |_| Err(
                    anyhow::anyhow!("iceberg_pk_index_resolver_start_fail").into()
                ));
                let Phase::Pausing {
                    render_result,
                    pause_barrier,
                    overwrite,
                } = std::mem::replace(&mut self.phase, Phase::Done)
                else {
                    unreachable!()
                };
                let resolve_epoch = pause_barrier.curr_epoch();
                self.add_partial_graph_and_inject_resolve(
                    partial_graph_manager,
                    &pause_barrier,
                    &render_result,
                )?;
                self.phase = Phase::Resolving {
                    render_result,
                    resolve_epoch,
                    overwrite,
                    delete_reports: Vec::new(),
                };
            }
            Phase::Committing { seal_epoch, .. } if main_graph_committed_epoch == *seal_epoch => {
                fail_point!("iceberg_pk_index_resolver_committed_notify_fail", |_| Err(
                    anyhow::anyhow!("iceberg_pk_index_resolver_committed_notify_fail").into()
                ));
                partial_graph_manager.remove_partial_graphs(vec![self.partial_graph_id]);
                partial_graph_manager
                    .control_stream_manager()
                    .control_compaction_writer(
                        to_partial_graph_id(self.database_id, None),
                        self.sink_id,
                        self.task_id,
                        *seal_epoch,
                        risingwave_pb::stream_service::streaming_control_stream_request::control_compaction_writer_request::Stage::Committed,
                        &self.writer_node_actors,
                    )?;
                self.phase = Phase::Done;
                self.completion.finish(true);
                return Ok(true);
            }
            _ => {}
        }
        Ok(false)
    }

    /// If this completing `epoch` is the resolver Stop checkpoint, return the compaction overwrite
    /// to co-commit and transition `Resolving -> Sealing`. A collected Stop checkpoint proves that
    /// the singleton resolver finished scanning and every apply actor committed its shard of `T`.
    pub(crate) fn take_overwrite_for_main_epoch(
        &mut self,
        epoch: u64,
    ) -> Option<CompactionOverwrite> {
        let Phase::AwaitingMainCommit {
            seal_epoch,
            delete_reports,
            ..
        } = &self.phase
        else {
            return None;
        };
        if epoch != *seal_epoch {
            return None;
        }
        let delete_report_count = delete_reports.len();
        let Phase::AwaitingMainCommit {
            render_result,
            overwrite,
            delete_reports,
            ..
        } = std::mem::replace(&mut self.phase, Phase::Done)
        else {
            unreachable!()
        };
        let OverwriteInput {
            sink_id,
            output_files,
            input_file_paths,
            read_snapshot_id,
        } = overwrite;
        info!(
            %sink_id, seal_epoch = epoch, delete_reports = delete_report_count,
            "pk-index resolver: attaching overwrite to main B2 checkpoint"
        );
        let contribution = CompactionOverwrite {
            sink_id,
            epoch,
            output_files,
            delete_reports,
            input_file_paths,
            read_snapshot_id,
        };
        self.phase = Phase::Committing {
            render_result,
            seal_epoch: epoch,
        };
        Some(contribution)
    }

    /// Bootstrap the resolver at the paused writer's current epoch without committing anything.
    fn bootstrap_partial_graph_barrier(barrier_info: &BarrierInfo) -> BarrierInfo {
        BarrierInfo {
            prev_epoch: TracedEpoch::new(Epoch(barrier_info.prev_epoch())),
            curr_epoch: TracedEpoch::new(Epoch(barrier_info.curr_epoch())),
            kind: BarrierKind::Barrier,
        }
    }

    /// Stop the resolver after its actors have committed local state at the main B2 epoch. This is
    /// deliberately non-checkpoint: only the main graph may own the Hummock sync.
    fn resolve_stop_barrier(barrier_info: &BarrierInfo) -> BarrierInfo {
        BarrierInfo {
            prev_epoch: TracedEpoch::new(Epoch(barrier_info.curr_epoch())),
            curr_epoch: TracedEpoch::new(Epoch(u64::MAX)),
            kind: BarrierKind::Barrier,
        }
    }

    fn add_partial_graph_and_inject_resolve(
        &self,
        partial_graph_manager: &mut PartialGraphManager,
        barrier_info: &BarrierInfo,
        render_result: &CompactionResolverRenderResult,
    ) -> MetaResult<()> {
        let bootstrap_barrier = Self::bootstrap_partial_graph_barrier(barrier_info);
        let initial_mutation = Mutation::Add(AddMutation {
            actor_dispatchers: Default::default(),
            added_actors: render_result.actor_ids.clone(),
            actor_splits: Default::default(),
            pause: false,
            subscriptions_to_add: Default::default(),
            backfill_nodes_to_pause: Default::default(),
            actor_cdc_table_snapshot_splits: None,
            new_upstream_sinks: Default::default(),
            dropped_actors: Default::default(),
            sink_log_store_flush: Default::default(),
        });
        let mut graph_adder = partial_graph_manager.add_partial_graph(
            self.partial_graph_id,
            CompactionResolveBarrierStats::new(self.job_id),
        );
        if let Err(e) = self.inject_barrier_inner(
            self.partial_graph_id,
            graph_adder.manager(),
            render_result,
            bootstrap_barrier,
            Some(render_result.actors_to_create_ref_clone()),
            Some(initial_mutation),
            false,
        ) {
            graph_adder.failed();
            return Err(e);
        }
        graph_adder.added();
        let stop_barrier = Self::resolve_stop_barrier(barrier_info);
        self.inject_barrier_inner(
            self.partial_graph_id,
            partial_graph_manager,
            render_result,
            stop_barrier,
            None,
            Some(Mutation::Stop(StopMutation {
                actors: render_result.actor_ids.clone(),
                dropped_sink_fragments: vec![],
            })),
            true,
        )
    }

    fn inject_barrier_inner(
        &self,
        partial_graph_id: PartialGraphId,
        partial_graph_manager: &mut PartialGraphManager,
        render_result: &CompactionResolverRenderResult,
        barrier_info: BarrierInfo,
        new_actors: Option<StreamJobActorsToCreate>,
        mutation: Option<Mutation>,
        is_stop: bool,
    ) -> MetaResult<()> {
        if is_stop {
            assert!(
                matches!(&mutation, Some(Mutation::Stop(_))),
                "stop barrier must carry a Stop mutation"
            );
        }
        let info = self.new_partial_graph_barrier_info(barrier_info);
        partial_graph_manager.inject_barrier_without_starting_table_epoch(
            partial_graph_id,
            mutation,
            &render_result.node_actors,
            std::iter::empty(),
            render_result.node_actors.keys().copied(),
            new_actors,
            info,
        )?;
        Ok(())
    }

    fn new_partial_graph_barrier_info(&self, barrier_info: BarrierInfo) -> PartialGraphBarrierInfo {
        PartialGraphBarrierInfo::new(
            // NOTE: NOT `CreateStreamingJob` — these tables already belong to the writer's job.
            PostCollectCommand::barrier(),
            barrier_info,
            vec![],
            HashSet::new(),
        )
    }

    /// Collect resolver-authored masking delete files from barrier responses. Completion itself is
    /// represented by collection of the terminal Stop checkpoint, not create-MV progress.
    pub(crate) fn collect(
        &mut self,
        collected_barrier: CollectedBarrier<'_>,
        partial_graph_manager: &mut PartialGraphManager,
    ) -> MetaResult<bool> {
        self.collect_delete_reports(&collected_barrier);
        let Phase::Resolving { resolve_epoch, .. } = &self.phase else {
            return Ok(false);
        };
        if collected_barrier.epoch.prev != *resolve_epoch {
            return Ok(false);
        }

        let resolve_epoch = *resolve_epoch;
        let mut drained_stop = false;
        let checkpoint = partial_graph_manager.start_completing(
            self.partial_graph_id,
            std::ops::Bound::Unbounded,
            |epoch, _resps, _post_collect_command| {
                if epoch.prev == resolve_epoch {
                    drained_stop = true;
                }
            },
        );
        assert!(
            checkpoint.is_none(),
            "resolver Stop must never produce an independent checkpoint"
        );
        assert!(
            drained_stop,
            "resolver Stop barrier should be drained locally"
        );

        let Phase::Resolving {
            render_result,
            resolve_epoch,
            overwrite,
            delete_reports,
        } = std::mem::replace(&mut self.phase, Phase::Done)
        else {
            unreachable!()
        };
        partial_graph_manager
            .control_stream_manager()
            .control_compaction_writer(
                to_partial_graph_id(self.database_id, None),
                self.sink_id,
                self.task_id,
                resolve_epoch,
                risingwave_pb::stream_service::streaming_control_stream_request::control_compaction_writer_request::Stage::SealReady,
                &self.writer_node_actors,
            )?;
        self.phase = Phase::AwaitingMainCommit {
            render_result,
            seal_epoch: resolve_epoch,
            overwrite,
            delete_reports,
        };
        Ok(false)
    }

    fn collect_delete_reports(&mut self, collected_barrier: &CollectedBarrier<'_>) {
        if let Phase::Resolving { delete_reports, .. } = &mut self.phase {
            for resp in collected_barrier.resps.values() {
                for meta in &resp.iceberg_pk_index_sink_metadata {
                    if PbIcebergPkIndexSinkRole::try_from(meta.role)
                        == Ok(PbIcebergPkIndexSinkRole::CompactionResolver)
                    {
                        delete_reports.push(meta.clone());
                    }
                }
            }
        }
    }

    pub(crate) fn on_partial_graph_reset(mut self) {
        match &mut self.phase {
            Phase::Resetting { notifiers } => {
                for notifier in notifiers.drain(..) {
                    notifier.notify_collected();
                }
            }
            _ => panic!(
                "pk-index resolver job {}: on_partial_graph_reset in unexpected phase {:?}",
                self.job_id, self.phase
            ),
        }
    }

    pub(crate) fn drop(
        &mut self,
        notifiers: &mut Vec<Notifier>,
        partial_graph_manager: &mut PartialGraphManager,
    ) -> bool {
        for notifier in &mut *notifiers {
            notifier.notify_started();
        }
        match &mut self.phase {
            Phase::Resetting {
                notifiers: existing,
            } => {
                existing.append(notifiers);
                true
            }
            Phase::Pausing { .. } => {
                // No partial graph was added yet; nothing to reset on the stream side.
                self.phase = Phase::Resetting {
                    notifiers: take(notifiers),
                };
                for notifier in self.take_resetting_notifiers() {
                    notifier.notify_collected();
                }
                true
            }
            Phase::Resolving { .. }
            | Phase::AwaitingMainCommit { .. }
            | Phase::Committing { .. } => {
                partial_graph_manager.reset_partial_graphs([self.partial_graph_id]);
                self.phase = Phase::Resetting {
                    notifiers: take(notifiers),
                };
                true
            }
            Phase::Done => {
                for notifier in take(notifiers) {
                    notifier.notify_collected();
                }
                true
            }
        }
    }

    fn take_resetting_notifiers(&mut self) -> Vec<Notifier> {
        if let Phase::Resetting { notifiers } = &mut self.phase {
            take(notifiers)
        } else {
            vec![]
        }
    }

    pub(crate) fn reset(self) -> bool {
        match self.phase {
            Phase::Pausing { .. }
            | Phase::Resolving { .. }
            | Phase::AwaitingMainCommit { .. }
            | Phase::Committing { .. }
            | Phase::Done => false,
            Phase::Resetting { notifiers } => {
                for notifier in notifiers {
                    notifier.notify_collected();
                }
                true
            }
        }
    }
}

impl CompactionResolverRenderResult {
    fn actors_to_create_ref_clone(&self) -> StreamJobActorsToCreate {
        self.actors_to_create.clone()
    }
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
