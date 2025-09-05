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

use std::sync::Arc;

use anyhow::Context;
use risingwave_common::catalog::{DatabaseId, FragmentTypeFlag, TableId};
use risingwave_pb::common::WorkerNode;
use risingwave_pb::hummock::HummockVersionStats;
use risingwave_pb::meta::object::PbObjectInfo;
use risingwave_pb::meta::subscribe_response::{Operation, PbInfo};
use risingwave_pb::meta::{PbObject, PbObjectGroup};
use risingwave_pb::stream_service::streaming_control_stream_request::PbInitRequest;
use risingwave_rpc_client::StreamingControlHandle;
use thiserror_ext::AsReport;

use crate::MetaResult;
use crate::barrier::command::CommandContext;
use crate::barrier::context::{GlobalBarrierWorkerContext, GlobalBarrierWorkerContextImpl};
use crate::barrier::progress::TrackingJob;
use crate::barrier::schedule::MarkReadyOptions;
use crate::barrier::{
    BarrierManagerStatus, BarrierWorkerRuntimeInfoSnapshot, Command, CreateStreamingJobCommandInfo,
    CreateStreamingJobType, DatabaseRuntimeInfoSnapshot, RecoveryReason, ReplaceStreamJobPlan,
    Scheduled,
};
use crate::hummock::CommitEpochInfo;
use crate::stream::SourceChange;

impl GlobalBarrierWorkerContext for GlobalBarrierWorkerContextImpl {
    #[await_tree::instrument]
    async fn commit_epoch(&self, commit_info: CommitEpochInfo) -> MetaResult<HummockVersionStats> {
        self.hummock_manager.commit_epoch(commit_info).await?;
        Ok(self.hummock_manager.get_version_stats().await)
    }

    #[await_tree::instrument("next_scheduled_barrier")]
    async fn next_scheduled(&self) -> Scheduled {
        self.scheduled_barriers.next_scheduled().await
    }

    fn abort_and_mark_blocked(
        &self,
        database_id: Option<DatabaseId>,
        recovery_reason: RecoveryReason,
    ) {
        if database_id.is_none() {
            self.set_status(BarrierManagerStatus::Recovering(recovery_reason));
        }

        // Mark blocked and abort buffered schedules, they might be dirty already.
        self.scheduled_barriers
            .abort_and_mark_blocked(database_id, "cluster is under recovering");
    }

    fn mark_ready(&self, options: MarkReadyOptions) {
        let is_global = matches!(&options, MarkReadyOptions::Global { .. });
        self.scheduled_barriers.mark_ready(options);
        if is_global {
            self.set_status(BarrierManagerStatus::Running);
        }
    }

    #[await_tree::instrument("post_collect_command({command})")]
    async fn post_collect_command<'a>(&'a self, command: &'a CommandContext) -> MetaResult<()> {
        command.post_collect(self).await
    }

    async fn notify_creating_job_failed(&self, database_id: Option<DatabaseId>, err: String) {
        self.metadata_manager
            .notify_finish_failed(database_id, err)
            .await
    }

    #[await_tree::instrument("finish_creating_job({job})")]
    async fn finish_creating_job(&self, job: TrackingJob) -> MetaResult<()> {
        job.finish(&self.metadata_manager).await
    }

    #[await_tree::instrument("finish_cdc_table_backfill({job})")]
    async fn finish_cdc_table_backfill(&self, job: TableId) -> MetaResult<()> {
        self.env.cdc_table_backfill_tracker.complete_job(job).await
    }

    #[await_tree::instrument("new_control_stream({})", node.id)]
    async fn new_control_stream(
        &self,
        node: &WorkerNode,
        init_request: &PbInitRequest,
    ) -> MetaResult<StreamingControlHandle> {
        self.new_control_stream_impl(node, init_request).await
    }

    async fn reload_runtime_info(&self) -> MetaResult<BarrierWorkerRuntimeInfoSnapshot> {
        self.reload_runtime_info_impl().await
    }

    async fn reload_database_runtime_info(
        &self,
        database_id: DatabaseId,
    ) -> MetaResult<Option<DatabaseRuntimeInfoSnapshot>> {
        self.reload_database_runtime_info_impl(database_id).await
    }

    async fn handle_load_finished_source_ids(
        &self,
        load_finished_source_ids: Vec<u32>,
    ) -> MetaResult<()> {
        use risingwave_common::catalog::TableId;

        tracing::info!(
            "Handling load finished source IDs: {:?}",
            load_finished_source_ids
        );

        use crate::barrier::Command;
        for associated_source_id in load_finished_source_ids {
            let res: MetaResult<()> = try {
                tracing::info!(%associated_source_id, "Scheduling LoadFinish command for refreshable batch source");

                // For refreshable batch sources, associated_source_id is the table_id
                let table_id = TableId::new(associated_source_id);
                let associated_source_id = table_id;

                // Find the database ID for this table
                let database_id = self
                    .metadata_manager
                    .catalog_controller
                    .get_object_database_id(table_id.table_id() as _)
                    .await
                    .context("Failed to get database id for table")?;

                // Create LoadFinish command
                let load_finish_command = Command::LoadFinish {
                    table_id,
                    associated_source_id,
                };

                // Schedule the command through the barrier system without waiting
                self.barrier_scheduler
                    .run_command_no_wait(
                        risingwave_common::catalog::DatabaseId::new(database_id as u32),
                        load_finish_command,
                    )
                    .context("Failed to schedule LoadFinish command")?;

                tracing::info!(%table_id, %associated_source_id, "LoadFinish command scheduled successfully");
            };
            if let Err(e) = res {
                tracing::error!(error = %e.as_report(),%associated_source_id, "Failed to handle source load finished");
            }
        }

        Ok(())
    }
}

impl GlobalBarrierWorkerContextImpl {
    fn set_status(&self, new_status: BarrierManagerStatus) {
        self.status.store(Arc::new(new_status));
    }
}

impl CommandContext {
    /// Do some stuffs after barriers are collected and the new storage version is committed, for
    /// the given command.
    pub async fn post_collect(
        &self,
        barrier_manager_context: &GlobalBarrierWorkerContextImpl,
    ) -> MetaResult<()> {
        let Some(command) = &self.command else {
            return Ok(());
        };
        match command {
            Command::Flush => {}

            Command::Throttle(_) => {}

            Command::Pause => {}

            Command::Resume => {}

            Command::SourceChangeSplit(split_assignment) => {
                barrier_manager_context
                    .metadata_manager
                    .update_actor_splits_by_split_assignment(split_assignment)
                    .await?;
                barrier_manager_context
                    .source_manager
                    .apply_source_change(SourceChange::SplitChange(split_assignment.clone()))
                    .await;
            }

            Command::DropStreamingJobs {
                unregistered_state_table_ids,
                ..
            } => {
                barrier_manager_context
                    .hummock_manager
                    .unregister_table_ids(unregistered_state_table_ids.iter().cloned())
                    .await?;
                let tables = barrier_manager_context
                    .metadata_manager
                    .catalog_controller
                    .complete_dropped_tables(
                        unregistered_state_table_ids
                            .iter()
                            .map(|id| id.table_id as _),
                    )
                    .await;
                let objects = tables
                    .into_iter()
                    .map(|t| PbObject {
                        object_info: Some(PbObjectInfo::Table(t)),
                    })
                    .collect();
                let group = PbInfo::ObjectGroup(PbObjectGroup { objects });
                barrier_manager_context
                    .env
                    .notification_manager()
                    .notify_hummock(Operation::Delete, group.clone())
                    .await;
                barrier_manager_context
                    .env
                    .notification_manager()
                    .notify_compactor(Operation::Delete, group)
                    .await;
            }
            Command::ConnectorPropsChange(obj_id_map_props) => {
                // todo: we dont know the type of the object id, it can be a source or a sink. Should carry more info in the barrier command.
                barrier_manager_context
                    .source_manager
                    .apply_source_change(SourceChange::UpdateSourceProps {
                        source_id_map_new_props: obj_id_map_props.clone(),
                    })
                    .await;
            }
            Command::CreateStreamingJob {
                info,
                job_type,
                cross_db_snapshot_backfill_info,
            } => {
                let mut replace_plan = None;
                match job_type {
                    CreateStreamingJobType::SinkIntoTable(plan) => {
                        replace_plan = Some(plan);
                    }
                    CreateStreamingJobType::Normal => {
                        barrier_manager_context
                            .metadata_manager
                            .catalog_controller
                            .fill_snapshot_backfill_epoch(
                                info.stream_job_fragments.fragments.iter().filter_map(
                                    |(fragment_id, fragment)| {
                                        if fragment.fragment_type_mask.contains(
                                            FragmentTypeFlag::CrossDbSnapshotBackfillStreamScan,
                                        ) {
                                            Some(*fragment_id as _)
                                        } else {
                                            None
                                        }
                                    },
                                ),
                                None,
                                cross_db_snapshot_backfill_info,
                            )
                            .await?
                    }
                    CreateStreamingJobType::SnapshotBackfill(snapshot_backfill_info) => {
                        barrier_manager_context
                            .metadata_manager
                            .catalog_controller
                            .fill_snapshot_backfill_epoch(
                                info.stream_job_fragments.fragments.iter().filter_map(
                                    |(fragment_id, fragment)| {
                                        if fragment.fragment_type_mask.contains_any([
                                            FragmentTypeFlag::SnapshotBackfillStreamScan,
                                            FragmentTypeFlag::CrossDbSnapshotBackfillStreamScan,
                                        ]) {
                                            Some(*fragment_id as _)
                                        } else {
                                            None
                                        }
                                    },
                                ),
                                Some(snapshot_backfill_info),
                                cross_db_snapshot_backfill_info,
                            )
                            .await?
                    }
                }

                // Do `post_collect_job_fragments` of the original streaming job in the end, so that in any previous failure,
                // we won't mark the job as `Creating`, and then the job will be later clean by the recovery triggered by the returned error.
                let CreateStreamingJobCommandInfo {
                    stream_job_fragments,
                    upstream_fragment_downstreams,
                    init_split_assignment,
                    ..
                } = info;
                barrier_manager_context
                    .metadata_manager
                    .catalog_controller
                    .post_collect_job_fragments(
                        stream_job_fragments.stream_job_id().table_id as _,
                        stream_job_fragments.actor_ids(),
                        upstream_fragment_downstreams,
                        init_split_assignment,
                        replace_plan,
                    )
                    .await?;

                if let Some(plan) = replace_plan {
                    barrier_manager_context
                        .source_manager
                        .handle_replace_job(
                            &plan.old_fragments,
                            plan.new_fragments.stream_source_fragments(),
                            init_split_assignment.clone(),
                            plan,
                        )
                        .await;
                }

                let source_change = SourceChange::CreateJob {
                    added_source_fragments: stream_job_fragments.stream_source_fragments(),
                    added_backfill_fragments: stream_job_fragments.source_backfill_fragments(),
                    split_assignment: init_split_assignment.clone(),
                };

                barrier_manager_context
                    .source_manager
                    .apply_source_change(source_change)
                    .await;
            }
            Command::RescheduleFragment {
                reschedules,
                post_updates,
                ..
            } => {
                barrier_manager_context
                    .scale_controller
                    .post_apply_reschedule(reschedules, post_updates)
                    .await?;
            }

            Command::ReplaceStreamJob(
                replace_plan @ ReplaceStreamJobPlan {
                    old_fragments,
                    new_fragments,
                    upstream_fragment_downstreams,
                    init_split_assignment,
                    to_drop_state_table_ids,
                    auto_refresh_schema_sinks,
                    ..
                },
            ) => {
                // Update actors and actor_dispatchers for new table fragments.
                barrier_manager_context
                    .metadata_manager
                    .catalog_controller
                    .post_collect_job_fragments(
                        new_fragments.stream_job_id.table_id as _,
                        new_fragments.actor_ids(),
                        upstream_fragment_downstreams,
                        init_split_assignment,
                        None,
                    )
                    .await?;

                if let Some(sinks) = auto_refresh_schema_sinks {
                    for sink in sinks {
                        barrier_manager_context
                            .metadata_manager
                            .catalog_controller
                            .post_collect_job_fragments(
                                sink.tmp_sink_id,
                                sink.actor_status.keys().cloned().collect(),
                                &Default::default(), // upstream_fragment_downstreams is already inserted in the job of upstream table
                                &Default::default(), // no split assignment
                                None, // no replace plan
                            )
                            .await?;
                    }
                }

                // Apply the split changes in source manager.
                barrier_manager_context
                    .source_manager
                    .handle_replace_job(
                        old_fragments,
                        new_fragments.stream_source_fragments(),
                        init_split_assignment.clone(),
                        replace_plan,
                    )
                    .await;
                barrier_manager_context
                    .hummock_manager
                    .unregister_table_ids(to_drop_state_table_ids.iter().cloned())
                    .await?;
            }

            Command::CreateSubscription {
                subscription_id, ..
            } => {
                barrier_manager_context
                    .metadata_manager
                    .catalog_controller
                    .finish_create_subscription_catalog(*subscription_id)
                    .await?
            }
            Command::DropSubscription { .. } => {}
            Command::MergeSnapshotBackfillStreamingJobs(_) => {}
            Command::StartFragmentBackfill { .. } => {}
            Command::Refresh { .. } => {}
            Command::LoadFinish { .. } => {}
        }

        Ok(())
    }
}
