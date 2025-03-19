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

use std::collections::HashMap;
use std::future::{Future, pending};
use std::mem::replace;
use std::sync::Arc;

use anyhow::Context;
use futures::future::try_join_all;
use prometheus::HistogramTimer;
use risingwave_common::catalog::{DatabaseId, TableId};
use risingwave_common::must_match;
use risingwave_pb::hummock::HummockVersionStats;
use tokio::task::JoinHandle;

use crate::barrier::checkpoint::CheckpointControl;
use crate::barrier::command::CommandContext;
use crate::barrier::context::GlobalBarrierWorkerContext;
use crate::barrier::notifier::Notifier;
use crate::barrier::progress::TrackingJob;
use crate::barrier::rpc::ControlStreamManager;
use crate::barrier::schedule::PeriodicBarriers;
use crate::hummock::CommitEpochInfo;
use crate::manager::MetaSrvEnv;
use crate::rpc::metrics::GLOBAL_META_METRICS;
use crate::{MetaError, MetaResult};

pub(super) enum CompletingTask {
    None,
    Completing {
        #[expect(clippy::type_complexity)]
        /// `database_id` -> (`Some(database_graph_committed_epoch)`, [(`creating_job_id`, `creating_job_committed_epoch`)])
        epochs_to_ack: HashMap<DatabaseId, (Option<u64>, Vec<(TableId, u64)>)>,

        // The join handle of a spawned task that completes the barrier.
        // The return value indicate whether there is some create streaming job command
        // that has finished but not checkpointed. If there is any, we will force checkpoint on the next barrier
        join_handle: JoinHandle<MetaResult<HummockVersionStats>>,
    },
    #[expect(dead_code)]
    Err(MetaError),
}

#[derive(Default)]
pub(super) struct CompleteBarrierTask {
    pub(super) commit_info: CommitEpochInfo,
    pub(super) finished_jobs: Vec<TrackingJob>,
    pub(super) notifiers: Vec<Notifier>,
    /// `database_id` -> (Some((`command_ctx`, `enqueue_time`)), vec!((`creating_job_id`, `epoch`)))
    #[expect(clippy::type_complexity)]
    pub(super) epoch_infos: HashMap<
        DatabaseId,
        (
            Option<(CommandContext, HistogramTimer)>,
            Vec<(TableId, u64)>,
        ),
    >,
}

impl CompleteBarrierTask {
    #[expect(clippy::type_complexity)]
    pub(super) fn epochs_to_ack(&self) -> HashMap<DatabaseId, (Option<u64>, Vec<(TableId, u64)>)> {
        self.epoch_infos
            .iter()
            .map(|(database_id, (command_context, creating_job_epochs))| {
                (
                    *database_id,
                    (
                        command_context
                            .as_ref()
                            .map(|(command, _)| command.barrier_info.prev_epoch.value().0),
                        creating_job_epochs.clone(),
                    ),
                )
            })
            .collect()
    }
}

impl CompleteBarrierTask {
    pub(super) async fn complete_barrier(
        self,
        context: &impl GlobalBarrierWorkerContext,
        env: MetaSrvEnv,
    ) -> MetaResult<HummockVersionStats> {
        let result: MetaResult<HummockVersionStats> = try {
            let wait_commit_timer = GLOBAL_META_METRICS
                .barrier_wait_commit_latency
                .start_timer();
            let version_stats = context.commit_epoch(self.commit_info).await?;
            for command_ctx in self
                .epoch_infos
                .values()
                .flat_map(|(command, _)| command.as_ref().map(|(command, _)| command))
            {
                context.post_collect_command(command_ctx).await?;
            }

            wait_commit_timer.observe_duration();
            version_stats
        };

        let version_stats = {
            let version_stats = match result {
                Ok(version_stats) => version_stats,
                Err(e) => {
                    for notifier in self.notifiers {
                        notifier.notify_collection_failed(e.clone());
                    }
                    return Err(e);
                }
            };
            self.notifiers.into_iter().for_each(|notifier| {
                notifier.notify_collected();
            });
            try_join_all(
                self.finished_jobs
                    .into_iter()
                    .map(|finished_job| context.finish_creating_job(finished_job)),
            )
            .await?;
            for (database_id, (command, _)) in self.epoch_infos {
                if let Some((command_ctx, enqueue_time)) = command {
                    let duration_sec = enqueue_time.stop_and_record();
                    Self::report_complete_event(&env, duration_sec, &command_ctx);
                    GLOBAL_META_METRICS
                        .last_committed_barrier_time
                        .with_label_values(&[database_id.database_id.to_string().as_str()])
                        .set(command_ctx.barrier_info.curr_epoch.value().as_unix_secs() as i64);
                }
            }
            version_stats
        };

        Ok(version_stats)
    }
}

impl CompleteBarrierTask {
    fn report_complete_event(env: &MetaSrvEnv, duration_sec: f64, command_ctx: &CommandContext) {
        // Record barrier latency in event log.
        use risingwave_pb::meta::event_log;
        let event = event_log::EventBarrierComplete {
            prev_epoch: command_ctx.barrier_info.prev_epoch(),
            cur_epoch: command_ctx.barrier_info.curr_epoch.value().0,
            duration_sec,
            command: command_ctx
                .command
                .as_ref()
                .map(|command| command.to_string())
                .unwrap_or_else(|| "barrier".to_owned()),
            barrier_kind: command_ctx.barrier_info.kind.as_str_name().to_owned(),
        };
        env.event_log_manager_ref()
            .add_event_logs(vec![event_log::Event::BarrierComplete(event)]);
    }
}

pub(super) struct BarrierCompleteOutput {
    #[expect(clippy::type_complexity)]
    /// `database_id` -> (`Some(database_graph_committed_epoch)`, [(`creating_job_id`, `creating_job_committed_epoch`)])
    pub epochs_to_ack: HashMap<DatabaseId, (Option<u64>, Vec<(TableId, u64)>)>,
    pub hummock_version_stats: HummockVersionStats,
}

impl CompletingTask {
    pub(super) fn next_completed_barrier<'a>(
        &'a mut self,
        periodic_barriers: &mut PeriodicBarriers,
        checkpoint_control: &mut CheckpointControl,
        control_stream_manager: &mut ControlStreamManager,
        context: &Arc<impl GlobalBarrierWorkerContext>,
        env: &MetaSrvEnv,
    ) -> impl Future<Output = MetaResult<BarrierCompleteOutput>> + 'a {
        // If there is no completing barrier, try to start completing the earliest barrier if
        // it has been collected.
        if let CompletingTask::None = self {
            if let Some(task) = checkpoint_control
                .next_complete_barrier_task(Some((periodic_barriers, control_stream_manager)))
            {
                {
                    let epochs_to_ack = task.epochs_to_ack();
                    let context = context.clone();
                    let env = env.clone();
                    let join_handle =
                        tokio::spawn(async move { task.complete_barrier(&*context, env).await });
                    *self = CompletingTask::Completing {
                        epochs_to_ack,
                        join_handle,
                    };
                }
            }
        }

        async move {
            if !matches!(self, CompletingTask::Completing { .. }) {
                return pending().await;
            };
            self.next_completed_barrier_inner().await
        }
    }

    pub(super) async fn wait_completing_task(
        &mut self,
    ) -> MetaResult<Option<BarrierCompleteOutput>> {
        match self {
            CompletingTask::None => Ok(None),
            CompletingTask::Completing { .. } => {
                self.next_completed_barrier_inner().await.map(Some)
            }
            CompletingTask::Err(_) => {
                unreachable!("should not be called on previous err")
            }
        }
    }

    async fn next_completed_barrier_inner(&mut self) -> MetaResult<BarrierCompleteOutput> {
        let CompletingTask::Completing { join_handle, .. } = self else {
            unreachable!()
        };

        {
            {
                let join_result: MetaResult<_> = try {
                    join_handle
                        .await
                        .context("failed to join completing command")??
                };
                // It's important to reset the completing_command after await no matter the result is err
                // or not, and otherwise the join handle will be polled again after ready.
                let next_completing_command_status = if let Err(e) = &join_result {
                    CompletingTask::Err(e.clone())
                } else {
                    CompletingTask::None
                };
                let completed_command = replace(self, next_completing_command_status);
                let hummock_version_stats = join_result?;

                must_match!(completed_command, CompletingTask::Completing {
                    epochs_to_ack,
                    ..
                } => {
                    Ok(BarrierCompleteOutput {
                        epochs_to_ack,
                        hummock_version_stats,
                    })
                })
            }
        }
    }
}
