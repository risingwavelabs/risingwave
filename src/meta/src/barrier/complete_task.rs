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

use std::collections::{BTreeMap, HashMap, HashSet};
use std::future::{pending, Future};
use std::mem::replace;
use std::sync::Arc;

use anyhow::Context;
use futures::future::try_join_all;
use prometheus::HistogramTimer;
use risingwave_common::catalog::{DatabaseId, TableId};
use risingwave_common::must_match;
use risingwave_meta_model::WorkerId;
use risingwave_pb::hummock::HummockVersionStats;
use risingwave_pb::stream_service::BarrierCompleteResponse;
use tokio::task::JoinHandle;

use crate::barrier::command::CommandContext;
use crate::barrier::context::GlobalBarrierWorkerContext;
use crate::barrier::notifier::Notifier;
use crate::barrier::progress::TrackingJob;
use crate::barrier::rpc::ControlStreamManager;
use crate::barrier::utils::collect_resp_info;
use crate::hummock::{CommitEpochInfo, NewTableFragmentInfo};
use crate::manager::MetaSrvEnv;
use crate::rpc::metrics::GLOBAL_META_METRICS;
use crate::{MetaError, MetaResult};

pub(super) enum CommittingTask {
    None,
    Committing {
        #[expect(clippy::type_complexity)]
        /// `database_id` -> (`Some(database_graph_committed_epoch)`, vec(`creating_job_id`, `creating_job_committed_epoch`, `is_finished`)])
        epochs_to_ack: HashMap<DatabaseId, (Option<u64>, Vec<(TableId, u64, bool)>)>,

        // The join handle of a spawned task that completes the barrier.
        // The return value indicate whether there is some create streaming job command
        // that has finished but not checkpointed. If there is any, we will force checkpoint on the next barrier
        join_handle: JoinHandle<MetaResult<HummockVersionStats>>,
    },
    #[expect(dead_code)]
    Err(MetaError),
}

#[derive(Debug)]
pub(super) struct DatabaseCompleteBarrierTask {
    pub(super) command: CommandContext,
    pub(super) enqueue_time: HistogramTimer,
    pub(super) backfill_pinned_upstream_log_epoch: HashMap<TableId, (u64, HashSet<TableId>)>,
    pub(super) workers: HashSet<WorkerId>,
}

#[derive(Debug)]
pub(super) struct CreatingJobCompleteBarrierTask {
    pub(super) job_id: TableId,
    pub(super) epoch: u64,
    pub(super) is_first_commit: bool,
    pub(super) tables_to_commit: HashSet<TableId>,
    pub(super) workers: HashSet<WorkerId>,
    pub(super) is_finished: bool,
}

#[derive(Default)]
pub(super) struct CompleteBarrierTask {
    pub(super) finished_jobs: Vec<TrackingJob>,
    pub(super) notifiers: Vec<Notifier>,
    pub(super) tasks: HashMap<
        DatabaseId,
        (
            Option<DatabaseCompleteBarrierTask>,
            Vec<CreatingJobCompleteBarrierTask>,
        ),
    >,
}

impl CompleteBarrierTask {
    #[expect(clippy::type_complexity)]
    pub(super) fn epochs_to_ack(
        &self,
    ) -> HashMap<DatabaseId, (Option<u64>, Vec<(TableId, u64, bool)>)> {
        self.tasks
            .iter()
            .map(|(database_id, (command_context, creating_job_epochs))| {
                (
                    *database_id,
                    (
                        command_context
                            .as_ref()
                            .map(|task| task.command.barrier_info.prev_epoch.value().0),
                        creating_job_epochs
                            .iter()
                            .map(|task| (task.job_id, task.epoch, task.is_finished))
                            .collect(),
                    ),
                )
            })
            .collect()
    }

    fn graph_to_complete(
        &self,
    ) -> impl Iterator<Item = (DatabaseId, Option<TableId>, &'_ HashSet<WorkerId>, u64)> + '_ {
        self.tasks
            .iter()
            .flat_map(|(database_id, (database, creating_jobs))| {
                database
                    .iter()
                    .map(|database| {
                        (
                            *database_id,
                            None,
                            &database.workers,
                            database.command.barrier_info.prev_epoch(),
                        )
                    })
                    .chain(
                        creating_jobs.iter().map(|task| {
                            (*database_id, Some(task.job_id), &task.workers, task.epoch)
                        }),
                    )
            })
    }
}

impl CommittingTask {
    pub(super) async fn commit_barrier(
        task: CompleteBarrierTask,
        commit_info: CommitEpochInfo,
        context: &impl GlobalBarrierWorkerContext,
        env: MetaSrvEnv,
    ) -> MetaResult<HummockVersionStats> {
        let result: MetaResult<HummockVersionStats> = try {
            let wait_commit_timer = GLOBAL_META_METRICS
                .barrier_wait_commit_latency
                .start_timer();
            let version_stats = context.commit_epoch(commit_info).await?;
            for command_ctx in task
                .tasks
                .values()
                .flat_map(|(command, _)| command.as_ref().map(|task| &task.command))
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
                    for notifier in task.notifiers {
                        notifier.notify_collection_failed(e.clone());
                    }
                    return Err(e);
                }
            };
            task.notifiers.into_iter().for_each(|notifier| {
                notifier.notify_collected();
            });
            try_join_all(
                task.finished_jobs
                    .into_iter()
                    .map(|finished_job| context.finish_creating_job(finished_job)),
            )
            .await?;
            for task in task.tasks.into_values().flat_map(|(task, _)| task) {
                let duration_sec = task.enqueue_time.stop_and_record();
                Self::report_complete_event(&env, duration_sec, &task.command);
                GLOBAL_META_METRICS
                    .last_committed_barrier_time
                    .set(task.command.barrier_info.curr_epoch.value().as_unix_secs() as i64);
            }
            version_stats
        };

        Ok(version_stats)
    }

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
                .unwrap_or_else(|| "barrier".to_string()),
            barrier_kind: command_ctx.barrier_info.kind.as_str_name().to_string(),
        };
        env.event_log_manager_ref()
            .add_event_logs(vec![event_log::Event::BarrierComplete(event)]);
    }
}

pub(super) struct BarrierCommitOutput {
    #[expect(clippy::type_complexity)]
    /// `database_id` -> (`Some(database_graph_committed_epoch)`, vec(`creating_job_id`, `creating_job_committed_epoch`, `is_finished`)])
    pub epochs_to_ack: HashMap<DatabaseId, (Option<u64>, Vec<(TableId, u64, bool)>)>,
    pub hummock_version_stats: HummockVersionStats,
}

pub(super) struct CompletingTask {
    node_to_collect: HashSet<WorkerId>,
    collected_resps: HashMap<WorkerId, BarrierCompleteResponse>,
    task: CompleteBarrierTask,
}

impl CompletingTask {
    fn new(
        task_id: u64,
        task: CompleteBarrierTask,
        control_stream_manager: &mut ControlStreamManager,
    ) -> MetaResult<Self> {
        let node_to_collect =
            control_stream_manager.complete_barrier(task_id, task.graph_to_complete())?;
        Ok(Self {
            node_to_collect,
            collected_resps: HashMap::new(),
            task,
        })
    }

    fn is_completed(&self) -> bool {
        self.node_to_collect.is_empty()
    }

    fn into_commit_info(self) -> (CommitEpochInfo, CompleteBarrierTask) {
        assert!(self.node_to_collect.is_empty());
        let (mut commit_info, old_value_ssts) = collect_resp_info(self.collected_resps);
        for (database_task, creating_jobs) in self.task.tasks.values() {
            if let Some(task) = database_task {
                task.command.collect_extra_commit_epoch_info(
                    &commit_info.sstables,
                    &old_value_ssts,
                    task.backfill_pinned_upstream_log_epoch.clone(),
                    &mut commit_info.tables_to_commit,
                    &mut commit_info.new_table_fragment_infos,
                    &mut commit_info.change_log_delta,
                )
            }
            for task in creating_jobs {
                task.tables_to_commit.iter().for_each(|table_id| {
                    commit_info
                        .tables_to_commit
                        .try_insert(*table_id, task.epoch)
                        .expect("non duplicate");
                });
                if task.is_first_commit {
                    commit_info
                        .new_table_fragment_infos
                        .push(NewTableFragmentInfo {
                            table_ids: task.tables_to_commit.clone(),
                        });
                };
            }
        }
        (commit_info, self.task)
    }
}

pub(super) struct CompletingTasks {
    next_task_id: u64,
    tasks: BTreeMap<u64, CompletingTask>,
}

impl CompletingTasks {
    pub(super) fn new() -> Self {
        Self {
            next_task_id: 0,
            tasks: Default::default(),
        }
    }

    pub(super) fn push(
        &mut self,
        task: CompleteBarrierTask,
        control_stream_manager: &mut ControlStreamManager,
    ) -> MetaResult<()> {
        let task_id = self.next_task_id;
        self.next_task_id += 1;
        let task = CompletingTask::new(task_id, task, control_stream_manager)?;
        self.tasks.insert(task_id, task);
        Ok(())
    }

    pub(super) fn next_completed_task(&mut self) -> Option<(CommitEpochInfo, CompleteBarrierTask)> {
        if let Some((_, task)) = self.tasks.first_key_value()
            && task.is_completed()
        {
            let (_, task) = self.tasks.pop_first().expect("non-empty");
            Some(task.into_commit_info())
        } else {
            None
        }
    }

    pub(super) fn on_barrier_complete_resp(
        &mut self,
        worker_id: WorkerId,
        resp: BarrierCompleteResponse,
    ) {
        let task = self.tasks.get_mut(&resp.task_id).expect("should exist");
        assert!(task.node_to_collect.remove(&worker_id));
        task.collected_resps
            .try_insert(worker_id, resp)
            .expect("non-duplicate");
    }

    pub(super) fn is_failed_at_worker_err(&self, worker_id: WorkerId) -> bool {
        self.tasks
            .values()
            .any(|task| task.node_to_collect.contains(&worker_id))
    }
}

impl CommittingTask {
    pub(super) fn next_committed_barrier<'a>(
        &'a mut self,
        completing_tasks: &mut CompletingTasks,
        context: &Arc<impl GlobalBarrierWorkerContext>,
        env: &MetaSrvEnv,
    ) -> impl Future<Output = MetaResult<BarrierCommitOutput>> + 'a {
        // If there is no completing barrier, try to start completing the earliest barrier if
        // it has been collected.
        if let CommittingTask::None = &self {
            if let Some((commit_info, task)) = completing_tasks.next_completed_task() {
                let epochs_to_ack = task.epochs_to_ack();
                let context = context.clone();
                let env = env.clone();
                let join_handle = tokio::spawn(async move {
                    CommittingTask::commit_barrier(task, commit_info, &*context, env).await
                });
                *self = CommittingTask::Committing {
                    epochs_to_ack,
                    join_handle,
                };
            }
        }

        self.next_committed_barrier_inner()
    }

    async fn next_committed_barrier_inner(&mut self) -> MetaResult<BarrierCommitOutput> {
        let CommittingTask::Committing { join_handle, .. } = self else {
            return pending().await;
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
                let next_committing_task_status = if let Err(e) = &join_result {
                    CommittingTask::Err(e.clone())
                } else {
                    CommittingTask::None
                };
                let committed_task = replace(self, next_committing_task_status);
                let hummock_version_stats = join_result?;

                must_match!(committed_task, CommittingTask::Committing {
                    epochs_to_ack,
                    ..
                } => {
                    Ok(BarrierCommitOutput {
                        epochs_to_ack,
                        hummock_version_stats,
                    })
                })
            }
        }
    }
}
