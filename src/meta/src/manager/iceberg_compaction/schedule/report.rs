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

use anyhow::{Context, ensure};
use risingwave_connector::sink::iceberg::IcebergCommitResult;
use risingwave_pb::iceberg_compaction::PkIndexCompactionResult;

use super::*;
use crate::MetaError;
use crate::barrier::{Command, IcebergPkIndexCompactionOverwrite};
use crate::manager::iceberg_pk_index_sink::is_iceberg_pk_index_sink;

enum CommandApplicationError {
    /// Rejected before admission. The command cannot execute later.
    NotStarted(MetaError),
    /// Injection or collection may have happened. Recovery must settle it.
    Unknown(MetaError),
}

#[derive(Debug)]
pub(super) struct PkIndexCompactionApply {
    pub(super) report: IcebergReportTask,
    pub(super) overwrite: IcebergPkIndexCompactionOverwrite,
}

fn decode_pk_index_result(
    result: PkIndexCompactionResult,
) -> anyhow::Result<IcebergPkIndexCompactionOverwrite> {
    let output_result = IcebergCommitResult::try_from(
        result
            .output_files
            .as_ref()
            .context("pk-index compaction result is missing output metadata")?,
    )?;
    ensure!(
        !result.input_file_paths.is_empty(),
        "pk-index compaction result is missing input file paths"
    );
    ensure!(
        result.read_snapshot_id > 0,
        "pk-index compaction result has an invalid read snapshot id"
    );
    Ok(IcebergPkIndexCompactionOverwrite {
        output_result,
        input_file_paths: result.input_file_paths,
        read_snapshot_id: result.read_snapshot_id,
    })
}

impl IcebergCompactionManager {
    pub fn handle_report_task(self: &Arc<Self>, report: IcebergReportTask) {
        if let Some(PkIndexCompactionApply {
            mut report,
            overwrite,
        }) = self.prepare_report_task(report)
        {
            let manager = self.clone();
            tokio::spawn(async move {
                match manager.apply_pk_index_compaction(&report, overwrite).await {
                    Ok(()) => manager.finish_report_task(report, true),
                    Err(CommandApplicationError::NotStarted(error)) => {
                        report.status = IcebergReportTaskStatus::Failed as i32;
                        report.error_message = Some(format!(
                            "failed to apply pk-index compaction: {}",
                            error.as_report()
                        ));
                        manager.finish_report_task(report, true);
                    }
                    Err(CommandApplicationError::Unknown(error)) => {
                        manager.mark_apply_unknown(report, error);
                    }
                }
            });
        }
    }

    pub(super) fn mark_apply_unknown(&self, report: IcebergReportTask, error: MetaError) {
        let mut guard = self.inner.write();
        if let Some(track) = guard.sink_schedules.get_mut(&report.sink_id)
            && let CompactionTrackState::Applying { task_id, attempt } = &track.state
            && *task_id == report.task_id
        {
            track.state = CompactionTrackState::NeedsRecovery {
                task_id: *task_id,
                attempt: attempt.clone(),
            };
            tracing::warn!(sink_id = %report.sink_id, task_id = %report.task_id,
                error = %error.as_report(), "pk-index compaction requires recovery settlement");
            if let Some(waiter) = guard.manual_compaction_waiters.remove(&report.sink_id) {
                let _ = waiter.send(Err(anyhow!(error)
                    .context(
                        "pk-index compaction application outcome is unknown; recovery required",
                    )
                    .into()));
            }
        }
    }

    pub(super) fn prepare_report_task(
        &self,
        mut report: IcebergReportTask,
    ) -> Option<PkIndexCompactionApply> {
        let sink_id = report.sink_id;
        let mut guard = self.inner.write();
        let Some(track) = guard
            .sink_schedules
            .get_mut(&sink_id)
            .filter(|track| track.is_in_flight_task(report.task_id))
        else {
            // In particular, duplicate reports cannot re-apply an Applying task
            // or complete its manual waiter before the barrier has committed.
            tracing::warn!(
                %sink_id,
                task_id = %report.task_id,
                "ignoring iceberg compaction report without a matching in-flight task",
            );
            return None;
        };
        let status = IcebergReportTaskStatus::try_from(report.status)
            .unwrap_or(IcebergReportTaskStatus::Unspecified);
        if matches!(
            status,
            IcebergReportTaskStatus::Success | IcebergReportTaskStatus::Drained
        ) && let Some(result) = report.pk_index_result.take()
        {
            let CompactionTrackState::InFlight { attempt, .. } = &track.state else {
                unreachable!()
            };
            let overwrite = if !attempt.pk_index_coordinated {
                Err(anyhow!(
                    "unexpected pk-index result for an uncoordinated task"
                ))
            } else if status == IcebergReportTaskStatus::Drained
                && attempt.max_file_sequence_number.is_none()
            {
                Err(anyhow!("unbounded compaction task cannot report Drained"))
            } else {
                decode_pk_index_result(result)
            };
            match overwrite {
                Ok(overwrite) => {
                    // A rewrite payload cannot prove the round drained, even
                    // when it was the last group in the planning snapshot.
                    report.status = IcebergReportTaskStatus::Success as i32;
                    let result = PkIndexCompactionApply { report, overwrite };
                    track.state = CompactionTrackState::Applying {
                        task_id: result.report.task_id,
                        attempt: attempt.clone(),
                    };
                    return Some(result);
                }
                Err(error) => {
                    report.status = IcebergReportTaskStatus::Failed as i32;
                    report.error_message = Some(format!(
                        "invalid pk-index compaction result: {}",
                        error.as_report()
                    ));
                }
            }
        }
        // A no-plan task has no rewrite payload, including a manual Full task
        // reporting Success. There is nothing to apply in that case.
        drop(guard);
        self.finish_report_task(report, false);
        None
    }

    async fn apply_pk_index_compaction(
        &self,
        report: &IcebergReportTask,
        overwrite: IcebergPkIndexCompactionOverwrite,
    ) -> Result<(), CommandApplicationError> {
        let sink = self
            .prepare_apply_sink(report)
            .await
            .map_err(|error| CommandApplicationError::NotStarted(error.into()))?;
        let completion = {
            let guard = self.inner.read();
            if !guard
                .sink_schedules
                .get(&report.sink_id)
                .is_some_and(|track| {
                    matches!(&track.state,
                    CompactionTrackState::Applying { task_id, .. } if *task_id == report.task_id)
                })
            {
                return Err(CommandApplicationError::NotStarted(
                    anyhow!("pk-index compaction was isolated before command submission").into(),
                ));
            }
            self.barrier_scheduler
                .schedule_command(
                    sink.database_id,
                    Command::ApplyIcebergPkIndexCompaction {
                        sink_id: report.sink_id,
                        task_id: report.task_id,
                        overwrite,
                    },
                )
                .map_err(CommandApplicationError::NotStarted)?
        };
        // Only synchronous rejection proves the command was never admitted.
        completion.await.map_err(CommandApplicationError::Unknown)
    }

    async fn prepare_apply_sink(&self, report: &IcebergReportTask) -> anyhow::Result<SinkCatalog> {
        let sink_id = report.sink_id;
        let sink = self
            .metadata_manager
            .catalog_controller
            .get_sink_by_id(sink_id)
            .await?
            .context("pk-index compaction sink no longer exists")?;
        let sink = SinkCatalog::from(sink);
        ensure!(
            is_iceberg_pk_index_sink(&sink.properties),
            "compaction sink {} is not a pk-index sink",
            sink_id
        );
        {
            let mut guard = self.inner.write();
            let track = guard.sink_schedules.get_mut(&sink_id);
            match track.map(|track| &mut track.state) {
                Some(CompactionTrackState::Applying { task_id, .. })
                    if *task_id == report.task_id => {}
                _ => anyhow::bail!(
                    "pk-index compaction task {} was cleared before application",
                    report.task_id
                ),
            }
        }
        Ok(sink)
    }
}
