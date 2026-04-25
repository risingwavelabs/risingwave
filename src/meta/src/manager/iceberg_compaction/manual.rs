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

use anyhow::anyhow;
use risingwave_connector::sink::catalog::SinkId;
use risingwave_pb::iceberg_compaction::IcebergCompactionTask;
use risingwave_pb::iceberg_compaction::iceberg_compaction_task::TaskType;
use risingwave_pb::iceberg_compaction::subscribe_iceberg_compaction_event_request::ReportTask as IcebergReportTask;
use risingwave_pb::iceberg_compaction::subscribe_iceberg_compaction_event_request::report_task::Status as IcebergReportTaskStatus;
use tokio::sync::oneshot;

use super::*;
use crate::hummock::sequence::next_compaction_task_id;

impl IcebergCompactionManager {
    fn register_manual_task_waiter(
        &self,
        task_id: u64,
    ) -> MetaResult<oneshot::Receiver<MetaResult<()>>> {
        let (tx, rx) = oneshot::channel();
        match self.inner.write().manual_task_waiters.entry(task_id) {
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(tx);
                Ok(rx)
            }
            std::collections::hash_map::Entry::Occupied(_) => Err(anyhow!(
                "manual iceberg compaction waiter already exists for task_id={}",
                task_id
            )
            .into()),
        }
    }

    fn clear_manual_task_waiter(&self, task_id: u64) {
        self.inner.write().manual_task_waiters.remove(&task_id);
    }

    pub(super) fn complete_manual_task_if_any(&self, report: &IcebergReportTask) -> bool {
        let Some(waiter) = self
            .inner
            .write()
            .manual_task_waiters
            .remove(&report.task_id)
        else {
            return false;
        };

        let status = IcebergReportTaskStatus::try_from(report.status)
            .unwrap_or(IcebergReportTaskStatus::Unspecified);
        let result = match status {
            IcebergReportTaskStatus::Success => Ok(()),
            IcebergReportTaskStatus::Failed | IcebergReportTaskStatus::Unspecified => {
                let message = report
                    .error_message
                    .clone()
                    .unwrap_or_else(|| "manual iceberg compaction failed".to_owned());
                Err(anyhow!(
                    "Manual iceberg compaction task {} for sink {} failed: {}",
                    report.task_id,
                    report.sink_id,
                    message
                )
                .into())
            }
        };

        let _ = waiter.send(result);
        true
    }

    pub async fn trigger_manual_compaction(&self, sink_id: SinkId) -> MetaResult<u64> {
        use risingwave_pb::iceberg_compaction::subscribe_iceberg_compaction_event_response::Event as IcebergResponseEvent;

        let compactor = self
            .iceberg_compactor_manager
            .next_compactor()
            .ok_or_else(|| anyhow!("No iceberg compactor available"))?;

        let task_id = next_compaction_task_id(&self.env).await?;
        let sink_param = self.get_sink_param(sink_id).await?;
        let waiter = self.register_manual_task_waiter(task_id)?;

        if let Err(error) =
            compactor.send_event(IcebergResponseEvent::CompactTask(IcebergCompactionTask {
                task_id,
                sink_id: sink_id.as_raw_id(),
                props: sink_param.properties,
                task_type: TaskType::Full as i32,
            }))
        {
            self.clear_manual_task_waiter(task_id);
            return Err(error);
        }

        tracing::info!(
            "Manual compaction triggered for sink {} with task ID {}, waiting for completion...",
            sink_id,
            task_id
        );

        self.wait_for_compaction_completion(&sink_id, task_id, waiter)
            .await?;

        Ok(task_id)
    }

    async fn wait_for_compaction_completion(
        &self,
        sink_id: &SinkId,
        task_id: u64,
        waiter: oneshot::Receiver<MetaResult<()>>,
    ) -> MetaResult<()> {
        let _cleanup_guard =
            scopeguard::guard(task_id, |task_id| self.clear_manual_task_waiter(task_id));

        match tokio::time::timeout(self.report_timeout(), waiter).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(anyhow!(
                "Manual iceberg compaction waiter dropped unexpectedly for sink {} (task_id={})",
                sink_id,
                task_id
            )
            .into()),
            Err(_) => Err(anyhow!(
                "Iceberg compaction did not report completion within {} seconds for sink {} (task_id={})",
                self.report_timeout().as_secs(),
                sink_id,
                task_id
            )
            .into()),
        }
    }
}
