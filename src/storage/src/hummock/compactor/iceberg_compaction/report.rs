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

use std::collections::VecDeque;
use std::time::SystemTime;

use risingwave_pb::iceberg_compaction::{
    SubscribeIcebergCompactionEventRequest, subscribe_iceberg_compaction_event_request,
};
use thiserror_ext::AsReport;
use tokio::sync::mpsc;

use super::TaskKey;

#[derive(Debug)]
pub(crate) struct IcebergPlanCompletion {
    pub(crate) task_key: TaskKey,
    pub(crate) error_message: Option<String>,
}

pub(crate) type IcebergTaskReport = subscribe_iceberg_compaction_event_request::ReportTask;

pub(crate) enum ReportSendResult {
    Sent,
    RestartStream,
}

pub(crate) struct IcebergTaskTracker {
    sink_id: u32,
    remaining_plans: usize,
    successful_plans: usize,
    first_error: Option<String>,
}

impl IcebergTaskTracker {
    pub(crate) fn new(sink_id: u32, remaining_plans: usize) -> Self {
        Self {
            sink_id,
            remaining_plans,
            successful_plans: 0,
            first_error: None,
        }
    }

    pub(crate) fn record_completion(&mut self, error_message: Option<String>) {
        debug_assert!(self.remaining_plans > 0);
        self.remaining_plans -= 1;
        if let Some(error_message) = error_message {
            if self.first_error.is_none() {
                self.first_error = Some(error_message);
            }
        } else {
            self.successful_plans += 1;
        }
    }

    pub(crate) fn is_finished(&self) -> bool {
        self.remaining_plans == 0
    }

    pub(crate) fn into_report(self, task_id: u64) -> IcebergTaskReport {
        let error_message = if self.successful_plans > 0 {
            None
        } else {
            Some(
                self.first_error
                    .unwrap_or_else(|| "All admitted iceberg compaction plans failed".to_owned()),
            )
        };
        make_iceberg_task_report(task_id, self.sink_id, error_message)
    }
}

pub(crate) fn build_iceberg_task_report(
    task_id: u64,
    sink_id: u32,
    error_message: Option<String>,
) -> IcebergTaskReport {
    make_iceberg_task_report(task_id, sink_id, error_message)
}

fn make_iceberg_task_report(
    task_id: u64,
    sink_id: u32,
    error_message: Option<String>,
) -> IcebergTaskReport {
    subscribe_iceberg_compaction_event_request::ReportTask {
        task_id,
        sink_id,
        status: if error_message.is_some() {
            subscribe_iceberg_compaction_event_request::report_task::Status::Failed as i32
        } else {
            subscribe_iceberg_compaction_event_request::report_task::Status::Success as i32
        },
        error_message,
    }
}

pub(crate) fn send_iceberg_task_report(
    request_sender: &mpsc::UnboundedSender<SubscribeIcebergCompactionEventRequest>,
    report_event: IcebergTaskReport,
) -> Result<(), IcebergTaskReport> {
    if let Err(e) = request_sender.send(SubscribeIcebergCompactionEventRequest {
        event: Some(
            subscribe_iceberg_compaction_event_request::Event::ReportTask(report_event.clone()),
        ),
        create_at: SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("Clock may have gone backwards")
            .as_millis() as u64,
    }) {
        tracing::warn!(
            error = %e.as_report(),
            task_id = report_event.task_id,
            sink_id = report_event.sink_id,
            "Failed to report iceberg compaction task result - will retry on stream restart"
        );
        return Err(report_event);
    }

    Ok(())
}

pub(crate) fn send_or_buffer_iceberg_task_report(
    request_sender: &mpsc::UnboundedSender<SubscribeIcebergCompactionEventRequest>,
    pending_task_reports: &mut VecDeque<IcebergTaskReport>,
    report: IcebergTaskReport,
) -> ReportSendResult {
    if let Err(report) = send_iceberg_task_report(request_sender, report) {
        pending_task_reports.push_back(report);
        return ReportSendResult::RestartStream;
    }
    ReportSendResult::Sent
}

pub(crate) fn flush_pending_iceberg_task_reports(
    request_sender: &mpsc::UnboundedSender<SubscribeIcebergCompactionEventRequest>,
    pending_task_reports: &mut VecDeque<IcebergTaskReport>,
) -> ReportSendResult {
    while let Some(report_event) = pending_task_reports.pop_front() {
        if let Err(report_event) = send_iceberg_task_report(request_sender, report_event) {
            pending_task_reports.push_front(report_event);
            return ReportSendResult::RestartStream;
        }
    }
    ReportSendResult::Sent
}
