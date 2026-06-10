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

use std::collections::VecDeque;
use std::time::SystemTime;

use iceberg::spec::SerializedDataFile;
use risingwave_pb::iceberg_compaction::{
    SubscribeIcebergCompactionEventRequest, subscribe_iceberg_compaction_event_request,
};
use thiserror_ext::AsReport;
use tokio::sync::mpsc;

use super::TaskKey;

/// Per-plan result of a V3 coordinated compaction run (rewrite without commit).
///
/// Produced by the compactor when the dispatched task has `v3_coordinated == true`. The actual
/// iceberg commit is performed later by the meta-side V3 sink manager, so the compactor only
/// surfaces the rewrite output, the input file paths, and the snapshot it read from.
#[derive(Clone)]
pub(crate) struct V3CompactionResult {
    /// Newly written data files produced by the rewrite.
    pub(crate) output_files: Vec<SerializedDataFile>,
    /// Paths of all input files (data + delete) consumed by the rewrite, taken directly from the
    /// compaction plan's `FileGroup`. No manifest walk required.
    pub(crate) input_file_paths: Vec<String>,
    /// Snapshot the rewrite plan read from.
    pub(crate) read_snapshot_id: i64,
}

// Manual Debug: `SerializedDataFile` does not implement `Debug`.
impl std::fmt::Debug for V3CompactionResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("V3CompactionResult")
            .field("output_file_count", &self.output_files.len())
            .field("input_file_count", &self.input_file_paths.len())
            .field("read_snapshot_id", &self.read_snapshot_id)
            .finish()
    }
}

#[derive(Debug)]
pub(crate) struct IcebergPlanCompletion {
    pub(crate) task_key: TaskKey,
    pub(crate) error_message: Option<String>,
    /// Present only for V3 coordinated plans that completed successfully.
    pub(crate) v3_result: Option<V3CompactionResult>,
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
    /// V3 coordinated rewrite results, aggregated across all plans of the task. Empty for
    /// non-V3 tasks.
    v3_results: Vec<V3CompactionResult>,
}

impl IcebergTaskTracker {
    pub(crate) fn new(sink_id: u32, remaining_plans: usize) -> Self {
        Self {
            sink_id,
            remaining_plans,
            successful_plans: 0,
            first_error: None,
            v3_results: Vec::new(),
        }
    }

    pub(crate) fn record_completion(
        &mut self,
        error_message: Option<String>,
        v3_result: Option<V3CompactionResult>,
    ) {
        debug_assert!(self.remaining_plans > 0);
        self.remaining_plans -= 1;
        if let Some(msg) = error_message {
            if self.first_error.is_none() {
                self.first_error = Some(msg);
            }
        } else {
            self.successful_plans += 1;
            if let Some(v3_result) = v3_result {
                self.v3_results.push(v3_result);
            }
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
        let mut report = build_iceberg_task_report(task_id, self.sink_id, error_message);
        populate_v3_report_fields(&mut report, self.v3_results);
        report
    }
}

/// Flatten the per-plan V3 results into the `ReportTask` payload fields.
fn populate_v3_report_fields(report: &mut IcebergTaskReport, v3_results: Vec<V3CompactionResult>) {
    if v3_results.is_empty() {
        return;
    }

    let read_snapshot_id = v3_results[0].read_snapshot_id;
    // The planner builds all plans of a task from one branch snapshot, so they must agree.
    // Guard against a future planner change silently reporting the wrong snapshot.
    debug_assert!(
        v3_results
            .iter()
            .all(|r| r.read_snapshot_id == read_snapshot_id),
        "V3 compaction plans for one task must share a single read snapshot id"
    );
    let mut output_files: Vec<SerializedDataFile> = Vec::new();
    let mut input_file_paths: Vec<String> = Vec::new();
    for v3_result in v3_results {
        output_files.extend(v3_result.output_files);
        input_file_paths.extend(v3_result.input_file_paths);
    }

    match serde_json::to_vec(&output_files) {
        Ok(bytes) => report.v3_output_files = Some(bytes),
        Err(e) => {
            tracing::warn!(
                error = %e.as_report(),
                task_id = report.task_id,
                sink_id = report.sink_id,
                "Failed to serialize V3 output files; skipping V3 payload in report"
            );
            return;
        }
    }
    // Input files are serialized as a JSON array of path strings.
    match serde_json::to_vec(&input_file_paths) {
        Ok(bytes) => report.v3_input_files = Some(bytes),
        Err(e) => {
            tracing::warn!(
                error = %e.as_report(),
                task_id = report.task_id,
                sink_id = report.sink_id,
                "Failed to serialize V3 input file paths; skipping V3 payload in report"
            );
            report.v3_output_files = None;
            return;
        }
    }
    report.v3_read_snapshot_id = Some(read_snapshot_id);
}

pub(crate) fn build_iceberg_task_report(
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
        ..Default::default()
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

#[cfg(test)]
mod tests {
    use risingwave_pb::iceberg_compaction::subscribe_iceberg_compaction_event_request;

    use super::*;

    #[test]
    fn test_send_iceberg_task_report_returns_payload_on_send_failure() {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        drop(rx);

        let report = build_iceberg_task_report(7, 9, Some("send failure".to_owned()));
        let failed_report = send_iceberg_task_report(&tx, report.clone()).unwrap_err();

        assert_eq!(failed_report.task_id, report.task_id);
        assert_eq!(failed_report.sink_id, report.sink_id);
        assert_eq!(failed_report.error_message, report.error_message);
    }

    #[test]
    fn test_build_iceberg_task_result_partial_enqueue_is_success_if_admitted_plan_succeeds() {
        let mut tracker = IcebergTaskTracker::new(9, 1);
        tracker.record_completion(None, None);

        let report = tracker.into_report(7);

        assert_eq!(
            report.status,
            subscribe_iceberg_compaction_event_request::report_task::Status::Success as i32
        );
        assert!(report.error_message.is_none());
    }

    #[test]
    fn test_into_report_populates_v3_fields_when_v3_result_present() {
        let mut tracker = IcebergTaskTracker::new(9, 2);
        tracker.record_completion(
            None,
            Some(V3CompactionResult {
                output_files: vec![],
                input_file_paths: vec![],
                read_snapshot_id: 42,
            }),
        );
        tracker.record_completion(
            None,
            Some(V3CompactionResult {
                output_files: vec![],
                input_file_paths: vec![],
                read_snapshot_id: 42,
            }),
        );

        let report = tracker.into_report(7);

        // Empty file vectors still serialize to a JSON array, and the snapshot id is surfaced.
        assert_eq!(report.v3_output_files.as_deref(), Some(b"[]".as_slice()));
        assert_eq!(report.v3_input_files.as_deref(), Some(b"[]".as_slice()));
        assert_eq!(report.v3_read_snapshot_id, Some(42));
    }

    #[test]
    fn test_into_report_leaves_v3_fields_none_for_non_v3_task() {
        let mut tracker = IcebergTaskTracker::new(9, 1);
        tracker.record_completion(None, None);

        let report = tracker.into_report(7);

        assert!(report.v3_output_files.is_none());
        assert!(report.v3_input_files.is_none());
        assert!(report.v3_read_snapshot_id.is_none());
    }

    #[test]
    fn test_build_iceberg_task_result_fails_if_all_admitted_plans_fail() {
        let mut tracker = IcebergTaskTracker::new(9, 2);
        tracker.record_completion(Some("first failure".to_owned()), None);
        tracker.record_completion(Some("second failure".to_owned()), None);

        let report = tracker.into_report(7);

        assert_eq!(
            report.status,
            subscribe_iceberg_compaction_event_request::report_task::Status::Failed as i32
        );
        assert_eq!(report.error_message.as_deref(), Some("first failure"));
    }
}
