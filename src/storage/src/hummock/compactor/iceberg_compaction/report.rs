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

use bytes::Bytes;
use iceberg::spec::{DataFile, SerializedDataFile};
use iceberg::table::Table;
use iceberg_compaction_core::executor::RowProvenance;
use risingwave_pb::iceberg_compaction::{
    SubscribeIcebergCompactionEventRequest, subscribe_iceberg_compaction_event_request,
};
use serde::Serialize;
use thiserror_ext::AsReport;
use tokio::sync::mpsc;

use super::TaskKey;
use crate::hummock::{HummockError, HummockResult};

/// RW-local mirror of `iceberg_compaction_core::executor::RowProvenance`.
///
/// The fork's `RowProvenance` intentionally does not derive `Serialize` (it's an internal type of
/// a third-party crate), so each row is mapped into this local struct before being JSON-encoded.
/// Field names and types must stay in sync with the fork's `RowProvenance` and with meta's
/// decoder (B3).
#[derive(Debug, Serialize)]
struct RowProvenanceEntry {
    input_file: String,
    input_pos: u64,
    output_file: String,
    output_pos: u64,
}

impl From<&RowProvenance> for RowProvenanceEntry {
    fn from(row: &RowProvenance) -> Self {
        Self {
            input_file: row.input_file.clone(),
            input_pos: row.input_pos,
            output_file: row.output_file.clone(),
            output_pos: row.output_pos,
        }
    }
}

/// Spills a single plan's per-row provenance mapping to the iceberg table's storage via `FileIO`,
/// so that meta's pk-index sink coordinator can read it back later through the same table's
/// `FileIO` (B3).
///
/// Encoded as newline-delimited JSON (one [`RowProvenanceEntry`] per line) under a
/// `_rw_pk_index_compaction/` prefix beneath the table's base location. Returns the absolute path
/// written.
pub(crate) async fn spill_pk_index_row_provenance(
    table: &Table,
    task_id: u64,
    plan_index: usize,
    row_provenance: &[RowProvenance],
) -> HummockResult<String> {
    let mut buf = Vec::new();
    for row in row_provenance {
        serde_json::to_writer(&mut buf, &RowProvenanceEntry::from(row)).map_err(|e| {
            HummockError::compaction_executor(format!(
                "failed to serialize pk-index row provenance entry for task {} plan {}: {}",
                task_id,
                plan_index,
                e.as_report()
            ))
        })?;
        buf.push(b'\n');
    }

    let path = format!(
        "{}/_rw_pk_index_compaction/{}-{}.jsonl",
        table.metadata().location().trim_end_matches('/'),
        task_id,
        plan_index
    );

    let output_file = table
        .file_io()
        .new_output(&path)
        .map_err(|e| HummockError::compaction_executor(e.as_report()))?;
    output_file
        .write(Bytes::from(buf))
        .await
        .map_err(|e| HummockError::compaction_executor(e.as_report()))?;

    Ok(path)
}

/// Per-plan result of a pk-index coordinated compaction run (rewrite without commit).
///
/// Produced by the compactor when the dispatched task has `pk_index_coordinated == true`. The
/// actual iceberg commit is performed later by meta's iceberg pk-index sink coordinator, so the
/// compactor only surfaces the rewrite output, the input file paths, and the snapshot it read
/// from.
#[derive(Clone)]
pub(crate) struct PkIndexCompactionResult {
    /// Newly written data files produced by the rewrite.
    pub(crate) output_files: Vec<SerializedDataFile>,
    /// Paths of all input files (data + delete) consumed by the rewrite, taken directly from the
    /// compaction plan's `FileGroup`. No manifest walk required.
    pub(crate) input_file_paths: Vec<String>,
    /// Snapshot the rewrite plan read from.
    pub(crate) read_snapshot_id: i64,
    /// Absolute path (written via the table's `FileIO`) of this plan's spilled per-row provenance
    /// mapping file. See [`spill_pk_index_row_provenance`].
    pub(crate) pk_index_mapping_path: String,
}

// Manual Debug: `SerializedDataFile` does not implement `Debug`.
impl std::fmt::Debug for PkIndexCompactionResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PkIndexCompactionResult")
            .field("output_file_count", &self.output_files.len())
            .field("input_file_count", &self.input_file_paths.len())
            .field("read_snapshot_id", &self.read_snapshot_id)
            .field("pk_index_mapping_path", &self.pk_index_mapping_path)
            .finish()
    }
}

/// Builds the pk-index coordinated report payload from a no-commit rewrite.
///
/// - Output files come from `data_files` (the rewrite's output, taken from `CompactionResult`)
///   and are converted to [`SerializedDataFile`] for JSON serialization, using `table`'s partition
///   type and format version.
/// - `input_file_paths` and `read_snapshot_id` must be captured by the caller *before* the
///   compaction plan is consumed by `compact_with_plan` (the plan's `FileGroup` already carries
///   each input file's path via `FileScanTask::data_file_path` — no manifest walk needed).
/// - `pk_index_mapping_path` is the path returned by [`spill_pk_index_row_provenance`], already
///   spilled by the caller before this is called.
pub(crate) fn build_pk_index_compaction_result(
    table: &Table,
    data_files: Vec<DataFile>,
    input_file_paths: Vec<String>,
    read_snapshot_id: i64,
    pk_index_mapping_path: String,
) -> HummockResult<PkIndexCompactionResult> {
    let partition_type = table.metadata().default_partition_type();
    let format_version = table.metadata().format_version();

    let output_files = data_files
        .into_iter()
        .map(|data_file| {
            SerializedDataFile::try_from(data_file, partition_type, format_version)
                .map_err(|e| HummockError::compaction_executor(e.as_report()))
        })
        .collect::<HummockResult<Vec<_>>>()?;

    Ok(PkIndexCompactionResult {
        output_files,
        input_file_paths,
        read_snapshot_id,
        pk_index_mapping_path,
    })
}

#[derive(Debug)]
pub(crate) struct IcebergPlanCompletion {
    pub(crate) task_key: TaskKey,
    pub(crate) error_message: Option<String>,
    /// Present only for pk-index coordinated plans that completed successfully.
    pub(crate) pk_index_result: Option<PkIndexCompactionResult>,
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
    /// Pk-index coordinated rewrite results, aggregated across all plans of the task. Empty for
    /// non-coordinated tasks.
    pk_index_results: Vec<PkIndexCompactionResult>,
}

impl IcebergTaskTracker {
    pub(crate) fn new(sink_id: u32, remaining_plans: usize) -> Self {
        Self {
            sink_id,
            remaining_plans,
            successful_plans: 0,
            first_error: None,
            pk_index_results: Vec::new(),
        }
    }

    pub(crate) fn record_completion(
        &mut self,
        error_message: Option<String>,
        pk_index_result: Option<PkIndexCompactionResult>,
    ) {
        debug_assert!(self.remaining_plans > 0);
        self.remaining_plans -= 1;
        if let Some(error_message) = error_message {
            if self.first_error.is_none() {
                self.first_error = Some(error_message);
            }
        } else {
            self.successful_plans += 1;
            if let Some(pk_index_result) = pk_index_result {
                self.pk_index_results.push(pk_index_result);
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
        populate_pk_index_report_fields(&mut report, self.pk_index_results);
        report
    }
}

/// Flattens the per-plan pk-index coordinated results into the `ReportTask` payload fields.
fn populate_pk_index_report_fields(
    report: &mut IcebergTaskReport,
    pk_index_results: Vec<PkIndexCompactionResult>,
) {
    if pk_index_results.is_empty() {
        return;
    }

    let read_snapshot_id = pk_index_results[0].read_snapshot_id;
    // The planner builds all plans of a task from one branch snapshot, so they must agree.
    // Guard against a future planner change silently reporting the wrong snapshot.
    debug_assert!(
        pk_index_results
            .iter()
            .all(|r| r.read_snapshot_id == read_snapshot_id),
        "pk-index coordinated compaction plans for one task must share a single read snapshot id"
    );

    let mut output_files: Vec<SerializedDataFile> = Vec::new();
    let mut input_file_paths: Vec<String> = Vec::new();
    let mut mapping_paths: Vec<String> = Vec::new();
    for pk_index_result in pk_index_results {
        output_files.extend(pk_index_result.output_files);
        input_file_paths.extend(pk_index_result.input_file_paths);
        mapping_paths.push(pk_index_result.pk_index_mapping_path);
    }

    match serde_json::to_vec(&output_files) {
        Ok(bytes) => report.pk_index_output_files = Some(bytes),
        Err(e) => {
            tracing::warn!(
                error = %e.as_report(),
                task_id = report.task_id,
                sink_id = report.sink_id,
                "Failed to serialize pk_index_output_files; skipping pk-index payload in report"
            );
            // This task is pk-index coordinated (that's why we're populating these fields at
            // all), so meta's sink coordinator relies on this payload to perform the actual
            // iceberg commit. Reporting Success without it would make meta silently treat the
            // rewrite as done while dropping the output files entirely. Fail the report instead
            // so meta retries the task.
            report.status =
                subscribe_iceberg_compaction_event_request::report_task::Status::Failed as i32;
            report.error_message = Some(format!(
                "failed to serialize pk-index compaction report payload: {}",
                e.as_report()
            ));
            return;
        }
    }
    // Input files are serialized as a JSON array of path strings.
    match serde_json::to_vec(&input_file_paths) {
        Ok(bytes) => report.pk_index_input_files = Some(bytes),
        Err(e) => {
            tracing::warn!(
                error = %e.as_report(),
                task_id = report.task_id,
                sink_id = report.sink_id,
                "Failed to serialize pk_index_input_files; skipping pk-index payload in report"
            );
            report.pk_index_output_files = None;
            // Same rationale as above: a coordinated rewrite that cannot be fully reported must
            // not be reported as success, or meta will silently drop the compaction output.
            report.status =
                subscribe_iceberg_compaction_event_request::report_task::Status::Failed as i32;
            report.error_message = Some(format!(
                "failed to serialize pk-index compaction report payload: {}",
                e.as_report()
            ));
            return;
        }
    }
    // Mapping paths (one per plan) are serialized as a JSON array string, since the proto field
    // is a single `optional string` but a task can admit multiple plans, each with its own
    // spilled provenance file.
    match serde_json::to_string(&mapping_paths) {
        Ok(s) => report.pk_index_mapping_path = Some(s),
        Err(e) => {
            tracing::warn!(
                error = %e.as_report(),
                task_id = report.task_id,
                sink_id = report.sink_id,
                "Failed to serialize pk_index_mapping_path; skipping pk-index payload in report"
            );
            report.pk_index_output_files = None;
            report.pk_index_input_files = None;
            // Same rationale as above: without the mapping path meta cannot reconcile concurrent
            // deletes against the rewrite output, so the report must not claim success.
            report.status =
                subscribe_iceberg_compaction_event_request::report_task::Status::Failed as i32;
            report.error_message = Some(format!(
                "failed to serialize pk-index compaction report payload: {}",
                e.as_report()
            ));
            return;
        }
    }
    report.pk_index_read_snapshot_id = Some(read_snapshot_id);
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
    fn test_into_report_populates_pk_index_fields_when_pk_index_result_present() {
        let mut tracker = IcebergTaskTracker::new(9, 2);
        tracker.record_completion(
            None,
            Some(PkIndexCompactionResult {
                output_files: vec![],
                input_file_paths: vec![],
                read_snapshot_id: 42,
                pk_index_mapping_path: "s3://bucket/table/_rw_pk_index_compaction/7-0.jsonl"
                    .to_owned(),
            }),
        );
        tracker.record_completion(
            None,
            Some(PkIndexCompactionResult {
                output_files: vec![],
                input_file_paths: vec![],
                read_snapshot_id: 42,
                pk_index_mapping_path: "s3://bucket/table/_rw_pk_index_compaction/7-1.jsonl"
                    .to_owned(),
            }),
        );

        let report = tracker.into_report(7);

        // Empty file vectors still serialize to a JSON array, and the snapshot id is surfaced.
        assert_eq!(
            report.pk_index_output_files.as_deref(),
            Some(b"[]".as_slice())
        );
        assert_eq!(
            report.pk_index_input_files.as_deref(),
            Some(b"[]".as_slice())
        );
        assert_eq!(report.pk_index_read_snapshot_id, Some(42));
        assert_eq!(
            report.pk_index_mapping_path.as_deref(),
            Some(
                r#"["s3://bucket/table/_rw_pk_index_compaction/7-0.jsonl","s3://bucket/table/_rw_pk_index_compaction/7-1.jsonl"]"#
            )
        );
    }

    #[test]
    fn test_into_report_leaves_pk_index_fields_none_for_non_pk_index_task() {
        let mut tracker = IcebergTaskTracker::new(9, 1);
        tracker.record_completion(None, None);

        let report = tracker.into_report(7);

        assert!(report.pk_index_output_files.is_none());
        assert!(report.pk_index_input_files.is_none());
        assert!(report.pk_index_read_snapshot_id.is_none());
        assert!(report.pk_index_mapping_path.is_none());
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
