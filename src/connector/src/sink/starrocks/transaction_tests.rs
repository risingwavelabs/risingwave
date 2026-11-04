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

//! Exercise the real HTTP client and writer, including log consumption and replay.
use std::collections::HashSet;
use std::sync::Mutex;
use std::time::Duration;

use axum::body::Bytes;
use axum::extract::{OriginalUri, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::{Json, Router};
use risingwave_common::catalog::Field;
use risingwave_common::test_prelude::StreamChunkTestExt;
use tokio::sync::Notify;
use tokio::task::JoinHandle;

use super::*;
use crate::sink::log_store::{LogStoreReadItem, LogStoreResult, TruncateOffset};
use crate::sink::{LogSinker, SinkLogReader};

#[derive(Clone, Debug)]
struct Event {
    operation: String,
    label: String,
    payload: Vec<u8>,
}

#[derive(Default)]
struct ServerState {
    events: Vec<Event>,
    pending: HashMap<String, Vec<Value>>,
    committed_labels: HashSet<String>,
    committed_rows: Vec<Value>,
    // Operation, occurrence, and whether the commit succeeds before losing its reply.
    fail: Option<(&'static str, usize, bool)>,
    block: Option<&'static str>,
    rollback_fails: bool,
}

struct TestServer {
    port: u16,
    state: Arc<Mutex<ServerState>>,
    changed: Arc<Notify>,
    task: JoinHandle<()>,
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn handle(
    State((state, changed)): State<(Arc<Mutex<ServerState>>, Arc<Notify>)>,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let operation = uri.path().rsplit('/').next().unwrap();
    let label = headers["label"].to_str().unwrap().to_owned();
    // The client first probes FE with an empty load, then sends its streaming body.
    if operation == "load" && body.is_empty() {
        return Json(serde_json::json!({})).into_response();
    }
    let (fail, block, rollback_failed) = {
        let mut state = state.lock().unwrap();
        state.events.push(Event {
            operation: operation.to_owned(),
            label: label.clone(),
            payload: body.to_vec(),
        });
        let occurrence = state
            .events
            .iter()
            .filter(|e| e.operation == operation)
            .count();
        let fail = state
            .fail
            .filter(|(op, n, _)| *op == operation && *n == occurrence);
        let block = state.block == Some(operation);
        let rollback_failed = operation == "rollback"
            && (state.rollback_fails || state.committed_labels.contains(&label));
        if !rollback_failed && (fail.is_none() || fail.is_some_and(|(_, _, committed)| committed)) {
            match operation {
                "load" => {
                    let rows = serde_json::Deserializer::from_slice(&body)
                        .into_iter::<Value>()
                        .map(|r| r.unwrap())
                        .collect::<Vec<_>>();
                    state.pending.entry(label.clone()).or_default().extend(rows);
                }
                "commit" => {
                    if state.committed_labels.insert(label.clone()) {
                        let rows = state.pending.remove(&label).unwrap_or_default();
                        state.committed_rows.extend(rows);
                    }
                }
                "rollback" => {
                    state.pending.remove(&label);
                }
                _ => {}
            }
        }
        (fail, block, rollback_failed)
    };
    changed.notify_waiters();
    if block {
        return std::future::pending().await;
    }
    if fail.is_some_and(|(_, _, committed)| committed) {
        // The server committed, but the client never receives a valid success reply.
        return (StatusCode::BAD_GATEWAY, "lost commit reply").into_response();
    }
    Json(serde_json::json!({
        "Status": if fail.is_some() || rollback_failed { "Fail" } else { "OK" },
        "Message": if rollback_failed { "rollback rejected" } else if fail.is_some() { "injected failure" } else { "" },
        "Label": label,
    }))
    .into_response()
}

impl TestServer {
    async fn start() -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let state = Arc::new(Mutex::new(ServerState::default()));
        let changed = Arc::new(Notify::new());
        let app = Router::new()
            .fallback(handle)
            .with_state((state.clone(), changed.clone()));
        let task = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        Self {
            port,
            state,
            changed,
            task,
        }
    }

    fn writer(
        &self,
        batch: Option<u64>,
        transaction: Option<u64>,
        append_only: bool,
    ) -> StarrocksSinkWriter {
        let mut properties = tests::base_properties();
        properties.insert("starrocks.httpport".to_owned(), self.port.to_string());
        properties.insert("starrocks.ignore_json_size".to_owned(), "true".to_owned());
        let mut config = StarrocksConfig::from_btreemap(properties).unwrap();
        config.max_batch_size_bytes = batch;
        config.max_transaction_size_bytes = transaction;
        StarrocksSinkWriter::new(
            config,
            Schema::new(vec![Field::with_name(DataType::Int64, "id")]),
            vec![0],
            append_only,
            chrono_tz::UTC,
        )
        .unwrap()
    }

    fn events(&self, operation: &str) -> Vec<Event> {
        self.state
            .lock()
            .unwrap()
            .events
            .iter()
            .filter(|e| e.operation == operation)
            .cloned()
            .collect()
    }

    async fn wait_for(&self, operation: &str) {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let notified = self.changed.notified();
                if !self.events(operation).is_empty() {
                    return;
                }
                notified.await;
            }
        })
        .await
        .unwrap();
    }
}

#[test]
fn transaction_limit_config_and_boundaries() {
    let option = "starrocks.max_transaction_size_bytes";
    for invalid in ["0", "-1", "invalid", "18446744073709551616"] {
        let mut properties = tests::base_properties();
        properties.insert(option.to_owned(), invalid.to_owned());
        assert!(StarrocksConfig::from_btreemap(properties).is_err());
    }
    for limit in [1, u64::MAX] {
        let mut properties = tests::base_properties();
        properties.insert(option.to_owned(), limit.to_string());
        assert_eq!(
            StarrocksConfig::from_btreemap(properties)
                .unwrap()
                .max_transaction_size_bytes,
            Some(limit)
        );
    }
    assert!(
        !decide_payload_size(3, 2, 5, option)
            .unwrap()
            .finish_current_payload
    );
    assert!(
        decide_payload_size(4, 2, 5, option)
            .unwrap()
            .finish_current_payload
    );
    assert!(
        decide_payload_size(u64::MAX, 1, u64::MAX, option)
            .unwrap()
            .finish_current_payload
    );
    assert!(
        decide_payload_size(0, 6, 5, option)
            .unwrap_err()
            .to_string()
            .contains(option)
    );
}

#[tokio::test]
async fn transaction_cap_accumulates_across_requests_and_barriers() {
    let server = TestServer::start().await;
    let mut writer = server.writer(Some(8), Some(16), true);
    writer
        .write_row_json(r#"{"id":1}"#.to_owned())
        .await
        .unwrap();
    writer.barrier(false).await.unwrap();
    writer
        .write_row_json(r#"{"id":2}"#.to_owned())
        .await
        .unwrap();
    assert_eq!(writer.current_transaction_size_bytes, 16);
    assert!(server.events("commit").is_empty());
    writer
        .write_row_json(r#"{"id":3}"#.to_owned())
        .await
        .unwrap();
    writer.barrier(true).await.unwrap();
    let loads = server.events("load");
    assert_eq!(
        loads.iter().map(|e| e.payload.len()).collect::<Vec<_>>(),
        [8, 8, 8]
    );
    assert_eq!(loads[0].label, loads[1].label);
    assert_ne!(loads[1].label, loads[2].label);
    assert_eq!(server.events("commit").len(), 2);
    assert_eq!(writer.current_transaction_size_bytes, 0);
    let state = server.state.lock().unwrap();
    let operations = state
        .events
        .iter()
        .map(|e| e.operation.as_str())
        .collect::<Vec<_>>();
    assert_eq!(
        operations,
        [
            "begin", "load", "load", "prepare", "commit", "begin", "load", "prepare", "commit"
        ]
    );
}

#[tokio::test]
async fn transaction_cap_without_batch_cap_and_default_behavior() {
    for (cap, expected_commits) in [(Some(8), 3), (None, 1)] {
        let server = TestServer::start().await;
        let mut writer = server.writer(None, cap, true);
        writer
            .write_batch(StreamChunk::from_pretty(" I\n + 1\n + 2\n + 3"))
            .await
            .unwrap();
        writer.barrier(true).await.unwrap();
        assert_eq!(server.events("commit").len(), expected_commits);
        assert_eq!(server.state.lock().unwrap().committed_rows.len(), 3);
        // A checkpoint after a completed commit must not create an empty transaction.
        writer.barrier(true).await.unwrap();
        assert_eq!(server.events("begin").len(), expected_commits);
    }
}

#[tokio::test]
async fn transaction_rollover_resets_batch_accounting() {
    let server = TestServer::start().await;
    let mut writer = server.writer(Some(32), Some(16), true);
    for id in [1, 2, 3] {
        writer
            .write_row_json(format!("{{\"id\":{id}}}"))
            .await
            .unwrap();
    }
    assert_eq!(writer.current_batch_size_bytes, 8);
    assert_eq!(writer.current_transaction_size_bytes, 8);
    writer.barrier(true).await.unwrap();
    assert_eq!(
        server
            .events("load")
            .iter()
            .map(|e| e.payload.len())
            .collect::<Vec<_>>(),
        [16, 8]
    );
}

#[tokio::test]
async fn transaction_cap_counts_encoded_upserts_deletes_and_escaping() {
    let server = TestServer::start().await;
    let mut writer = server.writer(None, Some(19), false);
    writer
        .write_batch(StreamChunk::from_pretty(" I\n + 1\n - 1\n U- 2\n U+ 3"))
        .await
        .unwrap();
    writer.barrier(true).await.unwrap();
    {
        let state = server.state.lock().unwrap();
        assert_eq!(state.committed_rows.len(), 3);
        assert_eq!(state.committed_rows[1]["__op"], "1");
        assert_eq!(state.committed_rows[2]["id"], 3);
    }
    assert_eq!(server.events("commit").len(), 3);
    assert!(
        server
            .events("load")
            .iter()
            .all(|event| event.payload.len() == 19)
    );
    let escaped = serde_json::json!({"text": "\"\\\n中文"}).to_string();
    let size = escaped.len() as u64;
    let mut writer = server.writer(Some(size), Some(size), true);
    writer.write_row_json(escaped.clone()).await.unwrap();
    assert_eq!(writer.current_transaction_size_bytes, size);
    writer.write_row_json(escaped.clone()).await.unwrap();
    writer.barrier(true).await.unwrap();
    let loads = server.events("load");
    assert_eq!(loads[loads.len() - 1].payload, escaped.as_bytes());
}

#[tokio::test]
async fn oversized_row_does_not_commit_pending_data_or_begin_transaction() {
    for (batch, transaction) in [(Some(8), Some(16)), (Some(16), Some(8))] {
        let server = TestServer::start().await;
        let mut writer = server.writer(batch, transaction, true);
        assert!(
            writer
                .write_row_json(r#"{"id":100}"#.to_owned())
                .await
                .is_err()
        );
        assert!(server.events("begin").is_empty());
        writer
            .write_row_json(r#"{"id":1}"#.to_owned())
            .await
            .unwrap();
        assert!(
            writer
                .write_row_json(r#"{"id":100}"#.to_owned())
                .await
                .is_err()
        );
        assert!(server.events("commit").is_empty());
        writer.abort().await.unwrap();
    }
}

#[derive(Default)]
struct TestLog {
    items: Vec<(u64, LogStoreReadItem)>,
    cursor: usize,
    truncated: Vec<TruncateOffset>,
}

impl SinkLogReader for &mut TestLog {
    async fn start_from(&mut self, _: Option<u64>) -> LogStoreResult<()> {
        // Recreate a reader from the durable truncation boundary, retaining the
        // original log entries so tests cannot accidentally omit replayed rows.
        let epoch = self.truncated.last().map_or(0, TruncateOffset::epoch);
        self.cursor = self
            .items
            .iter()
            .position(|(e, _)| *e > epoch)
            .unwrap_or(self.items.len());
        Ok(())
    }

    async fn next_item(&mut self) -> LogStoreResult<(u64, LogStoreReadItem)> {
        let (epoch, item) = self
            .items
            .get(self.cursor)
            .ok_or_else(|| anyhow!("end of test log"))?;
        let item = match item {
            LogStoreReadItem::StreamChunk { chunk, chunk_id } => LogStoreReadItem::StreamChunk {
                chunk: chunk.clone(),
                chunk_id: *chunk_id,
            },
            LogStoreReadItem::Barrier {
                is_checkpoint,
                new_vnode_bitmap,
                is_stop,
                schema_change,
            } => LogStoreReadItem::Barrier {
                is_checkpoint: *is_checkpoint,
                new_vnode_bitmap: new_vnode_bitmap.clone(),
                is_stop: *is_stop,
                schema_change: schema_change.clone(),
            },
        };
        self.cursor += 1;
        Ok((*epoch, item))
    }

    fn truncate(&mut self, offset: TruncateOffset) -> LogStoreResult<()> {
        self.truncated.push(offset);
        Ok(())
    }
}

impl TestLog {
    fn checkpoint(epoch: u64) -> (u64, LogStoreReadItem) {
        (
            epoch,
            LogStoreReadItem::Barrier {
                is_checkpoint: true,
                new_vnode_bitmap: None,
                is_stop: false,
                schema_change: None,
            },
        )
    }

    fn chunk(epoch: u64, data: &str) -> (u64, LogStoreReadItem) {
        (
            epoch,
            LogStoreReadItem::StreamChunk {
                chunk: StreamChunk::from_pretty(data),
                chunk_id: 0,
            },
        )
    }
}

async fn consume(writer: StarrocksSinkWriter, log: &mut TestLog, interval: u64) -> String {
    DecoupleCheckpointLogSinkerOf::new(
        writer,
        SinkWriterMetrics::for_test(),
        NonZeroU64::new(interval).unwrap(),
    )
    .consume_log_and_sink(log)
    .await
    .unwrap_err()
    .to_report_string()
}

#[tokio::test]
async fn early_commits_keep_checkpoint_truncation_schedule() {
    let server = TestServer::start().await;
    let mut log = TestLog::default();
    log.items.extend([
        TestLog::chunk(1, " I\n + 1\n + 2\n + 3"),
        TestLog::checkpoint(1),
    ]);
    assert_eq!(
        consume(server.writer(None, Some(8), true), &mut log, 2).await,
        "end of test log"
    );
    assert_eq!(server.events("commit").len(), 2);
    assert!(log.truncated.is_empty());
    // Replay from the unchanged truncation boundary, then reach the interval.
    log.items.push(TestLog::checkpoint(2));
    assert_eq!(
        consume(server.writer(None, Some(8), true), &mut log, 2).await,
        "end of test log"
    );
    assert_eq!(log.truncated, [TruncateOffset::Barrier { epoch: 2 }]);
}

#[tokio::test]
async fn failed_rollover_and_checkpoint_commits_do_not_truncate_and_can_replay() {
    // Occurrence 1 fails at rollover; occurrence 2 fails at the final checkpoint.
    for (operation, occurrence, lost_reply) in [
        ("load", 1, false),
        ("load", 2, false),
        ("prepare", 1, false),
        ("commit", 1, false),
        ("commit", 1, true),
        ("prepare", 2, false),
        ("commit", 2, false),
        ("commit", 2, true),
    ] {
        let server = TestServer::start().await;
        server.state.lock().unwrap().fail = Some((operation, occurrence, lost_reply));
        let mut log = TestLog::default();
        log.items
            .extend([TestLog::chunk(1, " I\n + 1\n + 2"), TestLog::checkpoint(1)]);
        let error = consume(server.writer(None, Some(8), true), &mut log, 1).await;
        assert_ne!(error, "end of test log", "{operation}/{occurrence}");
        assert!(log.truncated.is_empty());
        assert_eq!(
            server.events("begin").len(),
            if occurrence == 1 { 1 } else { 2 }
        );
        // Recreate the writer and read the same log from its truncation boundary.
        server.state.lock().unwrap().fail = None;
        assert_eq!(
            consume(server.writer(None, Some(8), true), &mut log, 1).await,
            "end of test log"
        );
        assert_eq!(log.truncated, [TruncateOffset::Barrier { epoch: 1 }]);
        let state = server.state.lock().unwrap();
        for id in [1, 2] {
            assert!(state.committed_rows.iter().any(|row| row["id"] == id));
        }
        let expected = 2 + usize::from(occurrence == 2) + usize::from(lost_reply);
        assert_eq!(
            state.committed_rows.len(),
            expected,
            "{operation}/{occurrence}/{lost_reply}"
        );
    }
}

#[tokio::test]
async fn cancellation_during_rollover_retains_label_for_drop_rollback() {
    for operation in ["begin", "prepare", "commit"] {
        let server = TestServer::start().await;
        let mut writer = server.writer(None, Some(8), true);
        if operation != "begin" {
            writer
                .write_row_json(r#"{"id":1}"#.to_owned())
                .await
                .unwrap();
        }
        server.state.lock().unwrap().block = Some(operation);
        let task =
            tokio::spawn(async move { writer.write_row_json(r#"{"id":2}"#.to_owned()).await });
        server.wait_for(operation).await;
        task.abort();
        let _ = task.await;
        server.wait_for("rollback").await;
        assert_eq!(
            server.events("rollback")[0].label,
            server.events("begin")[0].label
        );
        assert_eq!(server.events("begin").len(), 1);
    }
}

#[tokio::test]
async fn rollback_failure_preserves_the_original_commit_error() {
    let server = TestServer::start().await;
    {
        let mut state = server.state.lock().unwrap();
        state.fail = Some(("commit", 1, false));
        state.rollback_fails = true;
    }
    let mut log = TestLog::default();
    log.items
        .extend([TestLog::chunk(1, " I\n + 1\n + 2"), TestLog::checkpoint(1)]);
    let error = consume(server.writer(None, Some(8), true), &mut log, 1).await;
    assert!(error.contains("injected failure"), "{error}");
    assert!(!error.contains("rollback rejected"), "{error}");
    assert!(log.truncated.is_empty());
    assert_eq!(server.events("rollback").len(), 1);
}

#[tokio::test]
async fn begin_failures_preserve_the_original_error_and_can_replay() {
    for occurrence in [1, 2] {
        for rollback_fails in [false, true] {
            let server = TestServer::start().await;
            {
                let mut state = server.state.lock().unwrap();
                state.fail = Some(("begin", occurrence, false));
                state.rollback_fails = rollback_fails;
            }
            let mut log = TestLog::default();
            log.items
                .extend([TestLog::chunk(1, " I\n + 1\n + 2"), TestLog::checkpoint(1)]);
            let error = consume(server.writer(None, Some(8), true), &mut log, 1).await;
            assert!(error.contains("injected failure"), "{error}");
            assert!(!error.contains("rollback rejected"), "{error}");
            assert!(log.truncated.is_empty());
            assert_eq!(server.events("commit").len(), occurrence - 1);
            let rollbacks = server.events("rollback");
            assert_eq!(rollbacks.len(), 1);
            assert_eq!(
                rollbacks[0].label,
                server.events("begin")[occurrence - 1].label
            );

            // Replay all untruncated rows, including any committed before the failed begin.
            {
                let mut state = server.state.lock().unwrap();
                state.fail = None;
                state.rollback_fails = false;
            }
            assert_eq!(
                consume(server.writer(None, Some(8), true), &mut log, 1).await,
                "end of test log"
            );
            assert_eq!(log.truncated, [TruncateOffset::Barrier { epoch: 1 }]);
            let state = server.state.lock().unwrap();
            assert_eq!(state.committed_rows.len(), 2 + occurrence - 1);
            for id in [1, 2] {
                assert!(state.committed_rows.iter().any(|row| row["id"] == id));
            }
        }
    }
}

#[tokio::test]
async fn cancellation_during_failure_cleanup_retains_label_for_drop_rollback() {
    for operation in ["begin", "commit"] {
        let server = TestServer::start().await;
        let mut writer = server.writer(None, Some(8), true);
        if operation == "commit" {
            writer
                .write_row_json(r#"{"id":1}"#.to_owned())
                .await
                .unwrap();
        }
        {
            let mut state = server.state.lock().unwrap();
            state.fail = Some((operation, 1, false));
            state.block = Some("rollback");
        }
        let task =
            tokio::spawn(async move { writer.write_row_json(r#"{"id":2}"#.to_owned()).await });
        server.wait_for("rollback").await;
        task.abort();
        let _ = task.await;
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let notified = server.changed.notified();
                if server.events("rollback").len() >= 2 {
                    break;
                }
                notified.await;
            }
        })
        .await
        .unwrap();
        for rollback in server.events("rollback") {
            assert_eq!(rollback.label, server.events("begin")[0].label);
        }
        assert_eq!(server.events("begin").len(), 1);
    }
}

#[tokio::test]
async fn replay_skips_truncated_checkpoints_and_replays_only_the_untruncated_tail() {
    for lost_reply in [false, true] {
        let server = TestServer::start().await;
        server.state.lock().unwrap().fail = Some(("commit", 3, lost_reply));
        let mut log = TestLog::default();
        log.items.extend([
            TestLog::chunk(1, " I\n + 1\n + 2"),
            TestLog::checkpoint(1),
            TestLog::chunk(2, " I\n + 3\n + 4"),
            TestLog::checkpoint(2),
        ]);
        let error = consume(server.writer(None, Some(8), true), &mut log, 1).await;
        assert_ne!(error, "end of test log");
        assert_eq!(log.truncated, [TruncateOffset::Barrier { epoch: 1 }]);
        server.state.lock().unwrap().fail = None;
        assert_eq!(
            consume(server.writer(None, Some(8), true), &mut log, 1).await,
            "end of test log"
        );
        assert_eq!(
            log.truncated,
            [
                TruncateOffset::Barrier { epoch: 1 },
                TruncateOffset::Barrier { epoch: 2 }
            ]
        );
        let state = server.state.lock().unwrap();
        for id in [1, 2, 3, 4] {
            assert_eq!(
                state
                    .committed_rows
                    .iter()
                    .filter(|row| row["id"] == id)
                    .count(),
                1 + usize::from(id == 3 && lost_reply),
                "id={id}, lost_reply={lost_reply}"
            );
        }
    }
}
