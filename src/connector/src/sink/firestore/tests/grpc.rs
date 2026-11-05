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
use std::convert::Infallible;
use std::sync::atomic::{AtomicI32, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context as TaskContext, Poll};
use std::time::Duration;

use futures::future::BoxFuture;
use gcloud_sdk::google::firestore::admin::v1::Database;
use gcloud_sdk::google::firestore::v1::{BatchWriteRequest, BatchWriteResponse, WriteResult};
use risingwave_common::array::StreamChunkTestExt;
use tokio::sync::{Semaphore, mpsc};
use tokio::time::timeout;
use tonic::codegen::http;

use super::*;
use crate::sink::log_store::{LogStoreReadItem, LogStoreResult, TruncateOffset};
use crate::sink::{LogSinker, SinkLogReader};

#[derive(Clone)]
struct MockService(Arc<MockState>);

struct MockState {
    requests: Mutex<Vec<BatchWriteRequest>>,
    metadata_requests: Mutex<Vec<GetDatabaseRequest>>,
    edition: AtomicI32,
    metadata_error: AtomicI32,
    active: AtomicUsize,
    peak: AtomicUsize,
    started: Semaphore,
    gate: Semaphore,
    fail: bool,
    row_error_code: AtomicI32,
    rpc_error_code: AtomicI32,
}

#[derive(Clone)]
struct MockAdminService(Arc<MockState>);

impl tonic::server::NamedService for MockAdminService {
    const NAME: &'static str = "google.firestore.admin.v1.FirestoreAdmin";
}

impl tonic::server::UnaryService<GetDatabaseRequest> for MockAdminService {
    type Future = BoxFuture<'static, std::result::Result<tonic::Response<Database>, tonic::Status>>;
    type Response = Database;

    fn call(&mut self, request: tonic::Request<GetDatabaseRequest>) -> Self::Future {
        let state = self.0.clone();
        assert_eq!(
            request.metadata().get("authorization").unwrap(),
            "Bearer owner"
        );
        assert_eq!(
            request
                .metadata()
                .get("google-cloud-resource-prefix")
                .unwrap(),
            request.get_ref().name.as_str()
        );
        state
            .metadata_requests
            .lock()
            .unwrap()
            .push(request.into_inner());
        Box::pin(async move {
            let code = state.metadata_error.load(Ordering::SeqCst);
            if code != 0 {
                return Err(tonic::Status::new(
                    tonic::Code::from_i32(code),
                    "injected metadata error",
                ));
            }
            Ok(tonic::Response::new(Database {
                database_edition: state.edition.load(Ordering::SeqCst),
                ..Default::default()
            }))
        })
    }
}

impl tonic::codegen::Service<http::Request<tonic::body::Body>> for MockAdminService {
    type Error = Infallible;
    type Future = BoxFuture<'static, std::result::Result<Self::Response, Self::Error>>;
    type Response = http::Response<tonic::body::Body>;

    fn poll_ready(
        &mut self,
        _cx: &mut TaskContext<'_>,
    ) -> Poll<std::result::Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: http::Request<tonic::body::Body>) -> Self::Future {
        assert_eq!(
            request.uri().path(),
            "/google.firestore.admin.v1.FirestoreAdmin/GetDatabase"
        );
        let service = self.clone();
        Box::pin(async move {
            let mut grpc = tonic::server::Grpc::new(tonic_prost::ProstCodec::default());
            Ok(grpc.unary(service, request).await)
        })
    }
}

impl tonic::server::NamedService for MockService {
    const NAME: &'static str = "google.firestore.v1.Firestore";
}

impl tonic::server::UnaryService<BatchWriteRequest> for MockService {
    type Future =
        BoxFuture<'static, std::result::Result<tonic::Response<Self::Response>, tonic::Status>>;
    type Response = BatchWriteResponse;

    fn call(&mut self, request: tonic::Request<BatchWriteRequest>) -> Self::Future {
        let state = self.0.clone();
        Box::pin(async move {
            let request = request.into_inner();
            let count = request.writes.len();
            let invalid = request
                .writes
                .iter()
                .map(|write| write_document_path(write).ends_with("/bad"))
                .collect::<Vec<_>>();
            state.requests.lock().unwrap().push(request);
            let active = state.active.fetch_add(1, Ordering::SeqCst) + 1;
            state.peak.fetch_max(active, Ordering::SeqCst);
            state.started.add_permits(1);
            state.gate.acquire().await.unwrap().forget();
            state.active.fetch_sub(1, Ordering::SeqCst);
            let rpc_error = state.rpc_error_code.load(Ordering::SeqCst);
            if rpc_error != 0 && invalid.iter().any(|bad| *bad) {
                return Err(tonic::Status::new(
                    tonic::Code::from_i32(rpc_error),
                    "injected RPC error",
                ));
            }
            Ok(tonic::Response::new(BatchWriteResponse {
                status: (0..count)
                    .map(|index| Status {
                        code: if state.fail && index == 0 {
                            7
                        } else if invalid[index] {
                            state.row_error_code.load(Ordering::SeqCst)
                        } else {
                            0
                        },
                        ..Default::default()
                    })
                    .collect(),
                write_results: vec![WriteResult::default(); count],
            }))
        })
    }
}

impl tonic::codegen::Service<http::Request<tonic::body::Body>> for MockService {
    type Error = Infallible;
    type Future = BoxFuture<'static, std::result::Result<Self::Response, Self::Error>>;
    type Response = http::Response<tonic::body::Body>;

    fn poll_ready(
        &mut self,
        _cx: &mut TaskContext<'_>,
    ) -> Poll<std::result::Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: http::Request<tonic::body::Body>) -> Self::Future {
        assert_eq!(
            request.uri().path(),
            "/google.firestore.v1.Firestore/BatchWrite"
        );
        let service = self.clone();
        Box::pin(async move {
            let mut grpc = tonic::server::Grpc::new(tonic_prost::ProstCodec::default());
            Ok(grpc.unary(service, request).await)
        })
    }
}

struct MockServer {
    state: Arc<MockState>,
    task: tokio::task::JoinHandle<std::result::Result<(), tonic::transport::Error>>,
}

impl Drop for MockServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl MockServer {
    async fn writer(fail: bool) -> (Self, FirestoreSinkWriter) {
        let state = Arc::new(MockState {
            requests: Mutex::new(vec![]),
            metadata_requests: Mutex::new(vec![]),
            edition: AtomicI32::new(DatabaseEdition::Enterprise as i32),
            metadata_error: AtomicI32::new(0),
            active: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
            started: Semaphore::new(0),
            gate: Semaphore::new(0),
            fail,
            row_error_code: AtomicI32::new(0),
            rpc_error_code: AtomicI32::new(0),
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let incoming = futures::stream::unfold(listener, |listener| async {
            let accepted = listener.accept().await.map(|(socket, _)| socket);
            Some((accepted, listener))
        });
        let service = MockService(state.clone());
        let task = tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(service)
                .add_service(MockAdminService(state.clone()))
                .serve_with_incoming(incoming),
        );
        let config = test_config();
        let client = config
            .build_client_with_emulator(Some(address))
            .await
            .unwrap();
        let writer = FirestoreSinkWriter {
            client,
            config,
            schema: test_schema(),
            pk_indices: vec![0],
            doc_id_col_idx: Some(0),
            collection_path: parse_collection_path("sink-test", &test_schema()).unwrap(),
        };
        (Self { state, task }, writer)
    }

    async fn wait_requests(&self, count: u32) {
        timeout(
            Duration::from_secs(10),
            self.state.started.acquire_many(count),
        )
        .await
        .unwrap()
        .unwrap()
        .forget();
    }
}

#[tokio::test]
async fn decimal_metadata_checks_edition_and_preserves_mode_on_unknown() -> Result<()> {
    let (server, writer) = MockServer::writer(false).await;
    let schema = Schema::new(vec![Field::with_name(
        DataType::list(DataType::Decimal),
        "amounts",
    )]);
    let mut config = writer.config.clone();
    config.database_id = Some("named-database".to_owned());
    config.credentials = Some("must not be used for emulator".to_owned());
    let host = writer
        .client
        .get_options()
        .firebase_api_url
        .as_ref()
        .unwrap()
        .strip_prefix("http://")
        .unwrap()
        .to_owned();
    let client = config.build_client_with_emulator(Some(host)).await?;

    for edition in [
        DatabaseEdition::Enterprise as i32,
        DatabaseEdition::Unspecified as i32,
        99,
    ] {
        server.state.edition.store(edition, Ordering::SeqCst);
        config.validate_decimal_mode(&schema, &client).await?;
        assert_eq!(config.decimal_mode, FirestoreDecimalMode::Decimal128);
    }
    server
        .state
        .edition
        .store(DatabaseEdition::Standard as i32, Ordering::SeqCst);
    let error = config
        .validate_decimal_mode(&schema, &client)
        .await
        .unwrap_err();
    assert!(matches!(error, SinkError::Config(_)));
    use thiserror_ext::AsReport;
    assert!(
        error
            .to_report_string()
            .contains("firestore.decimal_mode='double'")
    );

    for code in [tonic::Code::PermissionDenied, tonic::Code::Unimplemented] {
        server
            .state
            .metadata_error
            .store(code as i32, Ordering::SeqCst);
        config.validate_decimal_mode(&schema, &client).await?;
        assert_eq!(config.decimal_mode, FirestoreDecimalMode::Decimal128);
    }
    for code in [
        tonic::Code::Unavailable,
        tonic::Code::Unauthenticated,
        tonic::Code::NotFound,
        tonic::Code::DeadlineExceeded,
    ] {
        server
            .state
            .metadata_error
            .store(code as i32, Ordering::SeqCst);
        assert!(
            config
                .validate_decimal_mode(&schema, &client)
                .await
                .is_err()
        );
    }
    let requests = server.state.metadata_requests.lock().unwrap().clone();
    assert_eq!(requests.len(), 10);
    assert!(
        requests
            .iter()
            .all(|request| request.name == "projects/demo-rw-firestore/databases/named-database")
    );
    config.decimal_mode = FirestoreDecimalMode::Double;
    config.validate_decimal_mode(&schema, &client).await?;
    config.decimal_mode = FirestoreDecimalMode::Decimal128;
    config
        .validate_decimal_mode(&test_schema(), &client)
        .await?;
    assert_eq!(
        server.state.metadata_requests.lock().unwrap().len(),
        requests.len()
    );
    Ok(())
}

#[tokio::test]
async fn decimal128_reaches_batch_write_without_float_rounding() -> Result<()> {
    use risingwave_common::array::{
        Array, ArrayBuilder, DecimalArrayBuilder, StreamChunk, Utf8ArrayBuilder,
    };
    use risingwave_common::types::Decimal;
    let (server, mut writer) = MockServer::writer(false).await;
    writer.schema = Schema::new(vec![
        Field::with_name(DataType::Varchar, "id"),
        Field::with_name(DataType::Decimal, "amount"),
    ]);
    writer
        .config
        .validate_decimal_mode(&writer.schema, &writer.client)
        .await?;
    let mut ids = Utf8ArrayBuilder::new(1);
    ids.append(Some("precise"));
    let mut amounts = DecimalArrayBuilder::new(1);
    let amount: Decimal = "9007199254740993".parse().unwrap();
    amounts.append(Some(amount));
    let chunk = StreamChunk::new(
        vec![Op::Insert],
        vec![ids.finish().into_ref(), amounts.finish().into_ref()],
    );
    server.state.gate.add_permits(1);
    let operations = writer.write_chunk_inner(chunk)?;
    writer.write_operations(operations).await?;
    let requests = server.state.requests.lock().unwrap();
    let Some(write::Operation::Update(document)) = &requests[0].writes[0].operation else {
        panic!()
    };
    assert_eq!(
        document.fields["amount"],
        format_datum(Some(ScalarRefImpl::Decimal(amount)), &DataType::Decimal)?
    );
    Ok(())
}

#[tokio::test]
async fn wire_requests_obey_concurrency_and_drain_on_error() {
    for fail in [false, true] {
        let (server, writer) = MockServer::writer(fail).await;
        let operations = (0..5)
            .map(|id| FirestoreOperation::Delete {
                path: format!("sink-test/{id}"),
            })
            .collect();
        let task = tokio::spawn(async move { writer.write_operations(operations).await });
        server.wait_requests(2).await;
        assert!(!task.is_finished());
        server.state.gate.add_permits(5);
        let result = timeout(Duration::from_secs(10), task)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(result.is_err(), fail);
        assert_eq!(server.state.requests.lock().unwrap().len(), 5);
        assert_eq!(server.state.peak.load(Ordering::SeqCst), 2);
        assert_eq!(server.state.active.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn per_row_parent_paths_keep_same_ids_independent_and_move_documents() -> Result<()> {
    let (server, mut writer) = MockServer::writer(false).await;
    writer.schema = Schema::new(vec![
        Field::with_name(DataType::Varchar, "id"),
        Field::with_name(DataType::Varchar, "parent"),
    ]);
    writer.collection_path = parse_collection_path("parents/{parent}/children", &writer.schema)?;
    writer.config.max_batch_size = 500;
    let operations =
        writer.write_chunk_inner(StreamChunk::from_pretty("T T\n+ child alice\n+ child bob"))?;
    assert_eq!(operations.len(), 2);
    server.state.gate.add_permits(10);
    writer.write_operations(operations).await?;
    let operations = writer.write_chunk_inner(StreamChunk::from_pretty(
        "T T\nU- child alice\nU+ child charlie",
    ))?;
    writer.write_operations(operations).await?;
    let requests = server.state.requests.lock().unwrap();
    let writes = &requests[0].writes;
    assert!(write_document_path(&writes[0]).ends_with("/parents/alice/children/child"));
    assert!(write_document_path(&writes[1]).ends_with("/parents/bob/children/child"));
    assert!(
        matches!(&requests[1].writes[0].operation, Some(write::Operation::Delete(path)) if path.ends_with("/parents/alice/children/child"))
    );
    assert!(
        write_document_path(&requests[1].writes[1]).ends_with("/parents/charlie/children/child")
    );
    Ok(())
}

#[tokio::test]
async fn bad_local_rows_do_not_block_good_rows_or_later_deletes() -> Result<()> {
    let (_server, mut writer) = MockServer::writer(false).await;
    let chunk = StreamChunk::from_pretty(
        "T I\n+ good 1\n+ a/b 2\n+ . 3\n+ __reserved__ 4\n+ (empty) 5\n+ later 6",
    );
    assert!(writer.write_chunk_inner(chunk.clone()).is_err());
    writer.config.skip_invalid_rows = true;
    let operations = writer.write_chunk_inner(chunk)?;
    assert_eq!(operations.len(), 2);
    assert_eq!(operations[0].document_path(), "sink-test/good");
    assert_eq!(operations[1].document_path(), "sink-test/later");
    assert!(validate_firestore_document_id("").is_err());
    let operations =
        writer.write_chunk_inner(StreamChunk::from_pretty("T I\n- a/b 2\n- . 5\n+ later 7"))?;
    assert_eq!(operations.len(), 1);
    writer.schema = Schema::new(vec![
        Field::with_name(DataType::Varchar, "id"),
        Field::with_name(DataType::Varchar, "value"),
    ]);
    let chunk = StreamChunk::from_pretty(&format!(
        "T T\n+ huge {}\n+ good small",
        "x".repeat(MAX_DOCUMENT_SIZE)
    ));
    assert_eq!(writer.write_chunk_inner(chunk.clone())?.len(), 1);
    writer.config.skip_invalid_rows = false;
    assert!(writer.write_chunk_inner(chunk).is_err());
    Ok(())
}

#[tokio::test]
async fn invalid_remote_rows_advance_log_but_system_errors_do_not() {
    for (rpc_error, row_error, succeeds) in [
        (0, 3, true),
        (0, 11, true),
        (3, 0, true),
        (0, 7, false),
        (0, 14, false),
        (14, 0, false),
    ] {
        let (server, mut writer) = MockServer::writer(false).await;
        server
            .state
            .rpc_error_code
            .store(rpc_error, Ordering::SeqCst);
        server
            .state
            .row_error_code
            .store(row_error, Ordering::SeqCst);
        server.state.gate.add_permits(20);
        writer.config.max_batch_size = 500;
        writer.config.skip_invalid_rows = true;
        let (tx, mut rx) = mpsc::unbounded_channel();
        let reader = TestReader {
            chunks: VecDeque::from([
                StreamChunk::from_pretty("T I\n+ bad 1\n+ good 2"),
                StreamChunk::from_pretty("T I\n+ later 3"),
            ]),
            next_id: 0,
            truncates: tx,
        };
        let task = tokio::spawn(writer.into_log_sinker(1).consume_log_and_sink(reader));
        if succeeds {
            timeout(Duration::from_secs(10), async {
                loop {
                    if rx.recv().await.unwrap()
                        == (TruncateOffset::Chunk {
                            epoch: 1,
                            chunk_id: 1,
                        })
                    {
                        break;
                    }
                }
            })
            .await
            .unwrap();
            let requests = server.state.requests.lock().unwrap();
            assert!(
                requests
                    .last()
                    .unwrap()
                    .writes
                    .iter()
                    .any(|write| write_document_path(write).ends_with("/later"))
            );
            if rpc_error == 3 {
                assert_eq!(
                    requests.len(),
                    4,
                    "one rejected batch, two isolated rows, then next chunk"
                );
            }
            task.abort();
        } else {
            assert!(
                timeout(Duration::from_secs(10), task)
                    .await
                    .unwrap()
                    .unwrap()
                    .is_err()
            );
            assert!(rx.try_recv().is_err());
            assert_eq!(server.state.requests.lock().unwrap().len(), 1);
        }
    }
}

struct TestReader {
    chunks: VecDeque<StreamChunk>,
    next_id: usize,
    truncates: mpsc::UnboundedSender<TruncateOffset>,
}

impl SinkLogReader for TestReader {
    async fn start_from(&mut self, _start_offset: Option<u64>) -> LogStoreResult<()> {
        Ok(())
    }

    async fn next_item(&mut self) -> LogStoreResult<(u64, LogStoreReadItem)> {
        let Some(chunk) = self.chunks.pop_front() else {
            return futures::future::pending().await;
        };
        let chunk_id = self.next_id;
        self.next_id += 1;
        Ok((1, LogStoreReadItem::StreamChunk { chunk, chunk_id }))
    }

    fn truncate(&mut self, offset: TruncateOffset) -> LogStoreResult<()> {
        self.truncates.send(offset).unwrap();
        Ok(())
    }
}

#[tokio::test]
async fn log_sinker_waits_for_ack_and_preserves_document_order() {
    for fail in [false, true] {
        let (server, mut writer) = MockServer::writer(fail).await;
        writer.config.max_batch_size = 2;
        let (tx, mut rx) = mpsc::unbounded_channel();
        let reader = TestReader {
            chunks: VecDeque::from([
                StreamChunk::from_pretty(if fail {
                    "T I\n+ doc 1\n+ other 3"
                } else {
                    "T I\n+ doc 1"
                }),
                StreamChunk::from_pretty("T I\n+ doc 2"),
            ]),
            next_id: 0,
            truncates: tx,
        };
        let task = tokio::spawn(writer.into_log_sinker(1).consume_log_and_sink(reader));
        server.wait_requests(1).await;
        assert!(rx.try_recv().is_err(), "unacknowledged chunk was truncated");
        server.state.gate.add_permits(1);
        if fail {
            assert!(
                timeout(Duration::from_secs(10), task)
                    .await
                    .unwrap()
                    .unwrap()
                    .is_err()
            );
            assert!(rx.try_recv().is_err(), "failed chunk was truncated");
            assert_eq!(server.state.requests.lock().unwrap().len(), 1);
        } else {
            server.wait_requests(1).await;
            server.state.gate.add_permits(1);
            let offset = timeout(Duration::from_secs(10), async {
                loop {
                    let offset = rx.recv().await.unwrap();
                    if offset
                        == (TruncateOffset::Chunk {
                            epoch: 1,
                            chunk_id: 1,
                        })
                    {
                        break offset;
                    }
                }
            })
            .await
            .unwrap();
            assert_eq!(
                offset,
                TruncateOffset::Chunk {
                    epoch: 1,
                    chunk_id: 1
                }
            );
            let requests = server.state.requests.lock().unwrap();
            for (index, request) in requests.iter().enumerate() {
                let Some(write::Operation::Update(doc)) = &request.writes[0].operation else {
                    panic!()
                };
                assert_eq!(
                    doc.fields["value"].value_type,
                    Some(ValueType::IntegerValue(index as i64 + 1))
                );
            }
            assert_eq!(requests.len(), 2);
            assert_eq!(server.state.peak.load(Ordering::SeqCst), 1);
            task.abort();
        }
    }
}

#[tokio::test]
#[ignore = "requires FIRESTORE_EMULATOR_HOST; use integration_tests/firestore-sink/run-emulator-test.sh"]
async fn emulator_crud_replay_and_native_values() -> Result<()> {
    assert!(std::env::var("FIRESTORE_EMULATOR_HOST").is_ok());
    let mut config = test_config();
    config.collection = format!("rw-sink-{}", uuid::Uuid::new_v4());
    let mut writer = FirestoreSinkWriter::new(config.clone(), test_schema(), vec![0]).await?;
    let path = |id: &str| {
        format!(
            "projects/demo-rw-firestore/databases/(default)/documents/{}/{}",
            config.collection, id
        )
    };

    let chunk = StreamChunk::from_pretty("T I\n+ old 1\n+ old 2\n+ other 3");
    let operations = writer.write_chunk_inner(chunk.clone())?;
    writer.write_operations(operations).await?;
    let doc = writer
        .client
        .client()
        .get()
        .get_document(gcloud_sdk::google::firestore::v1::GetDocumentRequest {
            name: path("old"),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        doc.fields["value"].value_type,
        Some(ValueType::IntegerValue(2))
    );

    // Recreate the client and replay an already delivered chunk.
    writer = FirestoreSinkWriter::new(config.clone(), test_schema(), vec![0]).await?;
    writer
        .write_operations(writer.write_chunk_inner(chunk)?)
        .await?;
    let changes = StreamChunk::from_pretty("T I\nU- old 2\nU+ new 4\n- other 3");
    writer
        .write_operations(writer.write_chunk_inner(changes)?)
        .await?;
    for id in ["old", "other"] {
        let error = writer
            .client
            .client()
            .get()
            .get_document(gcloud_sdk::google::firestore::v1::GetDocumentRequest {
                name: path(id),
                ..Default::default()
            })
            .await
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::NotFound);
    }
    let doc = writer
        .client
        .client()
        .get()
        .get_document(gcloud_sdk::google::firestore::v1::GetDocumentRequest {
            name: path("new"),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        doc.fields["value"].value_type,
        Some(ValueType::IntegerValue(4))
    );

    let json: JsonbVal =
        r#"{"a":[1,null,{"x":null}],"min":-9223372036854775808,"max":9223372036854775807}"#
            .parse()
            .unwrap();
    let timestamp: risingwave_common::types::Timestamp =
        "1969-12-31 23:59:59.123456789".parse().unwrap();
    let timestamptz: risingwave_common::types::Timestamptz =
        "1970-01-01 07:59:59.123456+08:00".parse().unwrap();
    let decimal: risingwave_common::types::Decimal = "12.50".parse().unwrap();
    let data = HashMap::from([
        (
            "timestamp".to_owned(),
            format_datum(
                Some(ScalarRefImpl::Timestamp(timestamp)),
                &DataType::Timestamp,
            )?,
        ),
        (
            "timestamptz".to_owned(),
            format_datum(
                Some(ScalarRefImpl::Timestamptz(timestamptz)),
                &DataType::Timestamptz,
            )?,
        ),
        (
            "decimal".to_owned(),
            super::super::format_datum(
                Some(ScalarRefImpl::Decimal(decimal)),
                &DataType::Decimal,
                FirestoreDecimalMode::Double,
            )?,
        ),
        (
            "nan".to_owned(),
            format_datum(
                Some(ScalarRefImpl::Float64(f64::NAN.into())),
                &DataType::Float64,
            )?,
        ),
        (
            "inf".to_owned(),
            format_datum(
                Some(ScalarRefImpl::Float64(f64::INFINITY.into())),
                &DataType::Float64,
            )?,
        ),
        (
            "json".to_owned(),
            format_datum(
                Some(ScalarRefImpl::Jsonb(json.as_scalar_ref())),
                &DataType::Jsonb,
            )?,
        ),
    ]);
    writer
        .write_operations(vec![FirestoreOperation::Upsert {
            path: format!("{}/types", config.collection),
            data: data.clone(),
        }])
        .await?;
    let doc = writer
        .client
        .client()
        .get()
        .get_document(gcloud_sdk::google::firestore::v1::GetDocumentRequest {
            name: path("types"),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    assert!(matches!(doc.fields["nan"].value_type, Some(ValueType::DoubleValue(v)) if v.is_nan()));
    assert_eq!(
        doc.fields["inf"].value_type,
        Some(ValueType::DoubleValue(f64::INFINITY))
    );
    assert_eq!(doc.fields["json"], data["json"]);
    for key in ["timestamp", "timestamptz", "decimal"] {
        assert_eq!(doc.fields[key], data[key]);
    }

    writer
        .write_operations(vec![
            FirestoreOperation::Delete {
                path: format!("{}/new", config.collection),
            },
            FirestoreOperation::Delete {
                path: format!("{}/types", config.collection),
            },
        ])
        .await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires FIRESTORE_EMULATOR_HOST; use integration_tests/firestore-sink/run-emulator-test.sh"]
async fn emulator_per_row_paths_and_bad_row_progress() -> Result<()> {
    let root = format!("rw-feedback-{}", uuid::Uuid::new_v4());
    let mut config = test_config();
    config.collection = format!("{root}/{{parent}}/children");
    config.skip_invalid_rows = true;
    config.max_batch_size = 500;
    let schema = Schema::new(vec![
        Field::with_name(DataType::Varchar, "id"),
        Field::with_name(DataType::Varchar, "parent"),
        Field::with_name(DataType::Varchar, "payload"),
    ]);
    let writer = FirestoreSinkWriter::new(config, schema, vec![0]).await?;
    let chunk = StreamChunk::from_pretty(&format!(
        "T T T\n+ child alice first\n+ child bob second\n+ a/b alice bad\n+ (empty) alice bad\n+ . alice bad\n+ child . bad\n+ child a/b bad\n+ huge alice {}\n+ good alice ok",
        "x".repeat(MAX_DOCUMENT_SIZE)
    ));
    writer
        .write_operations(writer.write_chunk_inner(chunk)?)
        .await?;
    for (parent, payload) in [("alice", "first"), ("bob", "second")] {
        let response = writer
            .client
            .client()
            .get()
            .get_document(gcloud_sdk::google::firestore::v1::GetDocumentRequest {
                name: format!(
                    "{}/{root}/{parent}/children/child",
                    writer.client.get_documents_path()
                ),
                ..Default::default()
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            response.fields["payload"].value_type,
            Some(ValueType::StringValue(payload.to_owned()))
        );
    }
    writer.write_operations(writer.write_chunk_inner(StreamChunk::from_pretty(
        "T T T\nU- child alice first\nU+ child charlie moved\n- a/b alice bad\n- . alice bad\n+ later alice works"
    ))?).await?;
    let error = writer
        .client
        .client()
        .get()
        .get_document(gcloud_sdk::google::firestore::v1::GetDocumentRequest {
            name: format!(
                "{}/{root}/alice/children/child",
                writer.client.get_documents_path()
            ),
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::NotFound);
    // Bypass local validation to exercise a real server rejection alongside a healthy row.
    writer
        .write_operations(vec![
            FirestoreOperation::Upsert {
                path: format!("{root}/alice/children/server-bad"),
                data: HashMap::from([(
                    "payload".to_owned(),
                    Value {
                        value_type: Some(ValueType::StringValue("x".repeat(MAX_DOCUMENT_SIZE))),
                    },
                )]),
            },
            FirestoreOperation::Upsert {
                path: format!("{root}/alice/children/server-good"),
                data: HashMap::from([(
                    "payload".to_owned(),
                    Value {
                        value_type: Some(ValueType::StringValue("ok".to_owned())),
                    },
                )]),
            },
        ])
        .await?;
    for path in [
        "bob/children/child",
        "charlie/children/child",
        "alice/children/good",
        "alice/children/later",
        "alice/children/server-good",
    ] {
        writer
            .client
            .client()
            .get()
            .get_document(gcloud_sdk::google::firestore::v1::GetDocumentRequest {
                name: format!("{}/{root}/{path}", writer.client.get_documents_path()),
                ..Default::default()
            })
            .await
            .unwrap();
    }
    Ok(())
}
