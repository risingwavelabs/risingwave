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
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context as TaskContext, Poll};
use std::time::Duration;

use futures::future::BoxFuture;
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
    active: AtomicUsize,
    peak: AtomicUsize,
    started: Semaphore,
    gate: Semaphore,
    fail: bool,
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
            state.requests.lock().unwrap().push(request);
            let active = state.active.fetch_add(1, Ordering::SeqCst) + 1;
            state.peak.fetch_max(active, Ordering::SeqCst);
            state.started.add_permits(1);
            state.gate.acquire().await.unwrap().forget();
            state.active.fetch_sub(1, Ordering::SeqCst);
            Ok(tonic::Response::new(BatchWriteResponse {
                status: (0..count)
                    .map(|index| Status {
                        code: if state.fail && index == 0 { 7 } else { 0 },
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
            active: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
            started: Semaphore::new(0),
            gate: Semaphore::new(0),
            fail,
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
async fn wire_requests_obey_concurrency_and_drain_on_error() {
    for fail in [false, true] {
        let (server, writer) = MockServer::writer(fail).await;
        let operations = (0..5)
            .map(|id| FirestoreOperation::Delete {
                doc_id: id.to_string(),
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
    let data = HashMap::from([
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
            doc_id: "types".to_owned(),
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

    writer
        .write_operations(vec![
            FirestoreOperation::Delete {
                doc_id: "new".to_owned(),
            },
            FirestoreOperation::Delete {
                doc_id: "types".to_owned(),
            },
        ])
        .await?;
    Ok(())
}
