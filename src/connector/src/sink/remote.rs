// Copyright 2023 RisingWave Labs
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
use std::marker::PhantomData;
use std::ops::Deref;
use std::pin::pin;
use std::time::Instant;

use anyhow::anyhow;
use async_trait::async_trait;
use futures::future::select;
use futures::{StreamExt, TryFutureExt, TryStreamExt};
use itertools::Itertools;
use jni::objects::{JByteArray, JValue, JValueOwned};
use jni::JavaVM;
use prost::Message;
use risingwave_common::array::StreamChunk;
use risingwave_common::error::anyhow_error;
use risingwave_common::types::DataType;
use risingwave_jni_core::jvm_runtime::JVM;
use risingwave_jni_core::{gen_class_name, gen_jni_sig, JniReceiverType, JniSenderType};
use risingwave_pb::connector_service::sink_coordinator_stream_request::StartCoordinator;
use risingwave_pb::connector_service::sink_writer_stream_request::write_batch::{
    Payload, StreamChunkPayload,
};
use risingwave_pb::connector_service::sink_writer_stream_request::{
    Request as SinkRequest, StartSink,
};
use risingwave_pb::connector_service::{
    sink_coordinator_stream_request, sink_coordinator_stream_response, sink_writer_stream_response,
    SinkCoordinatorStreamRequest, SinkCoordinatorStreamResponse, SinkMetadata, SinkPayloadFormat,
    SinkWriterStreamRequest, SinkWriterStreamResponse, ValidateSinkRequest, ValidateSinkResponse,
};
use risingwave_rpc_client::error::RpcError;
use risingwave_rpc_client::{
    BidiStreamReceiver, BidiStreamSender, SinkCoordinatorStreamHandle, SinkWriterStreamHandle,
    DEFAULT_BUFFER_SIZE,
};
use tokio::sync::mpsc;
use tokio::sync::mpsc::{unbounded_channel, Receiver, Sender};
use tokio::task::spawn_blocking;
use tokio_stream::wrappers::ReceiverStream;

use crate::sink::coordinate::CoordinatedSinkWriter;
use crate::sink::log_store::{LogReader, LogStoreReadItem, TruncateOffset};
use crate::sink::writer::{LogSinkerOf, SinkWriter, SinkWriterExt};
use crate::sink::{
    DummySinkCommitCoordinator, LogSinker, Result, Sink, SinkCommitCoordinator, SinkError,
    SinkMetrics, SinkParam, SinkWriterParam,
};
use crate::ConnectorParams;

macro_rules! def_remote_sink {
    () => {
        def_remote_sink! {
            { ElasticSearch, ElasticSearchSink, "elasticsearch" },
            { Cassandra, CassandraSink, "cassandra" },
            { Jdbc, JdbcSink, "jdbc" },
            { DeltaLake, DeltaLakeSink, "deltalake" }
        }
    };
    ($({ $variant_name:ident, $sink_type_name:ident, $sink_name:expr }),*) => {
        $(
            #[derive(Debug)]
            pub struct $variant_name;
            impl RemoteSinkTrait for $variant_name {
                const SINK_NAME: &'static str = $sink_name;
            }
            pub type $sink_type_name = RemoteSink<$variant_name>;
        )*
    };
}

def_remote_sink!();

pub trait RemoteSinkTrait: Send + Sync + 'static {
    const SINK_NAME: &'static str;
}

#[derive(Debug)]
pub struct RemoteSink<R: RemoteSinkTrait> {
    param: SinkParam,
    _phantom: PhantomData<R>,
}

impl<R: RemoteSinkTrait> TryFrom<SinkParam> for RemoteSink<R> {
    type Error = SinkError;

    fn try_from(param: SinkParam) -> std::result::Result<Self, Self::Error> {
        Ok(Self {
            param,
            _phantom: PhantomData,
        })
    }
}

impl<R: RemoteSinkTrait> Sink for RemoteSink<R> {
    type Coordinator = DummySinkCommitCoordinator;
    type LogSinker = RemoteLogSinker;

    const SINK_NAME: &'static str = R::SINK_NAME;

    async fn new_log_sinker(&self, writer_param: SinkWriterParam) -> Result<Self::LogSinker> {
        RemoteLogSinker::new(self.param.clone(), writer_param).await
    }

    async fn validate(&self) -> Result<()> {
        validate_remote_sink(&self.param).await
    }
}

async fn validate_remote_sink(param: &SinkParam) -> Result<()> {
    // FIXME: support struct and array in stream sink
    param.columns.iter().map(|col| {
        if matches!(
                col.data_type,
                DataType::Int16
                    | DataType::Int32
                    | DataType::Int64
                    | DataType::Float32
                    | DataType::Float64
                    | DataType::Boolean
                    | DataType::Decimal
                    | DataType::Timestamp
                    | DataType::Timestamptz
                    | DataType::Varchar
                    | DataType::Date
                    | DataType::Time
                    | DataType::Interval
                    | DataType::Jsonb
                    | DataType::Bytea
                    | DataType::List(_)
            ) {
            Ok(())
        } else {
            Err(SinkError::Remote(anyhow_error!(
                    "remote sink supports Int16, Int32, Int64, Float32, Float64, Boolean, Decimal, Time, Date, Interval, Jsonb, Timestamp, Timestamptz, List, Bytea and Varchar, got {:?}: {:?}",
                    col.name,
                    col.data_type,
                )))
        }
    }).try_collect()?;

    let jvm = JVM.get_or_init()?;
    let sink_param = param.to_proto();

    spawn_blocking(move || {
        let mut env = jvm
            .attach_current_thread()
            .map_err(|err| SinkError::Internal(err.into()))?;
        let validate_sink_request = ValidateSinkRequest {
            sink_param: Some(sink_param),
        };
        let validate_sink_request_bytes = env
            .byte_array_from_slice(&Message::encode_to_vec(&validate_sink_request))
            .map_err(|err| SinkError::Internal(err.into()))?;

        let response = env
            .call_static_method(
                "com/risingwave/connector/JniSinkValidationHandler",
                "validate",
                "([B)[B",
                &[JValue::Object(&validate_sink_request_bytes)],
            )
            .map_err(|err| SinkError::Internal(err.into()))?;

        let validate_sink_response_bytes = match response {
            JValueOwned::Object(o) => unsafe { JByteArray::from_raw(o.into_raw()) },
            _ => unreachable!(),
        };

        let validate_sink_response: ValidateSinkResponse = Message::decode(
            risingwave_jni_core::to_guarded_slice(&validate_sink_response_bytes, &mut env)
                .map_err(|err| SinkError::Internal(err.into()))?
                .deref(),
        )
        .map_err(|err| SinkError::Internal(err.into()))?;

        validate_sink_response.error.map_or_else(
            || Ok(()), // If there is no error message, return Ok here.
            |err| {
                Err(SinkError::Remote(anyhow!(format!(
                    "sink cannot pass validation: {}",
                    err.error_message
                ))))
            },
        )
    })
    .await
    .map_err(|e| anyhow!("unable to validate: {:?}", e))?
}

pub struct RemoteLogSinker {
    request_sender: BidiStreamSender<SinkWriterStreamRequest>,
    response_stream: BidiStreamReceiver<SinkWriterStreamResponse>,
    sink_metrics: SinkMetrics,
}

impl RemoteLogSinker {
    async fn new(sink_param: SinkParam, writer_param: SinkWriterParam) -> Result<Self> {
        let SinkWriterStreamHandle {
            request_sender,
            response_stream,
        } = EmbeddedConnectorClient::new()?
            .start_sink_writer_stream(sink_param, SinkPayloadFormat::StreamChunk)
            .await?;

        let sink_metrics = writer_param.sink_metrics;
        Ok(RemoteLogSinker {
            request_sender,
            response_stream,
            sink_metrics,
        })
    }
}

#[async_trait]
impl LogSinker for RemoteLogSinker {
    async fn consume_log_and_sink(self, mut log_reader: impl LogReader) -> Result<()> {
        let mut request_tx = self.request_sender;
        let mut response_err_stream_rx = self.response_stream;
        let sink_metrics = self.sink_metrics;

        let (response_tx, mut response_rx) = unbounded_channel();

        let poll_response_stream = pin!(async move {
            loop {
                let result = response_err_stream_rx.stream.try_next().await;
                match result {
                    Ok(Some(response)) => {
                        response_tx.send(response).map_err(|err| {
                            SinkError::Remote(anyhow!("unable to send response: {:?}", err.0))
                        })?;
                    }
                    Ok(None) => return Err(SinkError::Remote(anyhow!("end of response stream"))),
                    Err(e) => return Err(SinkError::Remote(anyhow!(e))),
                }
            }
        });

        let poll_consume_log_and_sink = pin!(async move {
            let mut prev_offset: Option<TruncateOffset> = None;

            log_reader.init().await?;

            loop {
                let (epoch, item): (u64, LogStoreReadItem) =
                    log_reader.next_item().map_err(SinkError::Internal).await?;

                match &prev_offset {
                    Some(TruncateOffset::Barrier { .. }) | None => {
                        // TODO: this start epoch is actually unnecessary
                        request_tx.start_epoch(epoch).await?;
                    }
                    _ => {}
                }

                match item {
                    LogStoreReadItem::StreamChunk { chunk, chunk_id } => {
                        let offset = TruncateOffset::Chunk { epoch, chunk_id };
                        if let Some(prev_offset) = &prev_offset {
                            prev_offset.check_next_offset(offset)?;
                        }
                        let cardinality = chunk.cardinality();
                        sink_metrics
                            .connector_sink_rows_received
                            .inc_by(cardinality as _);

                        let payload = build_chunk_payload(chunk);
                        request_tx
                            .write_batch(epoch, chunk_id as u64, payload)
                            .await?;
                        prev_offset = Some(offset);
                    }
                    LogStoreReadItem::Barrier { is_checkpoint } => {
                        let offset = TruncateOffset::Barrier { epoch };
                        if let Some(prev_offset) = &prev_offset {
                            prev_offset.check_next_offset(offset)?;
                        }
                        if is_checkpoint {
                            let start_time = Instant::now();
                            request_tx.barrier(epoch, true).await?;
                            match response_rx.recv().await.ok_or_else(|| {
                                SinkError::Remote(anyhow!("end of response stream"))
                            })? {
                                SinkWriterStreamResponse {
                                    response: Some(sink_writer_stream_response::Response::Commit(_)),
                                } => {}
                                response => {
                                    return Err(SinkError::Remote(anyhow!(
                                        "expected commit response, but get {:?}",
                                        response
                                    )));
                                }
                            };
                            sink_metrics
                                .sink_commit_duration_metrics
                                .observe(start_time.elapsed().as_millis() as f64);
                            log_reader
                                .truncate(TruncateOffset::Barrier { epoch })
                                .await?;
                        } else {
                            request_tx.barrier(epoch, false).await?;
                        }
                        prev_offset = Some(offset);
                    }
                    LogStoreReadItem::UpdateVnodeBitmap(_) => {}
                }
            }
        });

        select(poll_response_stream, poll_consume_log_and_sink)
            .await
            .factor_first()
            .0
    }
}

#[derive(Debug)]
pub struct CoordinatedRemoteSink<R: RemoteSinkTrait> {
    param: SinkParam,
    _phantom: PhantomData<R>,
}

impl<R: RemoteSinkTrait> TryFrom<SinkParam> for CoordinatedRemoteSink<R> {
    type Error = SinkError;

    fn try_from(param: SinkParam) -> std::result::Result<Self, Self::Error> {
        Ok(Self {
            param,
            _phantom: PhantomData,
        })
    }
}

impl<R: RemoteSinkTrait> Sink for CoordinatedRemoteSink<R> {
    type Coordinator = RemoteCoordinator;
    type LogSinker = LogSinkerOf<CoordinatedSinkWriter<CoordinatedRemoteSinkWriter>>;

    const SINK_NAME: &'static str = R::SINK_NAME;

    async fn validate(&self) -> Result<()> {
        validate_remote_sink(&self.param).await
    }

    async fn new_log_sinker(&self, writer_param: SinkWriterParam) -> Result<Self::LogSinker> {
        Ok(CoordinatedSinkWriter::new(
            writer_param
                .meta_client
                .expect("should have meta client")
                .sink_coordinate_client()
                .await,
            self.param.clone(),
            writer_param.vnode_bitmap.ok_or_else(|| {
                SinkError::Remote(anyhow_error!(
                    "sink needs coordination should not have singleton input"
                ))
            })?,
            CoordinatedRemoteSinkWriter::new(
                self.param.clone(),
                writer_param.connector_params,
                writer_param.sink_metrics.clone(),
            )
            .await?,
        )
        .await?
        .into_log_sinker(writer_param.sink_metrics))
    }

    async fn new_coordinator(&self) -> Result<Self::Coordinator> {
        RemoteCoordinator::new::<R>(self.param.clone()).await
    }
}

pub struct CoordinatedRemoteSinkWriter {
    properties: HashMap<String, String>,
    epoch: Option<u64>,
    batch_id: u64,
    stream_handle: SinkWriterStreamHandle,
    sink_metrics: SinkMetrics,
}

impl CoordinatedRemoteSinkWriter {
    pub async fn new(
        param: SinkParam,
        connector_params: ConnectorParams,
        sink_metrics: SinkMetrics,
    ) -> Result<Self> {
        let stream_handle = EmbeddedConnectorClient::new()?
            .start_sink_writer_stream(param.clone(), connector_params.sink_payload_format)
            .await?;

        Ok(Self {
            properties: param.properties,
            epoch: None,
            batch_id: 0,
            stream_handle,
            sink_metrics,
        })
    }

    fn for_test(
        response_receiver: Receiver<anyhow::Result<SinkWriterStreamResponse>>,
        request_sender: Sender<SinkWriterStreamRequest>,
    ) -> CoordinatedRemoteSinkWriter {
        let properties = HashMap::from([("output.path".to_string(), "/tmp/rw".to_string())]);

        let stream_handle = SinkWriterStreamHandle::for_test(
            request_sender,
            ReceiverStream::new(response_receiver)
                .map_err(RpcError::from)
                .boxed(),
        );

        CoordinatedRemoteSinkWriter {
            properties,
            epoch: None,
            batch_id: 0,
            stream_handle,
            sink_metrics: SinkMetrics::for_test(),
        }
    }
}

fn build_chunk_payload(chunk: StreamChunk) -> Payload {
    let prost_stream_chunk = chunk.to_protobuf();
    let binary_data = Message::encode_to_vec(&prost_stream_chunk);
    Payload::StreamChunkPayload(StreamChunkPayload { binary_data })
}

#[async_trait]
impl SinkWriter for CoordinatedRemoteSinkWriter {
    type CommitMetadata = Option<SinkMetadata>;

    async fn write_batch(&mut self, chunk: StreamChunk) -> Result<()> {
        let cardinality = chunk.cardinality();
        self.sink_metrics
            .connector_sink_rows_received
            .inc_by(cardinality as _);

        let payload = build_chunk_payload(chunk);

        let epoch = self.epoch.ok_or_else(|| {
            SinkError::Remote(anyhow_error!(
                "epoch has not been initialize, call `begin_epoch`"
            ))
        })?;
        let batch_id = self.batch_id;
        self.stream_handle
            .write_batch(epoch, batch_id, payload)
            .await?;
        self.batch_id += 1;
        Ok(())
    }

    async fn begin_epoch(&mut self, epoch: u64) -> Result<()> {
        self.stream_handle.start_epoch(epoch).await?;
        self.epoch = Some(epoch);
        Ok(())
    }

    async fn barrier(&mut self, is_checkpoint: bool) -> Result<Option<SinkMetadata>> {
        let epoch = self.epoch.ok_or_else(|| {
            SinkError::Remote(anyhow_error!(
                "epoch has not been initialize, call `begin_epoch`"
            ))
        })?;
        if is_checkpoint {
            // TODO: add metrics to measure commit time
            let rsp = self.stream_handle.commit(epoch).await?;
            rsp.metadata
                .ok_or_else(|| {
                    SinkError::Remote(anyhow_error!(
                        "get none metadata in commit response for coordinated sink writer"
                    ))
                })
                .map(Some)
        } else {
            self.stream_handle.barrier(epoch).await?;
            Ok(None)
        }
    }
}

pub struct RemoteCoordinator {
    stream_handle: SinkCoordinatorStreamHandle,
}

impl RemoteCoordinator {
    pub async fn new<R: RemoteSinkTrait>(param: SinkParam) -> Result<Self> {
        let stream_handle = EmbeddedConnectorClient::new()?
            .start_sink_coordinator_stream(param.clone())
            .await?;

        tracing::trace!(
            "{:?} RemoteCoordinator started with properties: {:?}",
            R::SINK_NAME,
            &param.properties
        );

        Ok(RemoteCoordinator { stream_handle })
    }
}

#[async_trait]
impl SinkCommitCoordinator for RemoteCoordinator {
    async fn init(&mut self) -> Result<()> {
        Ok(())
    }

    async fn commit(&mut self, epoch: u64, metadata: Vec<SinkMetadata>) -> Result<()> {
        Ok(self.stream_handle.commit(epoch, metadata).await?)
    }
}

struct EmbeddedConnectorClient {
    jvm: &'static JavaVM,
}

impl EmbeddedConnectorClient {
    fn new() -> Result<Self> {
        let jvm = JVM.get_or_init()?;
        Ok(EmbeddedConnectorClient { jvm })
    }

    async fn start_sink_writer_stream(
        &self,
        sink_param: SinkParam,
        sink_payload_format: SinkPayloadFormat,
    ) -> Result<SinkWriterStreamHandle> {
        let (handle, first_rsp) = SinkWriterStreamHandle::initialize(
            SinkWriterStreamRequest {
                request: Some(SinkRequest::Start(StartSink {
                    sink_param: Some(sink_param.to_proto()),
                    format: sink_payload_format as i32,
                })),
            },
            |rx| async move {
                let rx = self.start_jvm_worker_thread(
                    gen_class_name!(com.risingwave.connector.JniSinkWriterHandler),
                    "runJniSinkWriterThread",
                    rx,
                );
                Ok(ReceiverStream::new(rx).map_err(RpcError::from))
            },
        )
        .await?;

        match first_rsp {
            SinkWriterStreamResponse {
                response: Some(sink_writer_stream_response::Response::Start(_)),
            } => Ok(handle),
            msg => Err(SinkError::Internal(anyhow!(
                "should get start response but get {:?}",
                msg
            ))),
        }
    }

    async fn start_sink_coordinator_stream(
        &self,
        param: SinkParam,
    ) -> Result<SinkCoordinatorStreamHandle> {
        let (handle, first_rsp) = SinkCoordinatorStreamHandle::initialize(
            SinkCoordinatorStreamRequest {
                request: Some(sink_coordinator_stream_request::Request::Start(
                    StartCoordinator {
                        param: Some(param.to_proto()),
                    },
                )),
            },
            |rx| async move {
                let rx = self.start_jvm_worker_thread(
                    gen_class_name!(com.risingwave.connector.JniSinkCoordinatorHandler),
                    "runJniSinkCoordinatorThread",
                    rx,
                );
                Ok(ReceiverStream::new(rx).map_err(RpcError::from))
            },
        )
        .await?;

        match first_rsp {
            SinkCoordinatorStreamResponse {
                response: Some(sink_coordinator_stream_response::Response::Start(_)),
            } => Ok(handle),
            msg => Err(SinkError::Internal(anyhow!(
                "should get start response but get {:?}",
                msg
            ))),
        }
    }

    fn start_jvm_worker_thread<REQ: Send + 'static, RSP: Send + 'static>(
        &self,
        class_name: &'static str,
        method_name: &'static str,
        mut request_rx: JniReceiverType<REQ>,
    ) -> Receiver<std::result::Result<RSP, anyhow::Error>> {
        let (mut response_tx, response_rx): (JniSenderType<RSP>, _) =
            mpsc::channel(DEFAULT_BUFFER_SIZE);

        let jvm = self.jvm;
        std::thread::spawn(move || {
            let mut env = match jvm.attach_current_thread() {
                Ok(env) => env,
                Err(e) => {
                    let _ = response_tx
                        .blocking_send(Err(anyhow!("failed to attach current thread: {:?}", e)));
                    return;
                }
            };

            let result = env.call_static_method(
                class_name,
                method_name,
                gen_jni_sig!(void f(long, long)),
                &[
                    JValue::from(&mut request_rx as *mut JniReceiverType<REQ> as i64),
                    JValue::from(&mut response_tx as *mut JniSenderType<RSP> as i64),
                ],
            );

            match result {
                Ok(_) => {
                    tracing::info!("end of jni call {}::{}", class_name, method_name);
                }
                Err(e) => {
                    tracing::error!("jni call error: {:?}", e);
                }
            };
        });
        response_rx
    }
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use risingwave_common::array::StreamChunk;
    use risingwave_common::test_prelude::StreamChunkTestExt;
    use risingwave_pb::connector_service::sink_writer_stream_request::{Barrier, Request};
    use risingwave_pb::connector_service::sink_writer_stream_response::{CommitResponse, Response};
    use risingwave_pb::connector_service::{SinkWriterStreamRequest, SinkWriterStreamResponse};
    use tokio::sync::mpsc;

    use crate::sink::remote::{build_chunk_payload, CoordinatedRemoteSinkWriter};
    use crate::sink::SinkWriter;

    #[tokio::test]
    async fn test_epoch_check() {
        let (request_sender, mut request_recv) = mpsc::channel(16);
        let (_, resp_recv) = mpsc::channel(16);

        let mut sink = CoordinatedRemoteSinkWriter::for_test(resp_recv, request_sender);
        let chunk = StreamChunk::from_pretty(
            " i T
            + 1 Ripper
        ",
        );

        // test epoch check
        assert!(
            tokio::time::timeout(Duration::from_secs(10), sink.barrier(true))
                .await
                .expect("test failed: should not commit without epoch")
                .is_err(),
            "test failed: no epoch check for commit()"
        );
        assert!(
            request_recv.try_recv().is_err(),
            "test failed: unchecked epoch before request"
        );

        assert!(
            tokio::time::timeout(Duration::from_secs(1), sink.write_batch(chunk))
                .await
                .expect("test failed: should not write without epoch")
                .is_err(),
            "test failed: no epoch check for write_batch()"
        );
        assert!(
            request_recv.try_recv().is_err(),
            "test failed: unchecked epoch before request"
        );
    }

    #[tokio::test]
    async fn test_remote_sink() {
        let (request_sender, mut request_receiver) = mpsc::channel(16);
        let (response_sender, response_receiver) = mpsc::channel(16);
        let mut sink = CoordinatedRemoteSinkWriter::for_test(response_receiver, request_sender);

        let chunk_a = StreamChunk::from_pretty(
            " i T
            + 1 Alice
            + 2 Bob
            + 3 Clare
        ",
        );
        let chunk_b = StreamChunk::from_pretty(
            " i T
            + 4 David
            + 5 Eve
            + 6 Frank
        ",
        );

        // test write batch
        sink.begin_epoch(2022).await.unwrap();
        assert_eq!(sink.epoch, Some(2022));

        request_receiver
            .recv()
            .await
            .expect("test failed: failed to construct start_epoch request");

        sink.write_batch(chunk_a.clone()).await.unwrap();
        assert_eq!(sink.epoch, Some(2022));
        assert_eq!(sink.batch_id, 1);
        match request_receiver.recv().await {
            Some(SinkWriterStreamRequest {
                request: Some(Request::WriteBatch(write)),
            }) => {
                assert_eq!(write.epoch, 2022);
                assert_eq!(write.batch_id, 0);
                assert_eq!(write.payload.unwrap(), build_chunk_payload(chunk_a))
            }
            _ => panic!("test failed: failed to construct write request"),
        }

        // test commit
        response_sender
            .send(Ok(SinkWriterStreamResponse {
                response: Some(Response::Commit(CommitResponse {
                    epoch: 2022,
                    metadata: None,
                })),
            }))
            .await
            .expect("test failed: failed to sync epoch");
        sink.barrier(false).await.unwrap();
        let commit_request = request_receiver.recv().await.unwrap();
        match commit_request.request {
            Some(Request::Barrier(Barrier {
                epoch,
                is_checkpoint: false,
            })) => {
                assert_eq!(epoch, 2022);
            }
            _ => panic!("test failed: failed to construct sync request "),
        }

        // begin another epoch
        sink.begin_epoch(2023).await.unwrap();
        // simply keep the channel empty since we've tested begin_epoch
        let _ = request_receiver.recv().await.unwrap();
        assert_eq!(sink.epoch, Some(2023));

        // test another write
        sink.write_batch(chunk_b.clone()).await.unwrap();
        assert_eq!(sink.epoch, Some(2023));
        assert_eq!(sink.batch_id, 2);
        match request_receiver.recv().await {
            Some(SinkWriterStreamRequest {
                request: Some(Request::WriteBatch(write)),
            }) => {
                assert_eq!(write.epoch, 2023);
                assert_eq!(write.batch_id, 1);
                assert_eq!(write.payload.unwrap(), build_chunk_payload(chunk_b));
            }
            _ => panic!("test failed: failed to construct write request"),
        }
    }
}
