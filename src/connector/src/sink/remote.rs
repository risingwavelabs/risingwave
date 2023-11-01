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

use std::any::Any;
use std::collections::HashMap;
use std::future::Future;
use std::marker::PhantomData;
use std::ops::Deref;
use std::time::Instant;

use anyhow::anyhow;
use async_trait::async_trait;
use futures::{StreamExt, TryFutureExt, TryStreamExt};
use itertools::Itertools;
use jni::objects::{JByteArray, JValue, JValueOwned};
use jni::JavaVM;
use prost::Message;
use risingwave_common::array::StreamChunk;
use risingwave_common::error::anyhow_error;
use risingwave_common::types::DataType;
use risingwave_common::util::await_future_with_monitor_error_stream;
use risingwave_jni_core::jvm_runtime::JVM;
use risingwave_jni_core::{gen_class_name, gen_jni_sig, JniReceiverType, JniSenderType};
use risingwave_pb::connector_service::sink_coordinator_stream_request::StartCoordinator;
use risingwave_pb::connector_service::sink_writer_stream_request::write_batch::json_payload::RowOp;
use risingwave_pb::connector_service::sink_writer_stream_request::write_batch::{
    JsonPayload, Payload, StreamChunkPayload,
};
use risingwave_pb::connector_service::sink_writer_stream_request::{
    Request as SinkRequest, StartSink,
};
use risingwave_pb::connector_service::sink_writer_stream_response::CommitResponse;
use risingwave_pb::connector_service::{
    sink_coordinator_stream_request, sink_coordinator_stream_response, sink_writer_stream_response,
    SinkCoordinatorStreamRequest, SinkCoordinatorStreamResponse, SinkMetadata, SinkPayloadFormat,
    SinkWriterStreamRequest, SinkWriterStreamResponse, ValidateSinkRequest, ValidateSinkResponse,
};
use risingwave_rpc_client::error::RpcError;
use risingwave_rpc_client::{
    BidiStreamReceiver, SinkCoordinatorStreamHandle, SinkWriterStreamHandle, DEFAULT_BUFFER_SIZE,
};
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio_stream::wrappers::ReceiverStream;
use tracing::warn;

use super::encoder::{JsonEncoder, RowEncoder};
use crate::sink::coordinate::CoordinatedSinkWriter;
use crate::sink::encoder::{TimestampHandlingMode, TimestamptzHandlingMode};
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
    type LogSinker = RemoteLogSinker<R>;

    const SINK_NAME: &'static str = R::SINK_NAME;

    async fn new_log_sinker(&self, writer_param: SinkWriterParam) -> Result<Self::LogSinker> {
        RemoteLogSinker::new(self.param.clone(), writer_param).await
    }

    async fn validate(&self) -> Result<()> {
        // FIXME: support struct and array in stream sink
        self.param.columns.iter().map(|col| {
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

        let mut env = JVM
            .get_or_init()?
            .attach_current_thread()
            .map_err(|err| SinkError::Internal(err.into()))?;
        let validate_sink_request = ValidateSinkRequest {
            sink_param: Some(self.param.to_proto()),
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
    }
}

pub struct RemoteLogSinker<R: RemoteSinkTrait> {
    writer: RemoteSinkWriter<R>,
    sink_metrics: SinkMetrics,
}

impl<R: RemoteSinkTrait> RemoteLogSinker<R> {
    async fn new(sink_param: SinkParam, writer_param: SinkWriterParam) -> Result<Self> {
        let writer = RemoteSinkWriter::new(
            sink_param,
            writer_param.connector_params,
            writer_param.sink_metrics.clone(),
        )
        .await?;
        let sink_metrics = writer_param.sink_metrics;
        Ok(RemoteLogSinker {
            writer,
            sink_metrics,
        })
    }
}

/// Await the given future while monitoring on error of the receiver stream.
async fn await_future_with_monitor_receiver_err<O, F: Future<Output = Result<O>>>(
    receiver: &mut BidiStreamReceiver<impl Any>,
    future: F,
) -> Result<O> {
    match await_future_with_monitor_error_stream(&mut receiver.stream, future).await {
        Ok(result) => Ok(result?),
        Err(None) => Err(SinkError::Remote(anyhow!("end of remote receiver stream"))),
        Err(Some(err)) => Err(SinkError::Remote(err.into())),
    }
}

#[async_trait]
impl<R: RemoteSinkTrait> LogSinker for RemoteLogSinker<R> {
    async fn consume_log_and_sink(self, mut log_reader: impl LogReader) -> Result<()> {
        // Note: this is a total copy of the implementation of LogSinkerOf<impl SinkWriter>,
        // except that we monitor the future of `log_reader.next_item` with await_future_with_monitor_receiver_err
        // to monitor the error in the response stream.

        let mut sink_writer = self.writer;
        let sink_metrics = self.sink_metrics;
        #[derive(Debug)]
        enum LogConsumerState {
            /// Mark that the log consumer is not initialized yet
            Uninitialized,

            /// Mark that a new epoch has begun.
            EpochBegun { curr_epoch: u64 },

            /// Mark that the consumer has just received a barrier
            BarrierReceived { prev_epoch: u64 },
        }

        let mut state = LogConsumerState::Uninitialized;

        log_reader.init().await?;

        loop {
            let (epoch, item): (u64, LogStoreReadItem) = await_future_with_monitor_receiver_err(
                &mut sink_writer.stream_handle.response_stream,
                log_reader.next_item().map_err(SinkError::Internal),
            )
            .await?;
            if let LogStoreReadItem::UpdateVnodeBitmap(_) = &item {
                match &state {
                    LogConsumerState::BarrierReceived { .. } => {}
                    _ => unreachable!(
                        "update vnode bitmap can be accepted only right after \
                    barrier, but current state is {:?}",
                        state
                    ),
                }
            }
            // begin_epoch when not previously began
            state = match state {
                LogConsumerState::Uninitialized => {
                    sink_writer.begin_epoch(epoch).await?;
                    LogConsumerState::EpochBegun { curr_epoch: epoch }
                }
                LogConsumerState::EpochBegun { curr_epoch } => {
                    assert!(
                        epoch >= curr_epoch,
                        "new epoch {} should not be below the current epoch {}",
                        epoch,
                        curr_epoch
                    );
                    LogConsumerState::EpochBegun { curr_epoch: epoch }
                }
                LogConsumerState::BarrierReceived { prev_epoch } => {
                    assert!(
                        epoch > prev_epoch,
                        "new epoch {} should be greater than prev epoch {}",
                        epoch,
                        prev_epoch
                    );
                    sink_writer.begin_epoch(epoch).await?;
                    LogConsumerState::EpochBegun { curr_epoch: epoch }
                }
            };
            match item {
                LogStoreReadItem::StreamChunk { chunk, .. } => {
                    if let Err(e) = sink_writer.write_batch(chunk).await {
                        sink_writer.abort().await?;
                        return Err(e);
                    }
                }
                LogStoreReadItem::Barrier { is_checkpoint } => {
                    let prev_epoch = match state {
                        LogConsumerState::EpochBegun { curr_epoch } => curr_epoch,
                        _ => unreachable!("epoch must have begun before handling barrier"),
                    };
                    if is_checkpoint {
                        let start_time = Instant::now();
                        sink_writer.barrier(true).await?;
                        sink_metrics
                            .sink_commit_duration_metrics
                            .observe(start_time.elapsed().as_millis() as f64);
                        log_reader
                            .truncate(TruncateOffset::Barrier { epoch })
                            .await?;
                    } else {
                        sink_writer.barrier(false).await?;
                    }
                    state = LogConsumerState::BarrierReceived { prev_epoch }
                }
                LogStoreReadItem::UpdateVnodeBitmap(vnode_bitmap) => {
                    sink_writer.update_vnode_bitmap(vnode_bitmap).await?;
                }
            }
        }
    }
}

#[derive(Debug)]
pub struct CoordinatedRemoteSink<R: RemoteSinkTrait>(pub RemoteSink<R>);

impl<R: RemoteSinkTrait> TryFrom<SinkParam> for CoordinatedRemoteSink<R> {
    type Error = SinkError;

    fn try_from(param: SinkParam) -> std::result::Result<Self, Self::Error> {
        RemoteSink::try_from(param).map(Self)
    }
}

impl<R: RemoteSinkTrait> Sink for CoordinatedRemoteSink<R> {
    type Coordinator = RemoteCoordinator<R>;
    type LogSinker = LogSinkerOf<CoordinatedSinkWriter<CoordinatedRemoteSinkWriter<R>>>;

    const SINK_NAME: &'static str = R::SINK_NAME;

    async fn validate(&self) -> Result<()> {
        self.0.validate().await
    }

    async fn new_log_sinker(&self, writer_param: SinkWriterParam) -> Result<Self::LogSinker> {
        Ok(CoordinatedSinkWriter::new(
            writer_param
                .meta_client
                .expect("should have meta client")
                .sink_coordinate_client()
                .await,
            self.0.param.clone(),
            writer_param.vnode_bitmap.ok_or_else(|| {
                SinkError::Remote(anyhow_error!(
                    "sink needs coordination should not have singleton input"
                ))
            })?,
            CoordinatedRemoteSinkWriter::new(
                self.0.param.clone(),
                writer_param.connector_params,
                writer_param.sink_metrics.clone(),
            )
            .await?,
        )
        .await?
        .into_log_sinker(writer_param.sink_metrics))
    }

    async fn new_coordinator(&self) -> Result<Self::Coordinator> {
        RemoteCoordinator::new(self.0.param.clone()).await
    }
}

pub type RemoteSinkWriter<R> = RemoteSinkWriterInner<(), R>;
pub type CoordinatedRemoteSinkWriter<R> = RemoteSinkWriterInner<Option<SinkMetadata>, R>;

pub struct RemoteSinkWriterInner<SM, R: RemoteSinkTrait> {
    properties: HashMap<String, String>,
    epoch: Option<u64>,
    batch_id: u64,
    payload_format: SinkPayloadFormat,
    stream_handle: SinkWriterStreamHandle,
    json_encoder: JsonEncoder,
    sink_metrics: SinkMetrics,
    _phantom: PhantomData<(SM, R)>,
}

impl<SM, R: RemoteSinkTrait> RemoteSinkWriterInner<SM, R> {
    pub async fn new(
        param: SinkParam,
        connector_params: ConnectorParams,
        sink_metrics: SinkMetrics,
    ) -> Result<Self> {
        let stream_handle = EmbeddedConnectorClient::new()?
            .start_sink_writer_stream(param.clone(), connector_params.sink_payload_format)
            .await?;

        let schema = param.schema();

        Ok(Self {
            properties: param.properties,
            epoch: None,
            batch_id: 0,
            stream_handle,
            payload_format: connector_params.sink_payload_format,
            json_encoder: JsonEncoder::new(
                schema,
                None,
                TimestampHandlingMode::String,
                TimestamptzHandlingMode::UtcWithoutSuffix,
            ),
            sink_metrics,
            _phantom: PhantomData,
        })
    }

    fn for_test(
        response_receiver: Receiver<anyhow::Result<SinkWriterStreamResponse>>,
        request_sender: Sender<SinkWriterStreamRequest>,
    ) -> RemoteSinkWriter<R> {
        use risingwave_common::catalog::{Field, Schema};
        let properties = HashMap::from([("output.path".to_string(), "/tmp/rw".to_string())]);

        let schema = Schema::new(vec![
            Field {
                data_type: DataType::Int32,
                name: "id".into(),
                sub_fields: vec![],
                type_name: "".into(),
            },
            Field {
                data_type: DataType::Varchar,
                name: "name".into(),
                sub_fields: vec![],
                type_name: "".into(),
            },
        ]);

        let stream_handle = SinkWriterStreamHandle::for_test(
            request_sender,
            ReceiverStream::new(response_receiver)
                .map_err(RpcError::from)
                .boxed(),
        );

        RemoteSinkWriter {
            properties,
            epoch: None,
            batch_id: 0,
            json_encoder: JsonEncoder::new(
                schema,
                None,
                TimestampHandlingMode::String,
                TimestamptzHandlingMode::UtcWithoutSuffix,
            ),
            stream_handle,
            payload_format: SinkPayloadFormat::Json,
            sink_metrics: SinkMetrics::for_test(),
            _phantom: PhantomData,
        }
    }
}

trait HandleBarrierResponse {
    type SinkMetadata: Send;
    fn handle_commit_response(rsp: CommitResponse) -> Result<Self::SinkMetadata>;
    fn non_checkpoint_return_value() -> Self::SinkMetadata;
}

impl<R: RemoteSinkTrait> HandleBarrierResponse for RemoteSinkWriter<R> {
    type SinkMetadata = ();

    fn handle_commit_response(rsp: CommitResponse) -> Result<Self::SinkMetadata> {
        if rsp.metadata.is_some() {
            warn!("get metadata in commit response for non-coordinated remote sink writer");
        }
        Ok(())
    }

    fn non_checkpoint_return_value() -> Self::SinkMetadata {}
}

impl<R: RemoteSinkTrait> HandleBarrierResponse for CoordinatedRemoteSinkWriter<R> {
    type SinkMetadata = Option<SinkMetadata>;

    fn handle_commit_response(rsp: CommitResponse) -> Result<Self::SinkMetadata> {
        rsp.metadata
            .ok_or_else(|| {
                SinkError::Remote(anyhow_error!(
                    "get none metadata in commit response for coordinated sink writer"
                ))
            })
            .map(Some)
    }

    fn non_checkpoint_return_value() -> Self::SinkMetadata {
        None
    }
}

#[async_trait]
impl<SM: Send + 'static, R: RemoteSinkTrait> SinkWriter for RemoteSinkWriterInner<SM, R>
where
    Self: HandleBarrierResponse<SinkMetadata = SM>,
{
    type CommitMetadata = SM;

    async fn write_batch(&mut self, chunk: StreamChunk) -> Result<()> {
        let cardinality = chunk.cardinality();
        self.sink_metrics
            .connector_sink_rows_received
            .inc_by(cardinality as _);
        let payload = match self.payload_format {
            SinkPayloadFormat::Json => {
                let mut row_ops = Vec::with_capacity(cardinality);
                for (op, row_ref) in chunk.rows() {
                    let map = self.json_encoder.encode(row_ref)?;
                    let row_op = RowOp {
                        op_type: op.to_protobuf() as i32,
                        line: serde_json::to_string(&map)
                            .map_err(|e| SinkError::Remote(anyhow_error!("{:?}", e)))?,
                    };

                    row_ops.push(row_op);
                }
                Payload::JsonPayload(JsonPayload { row_ops })
            }
            SinkPayloadFormat::StreamChunk => {
                let prost_stream_chunk = chunk.to_protobuf();
                let binary_data = Message::encode_to_vec(&prost_stream_chunk);
                Payload::StreamChunkPayload(StreamChunkPayload { binary_data })
            }
            SinkPayloadFormat::FormatUnspecified => {
                unreachable!("should specify sink payload format")
            }
        };

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

    async fn barrier(&mut self, is_checkpoint: bool) -> Result<SM> {
        let epoch = self.epoch.ok_or_else(|| {
            SinkError::Remote(anyhow_error!(
                "epoch has not been initialize, call `begin_epoch`"
            ))
        })?;
        if is_checkpoint {
            // TODO: add metrics to measure commit time
            let rsp = self.stream_handle.commit(epoch).await?;
            Ok(<Self as HandleBarrierResponse>::handle_commit_response(
                rsp,
            )?)
        } else {
            self.stream_handle.barrier(epoch).await?;
            Ok(<Self as HandleBarrierResponse>::non_checkpoint_return_value())
        }
    }
}

pub struct RemoteCoordinator<R: RemoteSinkTrait> {
    stream_handle: SinkCoordinatorStreamHandle,
    _phantom: PhantomData<R>,
}

impl<R: RemoteSinkTrait> RemoteCoordinator<R> {
    pub async fn new(param: SinkParam) -> Result<Self> {
        let stream_handle = EmbeddedConnectorClient::new()?
            .start_sink_coordinator_stream(param.clone())
            .await?;

        tracing::trace!(
            "{:?} RemoteCoordinator started with properties: {:?}",
            R::SINK_NAME,
            &param.properties
        );

        Ok(RemoteCoordinator {
            stream_handle,
            _phantom: PhantomData,
        })
    }
}

#[async_trait]
impl<R: RemoteSinkTrait> SinkCommitCoordinator for RemoteCoordinator<R> {
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

    pub async fn start_sink_coordinator_stream(
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
    use risingwave_pb::connector_service::sink_writer_stream_request::write_batch::Payload;
    use risingwave_pb::connector_service::sink_writer_stream_request::{Barrier, Request};
    use risingwave_pb::connector_service::sink_writer_stream_response::{CommitResponse, Response};
    use risingwave_pb::connector_service::{SinkWriterStreamRequest, SinkWriterStreamResponse};
    use risingwave_pb::data;
    use tokio::sync::mpsc;

    use crate::sink::remote::{RemoteSinkTrait, RemoteSinkWriter};
    use crate::sink::SinkWriter;

    struct TestRemote;
    impl RemoteSinkTrait for TestRemote {
        const SINK_NAME: &'static str = "test-remote";
    }

    #[tokio::test]
    async fn test_epoch_check() {
        let (request_sender, mut request_recv) = mpsc::channel(16);
        let (_, resp_recv) = mpsc::channel(16);

        let mut sink = <RemoteSinkWriter<TestRemote>>::for_test(resp_recv, request_sender);
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
        let mut sink = <RemoteSinkWriter<TestRemote>>::for_test(response_receiver, request_sender);

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
                match write.payload.unwrap() {
                    Payload::JsonPayload(json) => {
                        let row_0 = json.row_ops.get(0).unwrap();
                        assert_eq!(row_0.line, "{\"id\":1,\"name\":\"Alice\"}");
                        assert_eq!(row_0.op_type, data::Op::Insert as i32);
                        let row_1 = json.row_ops.get(1).unwrap();
                        assert_eq!(row_1.line, "{\"id\":2,\"name\":\"Bob\"}");
                        assert_eq!(row_1.op_type, data::Op::Insert as i32);
                        let row_2 = json.row_ops.get(2).unwrap();
                        assert_eq!(row_2.line, "{\"id\":3,\"name\":\"Clare\"}");
                        assert_eq!(row_2.op_type, data::Op::Insert as i32);
                    }
                    _ => unreachable!("should be json payload"),
                }
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
        sink.barrier(true).await.unwrap();
        let commit_request = request_receiver.recv().await.unwrap();
        match commit_request.request {
            Some(Request::Barrier(Barrier {
                epoch,
                is_checkpoint: true,
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
                match write.payload.unwrap() {
                    Payload::JsonPayload(json) => {
                        let row_0 = json.row_ops.get(0).unwrap();
                        assert_eq!(row_0.line, "{\"id\":4,\"name\":\"David\"}");
                        assert_eq!(row_0.op_type, data::Op::Insert as i32);
                        let row_1 = json.row_ops.get(1).unwrap();
                        assert_eq!(row_1.line, "{\"id\":5,\"name\":\"Eve\"}");
                        assert_eq!(row_1.op_type, data::Op::Insert as i32);
                        let row_2 = json.row_ops.get(2).unwrap();
                        assert_eq!(row_2.line, "{\"id\":6,\"name\":\"Frank\"}");
                        assert_eq!(row_2.op_type, data::Op::Insert as i32);
                    }
                    _ => unreachable!("should be json payload"),
                }
            }
            _ => panic!("test failed: failed to construct write request"),
        }
    }
}
