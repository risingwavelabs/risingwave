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
use std::fmt::Formatter;
use std::future::Future;
use std::marker::PhantomData;
use std::ops::Deref;
use std::pin::pin;
use std::time::Instant;

use anyhow::anyhow;
use async_trait::async_trait;
use futures::future::select;
use futures::stream::Peekable;
use futures::{StreamExt, TryFutureExt, TryStreamExt};
use itertools::Itertools;
use jni::objects::{JByteArray, JValue, JValueOwned};
use prost::Message;
use risingwave_common::array::StreamChunk;
use risingwave_common::error::anyhow_error;
use risingwave_common::types::DataType;
use risingwave_common::util::await_future_with_monitor_error_stream;
use risingwave_jni_core::jvm_runtime::JVM;
use risingwave_pb::connector_service::sink_coordinator_stream_request::{
    CommitMetadata, StartCoordinator,
};
use risingwave_pb::connector_service::sink_writer_stream_request::write_batch::{
    Payload, StreamChunkPayload,
};
use risingwave_pb::connector_service::sink_writer_stream_request::{
    Barrier, BeginEpoch, Request as SinkRequest, StartSink, WriteBatch,
};
use risingwave_pb::connector_service::sink_writer_stream_response::CommitResponse;
use risingwave_pb::connector_service::{
    sink_coordinator_stream_request, sink_coordinator_stream_response, sink_writer_stream_response,
    PbSinkPayloadFormat, SinkCoordinatorStreamRequest, SinkCoordinatorStreamResponse, SinkMetadata,
    SinkWriterStreamRequest, SinkWriterStreamResponse, ValidateSinkRequest, ValidateSinkResponse,
};
use tokio::sync::mpsc;
use tokio::sync::mpsc::{unbounded_channel, Receiver, Sender};
use tokio_stream::wrappers::ReceiverStream;
use tracing::warn;

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
        RemoteLogSinker::new::<R>(self.param.clone(), writer_param).await
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
            .get_or_init()
            .map_err(|err| SinkError::Internal(err.into()))?
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

pub struct RemoteLogSinker {
    request_tx: SinkWriterStreamJniSender,
    response_rx: SinkWriterStreamJniReceiver,
    sink_metrics: SinkMetrics,
}

impl RemoteLogSinker {
    async fn new<R: RemoteSinkTrait>(
        sink_param: SinkParam,
        writer_param: SinkWriterParam,
    ) -> Result<Self> {
        let SinkWriterStreamJniHandle {
            request_tx,
            response_rx,
        } = SinkWriterStreamJniHandle::new::<R>(&sink_param).await?;

        let sink_metrics = writer_param.sink_metrics;
        Ok(RemoteLogSinker {
            request_tx,
            response_rx,
            sink_metrics,
        })
    }
}

/// Await the given future while monitoring on error of the receiver stream.
async fn await_future_with_monitor_receiver_err<O, F: Future<Output = Result<O>>>(
    receiver: &mut SinkWriterStreamJniReceiver,
    future: F,
) -> Result<O> {
    match await_future_with_monitor_error_stream(&mut receiver.response_stream, future).await {
        Ok(result) => result,
        Err(None) => Err(SinkError::Remote(anyhow!("end of remote receiver stream"))),
        Err(Some(err)) => Err(SinkError::Internal(err)),
    }
}

#[async_trait]
impl LogSinker for RemoteLogSinker {
    async fn consume_log_and_sink(self, mut log_reader: impl LogReader) -> Result<()> {
        let mut request_tx = self.request_tx;
        let mut response_err_stream_rx = self.response_rx;
        let sink_metrics = self.sink_metrics;

        let (response_tx, mut response_rx) = unbounded_channel();

        let poll_response_stream = pin!(async move {
            loop {
                let result = response_err_stream_rx.response_stream.try_next().await;
                match result {
                    Ok(Some(response)) => {
                        response_tx.send(response).map_err(|err| {
                            SinkError::Remote(anyhow!("unable to send response: {:?}", err.0))
                        })?;
                    }
                    Ok(None) => return Err(SinkError::Remote(anyhow!("end of response stream"))),
                    Err(e) => return Err(SinkError::Remote(e)),
                }
            }
        });

        let poll_consume_log_and_sink = pin!(async move {
            #[derive(Debug)]
            enum LogConsumerState {
                /// Mark that the log consumer is not initialized yet
                Uninitialized,

                /// Mark that a new epoch has begun.
                EpochBegun { curr_epoch: u64, next_batch_id: u64 },

                /// Mark that the consumer has just received a barrier
                BarrierReceived { prev_epoch: u64 },
            }

            let mut state = LogConsumerState::Uninitialized;

            log_reader.init().await?;

            loop {
                let (epoch, item): (u64, LogStoreReadItem) =
                    log_reader.next_item().map_err(SinkError::Internal).await?;

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
                        request_tx.start_epoch(epoch).await?;
                        LogConsumerState::EpochBegun {
                            curr_epoch: epoch,
                            next_batch_id: 0,
                        }
                    }
                    LogConsumerState::EpochBegun {
                        curr_epoch,
                        next_batch_id,
                    } => {
                        assert!(
                            epoch >= curr_epoch,
                            "new epoch {} should not be below the current epoch {}",
                            epoch,
                            curr_epoch
                        );
                        LogConsumerState::EpochBegun {
                            curr_epoch: epoch,
                            next_batch_id,
                        }
                    }
                    LogConsumerState::BarrierReceived { prev_epoch } => {
                        assert!(
                            epoch > prev_epoch,
                            "new epoch {} should be greater than prev epoch {}",
                            epoch,
                            prev_epoch
                        );
                        request_tx.start_epoch(epoch).await?;
                        LogConsumerState::EpochBegun {
                            curr_epoch: epoch,
                            next_batch_id: 0,
                        }
                    }
                };
                match item {
                    LogStoreReadItem::StreamChunk { chunk, .. } => {
                        let cardinality = chunk.cardinality();
                        sink_metrics
                            .connector_sink_rows_received
                            .inc_by(cardinality as _);
                        let (epoch, next_batch_id) = match &mut state {
                            LogConsumerState::EpochBegun {
                                curr_epoch,
                                next_batch_id,
                            } => (*curr_epoch, next_batch_id),
                            _ => unreachable!("epoch must have begun before handling stream chunk"),
                        };

                        let batch_id = *next_batch_id;
                        *next_batch_id += 1;

                        let payload = build_chunk_payload(chunk);
                        request_tx.write_batch(epoch, batch_id, payload).await?;
                    }
                    LogStoreReadItem::Barrier { is_checkpoint } => {
                        let prev_epoch = match state {
                            LogConsumerState::EpochBegun { curr_epoch, .. } => curr_epoch,
                            _ => unreachable!("epoch must have begun before handling barrier"),
                        };
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
                        state = LogConsumerState::BarrierReceived { prev_epoch }
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
pub struct CoordinatedRemoteSink<R: RemoteSinkTrait>(pub RemoteSink<R>);

impl<R: RemoteSinkTrait> TryFrom<SinkParam> for CoordinatedRemoteSink<R> {
    type Error = SinkError;

    fn try_from(param: SinkParam) -> std::result::Result<Self, Self::Error> {
        RemoteSink::try_from(param).map(Self)
    }
}

impl<R: RemoteSinkTrait> Sink for CoordinatedRemoteSink<R> {
    type Coordinator = RemoteCoordinator;
    type LogSinker = LogSinkerOf<CoordinatedSinkWriter<CoordinatedRemoteSinkWriter>>;

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
            CoordinatedRemoteSinkWriter::new::<R>(
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
        RemoteCoordinator::new::<R>(self.0.param.clone()).await
    }
}

#[derive(Debug)]
pub struct SinkCoordinatorStreamJniHandle {
    request_tx: Sender<SinkCoordinatorStreamRequest>,
    response_rx: Receiver<SinkCoordinatorStreamResponse>,
}

impl SinkCoordinatorStreamJniHandle {
    pub async fn commit(&mut self, epoch: u64, metadata: Vec<SinkMetadata>) -> Result<()> {
        self.request_tx
            .send(SinkCoordinatorStreamRequest {
                request: Some(sink_coordinator_stream_request::Request::Commit(
                    CommitMetadata { epoch, metadata },
                )),
            })
            .await
            .map_err(|err| SinkError::Internal(err.into()))?;

        match self.response_rx.recv().await {
            Some(SinkCoordinatorStreamResponse {
                response:
                    Some(sink_coordinator_stream_response::Response::Commit(
                        sink_coordinator_stream_response::CommitResponse {
                            epoch: response_epoch,
                        },
                    )),
            }) => {
                if epoch == response_epoch {
                    Ok(())
                } else {
                    Err(SinkError::Internal(anyhow!(
                        "get different response epoch to commit epoch: {} {}",
                        epoch,
                        response_epoch
                    )))
                }
            }
            msg => Err(SinkError::Internal(anyhow!(
                "should get Commit response but get {:?}",
                msg
            ))),
        }
    }
}

struct SinkWriterStreamJniSender {
    request_tx: Sender<SinkWriterStreamRequest>,
}

impl SinkWriterStreamJniSender {
    pub async fn start_epoch(&mut self, epoch: u64) -> Result<()> {
        self.request_tx
            .send(SinkWriterStreamRequest {
                request: Some(SinkRequest::BeginEpoch(BeginEpoch { epoch })),
            })
            .await
            .map_err(|err| SinkError::Internal(err.into()))
    }

    pub async fn write_batch(&mut self, epoch: u64, batch_id: u64, payload: Payload) -> Result<()> {
        self.request_tx
            .send(SinkWriterStreamRequest {
                request: Some(SinkRequest::WriteBatch(WriteBatch {
                    epoch,
                    batch_id,
                    payload: Some(payload),
                })),
            })
            .await
            .map_err(|err| SinkError::Internal(err.into()))
    }

    pub async fn barrier(&mut self, epoch: u64, is_checkpoint: bool) -> Result<()> {
        self.request_tx
            .send(SinkWriterStreamRequest {
                request: Some(SinkRequest::Barrier(Barrier {
                    epoch,
                    is_checkpoint,
                })),
            })
            .await
            .map_err(|err| SinkError::Internal(err.into()))
    }
}

struct SinkWriterStreamJniReceiver {
    response_stream: Peekable<ReceiverStream<anyhow::Result<SinkWriterStreamResponse>>>,
}

impl SinkWriterStreamJniReceiver {
    async fn next_commit_response(&mut self) -> Result<CommitResponse> {
        match self.response_stream.try_next().await {
            Ok(Some(SinkWriterStreamResponse {
                response: Some(sink_writer_stream_response::Response::Commit(rsp)),
            })) => Ok(rsp),
            msg => Err(SinkError::Internal(anyhow!(
                "should get Sync response but get {:?}",
                msg
            ))),
        }
    }
}

const DEFAULT_CHANNEL_SIZE: usize = 16;
struct SinkWriterStreamJniHandle {
    request_tx: SinkWriterStreamJniSender,
    response_rx: SinkWriterStreamJniReceiver,
}

impl std::fmt::Debug for SinkWriterStreamJniHandle {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SinkWriterStreamJniHandle").finish()
    }
}

impl SinkWriterStreamJniHandle {
    async fn start_epoch(&mut self, epoch: u64) -> Result<()> {
        await_future_with_monitor_receiver_err(
            &mut self.response_rx,
            self.request_tx.start_epoch(epoch),
        )
        .await
    }

    async fn write_batch(&mut self, epoch: u64, batch_id: u64, payload: Payload) -> Result<()> {
        await_future_with_monitor_receiver_err(
            &mut self.response_rx,
            self.request_tx.write_batch(epoch, batch_id, payload),
        )
        .await
    }

    async fn barrier(&mut self, epoch: u64) -> Result<()> {
        await_future_with_monitor_receiver_err(
            &mut self.response_rx,
            self.request_tx.barrier(epoch, false),
        )
        .await
    }

    async fn commit(&mut self, epoch: u64) -> Result<CommitResponse> {
        await_future_with_monitor_receiver_err(
            &mut self.response_rx,
            self.request_tx.barrier(epoch, true),
        )
        .await?;
        self.response_rx.next_commit_response().await
    }
}

impl SinkWriterStreamJniHandle {
    async fn new<R: RemoteSinkTrait>(param: &SinkParam) -> Result<Self> {
        let (request_tx, request_rx) = mpsc::channel(DEFAULT_CHANNEL_SIZE);
        let (response_tx, response_rx) = mpsc::channel(DEFAULT_CHANNEL_SIZE);

        let mut response_stream = ReceiverStream::new(response_rx).peekable();

        std::thread::spawn(move || {
            let mut env = JVM.get_or_init().unwrap().attach_current_thread().unwrap();

            let result = env.call_static_method(
                "com/risingwave/connector/JniSinkWriterHandler",
                "runJniSinkWriterThread",
                "(JJ)V",
                &[
                    JValue::from(&request_rx as *const Receiver<SinkWriterStreamRequest> as i64),
                    JValue::from(
                        &response_tx as *const Sender<anyhow::Result<SinkWriterStreamResponse>>
                            as i64,
                    ),
                ],
            );

            match result {
                Ok(_) => {
                    tracing::info!("end of jni call runJniSinkWriterThread");
                }
                Err(e) => {
                    tracing::error!("jni call error: {:?}", e);
                }
            };
        });

        let sink_writer_stream_request = SinkWriterStreamRequest {
            request: Some(SinkRequest::Start(StartSink {
                sink_param: Some(param.to_proto()),
                format: PbSinkPayloadFormat::StreamChunk as i32,
            })),
        };

        // First request
        request_tx
            .send(sink_writer_stream_request)
            .await
            .map_err(|err| {
                SinkError::Internal(anyhow!(
                    "fail to send start request for connector `{}`: {:?}",
                    R::SINK_NAME,
                    err
                ))
            })?;

        // First response
        match response_stream.try_next().await {
            Ok(Some(SinkWriterStreamResponse {
                response: Some(sink_writer_stream_response::Response::Start(_)),
            })) => {}
            Ok(msg) => {
                return Err(SinkError::Internal(anyhow!(
                    "should get start response for connector `{}` but get {:?}",
                    R::SINK_NAME,
                    msg
                )));
            }
            Err(e) => return Err(SinkError::Internal(e)),
        };

        tracing::trace!(
            "{:?} sink stream started with properties: {:?}",
            R::SINK_NAME,
            &param.properties
        );

        Ok(SinkWriterStreamJniHandle {
            request_tx: SinkWriterStreamJniSender { request_tx },
            response_rx: SinkWriterStreamJniReceiver { response_stream },
        })
    }
}

pub type RemoteSinkWriter = RemoteSinkWriterInner<()>;
pub type CoordinatedRemoteSinkWriter = RemoteSinkWriterInner<Option<SinkMetadata>>;

pub struct RemoteSinkWriterInner<SM> {
    properties: HashMap<String, String>,
    epoch: Option<u64>,
    batch_id: u64,
    stream_handle: SinkWriterStreamJniHandle,
    sink_metrics: SinkMetrics,
    _phantom: PhantomData<SM>,
}

impl<SM> RemoteSinkWriterInner<SM> {
    pub async fn new<R: RemoteSinkTrait>(
        param: SinkParam,
        _connector_params: ConnectorParams,
        sink_metrics: SinkMetrics,
    ) -> Result<Self> {
        let stream_handle = SinkWriterStreamJniHandle::new::<R>(&param).await?;

        Ok(Self {
            properties: param.properties,
            epoch: None,
            batch_id: 0,
            stream_handle,
            sink_metrics,
            _phantom: PhantomData,
        })
    }

    #[cfg(test)]
    fn for_test(
        response_receiver: Receiver<anyhow::Result<SinkWriterStreamResponse>>,
        request_sender: Sender<SinkWriterStreamRequest>,
    ) -> RemoteSinkWriter {
        let properties = HashMap::from([("output.path".to_string(), "/tmp/rw".to_string())]);

        let stream_handle = SinkWriterStreamJniHandle {
            request_tx: SinkWriterStreamJniSender {
                request_tx: request_sender,
            },
            response_rx: SinkWriterStreamJniReceiver {
                response_stream: ReceiverStream::new(response_receiver).peekable(),
            },
        };

        RemoteSinkWriter {
            properties,
            epoch: None,
            batch_id: 0,
            stream_handle,
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

impl HandleBarrierResponse for RemoteSinkWriter {
    type SinkMetadata = ();

    fn handle_commit_response(rsp: CommitResponse) -> Result<Self::SinkMetadata> {
        if rsp.metadata.is_some() {
            warn!("get metadata in commit response for non-coordinated remote sink writer");
        }
        Ok(())
    }

    fn non_checkpoint_return_value() -> Self::SinkMetadata {}
}

impl HandleBarrierResponse for CoordinatedRemoteSinkWriter {
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

fn build_chunk_payload(chunk: StreamChunk) -> Payload {
    let prost_stream_chunk = chunk.to_protobuf();
    let binary_data = Message::encode_to_vec(&prost_stream_chunk);
    Payload::StreamChunkPayload(StreamChunkPayload { binary_data })
}

#[async_trait]
impl<SM: Send + 'static> SinkWriter for RemoteSinkWriterInner<SM>
where
    Self: HandleBarrierResponse<SinkMetadata = SM>,
{
    type CommitMetadata = SM;

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

pub struct RemoteCoordinator {
    stream_handle: SinkCoordinatorStreamJniHandle,
}

impl RemoteCoordinator {
    pub async fn new<R: RemoteSinkTrait>(param: SinkParam) -> Result<Self> {
        let (request_tx, request_rx) = mpsc::channel(DEFAULT_CHANNEL_SIZE);
        let (response_tx, response_rx) = mpsc::channel(DEFAULT_CHANNEL_SIZE);

        let mut stream_handle = SinkCoordinatorStreamJniHandle {
            request_tx,
            response_rx,
        };

        std::thread::spawn(move || {
            let mut env = JVM.get_or_init().unwrap().attach_current_thread().unwrap();

            let result = env.call_static_method(
                "com/risingwave/connector/JniSinkCoordinatorHandler",
                "runJniSinkCoordinatorThread",
                "(JJ)V",
                &[
                    JValue::from(
                        &request_rx as *const Receiver<SinkCoordinatorStreamRequest> as i64,
                    ),
                    JValue::from(
                        &response_tx as *const Sender<SinkCoordinatorStreamResponse> as i64,
                    ),
                ],
            );

            match result {
                Ok(_) => {
                    tracing::info!("end of jni call runJniSinkCoordinatorThread");
                }
                Err(e) => {
                    tracing::error!("jni call error: {:?}", e);
                }
            };
        });

        let sink_coordinator_stream_request = SinkCoordinatorStreamRequest {
            request: Some(sink_coordinator_stream_request::Request::Start(
                StartCoordinator {
                    param: Some(param.to_proto()),
                },
            )),
        };

        // First request
        stream_handle
            .request_tx
            .send(sink_coordinator_stream_request)
            .await
            .map_err(|err| {
                SinkError::Internal(anyhow!(
                    "fail to send start request for connector `{}`: {:?}",
                    R::SINK_NAME,
                    err
                ))
            })?;

        // First response
        match stream_handle.response_rx.recv().await {
            Some(SinkCoordinatorStreamResponse {
                response: Some(sink_coordinator_stream_response::Response::Start(_)),
            }) => {}
            msg => {
                return Err(SinkError::Internal(anyhow!(
                    "should get start response for connector `{}` but get {:?}",
                    R::SINK_NAME,
                    msg
                )));
            }
        };

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

#[cfg(test)]
mod test {
    use std::time::Duration;

    use risingwave_common::array::StreamChunk;
    use risingwave_common::test_prelude::StreamChunkTestExt;
    use risingwave_pb::connector_service::sink_writer_stream_request::{Barrier, Request};
    use risingwave_pb::connector_service::sink_writer_stream_response::{CommitResponse, Response};
    use risingwave_pb::connector_service::{SinkWriterStreamRequest, SinkWriterStreamResponse};
    use tokio::sync::mpsc;

    use crate::sink::remote::{build_chunk_payload, RemoteSinkWriter};
    use crate::sink::SinkWriter;

    #[tokio::test]
    async fn test_epoch_check() {
        let (request_sender, mut request_recv) = mpsc::channel(16);
        let (_, resp_recv) = mpsc::channel(16);

        let mut sink = RemoteSinkWriter::for_test(resp_recv, request_sender);
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
        let mut sink = RemoteSinkWriter::for_test(response_receiver, request_sender);

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
                assert_eq!(write.payload.unwrap(), build_chunk_payload(chunk_b));
            }
            _ => panic!("test failed: failed to construct write request"),
        }
    }
}
