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

use anyhow::anyhow;
use async_trait::async_trait;
use itertools::Itertools;
use jni::objects::{JByteArray, JValue, JValueOwned};
use prost::Message;
use risingwave_common::array::StreamChunk;
use risingwave_common::catalog::Schema;
use risingwave_common::error::anyhow_error;
use risingwave_common::types::DataType;
use risingwave_jni_core::jvm_runtime::JVM;
use risingwave_pb::connector_service::sink_writer_stream_request::write_batch::json_payload::RowOp;
use risingwave_pb::connector_service::sink_writer_stream_request::write_batch::{
    JsonPayload, Payload, StreamChunkPayload,
};
use risingwave_pb::connector_service::sink_writer_stream_request::{
    Barrier, BeginEpoch, Request as SinkRequest, StartSink, WriteBatch,
};
use risingwave_pb::connector_service::sink_writer_stream_response::CommitResponse;
use risingwave_pb::connector_service::{
    sink_writer_stream_response, SinkMetadata, SinkPayloadFormat, SinkWriterStreamRequest,
    SinkWriterStreamResponse, ValidateSinkRequest, ValidateSinkResponse,
};
use risingwave_rpc_client::{ConnectorClient, SinkCoordinatorStreamHandle};
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::warn;

use super::encoder::{JsonEncoder, RowEncoder, TimestampHandlingMode};
use crate::sink::coordinate::CoordinatedSinkWriter;
use crate::sink::iceberg::REMOTE_ICEBERG_SINK;
use crate::sink::SinkError::Remote;
use crate::sink::{
    DummySinkCommitCoordinator, Result, Sink, SinkCommitCoordinator, SinkError, SinkParam,
    SinkWriter, SinkWriterParam,
};
use crate::ConnectorParams;

pub const VALID_REMOTE_SINKS: [&str; 5] = [
    "jdbc",
    REMOTE_ICEBERG_SINK,
    "deltalake",
    "elasticsearch",
    "cassandra",
];

pub fn is_valid_remote_sink(connector_type: &str) -> bool {
    VALID_REMOTE_SINKS.contains(&connector_type)
}

#[derive(Clone, Debug)]
pub struct RemoteConfig {
    pub connector_type: String,
    pub properties: HashMap<String, String>,
}

impl RemoteConfig {
    pub fn from_hashmap(values: HashMap<String, String>) -> Result<Self> {
        let connector_type = values
            .get("connector")
            .expect("sink type must be specified")
            .to_string();

        if !is_valid_remote_sink(connector_type.as_str()) {
            return Err(SinkError::Config(anyhow!(
                "invalid connector type: {connector_type}"
            )));
        }

        Ok(RemoteConfig {
            connector_type,
            properties: values,
        })
    }
}

#[derive(Debug)]
pub struct RemoteSink {
    config: RemoteConfig,
    param: SinkParam,
}

impl RemoteSink {
    pub fn new(config: RemoteConfig, param: SinkParam) -> Self {
        Self { config, param }
    }
}

#[async_trait]
impl Sink for RemoteSink {
    type Coordinator = DummySinkCommitCoordinator;
    type Writer = RemoteSinkWriter;

    async fn new_writer(&self, writer_param: SinkWriterParam) -> Result<Self::Writer> {
        Ok(RemoteSinkWriter::new(
            self.config.clone(),
            self.param.clone(),
            writer_param.connector_params,
        )
        .await?)
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
            .as_ref()
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

#[derive(Debug)]
pub struct CoordinatedRemoteSink(pub RemoteSink);

#[async_trait]
impl Sink for CoordinatedRemoteSink {
    type Coordinator = RemoteCoordinator;
    type Writer = CoordinatedSinkWriter<CoordinatedRemoteSinkWriter>;

    async fn validate(&self) -> Result<()> {
        self.0.validate().await
    }

    async fn new_writer(&self, writer_param: SinkWriterParam) -> Result<Self::Writer> {
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
                self.0.config.clone(),
                self.0.param.clone(),
                writer_param.connector_params,
            )
            .await?,
        )
        .await?)
    }

    async fn new_coordinator(
        &self,
        connector_client: Option<ConnectorClient>,
    ) -> Result<Self::Coordinator> {
        Ok(RemoteCoordinator::new(
            connector_client
                .ok_or_else(|| Remote(anyhow_error!("no connector client specified")))?,
            self.0.param.clone(),
        )
        .await?)
    }
}

const DEFAULT_CHANNEL_SIZE: usize = 16;
#[derive(Debug)]
pub struct SinkWriterStreamJniHandle {
    request_tx: Sender<SinkWriterStreamRequest>,
    response_rx: Receiver<SinkWriterStreamResponse>,
}

impl SinkWriterStreamJniHandle {
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

    pub async fn barrier(&mut self, epoch: u64) -> Result<()> {
        self.request_tx
            .send(SinkWriterStreamRequest {
                request: Some(SinkRequest::Barrier(Barrier {
                    epoch,
                    is_checkpoint: false,
                })),
            })
            .await
            .map_err(|err| SinkError::Internal(err.into()))
    }

    pub async fn commit(&mut self, epoch: u64) -> Result<CommitResponse> {
        self.request_tx
            .send(SinkWriterStreamRequest {
                request: Some(SinkRequest::Barrier(Barrier {
                    epoch,
                    is_checkpoint: true,
                })),
            })
            .await
            .map_err(|err| SinkError::Internal(err.into()))?;

        match self.response_rx.recv().await {
            Some(SinkWriterStreamResponse {
                response: Some(sink_writer_stream_response::Response::Commit(rsp)),
            }) => Ok(rsp),
            msg => Err(SinkError::Internal(anyhow!(
                "should get Sync response but get {:?}",
                msg
            ))),
        }
    }
}

pub type RemoteSinkWriter = RemoteSinkWriterInner<()>;
pub type CoordinatedRemoteSinkWriter = RemoteSinkWriterInner<Option<SinkMetadata>>;

pub struct RemoteSinkWriterInner<SM> {
    pub connector_type: String,
    properties: HashMap<String, String>,
    epoch: Option<u64>,
    batch_id: u64,
    schema: Schema,
    payload_format: SinkPayloadFormat,
    stream_handle: SinkWriterStreamJniHandle,
    json_encoder: JsonEncoder,
    _phantom: PhantomData<SM>,
}

impl<SM> RemoteSinkWriterInner<SM> {
    pub async fn new(
        config: RemoteConfig,
        param: SinkParam,
        connector_params: ConnectorParams,
    ) -> Result<Self> {
        let (request_tx, request_rx) = mpsc::channel(DEFAULT_CHANNEL_SIZE);
        let (response_tx, response_rx) = mpsc::channel(DEFAULT_CHANNEL_SIZE);

        let mut stream_handle = SinkWriterStreamJniHandle {
            request_tx,
            response_rx,
        };

        std::thread::spawn(move || {
            let mut env = JVM.as_ref().unwrap().attach_current_thread().unwrap();

            let result = env.call_static_method(
                "com/risingwave/connector/JniSinkWriterHandler",
                "runJniSinkWriterThread",
                "(JJ)V",
                &[
                    JValue::from(&request_rx as *const Receiver<SinkWriterStreamRequest> as i64),
                    JValue::from(&response_tx as *const Sender<SinkWriterStreamResponse> as i64),
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
                format: connector_params.sink_payload_format as i32,
            })),
        };

        // First request
        stream_handle
            .request_tx
            .send(sink_writer_stream_request)
            .await
            .map_err(|err| SinkError::Internal(err.into()))?;

        // First response
        match stream_handle.response_rx.recv().await {
            Some(SinkWriterStreamResponse {
                response: Some(sink_writer_stream_response::Response::Start(_)),
            }) => {}
            msg => {
                return Err(SinkError::Internal(anyhow!(
                    "should get start response but get {:?}",
                    msg
                )));
            }
        };

        tracing::trace!(
            "{:?} sink stream started with properties: {:?}",
            &config.connector_type,
            &config.properties
        );

        Ok(Self {
            connector_type: config.connector_type,
            properties: config.properties,
            epoch: None,
            batch_id: 0,
            schema: param.schema(),
            stream_handle,
            payload_format: connector_params.sink_payload_format,
            json_encoder: JsonEncoder::new(param.schema(), None, TimestampHandlingMode::String),
            _phantom: PhantomData,
        })
    }

    #[cfg(test)]
    fn for_test(
        response_receiver: Receiver<SinkWriterStreamResponse>,
        request_sender: Sender<SinkWriterStreamRequest>,
    ) -> RemoteSinkWriter {
        use risingwave_common::catalog::Field;
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

        let stream_handle = SinkWriterStreamJniHandle {
            request_tx: request_sender,
            response_rx: response_receiver,
        };

        RemoteSinkWriter {
            connector_type: "file".to_string(),
            properties,
            epoch: None,
            batch_id: 0,
            json_encoder: JsonEncoder::new(schema.clone(), None, TimestampHandlingMode::String),
            schema,
            stream_handle,
            payload_format: SinkPayloadFormat::Json,
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

#[async_trait]
impl<SM: Send + 'static> SinkWriter for RemoteSinkWriterInner<SM>
where
    Self: HandleBarrierResponse<SinkMetadata = SM>,
{
    type CommitMetadata = SM;

    async fn write_batch(&mut self, chunk: StreamChunk) -> Result<()> {
        let payload = match self.payload_format {
            SinkPayloadFormat::Json => {
                let mut row_ops = Vec::with_capacity(chunk.cardinality());
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

pub struct RemoteCoordinator {
    stream_handle: SinkCoordinatorStreamHandle,
}

impl RemoteCoordinator {
    pub async fn new(client: ConnectorClient, param: SinkParam) -> Result<Self> {
        let stream_handle = client
            .start_sink_coordinator_stream(param.to_proto())
            .await?;
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
    use risingwave_pb::connector_service::sink_writer_stream_request::write_batch::Payload;
    use risingwave_pb::connector_service::sink_writer_stream_request::{Barrier, Request};
    use risingwave_pb::connector_service::sink_writer_stream_response::{CommitResponse, Response};
    use risingwave_pb::connector_service::{SinkWriterStreamRequest, SinkWriterStreamResponse};
    use risingwave_pb::data;
    use tokio::sync::mpsc;

    use crate::sink::remote::RemoteSinkWriter;
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
            .send(SinkWriterStreamResponse {
                response: Some(Response::Commit(CommitResponse {
                    epoch: 2022,
                    metadata: None,
                })),
            })
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
