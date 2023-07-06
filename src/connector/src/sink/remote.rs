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

use anyhow::anyhow;
use async_trait::async_trait;
use itertools::Itertools;
use prost::Message;
use risingwave_common::array::StreamChunk;
use risingwave_common::catalog::Schema;
use risingwave_common::types::DataType;
use risingwave_common::util::addr::HostAddr;
use risingwave_pb::connector_service::sink_writer_request::write_batch::json_payload::RowOp;
use risingwave_pb::connector_service::sink_writer_request::write_batch::{
    JsonPayload, Payload, StreamChunkPayload,
};
use risingwave_pb::connector_service::sink_writer_request::{
    Barrier, BeginEpoch, Request as SinkRequest, WriteBatch,
};
use risingwave_pb::connector_service::{SinkPayloadFormat, SinkWriterRequest, SinkWriterResponse};
use risingwave_rpc_client::ConnectorClient;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio_stream::StreamExt;
use tonic::{Status, Streaming};
use tracing::error;

use crate::sink::utils::{record_to_json, TimestampHandlingMode};
use crate::sink::{
    DummySinkCommitCoordinator, Result, Sink, SinkError, SinkParam, SinkWriterParam, SinkWriterV1,
    SinkWriterV1Adapter,
};
use crate::ConnectorParams;

pub const VALID_REMOTE_SINKS: [&str; 4] = ["jdbc", "iceberg", "deltalake", "elasticsearch-7"];

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
    type Writer = SinkWriterV1Adapter<RemoteSinkWriter>;

    async fn new_writer(&self, writer_param: SinkWriterParam) -> Result<Self::Writer> {
        Ok(SinkWriterV1Adapter::new(
            RemoteSinkWriter::new(
                self.config.clone(),
                self.param.clone(),
                writer_param.connector_params,
            )
            .await?,
        ))
    }

    async fn validate(&self, connector_rpc_endpoint: Option<String>) -> Result<()> {
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
                Err(SinkError::Remote(format!(
                    "remote sink supports Int16, Int32, Int64, Float32, Float64, Boolean, Decimal, Time, Date, Interval, Jsonb, Timestamp, Timestamptz, List, Bytea and Varchar, got {:?}: {:?}",
                    col.name,
                    col.data_type,
                )))
            }
        }).try_collect()?;

        let address = connector_rpc_endpoint.clone().ok_or_else(|| {
            SinkError::Remote("connector sink endpoint not specified".parse().unwrap())
        })?;
        let host_addr = HostAddr::try_from(&address).map_err(SinkError::from)?;
        let client = ConnectorClient::new(host_addr).await.map_err(|err| {
            SinkError::Remote(format!(
                "failed to connect to connector endpoint `{}`: {:?}",
                &address, err
            ))
        })?;

        // We validate a remote sink's accessibility as well as the pk.
        client
            .validate_sink_properties(self.param.to_proto())
            .await
            .map_err(SinkError::from)
    }
}

#[derive(Debug)]
enum ResponseStreamImpl {
    Grpc(Streaming<SinkWriterResponse>),
    Receiver(UnboundedReceiver<SinkWriterResponse>),
}

impl ResponseStreamImpl {
    pub async fn next(&mut self) -> Result<SinkWriterResponse> {
        match self {
            ResponseStreamImpl::Grpc(ref mut response) => response
                .next()
                .await
                .unwrap_or_else(|| Err(Status::cancelled("response stream closed unexpectedly")))
                .map_err(|e| SinkError::Remote(e.message().to_string())),
            ResponseStreamImpl::Receiver(ref mut receiver) => {
                receiver.recv().await.ok_or_else(|| {
                    SinkError::Remote("response stream closed unexpectedly".to_string())
                })
            }
        }
    }
}

#[derive(Debug)]
pub struct RemoteSinkWriter {
    pub connector_type: String,
    properties: HashMap<String, String>,
    epoch: Option<u64>,
    batch_id: u64,
    schema: Schema,
    _client: Option<ConnectorClient>,
    request_sender: Option<UnboundedSender<SinkWriterRequest>>,
    response_stream: ResponseStreamImpl,
    payload_format: SinkPayloadFormat,
}

impl RemoteSinkWriter {
    pub async fn new(
        config: RemoteConfig,
        param: SinkParam,
        connector_params: ConnectorParams,
    ) -> Result<Self> {
        let address = connector_params.connector_rpc_endpoint.ok_or_else(|| {
            SinkError::Remote("connector sink endpoint not specified".parse().unwrap())
        })?;
        let host_addr = HostAddr::try_from(&address).map_err(SinkError::from)?;
        let client = ConnectorClient::new(host_addr).await.map_err(|err| {
            let msg = format!(
                "failed to connect to connector endpoint `{}`: {:?}",
                &address, err
            );
            tracing::warn!(msg);
            SinkError::Remote(msg)
        })?;
        let (request_sender, response) = client
            .start_sink_stream(param.to_proto(), connector_params.sink_payload_format)
            .await
            .inspect_err(|e| {
                error!(
                    "failed to start sink stream for connector `{}`: {:?}",
                    &config.connector_type, e
                )
            })?;
        tracing::info!(
            "{:?} sink stream started with properties: {:?}",
            &config.connector_type,
            &config.properties
        );

        Ok(RemoteSinkWriter {
            connector_type: config.connector_type,
            properties: config.properties,
            epoch: None,
            batch_id: 0,
            schema: param.schema(),
            _client: Some(client),
            request_sender: Some(request_sender),
            response_stream: ResponseStreamImpl::Grpc(response),
            payload_format: connector_params.sink_payload_format,
        })
    }

    fn on_sender_alive(&mut self) -> Result<&UnboundedSender<SinkWriterRequest>> {
        self.request_sender
            .as_ref()
            .ok_or_else(|| SinkError::Remote("sink has been dropped".to_string()))
    }

    #[cfg(test)]
    fn for_test(
        response_receiver: UnboundedReceiver<SinkWriterResponse>,
        request_sender: UnboundedSender<SinkWriterRequest>,
    ) -> Self {
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

        Self {
            connector_type: "file".to_string(),
            properties,
            epoch: None,
            batch_id: 0,
            schema,
            _client: None,
            request_sender: Some(request_sender),
            response_stream: ResponseStreamImpl::Receiver(response_receiver),
            payload_format: SinkPayloadFormat::Json,
        }
    }
}

#[async_trait]
impl SinkWriterV1 for RemoteSinkWriter {
    async fn write_batch(&mut self, chunk: StreamChunk) -> Result<()> {
        let payload = match self.payload_format {
            SinkPayloadFormat::Json => {
                let mut row_ops = vec![];
                for (op, row_ref) in chunk.rows() {
                    let map = record_to_json(
                        row_ref,
                        &self.schema.fields,
                        TimestampHandlingMode::String,
                    )?;
                    let row_op = RowOp {
                        op_type: op.to_protobuf() as i32,
                        line: serde_json::to_string(&map)
                            .map_err(|e| SinkError::Remote(format!("{:?}", e)))?,
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
            SinkError::Remote("epoch has not been initialize, call `begin_epoch`".to_string())
        })?;
        let batch_id = self.batch_id;
        self.on_sender_alive()?
            .send(SinkWriterRequest {
                request: Some(SinkRequest::WriteBatch(WriteBatch {
                    epoch,
                    batch_id,
                    payload: Some(payload),
                })),
            })
            .map_err(|e| SinkError::Remote(e.to_string()))?;
        self.batch_id += 1;
        Ok(())
    }

    async fn begin_epoch(&mut self, epoch: u64) -> Result<()> {
        self.on_sender_alive()?
            .send(SinkWriterRequest {
                request: Some(SinkRequest::BeginEpoch(BeginEpoch { epoch })),
            })
            .map_err(|e| SinkError::Remote(e.to_string()))?;
        self.epoch = Some(epoch);
        Ok(())
    }

    async fn commit(&mut self) -> Result<()> {
        let epoch = self.epoch.ok_or_else(|| {
            SinkError::Remote("epoch has not been initialize, call `begin_epoch`".to_string())
        })?;
        self.on_sender_alive()?
            .send(SinkWriterRequest {
                request: Some(SinkRequest::Barrier(Barrier {
                    epoch,
                    is_checkpoint: true,
                })),
            })
            .map_err(|e| SinkError::Remote(e.to_string()))?;
        self.response_stream.next().await.map(|_| ())
    }

    async fn abort(&mut self) -> Result<()> {
        self.request_sender = None;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use risingwave_common::array::StreamChunk;
    use risingwave_common::test_prelude::StreamChunkTestExt;
    use risingwave_pb::connector_service::sink_writer_request::write_batch::Payload;
    use risingwave_pb::connector_service::sink_writer_request::{Barrier, Request};
    use risingwave_pb::connector_service::sink_writer_response::{Response, SyncResponse};
    use risingwave_pb::connector_service::{SinkWriterRequest, SinkWriterResponse};
    use risingwave_pb::data;
    use tokio::sync::mpsc;

    use crate::sink::remote::RemoteSinkWriter;
    use crate::sink::SinkWriterV1;

    #[tokio::test]
    async fn test_epoch_check() {
        let (request_sender, mut request_recv) = mpsc::unbounded_channel();
        let (_, resp_recv) = mpsc::unbounded_channel();

        let mut sink = RemoteSinkWriter::for_test(resp_recv, request_sender);
        let chunk = StreamChunk::from_pretty(
            " i T
            + 1 Ripper
        ",
        );

        // test epoch check
        assert!(
            tokio::time::timeout(Duration::from_secs(10), sink.commit())
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
        let (request_sender, mut request_receiver) = mpsc::unbounded_channel();
        let (response_sender, response_receiver) = mpsc::unbounded_channel();
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
            Some(SinkWriterRequest {
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
            .send(SinkWriterResponse {
                response: Some(Response::Sync(SyncResponse { epoch: 2022 })),
            })
            .expect("test failed: failed to sync epoch");
        sink.commit().await.unwrap();
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
            Some(SinkWriterRequest {
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
