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
#[cfg(test)]
use risingwave_common::catalog::Field;
use risingwave_common::catalog::Schema;
use risingwave_common::types::DataType;
use risingwave_common::util::addr::HostAddr;
use risingwave_pb::connector_service::sink_stream_request::write_batch::json_payload::RowOp;
use risingwave_pb::connector_service::sink_stream_request::write_batch::{
    JsonPayload, Payload, StreamChunkPayload,
};
use risingwave_pb::connector_service::sink_stream_request::{
    Request as SinkRequest, StartEpoch, SyncBatch, WriteBatch,
};
use risingwave_pb::connector_service::table_schema::Column;
use risingwave_pb::connector_service::{
    SinkPayloadFormat, SinkResponse, SinkStreamRequest, TableSchema,
};
use risingwave_rpc_client::ConnectorClient;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio_stream::StreamExt;
use tonic::{Status, Streaming};

use super::catalog::SinkCatalog;
use crate::sink::{record_to_json, Result, Sink, SinkError};
use crate::ConnectorParams;

pub const VALID_REMOTE_SINKS: [&str; 3] = ["jdbc", "file", "iceberg"];

pub fn is_valid_remote_sink(connector_type: &str) -> bool {
    VALID_REMOTE_SINKS.contains(&connector_type)
}

#[derive(Clone, Debug)]
pub struct RemoteConfig {
    pub connector_type: String,
    pub sink_id: u64,
    pub properties: HashMap<String, String>,
}

impl RemoteConfig {
    pub fn from_hashmap(sink_id: u64, values: HashMap<String, String>) -> Result<Self> {
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
            sink_id,
            properties: values,
        })
    }
}

#[derive(Debug)]
enum ResponseStreamImpl {
    Grpc(Streaming<SinkResponse>),
    Receiver(UnboundedReceiver<SinkResponse>),
}

impl ResponseStreamImpl {
    pub async fn next(&mut self) -> Result<SinkResponse> {
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
pub struct RemoteSink<const APPEND_ONLY: bool> {
    pub connector_type: String,
    properties: HashMap<String, String>,
    epoch: Option<u64>,
    batch_id: u64,
    schema: Schema,
    _client: Option<ConnectorClient>,
    request_sender: Option<UnboundedSender<SinkStreamRequest>>,
    response_stream: ResponseStreamImpl,
    payload_format: SinkPayloadFormat,
}

impl<const APPEND_ONLY: bool> RemoteSink<APPEND_ONLY> {
    pub async fn new(
        config: RemoteConfig,
        schema: Schema,
        pk_indices: Vec<usize>,
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

        let table_schema = Some(TableSchema {
            columns: schema
                .fields()
                .iter()
                .map(|c| Column {
                    name: c.name.clone(),
                    data_type: c.data_type().to_protobuf().type_name,
                })
                .collect(),
            pk_indices: pk_indices.iter().map(|i| *i as u32).collect(),
        });
        let (request_sender, mut response) = client
            .start_sink_stream(
                config.connector_type.clone(),
                config.sink_id,
                config.properties.clone(),
                table_schema,
                connector_params.sink_payload_format,
            )
            .await
            .map_err(SinkError::from)?;
        response.next().await.unwrap().map_err(|e| {
            let msg = format!(
                "failed to start sink stream for connector `{}` with error code: {}, message: {:?}",
                &config.connector_type,
                e.code(),
                e.message()
            );
            tracing::warn!(msg);
            SinkError::Remote(msg)
        })?;
        tracing::info!(
            "{:?} sink stream started with properties: {:?}",
            &config.connector_type,
            &config.properties
        );

        Ok(RemoteSink {
            connector_type: config.connector_type,
            properties: config.properties,
            epoch: None,
            batch_id: 0,
            schema,
            _client: Some(client),
            request_sender: Some(request_sender),
            response_stream: ResponseStreamImpl::Grpc(response),
            payload_format: connector_params.sink_payload_format,
        })
    }

    pub async fn validate(
        config: RemoteConfig,
        sink_catalog: SinkCatalog,
        connector_rpc_endpoint: Option<String>,
    ) -> Result<()> {
        // FIXME: support struct and array in stream sink
        let columns = sink_catalog
            .columns
            .iter()
            .map(|column| {
                if matches!(
                column.column_desc.data_type,
                DataType::Int16
                    | DataType::Int32
                    | DataType::Int64
                    | DataType::Float32
                    | DataType::Float64
                    | DataType::Boolean
                    | DataType::Decimal
                    | DataType::Timestamp
                    | DataType::Varchar
            ) {
                Ok( Column {
                    name: column.column_desc.name.clone(),
                    data_type: column.column_desc.data_type.to_protobuf().type_name,
                })
                } else {
                    Err(SinkError::Remote(format!(
                        "remote sink supports Int16, Int32, Int64, Float32, Float64, Boolean, Decimal, Timestamp and Varchar, got {:?}: {:?}",
                        column.column_desc.name,
                        column.column_desc.data_type
                    )))
                }
               })
            .collect::<Result<Vec<_>>>()?;

        let address = connector_rpc_endpoint.ok_or_else(|| {
            SinkError::Remote("connector sink endpoint not specified".parse().unwrap())
        })?;
        let host_addr = HostAddr::try_from(&address).map_err(SinkError::from)?;
        let client = ConnectorClient::new(host_addr).await.map_err(|err| {
            SinkError::Remote(format!(
                "failed to connect to connector endpoint `{}`: {:?}",
                &address, err
            ))
        })?;
        let table_schema = TableSchema {
            columns,
            pk_indices: sink_catalog
                .downstream_pk_indices()
                .iter()
                .map(|i| *i as _)
                .collect_vec(),
        };

        // We validate a remote sink's accessibility as well as the pk.
        client
            .validate_sink_properties(
                config.connector_type,
                config.properties,
                Some(table_schema),
                sink_catalog.sink_type.to_proto(),
            )
            .await
            .map_err(SinkError::from)
    }

    fn on_sender_alive(&mut self) -> Result<&UnboundedSender<SinkStreamRequest>> {
        self.request_sender
            .as_ref()
            .ok_or_else(|| SinkError::Remote("sink has been dropped".to_string()))
    }

    #[cfg(test)]
    fn for_test(
        response_receiver: UnboundedReceiver<SinkResponse>,
        request_sender: UnboundedSender<SinkStreamRequest>,
    ) -> Self {
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
impl<const APPEND_ONLY: bool> Sink for RemoteSink<APPEND_ONLY> {
    async fn write_batch(&mut self, chunk: StreamChunk) -> Result<()> {
        let payload = match self.payload_format {
            SinkPayloadFormat::Json => {
                let mut row_ops = vec![];
                for (op, row_ref) in chunk.rows() {
                    let map = record_to_json(row_ref, &self.schema.fields)?;
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
            .send(SinkStreamRequest {
                request: Some(SinkRequest::Write(WriteBatch {
                    epoch,
                    batch_id,
                    payload: Some(payload),
                })),
            })
            .map_err(|e| SinkError::Remote(e.to_string()))?;
        self.response_stream
            .next()
            .await
            .map(|_| self.batch_id += 1)
    }

    async fn begin_epoch(&mut self, epoch: u64) -> Result<()> {
        self.on_sender_alive()?
            .send(SinkStreamRequest {
                request: Some(SinkRequest::StartEpoch(StartEpoch { epoch })),
            })
            .map_err(|e| SinkError::Remote(e.to_string()))?;
        self.response_stream
            .next()
            .await
            .map(|_| self.epoch = Some(epoch))
    }

    async fn commit(&mut self) -> Result<()> {
        let epoch = self.epoch.ok_or_else(|| {
            SinkError::Remote("epoch has not been initialize, call `begin_epoch`".to_string())
        })?;
        self.on_sender_alive()?
            .send(SinkStreamRequest {
                request: Some(SinkRequest::Sync(SyncBatch { epoch })),
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
    use std::sync::Arc;
    use std::time::Duration;

    use risingwave_common::array;
    use risingwave_common::array::column::Column;
    use risingwave_common::array::{ArrayImpl, I32Array, Op, StreamChunk, Utf8Array};
    use risingwave_pb::connector_service::sink_response::{
        Response, StartEpochResponse, SyncResponse, WriteResponse,
    };
    use risingwave_pb::connector_service::sink_stream_request::write_batch::Payload;
    use risingwave_pb::connector_service::sink_stream_request::Request;
    use risingwave_pb::connector_service::{SinkResponse, SinkStreamRequest};
    use risingwave_pb::data;
    use tokio::sync::mpsc;

    use crate::sink::remote::RemoteSink;
    use crate::sink::Sink;

    #[tokio::test]
    async fn test_epoch_check() {
        let (request_sender, mut request_recv) = mpsc::unbounded_channel();
        let (_, resp_recv) = mpsc::unbounded_channel();

        let mut sink = RemoteSink::<true>::for_test(resp_recv, request_sender);
        let chunk = StreamChunk::new(
            vec![Op::Insert],
            vec![
                Column::new(Arc::new(ArrayImpl::from(array!(I32Array, [Some(1)])))),
                Column::new(Arc::new(ArrayImpl::from(array!(
                    Utf8Array,
                    [Some("Ripper")]
                )))),
            ],
            None,
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
        let mut sink = RemoteSink::<true>::for_test(response_receiver, request_sender);

        let chunk_a = StreamChunk::new(
            vec![Op::Insert, Op::Insert, Op::Insert],
            vec![
                Column::new(Arc::new(ArrayImpl::from(array!(
                    I32Array,
                    [Some(1), Some(2), Some(3)]
                )))),
                Column::new(Arc::new(ArrayImpl::from(array!(
                    Utf8Array,
                    [Some("Alice"), Some("Bob"), Some("Clare")]
                )))),
            ],
            None,
        );

        let chunk_b = StreamChunk::new(
            vec![Op::Insert, Op::Insert, Op::Insert],
            vec![
                Column::new(Arc::new(ArrayImpl::from(array!(
                    I32Array,
                    [Some(4), Some(5), Some(6)]
                )))),
                Column::new(Arc::new(ArrayImpl::from(array!(
                    Utf8Array,
                    [Some("David"), Some("Eve"), Some("Frank")]
                )))),
            ],
            None,
        );

        // test write batch
        response_sender
            .send(SinkResponse {
                response: Some(Response::StartEpoch(StartEpochResponse { epoch: 2022 })),
            })
            .expect("test failed: failed to start epoch");
        sink.begin_epoch(2022).await.unwrap();
        assert_eq!(sink.epoch, Some(2022));

        request_receiver
            .recv()
            .await
            .expect("test failed: failed to construct start_epoch request");

        response_sender
            .send(SinkResponse {
                response: Some(Response::Write(WriteResponse {
                    epoch: 2022,
                    batch_id: 0,
                })),
            })
            .expect("test failed: failed to start epoch");
        sink.write_batch(chunk_a.clone()).await.unwrap();
        assert_eq!(sink.epoch, Some(2022));
        assert_eq!(sink.batch_id, 1);
        match request_receiver.recv().await {
            Some(SinkStreamRequest {
                request: Some(Request::Write(write)),
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
            .send(SinkResponse {
                response: Some(Response::Sync(SyncResponse { epoch: 2022 })),
            })
            .expect("test failed: failed to sync epoch");
        sink.commit().await.unwrap();
        let commit_request = request_receiver.recv().await.unwrap();
        match commit_request.request {
            Some(Request::Sync(sync_batch)) => {
                assert_eq!(sync_batch.epoch, 2022);
            }
            _ => panic!("test failed: failed to construct sync request "),
        }

        // begin another epoch
        response_sender
            .send(SinkResponse {
                response: Some(Response::StartEpoch(StartEpochResponse { epoch: 2023 })),
            })
            .expect("test failed: failed to start epoch");
        sink.begin_epoch(2023).await.unwrap();
        // simply keep the channel empty since we've tested begin_epoch
        let _ = request_receiver.recv().await.unwrap();
        assert_eq!(sink.epoch, Some(2023));

        // test another write
        response_sender
            .send(SinkResponse {
                response: Some(Response::Write(WriteResponse {
                    epoch: 2022,
                    batch_id: 1,
                })),
            })
            .expect("test failed: failed to start epoch");
        sink.write_batch(chunk_b.clone()).await.unwrap();
        assert_eq!(sink.epoch, Some(2023));
        assert_eq!(sink.batch_id, 2);
        match request_receiver.recv().await {
            Some(SinkStreamRequest {
                request: Some(Request::Write(write)),
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
