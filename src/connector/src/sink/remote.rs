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
use std::time::Duration;

use anyhow::anyhow;
use async_trait::async_trait;
use risingwave_common::array::StreamChunk;
#[cfg(test)]
use risingwave_common::catalog::Field;
use risingwave_common::catalog::Schema;
use risingwave_common::config::{MAX_CONNECTION_WINDOW_SIZE, STREAM_WINDOW_SIZE};
use risingwave_common::row::Row;
#[cfg(test)]
use risingwave_common::types::DataType;
use risingwave_common::types::{DatumRef, ScalarRefImpl};
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_pb::connector_service::connector_service_client::ConnectorServiceClient;
use risingwave_pb::connector_service::sink_stream_request::write_batch::json_payload::RowOp;
use risingwave_pb::connector_service::sink_stream_request::write_batch::{JsonPayload, Payload};
use risingwave_pb::connector_service::sink_stream_request::{
    Request as SinkRequest, StartEpoch, StartSink, SyncBatch, WriteBatch,
};
use risingwave_pb::connector_service::table_schema::Column;
use risingwave_pb::connector_service::{SinkConfig, SinkResponse, SinkStreamRequest, TableSchema};
use serde_json::Value;
use serde_json::Value::Number;
use tokio::sync::mpsc;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_stream::StreamExt;
use tonic::transport::{Channel, Endpoint};
use tonic::{Request, Status, Streaming};

use crate::sink::{Result, Sink, SinkError};
use crate::ConnectorParams;

pub const VALID_REMOTE_SINKS: [&str; 3] = ["jdbc", "file", "iceberg"];

pub fn is_valid_remote_sink(sink_type: &str) -> bool {
    VALID_REMOTE_SINKS.contains(&sink_type)
}

#[derive(Clone, Debug)]
pub struct RemoteConfig {
    pub sink_type: String,
    pub properties: HashMap<String, String>,
}

impl RemoteConfig {
    pub fn from_hashmap(values: HashMap<String, String>) -> Result<Self> {
        let sink_type = values
            .get("connector")
            .expect("sink type must be specified")
            .to_string();

        if !is_valid_remote_sink(sink_type.as_str()) {
            return Err(SinkError::Config(anyhow!("invalid sink type: {sink_type}")));
        }

        Ok(RemoteConfig {
            sink_type,
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
    pub sink_type: String,
    properties: HashMap<String, String>,
    epoch: Option<u64>,
    batch_id: u64,
    schema: Schema,
    _client: Option<ConnectorServiceClient<Channel>>,
    request_sender: Option<UnboundedSender<SinkStreamRequest>>,
    response_stream: ResponseStreamImpl,
}

impl<const APPEND_ONLY: bool> RemoteSink<APPEND_ONLY> {
    pub async fn new(
        config: RemoteConfig,
        schema: Schema,
        pk_indices: Vec<usize>,
        connector_params: ConnectorParams,
    ) -> Result<Self> {
        let address = format!(
            "http://{}",
            connector_params
                .connector_rpc_endpoint
                .ok_or_else(|| SinkError::Remote(
                    "connector sink endpoint not specified".parse().unwrap()
                ))?
        );
        let channel = Endpoint::from_shared(address.clone())
            .map_err(|e| {
                SinkError::Remote(format!(
                    "invalid connector endpoint `{}`: {:?}",
                    &address, e
                ))
            })?
            .initial_connection_window_size(MAX_CONNECTION_WINDOW_SIZE)
            .initial_stream_window_size(STREAM_WINDOW_SIZE)
            .tcp_nodelay(true)
            .connect_timeout(Duration::from_secs(5))
            .connect()
            .await
            .map_err(|e| {
                SinkError::Remote(format!(
                    "failed to connect to connector endpoint `{}`: {:?}",
                    &address, e
                ))
            })?; // create client and start sink
        let mut client = ConnectorServiceClient::new(channel);

        let (request_sender, request_receiver) = mpsc::unbounded_channel::<SinkStreamRequest>();

        // send initial request in case of the blocking receive call from creating streaming request
        request_sender
            .send(SinkStreamRequest {
                request: Some(SinkRequest::Start(StartSink {
                    sink_config: Some(SinkConfig {
                        sink_type: config.sink_type.clone(),
                        properties: config.properties.clone(),
                        table_schema: Some(TableSchema {
                            columns: schema
                                .fields()
                                .iter()
                                .map(|c| Column {
                                    name: c.name.clone(),
                                    data_type: c.data_type().to_protobuf().type_name,
                                })
                                .collect(),
                            pk_indices: pk_indices.iter().map(|i| *i as u32).collect(),
                        }),
                    }),
                })),
            })
            .map_err(|e| SinkError::Remote(e.to_string()))?;

        let mut response = client
            .sink_stream(Request::new(UnboundedReceiverStream::new(request_receiver)))
            .await
            .map_err(|e| SinkError::Remote(format!("failed to start sink: {:?}", e)))?
            .into_inner();
        let _ = response.next().await.unwrap();

        Ok(RemoteSink {
            sink_type: config.sink_type,
            properties: config.properties,
            epoch: None,
            batch_id: 0,
            schema,
            _client: Some(client),
            request_sender: Some(request_sender),
            response_stream: ResponseStreamImpl::Grpc(response),
        })
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
        let properties = HashMap::from([("output_path".to_string(), "/tmp/rw".to_string())]);

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
            sink_type: "file".to_string(),
            properties,
            epoch: None,
            batch_id: 0,
            schema,
            _client: None,
            request_sender: Some(request_sender),
            response_stream: ResponseStreamImpl::Receiver(response_receiver),
        }
    }
}

#[async_trait]
impl<const APPEND_ONLY: bool> Sink for RemoteSink<APPEND_ONLY> {
    async fn write_batch(&mut self, chunk: StreamChunk) -> Result<()> {
        // let mut row_ops = vec![];
        // for (op, row_ref) in chunk.rows() {
        //     let mut map = serde_json::Map::new();
        //     row_ref
        //         .iter()
        //         .zip_eq_fast(self.schema.fields.iter())
        //         .for_each(|(v, f)| {
        //             map.insert(f.name.clone(), parse_datum(v));
        //         });
        //     let row_op = RowOp {
        //         op_type: op.to_protobuf() as i32,
        //         line: serde_json::to_string(&map)
        //             .map_err(|e| SinkError::Remote(format!("{:?}", e)))?,
        //     };

        //     row_ops.push(row_op);
        // }
        let binary_data: Vec<u8> = bincode::serialize(&chunk).unwrap();

        let epoch = self.epoch.ok_or_else(|| {
            SinkError::Remote("epoch has not been initialize, call `begin_epoch`".to_string())
        })?;
        let batch_id = self.batch_id;
        self.on_sender_alive()?
            .send(SinkStreamRequest {
                request: Some(SinkRequest::Write(WriteBatch {
                    epoch,
                    batch_id,
                    // payload: Some(Payload::JsonPayload(JsonPayload { row_ops })),
                    payload: Some(Payload::StreamChunkPayload(StreamChunkPayload{binary_data})),
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

fn parse_datum(datum: DatumRef<'_>) -> Value {
    match datum {
        None => Value::Null,
        Some(ScalarRefImpl::Int32(v)) => Value::from(v),
        Some(ScalarRefImpl::Int64(v)) => Value::from(v),
        Some(ScalarRefImpl::Float32(v)) => Value::from(v.into_inner()),
        Some(ScalarRefImpl::Float64(v)) => Value::from(v.into_inner()),
        Some(ScalarRefImpl::Decimal(v)) => Number(v.to_string().parse().unwrap()),
        Some(ScalarRefImpl::Utf8(v)) => Value::from(v),
        Some(ScalarRefImpl::Bool(v)) => Value::from(v),
        Some(ScalarRefImpl::NaiveDate(v)) => Value::from(v.to_string()),
        Some(ScalarRefImpl::NaiveTime(v)) => Value::from(v.to_string()),
        Some(ScalarRefImpl::Interval(v)) => Value::from(v.to_string()),
        Some(ScalarRefImpl::Struct(v)) => Value::from(
            v.fields_ref()
                .iter()
                .map(|v| parse_datum(*v))
                .collect::<Vec<_>>(),
        ),
        Some(ScalarRefImpl::List(v)) => Value::from(
            v.values_ref()
                .iter()
                .map(|v| parse_datum(*v))
                .collect::<Vec<_>>(),
        ),
        _ => unimplemented!(),
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
                }
            }
            _ => panic!("test failed: failed to construct write request"),
        }
    }
}
