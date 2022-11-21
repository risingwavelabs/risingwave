// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;
use std::time::Duration;

use async_trait::async_trait;
use itertools::Itertools;
use risingwave_common::array::{stream_chunk, StreamChunk};
use risingwave_common::catalog::Schema;
use risingwave_common::config::{MAX_CONNECTION_WINDOW_SIZE, STREAM_WINDOW_SIZE};
use risingwave_common::types::{DataType, DatumRef, ScalarRefImpl};
use risingwave_pb::connector_service::connector_service_client::ConnectorServiceClient;
use risingwave_pb::connector_service::sink_config::table_schema::Column;
use risingwave_pb::connector_service::sink_config::TableSchema;
use risingwave_pb::connector_service::sink_task::write_batch::json_payload::RowOp;
use risingwave_pb::connector_service::sink_task::write_batch::{JsonPayload, Payload};
use risingwave_pb::connector_service::sink_task::{
    Request as SinkRequest, StartEpoch, StartSink, SyncBatch, WriteBatch,
};
use risingwave_pb::connector_service::{SinkConfig, SinkResponse, SinkTask};
use risingwave_pb::data;
use risingwave_pb::data::data_type::TypeName;
use serde_json::Value;
use serde_json::Value::Number;
use tokio::sync::mpsc;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_stream::StreamExt;
use tonic::transport::{Channel, Endpoint};
use tonic::{Request, Status, Streaming};

use crate::sink::{Result, Sink, SinkError};

pub const VALID_REMOTE_SINKS: [&str; 2] = ["jdbc", "file"];

pub fn is_valid_remote_sink(sink_type: String) -> bool {
    return VALID_REMOTE_SINKS.contains(&sink_type.as_str());
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

        if !is_valid_remote_sink(sink_type.clone()) {
            return Err(SinkError::Config(format!("invalid sink type: {sink_type}")));
        }

        Ok(RemoteConfig {
            sink_type,
            properties: values,
        })
    }
}

#[derive(Debug)]
pub enum RemoteSinkResponseImpl {
    Grpc(Streaming<SinkResponse>),
    Receiver(UnboundedReceiver<SinkResponse>),
}

impl RemoteSinkResponseImpl {
    pub async fn on_response_ok(&mut self) -> std::result::Result<SinkResponse, SinkError> {
        return match self {
            RemoteSinkResponseImpl::Grpc(ref mut response) => response
                .next()
                .await
                .unwrap_or_else(|| Err(Status::cancelled("response stream closed unexpectedly")))
                .map_err(|e| SinkError::Remote(e.message().to_string())),
            RemoteSinkResponseImpl::Receiver(ref mut receiver) => receiver
                .recv()
                .await
                .ok_or_else(|| SinkError::Remote("response stream closed unexpectedly".to_string()))
        };
    }
}

#[derive(Debug)]
pub enum ConnectorClientImpl {
    Grpc(ConnectorServiceClient<Channel>),
    Mock,
}

#[derive(Debug)]
pub struct RemoteSink {
    pub sink_type: String,
    properties: HashMap<String, String>,
    epoch: Option<u64>,
    batch_id: u64,
    schema: Schema,
    client: ConnectorClientImpl,
    pub request_sender: Option<UnboundedSender<SinkTask>>,
    pub response_stream: RemoteSinkResponseImpl,
}

pub struct RemoteSinkParams {
    pub connector_addr: String,
}

impl RemoteSink {
    pub async fn new(
        config: RemoteConfig,
        schema: Schema,
        pk_indices: Vec<usize>,
        sink_params: RemoteSinkParams,
    ) -> Result<Self> {
        let channel = Endpoint::from_shared(format!("http://{}", sink_params.connector_addr))
            .map_err(|e| SinkError::Remote(format!("failed to connect channel: {:?}", e)))?
            .initial_connection_window_size(MAX_CONNECTION_WINDOW_SIZE)
            .initial_stream_window_size(STREAM_WINDOW_SIZE)
            .tcp_nodelay(true)
            .connect_timeout(Duration::from_secs(5))
            .connect()
            .await
            .map_err(|e| SinkError::Remote(format!("failed to connect channel: {:?}", e)))?; // create client and start sink
        let mut client = ConnectorServiceClient::new(channel);

        let (request_sender, request_receiver) = mpsc::unbounded_channel::<SinkTask>();
        request_sender
            .send(SinkTask {
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
                                    data_type: parse_data_type(c.data_type.clone()) as i32,
                                })
                                .collect(),
                            pk_indices: pk_indices.iter().map(|i| *i as u32).collect(),
                        }),
                    }),
                })),
            })
            .map_err(|e| SinkError::Remote(e.to_string()))?;

        let mut response = tokio::time::timeout(
            Duration::from_secs(3),
            client.sink_stream(Request::new(UnboundedReceiverStream::new(request_receiver))),
        )
        .await
        .map_err(|e| SinkError::Remote(format!("failed to start sink: {:?}", e)))?
        .map_err(|e| SinkError::Remote(format!("{:?}", e)))?
        .into_inner();
        let _ = response.next();

        Ok(RemoteSink {
            sink_type: config.sink_type,
            properties: config.properties,
            epoch: None,
            batch_id: 0,
            schema,
            client: ConnectorClientImpl::Grpc(client),
            request_sender: Some(request_sender),
            response_stream: RemoteSinkResponseImpl::Grpc(response),
        })
    }

    fn on_sender_alive(
        &mut self,
    ) -> std::result::Result<&mut UnboundedSender<SinkTask>, SinkError> {
        self.request_sender
            .as_mut()
            .ok_or_else(|| SinkError::Remote("sink has been dropped".to_string()))
    }
}

fn parse_data_type(data_type: DataType) -> TypeName {
    match data_type {
        DataType::Boolean => TypeName::Boolean,
        DataType::Int16 => TypeName::Int16,
        DataType::Int32 => TypeName::Int32,
        DataType::Int64 => TypeName::Int64,
        DataType::Float32 => TypeName::Float,
        DataType::Float64 => TypeName::Float,
        DataType::Time => TypeName::Time,
        DataType::Date => TypeName::Date,
        DataType::Decimal => TypeName::Decimal,
        DataType::Timestamp => TypeName::Timestamp,
        DataType::Timestampz => TypeName::Timestampz,
        DataType::Interval => TypeName::Interval,
        DataType::List { datatype: _ } => TypeName::List,
        DataType::Struct(_) => TypeName::Struct,
        DataType::Varchar => TypeName::Varchar,
    }
}

#[async_trait]
impl Sink for RemoteSink {
    async fn write_batch(&mut self, chunk: StreamChunk) -> Result<()> {
        let mut row_ops = vec![];
        for (op, row_ref) in chunk.rows() {
            let mut map = serde_json::Map::new();
            row_ref
                .values()
                .zip_eq(self.schema.fields.iter())
                .for_each(|(v, f)| {
                    map.insert(f.name.clone(), parse_datum(v));
                });
            let row_op = RowOp {
                op_type: parse_stream_op(op),
                line: serde_json::to_string(&map).unwrap(),
            };

            row_ops.push(row_op);
        }

        let epoch = self.epoch.ok_or_else(|| {
            SinkError::Remote("epoch has not been initialize, call `begin_epoch`".to_string())
        })?;
        let batch_id = self.batch_id;
        self.on_sender_alive()?
            .send(SinkTask {
                request: Some(SinkRequest::Write(WriteBatch {
                    epoch,
                    batch_id,
                    payload: Some(Payload::JsonPayload(JsonPayload { row_ops })),
                })),
            })
            .map_err(|e| SinkError::Remote(e.to_string()))?;
        self.response_stream
            .on_response_ok()
            .await
            .map(|_| self.batch_id += 1)
    }

    async fn begin_epoch(&mut self, epoch: u64) -> Result<()> {
        self.on_sender_alive()?
            .send(SinkTask {
                request: Some(SinkRequest::StartEpoch(StartEpoch { epoch })),
            })
            .map_err(|e| SinkError::Remote(e.to_string()))?;
        self.response_stream
            .on_response_ok()
            .await
            .map(|_| self.epoch = Some(epoch))
    }

    async fn commit(&mut self) -> Result<()> {
        let epoch = self.epoch.ok_or_else(|| {
            SinkError::Remote("epoch has not been initialize, call `begin_epoch`".to_string())
        })?;
        self.on_sender_alive()?
            .send(SinkTask {
                request: Some(SinkRequest::Sync(SyncBatch { epoch })),
            })
            .map_err(|e| SinkError::Remote(e.to_string()))?;
        self.response_stream.on_response_ok().await.map(|_| ())
    }

    async fn abort(&mut self) -> Result<()> {
        self.request_sender = None;
        Ok(())
    }
}

fn parse_stream_op(op: stream_chunk::Op) -> i32 {
    match op {
        stream_chunk::Op::Insert => data::Op::Insert as i32,
        stream_chunk::Op::UpdateDelete => data::Op::UpdateDelete as i32,
        stream_chunk::Op::UpdateInsert => data::Op::UpdateInsert as i32,
        stream_chunk::Op::Delete => data::Op::Delete as i32,
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
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Duration;

    use risingwave_common::array;
    use risingwave_common::array::column::Column;
    use risingwave_common::array::{ArrayImpl, I32Array, Op, StreamChunk, Utf8Array};
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::types::DataType;
    use risingwave_pb::connector_service::sink_response::{
        Response, StartEpochResponse, SyncResponse, WriteResponse,
    };
    use risingwave_pb::connector_service::sink_task::write_batch::Payload;
    use risingwave_pb::connector_service::sink_task::Request;
    use risingwave_pb::connector_service::{SinkResponse, SinkTask};
    use risingwave_pb::data;
    use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

    use crate::sink::remote::{ConnectorClientImpl, RemoteSink, RemoteSinkResponseImpl};
    use crate::sink::Sink;

    fn create_mock_sink() -> (
        RemoteSink,
        UnboundedSender<SinkResponse>,
        UnboundedReceiver<SinkTask>,
    ) {
        let properties = HashMap::from([("output_path".to_string(), "/tmp/rw".to_string())]);

        let _schema = Schema::new(vec![
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

        let (request_sender, request_recv) = tokio::sync::mpsc::unbounded_channel();
        let (resp_sender, resp_recv) = tokio::sync::mpsc::unbounded_channel();

        (
            RemoteSink {
                sink_type: "file".to_string(),
                properties,
                epoch: None,
                batch_id: 0,
                schema: _schema,
                client: ConnectorClientImpl::Mock,
                request_sender: Some(request_sender),
                response_stream: RemoteSinkResponseImpl::Receiver(resp_recv),
            },
            resp_sender,
            request_recv,
        )
    }

    #[tokio::test]
    async fn test_epoch_init_check() {
        let (mut sink, _, mut request_recv) = create_mock_sink();
        let _chunk = StreamChunk::new(
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
        tokio::time::timeout(Duration::from_secs(1), async {
            match sink.commit().await {
                Err(_) => (),
                _ => panic!("test failed: unchecked epoch"),
            }
        })
        .await
        .expect("test failed: invalid epoch error not thrown");

        // assert that request is not sent
        if request_recv.try_recv().is_ok() {
            panic!("test failed: unchecked epoch")
        }

        println!("testing write_batch epoch check");
        tokio::time::timeout(Duration::from_secs(1), async {
            match sink.write_batch(_chunk.clone()).await {
                Err(_) => (),
                _ => panic!("test failed: unchecked epoch"),
            }
        })
        .await
        .expect("test failed: invalid epoch error not thrown");
        if request_recv.try_recv().is_ok() {
            panic!("test failed: unchecked epoch")
        }
    }

    #[tokio::test]
    async fn test_remote_sink() {
        let (mut sink, response_sender, mut request_receiver) = create_mock_sink();
        let _chunk = StreamChunk::new(
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

        let _chunk_2 = StreamChunk::new(
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
        println!("testing begin_epoch");
        response_sender
            .send(SinkResponse {
                response: Some(Response::StartEpoch(StartEpochResponse { epoch: 2022 })),
            })
            .expect("test failed: failed to start epoch");
        sink.begin_epoch(2022).await.unwrap();
        assert_eq!(sink.epoch, Some(2022));

        match request_receiver.recv().await {
            Some(SinkTask {
                request: Some(Request::StartEpoch(_)),
            }) => {}
            _ => panic!("test failed: failed to construct start_epoch request"),
        }

        println!("testing write_batch");
        response_sender
            .send(SinkResponse {
                response: Some(Response::Write(WriteResponse {
                    epoch: 2022,
                    batch_id: 0,
                })),
            })
            .expect("test failed: failed to start epoch");
        sink.write_batch(_chunk.clone()).await.unwrap();
        assert_eq!(sink.epoch, Some(2022));
        assert_eq!(sink.batch_id, 1);
        match request_receiver.recv().await {
            Some(SinkTask {
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
        println!("testing commit");
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
        println!("testing begin_epoch");
        response_sender
            .send(SinkResponse {
                response: Some(Response::StartEpoch(StartEpochResponse { epoch: 2023 })),
            })
            .expect("test failed: failed to start epoch");
        sink.begin_epoch(2023).await.unwrap();
        let _ = request_receiver.recv().await.unwrap();
        assert_eq!(sink.epoch, Some(2023));

        // test another write
        println!("testing write_batch chunk 2");
        response_sender
            .send(SinkResponse {
                response: Some(Response::Write(WriteResponse {
                    epoch: 2022,
                    batch_id: 1,
                })),
            })
            .expect("test failed: failed to start epoch");
        sink.write_batch(_chunk_2.clone()).await.unwrap();
        assert_eq!(sink.epoch, Some(2023));
        assert_eq!(sink.batch_id, 2);
        match request_receiver.recv().await {
            Some(SinkTask {
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
