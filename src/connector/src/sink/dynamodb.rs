// Copyright 2024 RisingWave Labs
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

use std::collections::{HashMap, HashSet};
use std::usize;

use anyhow::{anyhow, Context};
use aws_sdk_dynamodb as dynamodb;
use aws_sdk_dynamodb::client::Client;
use aws_smithy_types::Blob;
use dynamodb::types::{
    AttributeValue, DeleteRequest, PutRequest, ReturnConsumedCapacity, ReturnItemCollectionMetrics,
    TableStatus, WriteRequest,
};
use maplit::hashmap;
use risingwave_common::array::{Op, RowRef, StreamChunk};
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::row::Row as _;
use risingwave_common::types::{DataType, ScalarRefImpl, ToText};
use risingwave_common::util::iter_util::ZipEqDebug;
use serde_derive::Deserialize;
use serde_with::{serde_as, DisplayFromStr};
use with_options::WithOptions;

use super::log_store::DeliveryFutureManagerAddFuture;
use super::writer::{
    AsyncTruncateLogSinkerOf, AsyncTruncateSinkWriter, AsyncTruncateSinkWriterExt,
};
use super::{DummySinkCommitCoordinator, Result, Sink, SinkError, SinkParam, SinkWriterParam};
use crate::connector_common::AwsAuthProps;
use crate::error::ConnectorResult;

pub const DYNAMO_DB_SINK: &str = "dynamodb";

#[serde_as]
#[derive(Deserialize, Debug, Clone, WithOptions)]
pub struct DynamoDbConfig {
    #[serde(rename = "table", alias = "dynamodb.table")]
    pub table: String,

    #[serde(rename = "dynamodb.max_batch_rows", default = "default_max_batch_rows")]
    #[serde_as(as = "DisplayFromStr")]
    pub max_batch_rows: usize,

    #[serde(flatten)]
    pub aws_auth_props: AwsAuthProps,
}

fn default_max_batch_rows() -> usize {
    1024
}

impl DynamoDbConfig {
    pub async fn build_client(&self) -> ConnectorResult<Client> {
        let config = &self.aws_auth_props;
        let aws_config = config.build_config().await?;

        Ok(Client::new(&aws_config))
    }

    fn from_hashmap(values: HashMap<String, String>) -> Result<Self> {
        serde_json::from_value::<DynamoDbConfig>(serde_json::to_value(values).unwrap())
            .map_err(|e| SinkError::Config(anyhow!(e)))
    }
}

#[derive(Clone, Debug)]
pub struct DynamoDbSink {
    pub config: DynamoDbConfig,
    schema: Schema,
    pk_indices: Vec<usize>,
}

impl Sink for DynamoDbSink {
    type Coordinator = DummySinkCommitCoordinator;
    type LogSinker = AsyncTruncateLogSinkerOf<DynamoDbSinkWriter>;

    const SINK_NAME: &'static str = DYNAMO_DB_SINK;

    async fn validate(&self) -> Result<()> {
        let client = (self.config.build_client().await)
            .context("validate DynamoDB sink error")
            .map_err(SinkError::DynamoDb)?;

        let table_name = &self.config.table;
        let output = client
            .describe_table()
            .table_name(table_name)
            .send()
            .await
            .map_err(|e| anyhow!(e))?;
        let Some(table) = output.table else {
            return Err(SinkError::DynamoDb(anyhow!(
                "table {} not found",
                table_name
            )));
        };
        if !matches!(table.table_status(), Some(TableStatus::Active)) {
            return Err(SinkError::DynamoDb(anyhow!(
                "table {} is not active",
                table_name
            )));
        }
        let pk_set: HashSet<String> = self
            .schema
            .fields()
            .iter()
            .enumerate()
            .filter(|(k, _)| self.pk_indices.contains(k))
            .map(|(_, v)| v.name.clone())
            .collect();
        let key_schema = table.key_schema();

        for key_element in key_schema.iter().map(|x| x.attribute_name()) {
            if !pk_set.contains(key_element) {
                return Err(SinkError::DynamoDb(anyhow!(
                    "table {} key field {} not found in schema or not primary key",
                    table_name,
                    key_element
                )));
            }
        }

        Ok(())
    }

    async fn new_log_sinker(&self, _writer_param: SinkWriterParam) -> Result<Self::LogSinker> {
        Ok(
            DynamoDbSinkWriter::new(self.config.clone(), self.schema.clone())
                .await?
                .into_log_sinker(usize::MAX),
        )
    }
}

impl TryFrom<SinkParam> for DynamoDbSink {
    type Error = SinkError;

    fn try_from(param: SinkParam) -> std::result::Result<Self, Self::Error> {
        let schema = param.schema();
        let config = DynamoDbConfig::from_hashmap(param.properties)?;

        Ok(Self {
            config,
            schema,
            pk_indices: param.downstream_pk,
        })
    }
}

#[derive(Debug)]
struct DynamoDbRequest {
    inner: WriteRequest,
    key_items: Vec<String>,
}

impl DynamoDbRequest {
    fn extract_pk_values(&self) -> Option<Vec<AttributeValue>> {
        let key = match (&self.inner.put_request(), &self.inner.delete_request()) {
            (Some(put_req), None) => &put_req.item,
            (None, Some(del_req)) => &del_req.key,
            _ => return None,
        };
        let vs = key
            .iter()
            .filter(|(k, _)| self.key_items.contains(k))
            .map(|(_, v)| v.clone())
            .collect();
        Some(vs)
    }
}

struct DynamoDbPayloadWriter {
    request_items: Vec<DynamoDbRequest>,
    client: Client,
    table: String,
    dynamodb_keys: Vec<String>,
}

impl DynamoDbPayloadWriter {
    fn write_one_insert(&mut self, item: HashMap<String, AttributeValue>) {
        let put_req = PutRequest::builder().set_item(Some(item)).build().unwrap();
        let req = WriteRequest::builder().put_request(put_req).build();
        self.write_one_req(req);
    }

    fn write_one_delete(&mut self, key: HashMap<String, AttributeValue>) {
        let key = key
            .into_iter()
            .filter(|(k, _)| self.dynamodb_keys.contains(k))
            .collect();
        let del_req = DeleteRequest::builder().set_key(Some(key)).build().unwrap();
        let req = WriteRequest::builder().delete_request(del_req).build();
        self.write_one_req(req);
    }

    fn write_one_req(&mut self, req: WriteRequest) {
        let r_req = DynamoDbRequest {
            inner: req,
            key_items: self.dynamodb_keys.clone(),
        };
        if let Some(v) = r_req.extract_pk_values() {
            self.request_items.retain(|item| {
                !item
                    .extract_pk_values()
                    .unwrap_or_default()
                    .iter()
                    .all(|x| v.contains(x))
            });
        }
        self.request_items.push(r_req);
    }

    async fn write_chunk(&mut self) -> Result<()> {
        if !self.request_items.is_empty() {
            let table = self.table.clone();
            let req_items = std::mem::take(&mut self.request_items)
                .into_iter()
                .map(|r| r.inner)
                .collect();
            let reqs = hashmap! {
                table => req_items,
            };
            self.client
                .batch_write_item()
                .set_request_items(Some(reqs))
                .return_consumed_capacity(ReturnConsumedCapacity::None)
                .return_item_collection_metrics(ReturnItemCollectionMetrics::None)
                .send()
                .await
                .map_err(|e| {
                    SinkError::DynamoDb(
                        anyhow!(e).context("failed to delete item from DynamoDB sink"),
                    )
                })?;
        }

        Ok(())
    }
}

pub struct DynamoDbSinkWriter {
    max_batch_rows: usize,
    payload_writer: DynamoDbPayloadWriter,
    formatter: DynamoDbFormatter,
}

impl DynamoDbSinkWriter {
    pub async fn new(config: DynamoDbConfig, schema: Schema) -> Result<Self> {
        let client = config.build_client().await?;
        let table_name = &config.table;
        let output = client
            .describe_table()
            .table_name(table_name)
            .send()
            .await
            .map_err(|e| anyhow!(e))?;
        let Some(table) = output.table else {
            return Err(SinkError::DynamoDb(anyhow!(
                "table {} not found",
                table_name
            )));
        };
        let dynamodb_keys = table
            .key_schema
            .unwrap_or_default()
            .into_iter()
            .map(|k| k.attribute_name)
            .collect();

        let payload_writer = DynamoDbPayloadWriter {
            request_items: Vec::new(),
            client,
            table: config.table,
            dynamodb_keys,
        };

        Ok(Self {
            max_batch_rows: config.max_batch_rows,
            payload_writer,
            formatter: DynamoDbFormatter { schema },
        })
    }

    async fn write_chunk_inner(&mut self, chunk: StreamChunk) -> Result<()> {
        for (op, row) in chunk.rows() {
            let items = self.formatter.format_row(row)?;
            match op {
                Op::Insert | Op::UpdateInsert => {
                    self.payload_writer.write_one_insert(items);
                }
                Op::Delete => {
                    self.payload_writer.write_one_delete(items);
                }
                Op::UpdateDelete => {}
            }
        }
        if self.payload_writer.request_items.len() >= self.max_batch_rows {
            self.payload_writer.write_chunk().await?;
        }
        Ok(())
    }

    async fn flush(&mut self) -> Result<()> {
        self.payload_writer.write_chunk().await
    }
}

impl AsyncTruncateSinkWriter for DynamoDbSinkWriter {
    async fn write_chunk<'a>(
        &'a mut self,
        chunk: StreamChunk,
        _add_future: DeliveryFutureManagerAddFuture<'a, Self::DeliveryFuture>,
    ) -> Result<()> {
        self.write_chunk_inner(chunk).await
    }

    async fn barrier(&mut self, is_checkpoint: bool) -> Result<()> {
        if is_checkpoint {
            self.flush().await?;
        }
        Ok(())
    }
}

struct DynamoDbFormatter {
    schema: Schema,
}

impl DynamoDbFormatter {
    fn format_row(&self, row: RowRef<'_>) -> Result<HashMap<String, AttributeValue>> {
        row.iter()
            .zip_eq_debug((self.schema.clone()).into_fields())
            .map(|(scalar, field)| {
                map_data_type(scalar, &field.data_type()).map(|attr| (field.name, attr))
            })
            .collect()
    }
}

fn map_data_type(
    scalar_ref: Option<ScalarRefImpl<'_>>,
    data_type: &DataType,
) -> Result<AttributeValue> {
    let Some(scalar_ref) = scalar_ref else {
        return Ok(AttributeValue::Null(true));
    };
    let attr = match data_type {
        DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::Int256
        | DataType::Float32
        | DataType::Float64
        | DataType::Decimal
        | DataType::Serial => AttributeValue::N(scalar_ref.to_text_with_type(data_type)),
        // TODO: jsonb as dynamic type (https://github.com/risingwavelabs/risingwave/issues/11699)
        DataType::Varchar
        | DataType::Interval
        | DataType::Date
        | DataType::Time
        | DataType::Timestamp
        | DataType::Timestamptz
        | DataType::Jsonb => AttributeValue::S(scalar_ref.to_text_with_type(data_type)),
        DataType::Boolean => AttributeValue::Bool(scalar_ref.into_bool()),
        DataType::Bytea => AttributeValue::B(Blob::new(scalar_ref.into_bytea())),
        DataType::List(datatype) => {
            let list_attr = scalar_ref
                .into_list()
                .iter()
                .map(|x| map_data_type(x, datatype))
                .collect::<Result<Vec<_>>>()?;
            AttributeValue::L(list_attr)
        }
        DataType::Struct(st) => {
            let mut map = HashMap::with_capacity(st.len());
            for (sub_datum_ref, sub_field) in
                scalar_ref.into_struct().iter_fields_ref().zip_eq_debug(
                    st.iter()
                        .map(|(name, dt)| Field::with_name(dt.clone(), name)),
                )
            {
                let attr = map_data_type(sub_datum_ref, &sub_field.data_type())?;
                map.insert(sub_field.name.clone(), attr);
            }
            AttributeValue::M(map)
        }
    };
    Ok(attr)
}
