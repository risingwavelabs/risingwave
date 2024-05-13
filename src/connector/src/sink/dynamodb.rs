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
use dynamodb::types::{AttributeValue, TableStatus};
use risingwave_common::array::{Op, RowRef, StreamChunk};
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::row::Row as _;
use risingwave_common::types::{DataType, ScalarRefImpl, ToText};
use risingwave_common::util::iter_util::ZipEqDebug;
use serde_derive::Deserialize;
use serde_with::serde_as;
use with_options::WithOptions;

use super::log_store::DeliveryFutureManagerAddFuture;
use super::writer::{
    AsyncTruncateLogSinkerOf, AsyncTruncateSinkWriter, AsyncTruncateSinkWriterExt,
};
use super::{DummySinkCommitCoordinator, Result, Sink, SinkError, SinkParam, SinkWriterParam};
use crate::connector_common::common::DynamoDbCommon;

pub const DYNAMO_DB_SINK: &str = "dynamodb";

#[serde_as]
#[derive(Clone, Debug, Deserialize, WithOptions)]
pub struct DynamoDbConfig {
    #[serde(flatten)]
    pub common: DynamoDbCommon,
}

impl DynamoDbConfig {
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
        let client = (self.config.common.build_client().await)
            .context("validate DynamoDB sink error")
            .map_err(SinkError::DynamoDb)?;

        let table_name = &self.config.common.table;
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

        let all_set: HashSet<String> = self
            .schema
            .fields()
            .iter()
            .map(|f| f.name.clone())
            .collect();
        let pk_set: HashSet<String> = self
            .schema
            .fields()
            .iter()
            .enumerate()
            .filter(|(k, _)| self.pk_indices.contains(k))
            .map(|(_, v)| v.name.clone())
            .collect();
        let key_schema = table.key_schema();

        // 1. validate all DynamoDb key element are in RisingWave schema and are primary key
        for key_element in key_schema.iter().map(|x| x.attribute_name()) {
            if !pk_set.iter().any(|x| x == key_element) {
                return Err(SinkError::DynamoDb(anyhow!(
                    "table {} key field {} not found in schema or not primary key",
                    table_name,
                    key_element
                )));
            }
        }
        // 2. validate RisingWave schema fields are subset of dynamodb key fields
        for ref field in all_set {
            if !key_schema.iter().any(|x| x.attribute_name() == field) {
                return Err(SinkError::DynamoDb(anyhow!(
                    "table {} field {} not found in dynamodb key",
                    table_name,
                    field
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

struct DynamoDbPayloadWriter {
    insert_items: HashMap<String, AttributeValue>,
    delete_items: HashMap<String, AttributeValue>,
    client: Client,
    table: String,
}

impl DynamoDbPayloadWriter {
    fn write_one_insert(&mut self, key: String, value: AttributeValue) {
        self.insert_items.insert(key, value);
    }

    fn write_one_delete(&mut self, key: String, value: AttributeValue) {
        self.delete_items.insert(key, value);
    }

    async fn write_chunk(&mut self) -> Result<()> {
        if !self.insert_items.is_empty() {
            let new_items = std::mem::take(&mut self.insert_items);
            self.client
                .put_item()
                .table_name(self.table.clone())
                .set_item(Some(new_items))
                .send()
                .await
                .map_err(|e| {
                    SinkError::DynamoDb(anyhow!(e).context("failed to put item to DynamoDB sink"))
                })
                .map(|_| ())?
        }
        if !self.delete_items.is_empty() {
            let new_items = std::mem::take(&mut self.delete_items);
            self.client
                .delete_item()
                .table_name(self.table.clone())
                .set_key(Some(new_items))
                .send()
                .await
                .map_err(|e| {
                    SinkError::DynamoDb(
                        anyhow!(e).context("failed to delete item from DynamoDB sink"),
                    )
                })
                .map(|_| ())?
        }

        Ok(())
    }
}

pub struct DynamoDbSinkWriter {
    payload_writer: DynamoDbPayloadWriter,
    formatter: DynamoDbFormatter,
}

impl DynamoDbSinkWriter {
    pub async fn new(config: DynamoDbConfig, schema: Schema) -> Result<Self> {
        let client = config.common.build_client().await?;

        let payload_writer = DynamoDbPayloadWriter {
            insert_items: HashMap::new(),
            delete_items: HashMap::new(),
            client,
            table: config.common.table,
        };

        Ok(Self {
            payload_writer,
            formatter: DynamoDbFormatter { schema },
        })
    }

    async fn write_chunk_inner(&mut self, chunk: StreamChunk) -> Result<()> {
        for (op, row) in chunk.rows() {
            match op {
                Op::Insert | Op::UpdateInsert => {
                    for (k, v) in self.formatter.format_row(row)? {
                        self.payload_writer.write_one_insert(k, v);
                    }
                }
                Op::Delete | Op::UpdateDelete => {
                    for (k, v) in self.formatter.format_row(row)? {
                        self.payload_writer.write_one_delete(k, v);
                    }
                }
            }
        }
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
}

struct DynamoDbFormatter {
    schema: Schema,
}

impl DynamoDbFormatter {
    fn format_row(&self, row: RowRef<'_>) -> Result<Vec<(String, AttributeValue)>> {
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
    let attr = match (data_type, scalar_ref) {
        (DataType::Int16, ScalarRefImpl::Int16(_))
        | (DataType::Int32, ScalarRefImpl::Int32(_))
        | (DataType::Int64, ScalarRefImpl::Int64(_))
        | (DataType::Int256, ScalarRefImpl::Int256(_))
        | (DataType::Float32, ScalarRefImpl::Float32(_))
        | (DataType::Float64, ScalarRefImpl::Float64(_))
        | (DataType::Decimal, ScalarRefImpl::Decimal(_))
        | (DataType::Serial, ScalarRefImpl::Serial(_)) => {
            AttributeValue::N(scalar_ref.to_text_with_type(data_type))
        }
        // TODO: jsonb as dynamic type (https://github.com/risingwavelabs/risingwave/issues/11699)
        (DataType::Varchar, ScalarRefImpl::Utf8(_))
        | (DataType::Interval, ScalarRefImpl::Interval(_))
        | (DataType::Date, ScalarRefImpl::Date(_))
        | (DataType::Time, ScalarRefImpl::Time(_))
        | (DataType::Timestamp, ScalarRefImpl::Timestamp(_))
        | (DataType::Timestamptz, ScalarRefImpl::Timestamptz(_))
        | (DataType::Jsonb, ScalarRefImpl::Jsonb(_)) => {
            AttributeValue::S(scalar_ref.to_text_with_type(data_type))
        }
        (DataType::Boolean, ScalarRefImpl::Bool(v)) => AttributeValue::Bool(v),
        (DataType::Bytea, ScalarRefImpl::Bytea(v)) => AttributeValue::B(Blob::new(v)),
        (DataType::List(datatype), ScalarRefImpl::List(list_ref)) => {
            let list_attr = list_ref
                .iter()
                .map(|x| map_data_type(x, datatype))
                .collect::<Result<Vec<_>>>()?;
            AttributeValue::L(list_attr)
        }
        (DataType::Struct(st), ScalarRefImpl::Struct(struct_ref)) => {
            let mut map = HashMap::with_capacity(st.len());
            for (sub_datum_ref, sub_field) in struct_ref.iter_fields_ref().zip_eq_debug(
                st.iter()
                    .map(|(name, dt)| Field::with_name(dt.clone(), name)),
            ) {
                let attr = map_data_type(sub_datum_ref, &sub_field.data_type())?;
                map.insert(sub_field.name.clone(), attr);
            }
            AttributeValue::M(map)
        }
        (data_type, scalar_ref) => {
            return Err(SinkError::DynamoDb(anyhow!(format!(
                "map_data_type: unsupported data type: logical type: {:?}, physical type: {:?}",
                data_type, scalar_ref
            ),)));
        }
    };
    Ok(attr)
}
