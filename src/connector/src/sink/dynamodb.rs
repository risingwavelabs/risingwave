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

use std::collections::HashMap;
use std::usize;

use anyhow::{anyhow, Context};
use aws_sdk_dynamodb as dynamodb;
use aws_sdk_dynamodb::client::Client;
use aws_smithy_types::Blob;
use dynamodb::types::{AttributeValue, TableStatus};
use risingwave_common::array::{Op, RowRef, StreamChunk};
use risingwave_common::catalog::Schema;
use risingwave_common::row::Row as _;
use risingwave_common::types::{ScalarRefImpl, ToText};
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

        // validate all key are in schema
        let fields = self.schema.fields();
        let keys = table.key_schema();
        for key in keys {
            if !fields.iter().any(|f| f.name == key.attribute_name()) {
                return Err(SinkError::DynamoDb(anyhow!(
                    "DynamoDB table key {} not found in schema",
                    key.attribute_name()
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

        Ok(Self { config, schema })
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
                .map(|_| ())
        } else if !self.delete_items.is_empty() {
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
                .map(|_| ())
        } else {
            Ok(())
        }
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
                    for (k, v) in self.formatter.format_row(row) {
                        self.payload_writer.write_one_insert(k, v);
                    }
                }
                Op::Delete | Op::UpdateDelete => {
                    for (k, v) in self.formatter.format_row(row) {
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
    fn format_row(&self, row: RowRef<'_>) -> Vec<(String, AttributeValue)> {
        let mut items = Vec::new();
        for (scalar, field) in row.iter().zip_eq_debug((self.schema.clone()).into_fields()) {
            let attr_value = map_data_type(scalar);
            items.push((field.name, attr_value));
        }

        items
    }
}

fn map_data_type(scalar: Option<ScalarRefImpl<'_>>) -> AttributeValue {
    match scalar {
        None => AttributeValue::Null(true),
        Some(s) => match s {
            number @ (ScalarRefImpl::Int16(_)
            | ScalarRefImpl::Int32(_)
            | ScalarRefImpl::Int64(_)
            | ScalarRefImpl::Int256(_)
            | ScalarRefImpl::Float32(_)
            | ScalarRefImpl::Float64(_)
            | ScalarRefImpl::Decimal(_)
            | ScalarRefImpl::Serial(_)) => AttributeValue::N(number.to_text()),
            string @ (ScalarRefImpl::Utf8(_)
            | ScalarRefImpl::Interval(_)
            | ScalarRefImpl::Date(_)
            | ScalarRefImpl::Time(_)
            | ScalarRefImpl::Timestamp(_)
            | ScalarRefImpl::Timestamptz(_)
            | ScalarRefImpl::Struct(_)
            | ScalarRefImpl::Jsonb(_)) => AttributeValue::S(string.to_text()),
            ScalarRefImpl::Bool(x) => AttributeValue::Bool(x),
            ScalarRefImpl::Bytea(x) => AttributeValue::B(Blob::new(x)),
            ScalarRefImpl::List(x) => AttributeValue::L(x.iter().map(map_data_type).collect()),
        },
    }
}
