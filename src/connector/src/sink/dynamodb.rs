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

use std::collections::{BTreeMap, HashMap, HashSet};

use anyhow::{Context, anyhow};
use aws_sdk_dynamodb as dynamodb;
use aws_sdk_dynamodb::client::Client;
use aws_smithy_types::Blob;
use dynamodb::types::{AttributeValue, TableStatus, WriteRequest};
use futures::prelude::TryFuture;
use futures::prelude::future::TryFutureExt;
use risingwave_common::array::{Op, RowRef, StreamChunk};
use risingwave_common::catalog::Schema;
use risingwave_common::row::Row as _;
use risingwave_common::types::{DataType, ScalarRefImpl, ToText};
use risingwave_common::util::iter_util::ZipEqDebug;
use serde::Deserialize;
use serde_with::{DisplayFromStr, serde_as};
use with_options::WithOptions;
use write_chunk_future::{DynamoDbPayloadWriter, WriteChunkFuture};

use super::log_store::DeliveryFutureManagerAddFuture;
use super::writer::{
    AsyncTruncateLogSinkerOf, AsyncTruncateSinkWriter, AsyncTruncateSinkWriterExt,
};
use super::{Result, Sink, SinkError, SinkParam, SinkWriterParam};
use crate::connector_common::AwsAuthProps;
use crate::enforce_secret::EnforceSecret;
use crate::error::ConnectorResult;

pub const DYNAMO_DB_SINK: &str = "dynamodb";

#[serde_as]
#[derive(Deserialize, Debug, Clone, WithOptions)]
pub struct DynamoDbConfig {
    #[serde(rename = "table", alias = "dynamodb.table")]
    pub table: String,

    #[serde(rename = "dynamodb.max_batch_rows", default = "default_max_batch_rows")]
    #[serde_as(as = "DisplayFromStr")]
    #[deprecated]
    pub max_batch_rows: usize,

    #[serde(flatten)]
    pub aws_auth_props: AwsAuthProps,

    #[serde(
        rename = "dynamodb.max_batch_item_nums",
        default = "default_max_batch_item_nums"
    )]
    #[serde_as(as = "DisplayFromStr")]
    pub max_batch_item_nums: usize,

    #[serde(
        rename = "dynamodb.max_future_send_nums",
        default = "default_max_future_send_nums"
    )]
    #[serde_as(as = "DisplayFromStr")]
    pub max_future_send_nums: usize,
}

impl EnforceSecret for DynamoDbConfig {
    fn enforce_one(prop: &str) -> crate::error::ConnectorResult<()> {
        AwsAuthProps::enforce_one(prop)
    }
}

fn default_max_batch_item_nums() -> usize {
    25
}

fn default_max_future_send_nums() -> usize {
    256
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

    fn from_btreemap(values: BTreeMap<String, String>) -> Result<Self> {
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

impl EnforceSecret for DynamoDbSink {
    fn enforce_secret<'a>(
        prop_iter: impl Iterator<Item = &'a str>,
    ) -> crate::error::ConnectorResult<()> {
        for prop in prop_iter {
            DynamoDbConfig::enforce_one(prop)?;
        }
        Ok(())
    }
}

impl Sink for DynamoDbSink {
    type LogSinker = AsyncTruncateLogSinkerOf<DynamoDbSinkWriter>;

    const SINK_NAME: &'static str = DYNAMO_DB_SINK;

    async fn validate(&self) -> Result<()> {
        risingwave_common::license::Feature::DynamoDbSink
            .check_available()
            .map_err(|e| anyhow::anyhow!(e))?;
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
                .into_log_sinker(self.config.max_future_send_nums),
        )
    }
}

impl TryFrom<SinkParam> for DynamoDbSink {
    type Error = SinkError;

    fn try_from(param: SinkParam) -> std::result::Result<Self, Self::Error> {
        let schema = param.schema();
        let pk_indices = param.downstream_pk_or_empty();
        let config = DynamoDbConfig::from_btreemap(param.properties)?;

        Ok(Self {
            config,
            schema,
            pk_indices,
        })
    }
}

#[derive(Debug)]
struct DynamoDbRequest {
    inner: WriteRequest,
    key_items: Vec<String>,
}

impl DynamoDbRequest {
    fn extract_pk(&self) -> Option<HashMap<String, AttributeValue>> {
        let key = match (&self.inner.put_request(), &self.inner.delete_request()) {
            (Some(put_req), None) => &put_req.item,
            (None, Some(del_req)) => &del_req.key,
            _ => return None,
        };
        let pk = key
            .iter()
            .filter(|(k, _)| self.key_items.contains(k))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        Some(pk)
    }

    fn has_same_pk(&self, other: &Self) -> bool {
        match (self.extract_pk(), other.extract_pk()) {
            (Some(pk), Some(other_pk)) => pk == other_pk,
            _ => false,
        }
    }
}

pub struct DynamoDbSinkWriter {
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
            client,
            table: config.table.clone(),
            dynamodb_keys,
            max_batch_item_nums: config.max_batch_item_nums,
        };

        Ok(Self {
            payload_writer,
            formatter: DynamoDbFormatter { schema },
        })
    }

    fn write_chunk_inner(&mut self, chunk: StreamChunk) -> Result<WriteChunkFuture> {
        let mut request_items = Vec::new();
        for (op, row) in chunk.rows() {
            let items = self.formatter.format_row(row)?;
            match op {
                Op::Insert | Op::UpdateInsert => {
                    self.payload_writer
                        .write_one_insert(items, &mut request_items);
                }
                Op::Delete => {
                    self.payload_writer
                        .write_one_delete(items, &mut request_items);
                }
                Op::UpdateDelete => {}
            }
        }
        Ok(self.payload_writer.write_chunk(request_items))
    }
}

pub type DynamoDbSinkDeliveryFuture = impl TryFuture<Ok = (), Error = SinkError> + Unpin + 'static;

impl AsyncTruncateSinkWriter for DynamoDbSinkWriter {
    type DeliveryFuture = DynamoDbSinkDeliveryFuture;

    #[define_opaque(DynamoDbSinkDeliveryFuture)]
    async fn write_chunk<'a>(
        &'a mut self,
        chunk: StreamChunk,
        mut add_future: DeliveryFutureManagerAddFuture<'a, Self::DeliveryFuture>,
    ) -> Result<()> {
        let futures = self.write_chunk_inner(chunk)?;
        add_future
            .add_future_may_await(futures.map_ok(|_: Vec<()>| ()))
            .await?;
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
                map_data(scalar, &field.data_type()).map(|attr| (field.name, attr))
            })
            .collect()
    }
}

fn map_data(scalar_ref: Option<ScalarRefImpl<'_>>, data_type: &DataType) -> Result<AttributeValue> {
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
        DataType::List(lt) => {
            let list_attr = scalar_ref
                .into_list()
                .iter()
                .map(|x| map_data(x, lt.elem()))
                .collect::<Result<Vec<_>>>()?;
            AttributeValue::L(list_attr)
        }
        DataType::Struct(st) => {
            let mut map = HashMap::with_capacity(st.len());
            for (sub_datum_ref, (name, data_type)) in scalar_ref
                .into_struct()
                .iter_fields_ref()
                .zip_eq_debug(st.iter())
            {
                let attr = map_data(sub_datum_ref, data_type)?;
                map.insert(name.to_owned(), attr);
            }
            AttributeValue::M(map)
        }
        DataType::Map(_m) => {
            return Err(SinkError::DynamoDb(anyhow!("map is not supported yet")));
        }
        DataType::Vector(_) => {
            return Err(SinkError::DynamoDb(anyhow!("vector is not supported yet")));
        }
    };
    Ok(attr)
}

mod write_chunk_future {
    use std::collections::HashMap;
    use std::time::Duration;

    use anyhow::anyhow;
    use aws_sdk_dynamodb as dynamodb;
    use aws_sdk_dynamodb::client::Client;
    use dynamodb::types::{
        AttributeValue, DeleteRequest, PutRequest, ReturnConsumedCapacity,
        ReturnItemCollectionMetrics, WriteRequest,
    };
    use futures::future::TryJoinAll;
    use futures::prelude::Future;
    use futures::prelude::future::try_join_all;
    use itertools::Itertools;
    use maplit::hashmap;
    use tokio::time::sleep;

    use super::{DynamoDbRequest, Result, SinkError};

    const MAX_BATCH_WRITE_RETRY_TIMES: usize = 3;
    const MIN_BATCH_WRITE_RETRY_DELAY_MS: u64 = 100;
    const MAX_BATCH_WRITE_RETRY_DELAY_MS: u64 = 500;

    pub type WriteChunkFuture = TryJoinAll<impl Future<Output = Result<()>>>;

    pub struct DynamoDbPayloadWriter {
        pub client: Client,
        pub table: String,
        pub dynamodb_keys: Vec<String>,
        pub max_batch_item_nums: usize,
    }

    impl DynamoDbPayloadWriter {
        pub fn write_one_insert(
            &mut self,
            item: HashMap<String, AttributeValue>,
            request_items: &mut Vec<DynamoDbRequest>,
        ) {
            let put_req = PutRequest::builder().set_item(Some(item)).build().unwrap();
            let req = WriteRequest::builder().put_request(put_req).build();
            self.write_one_req(req, request_items);
        }

        pub fn write_one_delete(
            &mut self,
            key: HashMap<String, AttributeValue>,
            request_items: &mut Vec<DynamoDbRequest>,
        ) {
            let key = key
                .into_iter()
                .filter(|(k, _)| self.dynamodb_keys.contains(k))
                .collect();
            let del_req = DeleteRequest::builder().set_key(Some(key)).build().unwrap();
            let req = WriteRequest::builder().delete_request(del_req).build();
            self.write_one_req(req, request_items);
        }

        pub fn write_one_req(
            &mut self,
            req: WriteRequest,
            request_items: &mut Vec<DynamoDbRequest>,
        ) {
            let r_req = DynamoDbRequest {
                inner: req,
                key_items: self.dynamodb_keys.clone(),
            };
            request_items.retain(|item| !item.has_same_pk(&r_req));
            request_items.push(r_req);
        }

        #[define_opaque(WriteChunkFuture)]
        pub fn write_chunk(&mut self, request_items: Vec<DynamoDbRequest>) -> WriteChunkFuture {
            let table = self.table.clone();
            let chunks = request_items
                .into_iter()
                .map(|r| r.inner)
                .chunks(self.max_batch_item_nums);
            let futures = chunks.into_iter().map(|chunk| {
                let client = self.client.clone();
                let table = table.clone();
                let req_items = chunk.collect::<Vec<_>>();
                async move {
                    let mut req_items = req_items;
                    let mut retry_count = 0;

                    loop {
                        let reqs = hashmap! {
                            table.clone() => req_items.clone(),
                        };
                        let result = client
                            .batch_write_item()
                            .set_request_items(Some(reqs))
                            .return_consumed_capacity(ReturnConsumedCapacity::None)
                            .return_item_collection_metrics(ReturnItemCollectionMetrics::None)
                            .send()
                            .await;

                        match result {
                            Ok(output) => {
                                let unprocessed_items =
                                    output.unprocessed_items().cloned().unwrap_or_default();
                                if unprocessed_items.is_empty() {
                                    return Ok(());
                                }

                                req_items = unprocessed_items.into_values().flatten().collect();
                                if retry_count >= MAX_BATCH_WRITE_RETRY_TIMES {
                                    return Err(SinkError::DynamoDb(anyhow!(
                                        "failed to write {} unprocessed items to DynamoDB sink after {} retries",
                                        req_items.len(),
                                        MAX_BATCH_WRITE_RETRY_TIMES,
                                    )));
                                }
                            }
                            Err(e) => {
                                return Err(SinkError::DynamoDb(
                                    anyhow!(e).context("failed to write items to DynamoDB sink"),
                                ));
                            }
                        }

                        retry_count += 1;
                        let delay_ms = rand::random_range(
                            MIN_BATCH_WRITE_RETRY_DELAY_MS..=MAX_BATCH_WRITE_RETRY_DELAY_MS,
                        );
                        tracing::warn!(
                            retry_count,
                            delay_ms,
                            "retrying DynamoDB batch write"
                        );
                        sleep(Duration::from_millis(delay_ms)).await;
                    }
                }
            });
            try_join_all(futures)
        }
    }
}

#[cfg(test)]
mod tests {
    use aws_sdk_dynamodb::types::PutRequest;

    use super::*;

    fn dynamodb_request(
        items: impl IntoIterator<Item = (&'static str, &'static str)>,
    ) -> DynamoDbRequest {
        let item = items
            .into_iter()
            .map(|(k, v)| (k.to_owned(), AttributeValue::S(v.to_owned())))
            .collect();
        let put_req = PutRequest::builder().set_item(Some(item)).build().unwrap();
        DynamoDbRequest {
            inner: WriteRequest::builder().put_request(put_req).build(),
            key_items: vec!["pk".to_owned(), "sk".to_owned()],
        }
    }

    #[test]
    fn dynamodb_request_compares_pk_by_key_attribute() {
        let req = dynamodb_request([("pk", "a"), ("sk", "b")]);
        let swapped_values = dynamodb_request([("pk", "b"), ("sk", "a")]);
        let same_pk = dynamodb_request([("pk", "a"), ("sk", "b")]);

        assert!(!req.has_same_pk(&swapped_values));
        assert!(req.has_same_pk(&same_pk));
    }
}
