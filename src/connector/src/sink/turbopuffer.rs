// Copyright 2026 RisingWave Labs
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
use itertools::Itertools;
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE, HeaderMap, HeaderValue};
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::catalog::Schema;
use risingwave_common::row::Row;
use risingwave_common::types::{DataType, ScalarRefImpl};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use serde_with::{DisplayFromStr, serde_as};
use thiserror_ext::AsReport;
use with_options::WithOptions;

use crate::enforce_secret::EnforceSecret;
use crate::sink::encoder::{JsonEncoder, RowEncoder};
use crate::sink::log_store::DeliveryFutureManagerAddFuture;
use crate::sink::writer::{
    AsyncTruncateLogSinkerOf, AsyncTruncateSinkWriter, AsyncTruncateSinkWriterExt,
};
use crate::sink::{Result, Sink, SinkError, SinkParam, SinkWriterParam};

pub const TURBOPUFFER_SINK: &str = "turbopuffer";

#[serde_as]
#[derive(Clone, Debug, Deserialize, WithOptions)]
pub struct TurbopufferConfig {
    pub base_url: String,
    pub namespace: Option<String>,
    pub namespace_column: Option<String>,
    pub api_key: String,
    pub distance_metric: Option<String>,
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub disable_backpressure: Option<bool>,
    pub full_text_search_columns: Option<String>,
    pub filterable_columns: Option<String>,
    pub r#type: String, // accept "append-only" or "upsert"
}

impl EnforceSecret for TurbopufferConfig {
    const ENFORCE_SECRET_PROPERTIES: phf::Set<&'static str> = phf::phf_set! {
        "api_key",
    };
}

impl TurbopufferConfig {
    fn from_btreemap(values: BTreeMap<String, String>) -> Result<Self> {
        serde_json::from_value::<TurbopufferConfig>(
            serde_json::to_value(values).expect("serialize sink properties"),
        )
        .map_err(|e| SinkError::Config(anyhow!(e)))
    }
}

#[derive(Clone, Debug)]
enum TurbopufferNamespace {
    Static(String),
    Dynamic { index: usize },
}

#[derive(Clone, Debug)]
pub struct TurbopufferSink {
    config: TurbopufferConfig,
    schema: Schema,
    is_append_only: bool,
    pk_index: usize,
    namespace: TurbopufferNamespace,
    attribute_indices: Vec<usize>,
    generated_schema: Value,
}

impl EnforceSecret for TurbopufferSink {
    fn enforce_secret<'a>(
        prop_iter: impl Iterator<Item = &'a str>,
    ) -> crate::error::ConnectorResult<()> {
        for prop in prop_iter {
            TurbopufferConfig::enforce_one(prop)?;
        }
        Ok(())
    }
}

impl TryFrom<SinkParam> for TurbopufferSink {
    type Error = SinkError;

    fn try_from(param: SinkParam) -> std::result::Result<Self, Self::Error> {
        let schema = param.schema();
        let is_append_only = param.sink_type.is_append_only();
        let pk_indices = param.downstream_pk_or_empty();
        let [pk_index] = pk_indices.as_slice() else {
            return Err(SinkError::Config(anyhow!(
                "Turbopuffer sink requires exactly one primary_key column"
            )));
        };
        let pk_index = *pk_index;
        match schema[pk_index].data_type() {
            DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Serial
            | DataType::Varchar => {}
            data_type => {
                return Err(SinkError::Config(anyhow!(
                    "Turbopuffer document id column must be an integer or varchar, got {:?}",
                    data_type
                )));
            }
        };
        let config = TurbopufferConfig::from_btreemap(param.properties)?;

        let namespace = match (&config.namespace, &config.namespace_column) {
            (Some(namespace), None) => {
                validate_namespace(namespace)?;
                TurbopufferNamespace::Static(namespace.clone())
            }
            (None, Some(namespace_column)) => {
                let index = schema
                    .fields()
                    .iter()
                    .position(|field| field.name == *namespace_column)
                    .ok_or_else(|| {
                        SinkError::Config(anyhow!(
                            "Turbopuffer namespace_column '{}' not found in sink schema",
                            namespace_column
                        ))
                    })?;
                if schema[index].data_type != DataType::Varchar {
                    return Err(SinkError::Config(anyhow!(
                        "Turbopuffer namespace_column must be varchar, got {:?}",
                        schema[index].data_type
                    )));
                }
                TurbopufferNamespace::Dynamic { index }
            }
            (Some(_), Some(_)) => {
                return Err(SinkError::Config(anyhow!(
                    "Turbopuffer sink requires only one of namespace or namespace_column"
                )));
            }
            (None, None) => {
                return Err(SinkError::Config(anyhow!(
                    "Turbopuffer sink requires either namespace or namespace_column"
                )));
            }
        };

        // Turbopuffer treats `id` as the document ID in write requests; it is not a schema
        // attribute. Dynamic namespace is also metadata for routing, not a document attribute.
        let excluded_indices = match &namespace {
            TurbopufferNamespace::Static(_) => HashSet::from([pk_index]),
            TurbopufferNamespace::Dynamic { index } => HashSet::from([pk_index, *index]),
        };
        let attribute_indices = (0..schema.len())
            .filter(|idx| !excluded_indices.contains(idx))
            .collect_vec();
        for index in &attribute_indices {
            if schema[*index].name == "id" {
                return Err(SinkError::Config(anyhow!(
                    "Turbopuffer attribute column must not be named id"
                )));
            }
        }
        let full_text_search_columns = parse_column_selection(
            config.full_text_search_columns.as_deref(),
            &schema,
            &attribute_indices,
        )?;
        let filterable_columns = parse_column_selection(
            config.filterable_columns.as_deref(),
            &schema,
            &attribute_indices,
        )?;
        let has_vector = attribute_indices
            .iter()
            .any(|idx| matches!(schema[*idx].data_type, DataType::Vector(_)));
        if has_vector && config.distance_metric.is_none() {
            return Err(SinkError::Config(anyhow!(
                "Turbopuffer sink requires distance_metric when sink schema contains vector columns"
            )));
        }
        // This validates every document attribute type before the writer is created:
        // `build_turbopuffer_schema` calls `turbopuffer_type` for each attribute and
        // returns a config error for unsupported types.
        let generated_schema = build_turbopuffer_schema(
            &schema,
            &attribute_indices,
            &full_text_search_columns,
            &filterable_columns,
        )?;

        Ok(Self {
            config,
            schema,
            is_append_only,
            pk_index,
            namespace,
            attribute_indices,
            generated_schema,
        })
    }
}

impl Sink for TurbopufferSink {
    type LogSinker = AsyncTruncateLogSinkerOf<TurbopufferSinkWriter>;

    const SINK_NAME: &'static str = TURBOPUFFER_SINK;

    async fn validate(&self) -> Result<()> {
        Ok(())
    }

    async fn new_log_sinker(&self, _writer_param: SinkWriterParam) -> Result<Self::LogSinker> {
        Ok(TurbopufferSinkWriter::new(
            self.config.clone(),
            self.schema.clone(),
            self.is_append_only,
            self.pk_index,
            self.namespace.clone(),
            self.attribute_indices.clone(),
            self.generated_schema.clone(),
        )?
        .into_log_sinker(usize::MAX))
    }
}

pub struct TurbopufferSinkWriter {
    client: reqwest::Client,
    base_url: String,
    distance_metric: Option<String>,
    disable_backpressure: Option<bool>,
    schema: Value,
    is_append_only: bool,
    pk_index: usize,
    namespace: TurbopufferNamespace,
    row_encoder: JsonEncoder,
}

impl TurbopufferSinkWriter {
    fn new(
        config: TurbopufferConfig,
        schema: Schema,
        is_append_only: bool,
        pk_index: usize,
        namespace: TurbopufferNamespace,
        attribute_indices: Vec<usize>,
        generated_schema: Value,
    ) -> Result<Self> {
        let mut header_map = HeaderMap::new();
        header_map.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        let authorization = format!("Bearer {}", config.api_key);
        header_map.insert(
            AUTHORIZATION,
            authorization
                .parse()
                .context("invalid turbopuffer api_key")
                .map_err(SinkError::Config)?,
        );
        let client = reqwest::Client::builder()
            .default_headers(header_map)
            .build()
            .context("failed to build turbopuffer HTTP client")
            .map_err(SinkError::Http)?;
        let base_url = config
            .base_url
            .parse::<reqwest::Url>()
            .context("invalid turbopuffer base_url")
            .map_err(SinkError::Config)?
            .to_string()
            .trim_end_matches('/')
            .to_owned();
        let row_encoder = JsonEncoder::new_with_turbopuffer(schema, Some(attribute_indices));
        Ok(Self {
            client,
            base_url,
            distance_metric: config.distance_metric,
            disable_backpressure: config.disable_backpressure,
            schema: generated_schema,
            is_append_only,
            pk_index,
            namespace,
            row_encoder,
        })
    }

    fn url_for_row(&self, row: &impl Row) -> Result<String> {
        match &self.namespace {
            TurbopufferNamespace::Static(namespace) => Ok(format!(
                "{}/v2/namespaces/{}",
                self.base_url,
                namespace.as_str()
            )),
            TurbopufferNamespace::Dynamic { index } => {
                let namespace = match row.datum_at(*index) {
                    Some(ScalarRefImpl::Utf8(namespace)) => namespace,
                    None => {
                        return Err(SinkError::Http(anyhow!(
                            "Turbopuffer namespace_column cannot be null"
                        )));
                    }
                    Some(_) => {
                        return Err(SinkError::Http(anyhow!(
                            "unexpected namespace_column type, expected varchar"
                        )));
                    }
                };
                validate_namespace(namespace)?;
                Ok(format!("{}/v2/namespaces/{}", self.base_url, namespace))
            }
        }
    }

    // Turbopuffer document IDs are unsigned 64-bit integers, UUIDs, or strings up to 64 bytes.
    // RisingWave UUID IDs can be represented with varchar.
    fn id_for_row(&self, row: &impl Row) -> Result<DocumentId> {
        let datum = row.datum_at(self.pk_index).ok_or_else(|| {
            SinkError::Http(anyhow!("Turbopuffer document id column cannot be null"))
        })?;
        match datum {
            ScalarRefImpl::Int16(value) => Ok(document_id_from_i64(value as i64)),
            ScalarRefImpl::Int32(value) => Ok(document_id_from_i64(value as i64)),
            ScalarRefImpl::Int64(value) => Ok(document_id_from_i64(value)),
            ScalarRefImpl::Serial(value) => Ok(document_id_from_i64(value.into_inner())),
            ScalarRefImpl::Utf8(value) => {
                if value.len() > 64 {
                    return Err(SinkError::Http(anyhow!(
                        "Turbopuffer string document id exceeds 64 bytes"
                    )));
                }
                Ok(DocumentId::String(value.to_owned()))
            }
            _ => Err(SinkError::Http(anyhow!(
                "Turbopuffer document id column must be an integer or varchar"
            ))),
        }
    }

    fn upsert_row(&self, row: &impl Row, id: DocumentId) -> Result<Map<String, Value>> {
        let mut value = self.row_encoder.encode(row)?;
        value.insert(
            "id".to_owned(),
            serde_json::to_value(id).expect("serialize document id"),
        );
        Ok(value)
    }

    fn request_body(
        &self,
        upsert_rows: Vec<Map<String, Value>>,
        deletes: Vec<DocumentId>,
    ) -> Value {
        let mut body = Map::new();
        if let Some(distance_metric) = &self.distance_metric {
            body.insert(
                "distance_metric".to_owned(),
                Value::String(distance_metric.clone()),
            );
        }
        if !upsert_rows.is_empty() {
            if let Some(disable_backpressure) = self.disable_backpressure {
                body.insert(
                    "disable_backpressure".to_owned(),
                    Value::Bool(disable_backpressure),
                );
            }
            body.insert("schema".to_owned(), self.schema.clone());
            body.insert(
                "upsert_rows".to_owned(),
                Value::Array(upsert_rows.into_iter().map(Value::Object).collect()),
            );
        }
        if !deletes.is_empty() {
            body.insert(
                "deletes".to_owned(),
                Value::Array(
                    deletes
                        .into_iter()
                        .map(|id| serde_json::to_value(id).expect("serialize document id"))
                        .collect(),
                ),
            );
        }
        Value::Object(body)
    }
}

impl AsyncTruncateSinkWriter for TurbopufferSinkWriter {
    async fn write_chunk<'a>(
        &'a mut self,
        chunk: StreamChunk,
        _add_future: DeliveryFutureManagerAddFuture<'a, Self::DeliveryFuture>,
    ) -> Result<()> {
        if self.is_append_only {
            let mut batches = BTreeMap::<String, Vec<Map<String, Value>>>::new();
            for (op, row) in chunk.rows() {
                match op {
                    Op::Insert | Op::UpdateInsert => {
                        let id = match self.id_for_row(&row) {
                            Ok(id) => id,
                            Err(err) => {
                                tracing::warn!(error = %err.as_report(), "skip turbopuffer row with invalid document id");
                                continue;
                            }
                        };
                        let upsert_row = match self.upsert_row(&row, id) {
                            Ok(row) => row,
                            Err(err) => {
                                tracing::warn!(error = %err.as_report(), "skip turbopuffer row failed to encode upsert payload");
                                continue;
                            }
                        };
                        let url = match self.url_for_row(&row) {
                            Ok(url) => url,
                            Err(err) => {
                                tracing::warn!(error = %err.as_report(), "skip turbopuffer row with invalid namespace");
                                continue;
                            }
                        };
                        batches.entry(url).or_default().push(upsert_row);
                    }
                    Op::Delete | Op::UpdateDelete => {
                        return Err(SinkError::Http(anyhow!(
                            "`Delete` or `UpdateDelete` operation is not supported in append-only turbopuffer sink"
                        )));
                    }
                }
            }

            for (url, upsert_rows) in batches {
                if upsert_rows.is_empty() {
                    continue;
                }
                let resp = self
                    .client
                    .post(url)
                    .json(&self.request_body(upsert_rows, Vec::new()))
                    .send()
                    .await
                    .context("turbopuffer write request failed")
                    .map_err(SinkError::Http)?;

                if !resp.status().is_success() {
                    let status = resp.status();
                    let body = resp.text().await.unwrap_or_default();
                    return Err(SinkError::Http(anyhow!(
                        "Turbopuffer sink received non-success response: {} {}",
                        status,
                        body
                    )));
                }
            }
        } else {
            let mut batches = BTreeMap::<String, HashMap<DocumentId, CompactedOp>>::new();
            for (op, row) in chunk.rows() {
                let id = match self.id_for_row(&row) {
                    Ok(id) => id,
                    Err(err) => {
                        tracing::warn!(error = %err.as_report(), "skip turbopuffer row with invalid document id");
                        continue;
                    }
                };
                let url = match self.url_for_row(&row) {
                    Ok(url) => url,
                    Err(err) => {
                        tracing::warn!(error = %err.as_report(), "skip turbopuffer row with invalid namespace");
                        continue;
                    }
                };
                match op {
                    Op::Insert | Op::UpdateInsert => {
                        let upsert_row = match self.upsert_row(&row, id.clone()) {
                            Ok(row) => row,
                            Err(err) => {
                                tracing::warn!(error = %err.as_report(), "skip turbopuffer row failed to encode upsert payload");
                                continue;
                            }
                        };
                        batches
                            .entry(url)
                            .or_default()
                            .insert(id, CompactedOp::Upsert(upsert_row));
                    }
                    Op::Delete | Op::UpdateDelete => {
                        batches
                            .entry(url)
                            .or_default()
                            .insert(id, CompactedOp::Delete);
                    }
                }
            }

            for (url, batch) in batches {
                let mut upsert_rows = Vec::new();
                let mut deletes = Vec::new();
                for (id, op) in batch {
                    match op {
                        CompactedOp::Upsert(row) => upsert_rows.push(row),
                        CompactedOp::Delete => deletes.push(id),
                    }
                }
                if upsert_rows.is_empty() && deletes.is_empty() {
                    continue;
                }
                let resp = self
                    .client
                    .post(url)
                    .json(&self.request_body(upsert_rows, deletes))
                    .send()
                    .await
                    .context("turbopuffer write request failed")
                    .map_err(SinkError::Http)?;

                if !resp.status().is_success() {
                    let status = resp.status();
                    let body = resp.text().await.unwrap_or_default();
                    return Err(SinkError::Http(anyhow!(
                        "Turbopuffer sink received non-success response: {} {}",
                        status,
                        body
                    )));
                }
            }
        }

        Ok(())
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize)]
#[serde(untagged)]
enum DocumentId {
    U64(u64),
    String(String),
}

#[derive(Debug)]
enum CompactedOp {
    Upsert(Map<String, Value>),
    Delete,
}

fn document_id_from_i64(value: i64) -> DocumentId {
    if value < 0 {
        tracing::warn!(
            value,
            "cast negative turbopuffer integer document id to unsigned integer"
        );
    }
    DocumentId::U64(value as u64)
}

fn validate_namespace(namespace: &str) -> Result<()> {
    if namespace.is_empty() || namespace.len() > 128 {
        return Err(SinkError::Config(anyhow!(
            "Turbopuffer namespace must be 1 to 128 bytes"
        )));
    }
    if !namespace
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
    {
        return Err(SinkError::Config(anyhow!(
            "Turbopuffer namespace must match [A-Za-z0-9-_.]{{1,128}}"
        )));
    }
    Ok(())
}

fn parse_column_selection(
    value: Option<&str>,
    schema: &Schema,
    attribute_indices: &[usize],
) -> Result<HashSet<String>> {
    let attribute_names = attribute_indices
        .iter()
        .map(|index| schema[*index].name.as_str())
        .collect::<HashSet<_>>();
    let columns: HashSet<String> = match value {
        Some(value) if value.trim() == "*" => {
            return Ok(attribute_names
                .into_iter()
                .map(str::to_owned)
                .collect::<HashSet<_>>());
        }
        Some(value) => value
            .split(',')
            .map(str::trim)
            .filter(|name| !name.is_empty())
            .map(str::to_owned)
            .collect(),
        None => return Ok(HashSet::new()),
    };
    for column in &columns {
        if !attribute_names.contains(column.as_str()) {
            return Err(SinkError::Config(anyhow!(
                "Turbopuffer schema option references unknown attribute column '{}'",
                column
            )));
        }
    }
    Ok(columns)
}

fn build_turbopuffer_schema(
    schema: &Schema,
    attribute_indices: &[usize],
    full_text_search_columns: &HashSet<String>,
    filterable_columns: &HashSet<String>,
) -> Result<Value> {
    let mut result = Map::new();
    for index in attribute_indices {
        let field = &schema[*index];
        let mut config = Map::new();
        let data_type = field.data_type();
        let turbopuffer_type = turbopuffer_type(&data_type)?;
        let is_vector = matches!(data_type, DataType::Vector(_));
        let is_full_text_search = full_text_search_columns.contains(&field.name);
        if is_full_text_search && !supports_full_text_search(&data_type) {
            return Err(SinkError::Config(anyhow!(
                "Turbopuffer full_text_search column '{}' must be string or []string",
                field.name
            )));
        }
        config.insert("type".to_owned(), Value::String(turbopuffer_type));
        if filterable_columns.contains(&field.name) {
            config.insert("filterable".to_owned(), Value::Bool(true));
        }
        if is_full_text_search {
            config.insert("full_text_search".to_owned(), Value::Bool(true));
        }
        if is_vector {
            config.insert("ann".to_owned(), Value::Bool(true));
        }
        result.insert(field.name.clone(), Value::Object(config));
    }
    Ok(Value::Object(result))
}

fn supports_full_text_search(data_type: &DataType) -> bool {
    match data_type {
        DataType::Varchar => true,
        DataType::List(list_type) => matches!(list_type.elem(), DataType::Varchar),
        _ => false,
    }
}

// Mapping from RisingWave attribute types to generated turbopuffer schema types and
// the JSON value shapes emitted by `JsonEncoder`:
//
// | RisingWave type                  | turbopuffer type | JSON payload                  |
// |----------------------------------|------------------|-------------------------------|
// | boolean                          | bool             | boolean                       |
// | int16, int32, int64              | int              | number                        |
// | float32, float64                 | float            | number                        |
// | varchar                          | string           | string                        |
// | date                             | datetime         | string: YYYY-MM-DD            |
// | timestamp                        | datetime         | string: YYYY-MM-DD HH:MM:SS   |
// | boolean[]                        | []bool           | array of booleans             |
// | int16[], int32[], int64[]        | []int            | array of numbers              |
// | float32[], float64[]             | []float          | array of numbers              |
// | varchar[]                        | []string         | array of strings              |
// | date[], timestamp[]              | []datetime       | array of datetime strings     |
// | vector(N)                        | [N]f32           | array of numbers              |
// | serial                           | int              | number                        |
// | decimal                          | float            | number, converted through f64 |
// | serial[]                         | []int            | array of numbers              |
// | decimal[]                        | []float          | array of f64-converted numbers|
//
// The primary key column is encoded separately as the turbopuffer document id, so
// it does not participate in this schema mapping.
fn turbopuffer_type(data_type: &DataType) -> Result<String> {
    match data_type {
        DataType::Boolean => Ok("bool".to_owned()),
        DataType::Int16 | DataType::Int32 | DataType::Int64 | DataType::Serial => {
            Ok("int".to_owned())
        }
        DataType::Float32 | DataType::Float64 | DataType::Decimal => Ok("float".to_owned()),
        DataType::Varchar => Ok("string".to_owned()),
        DataType::Date | DataType::Timestamp => Ok("datetime".to_owned()),
        DataType::List(list_type) => match list_type.elem() {
            DataType::Boolean => Ok("[]bool".to_owned()),
            DataType::Int16 | DataType::Int32 | DataType::Int64 | DataType::Serial => {
                Ok("[]int".to_owned())
            }
            DataType::Float32 | DataType::Float64 | DataType::Decimal => Ok("[]float".to_owned()),
            DataType::Varchar => Ok("[]string".to_owned()),
            DataType::Date | DataType::Timestamp => Ok("[]datetime".to_owned()),
            elem_type => Err(unsupported_type(&format!("list element {:?}", elem_type))),
        },
        DataType::Vector(dimension) => Ok(format!("[{}]f32", dimension)),
        data_type => Err(unsupported_type(&format!("{:?}", data_type))),
    }
}

fn unsupported_type(data_type: &str) -> SinkError {
    SinkError::Config(anyhow!(
        "Turbopuffer sink does not support column type {}",
        data_type
    ))
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::mpsc;
    use std::thread;

    use risingwave_common::array::stream_chunk::StreamChunkTestExt as _;
    use risingwave_common::array::{ListValue, StreamChunk, VectorVal};
    use risingwave_common::catalog::Field;
    use risingwave_common::row::OwnedRow;
    use risingwave_common::types::{ListType, ScalarImpl, Timestamp};
    use serde_json::json;

    use super::*;
    use crate::sink::log_store::DeliveryFutureManager;

    #[test]
    fn test_build_schema_flags() {
        let schema = Schema::new(vec![
            Field::with_name(DataType::Varchar, "id"),
            Field::with_name(DataType::Varchar, "body"),
            Field::with_name(DataType::List(ListType::new(DataType::Varchar)), "tags"),
            Field::with_name(DataType::Boolean, "flag"),
            Field::with_name(DataType::Vector(384), "vector"),
        ]);
        let generated = build_turbopuffer_schema(
            &schema,
            &[1, 2, 3, 4],
            &parse_column_selection(Some("body,tags"), &schema, &[1, 2, 3, 4]).unwrap(),
            &parse_column_selection(Some("*"), &schema, &[1, 2, 3, 4]).unwrap(),
        )
        .unwrap();

        assert_eq!(generated["body"]["type"], json!("string"));
        assert_eq!(generated["body"]["filterable"], json!(true));
        assert_eq!(generated["body"]["full_text_search"], json!(true));
        assert_eq!(generated["tags"]["type"], json!("[]string"));
        assert_eq!(generated["tags"]["full_text_search"], json!(true));
        assert_eq!(generated["vector"]["type"], json!("[384]f32"));
        assert_eq!(generated["vector"]["ann"], json!(true));
    }

    #[tokio::test]
    async fn test_write_chunk_posts_batched_payload_and_headers() {
        let (base_url, request_rx, server_thread) = spawn_mock_http_server();
        let schema = Schema::new(vec![
            Field::with_name(DataType::Varchar, "id"),
            Field::with_name(DataType::Varchar, "body"),
            Field::with_name(DataType::Varchar, "workspace_id"),
        ]);
        let payload_indices = vec![1];
        let generated_schema = build_turbopuffer_schema(
            &schema,
            &payload_indices,
            &parse_column_selection(Some("body"), &schema, &payload_indices).unwrap(),
            &parse_column_selection(Some("*"), &schema, &payload_indices).unwrap(),
        )
        .unwrap();
        let config = TurbopufferConfig {
            base_url,
            namespace: None,
            namespace_column: Some("workspace_id".to_owned()),
            api_key: "tpuf_test_key".to_owned(),
            distance_metric: None,
            disable_backpressure: Some(true),
            full_text_search_columns: Some("body".to_owned()),
            filterable_columns: Some("*".to_owned()),
            r#type: "upsert".to_owned(),
        };
        let mut writer = TurbopufferSinkWriter::new(
            config,
            schema,
            false,
            0,
            TurbopufferNamespace::Dynamic { index: 2 },
            payload_indices,
            generated_schema,
        )
        .unwrap();
        let chunk = StreamChunk::from_pretty(
            "T  T        T
            U- old-id   old_body ns_1
            U+ new-id   new_body ns_1",
        );
        let mut future_manager = DeliveryFutureManager::new(0);

        writer
            .write_chunk(chunk, future_manager.start_write_chunk(0, 0))
            .await
            .unwrap();
        let request = request_rx.recv().unwrap();
        server_thread.join().unwrap();

        assert!(request.starts_with("post /v2/namespaces/ns_1 http/1.1"));
        assert!(request.contains("authorization: bearer tpuf_test_key"));
        assert!(request.contains("content-type: application/json"));

        let body = request.split("\r\n\r\n").nth(1).unwrap();
        let body: Value = serde_json::from_str(body).unwrap();
        assert_eq!(body["disable_backpressure"], json!(true));
        assert_eq!(body["schema"]["body"]["type"], json!("string"));
        assert_eq!(body["schema"]["body"]["filterable"], json!(true));
        assert_eq!(body["schema"]["body"]["full_text_search"], json!(true));
        assert_eq!(body["deletes"], json!(["old-id"]));
        assert_eq!(body["upsert_rows"].as_array().unwrap().len(), 1);
        assert_eq!(body["upsert_rows"][0]["id"], json!("new-id"));
        assert_eq!(body["upsert_rows"][0]["body"], json!("new_body"));
        assert!(body.get("distance_metric").is_none());
    }

    #[test]
    fn test_decimal_and_serial_schema_types() {
        assert_eq!(turbopuffer_type(&DataType::Decimal).unwrap(), "float");
        assert_eq!(turbopuffer_type(&DataType::Serial).unwrap(), "int");
        assert_eq!(
            turbopuffer_type(&DataType::List(ListType::new(DataType::Decimal))).unwrap(),
            "[]float"
        );
        assert_eq!(
            turbopuffer_type(&DataType::List(ListType::new(DataType::Serial))).unwrap(),
            "[]int"
        );
    }

    #[test]
    fn test_manual_http_sink_schema_and_payload_shape() {
        let schema = Schema::new(vec![
            Field::with_name(DataType::Varchar, "id"),
            Field::with_name(DataType::Varchar, "workspaceId"),
            Field::with_name(DataType::Varchar, "inboxFeedItemId"),
            Field::with_name(DataType::Varchar, "body"),
            Field::with_name(
                DataType::List(ListType::new(DataType::Varchar)),
                "noteContents",
            ),
            Field::with_name(DataType::Varchar, "communityMemberHandle"),
            Field::with_name(DataType::Varchar, "communityMemberIdentifier"),
            Field::with_name(DataType::Varchar, "inboxFeedItemTitle"),
            Field::with_name(DataType::Boolean, "isInboxFeedItemStarred"),
            Field::with_name(DataType::Boolean, "isInboxFeedItemAnswered"),
            Field::with_name(DataType::Timestamp, "inboxFeedItemPreviewTimestamp"),
            Field::with_name(DataType::Timestamp, "inboxFeedItemPublishTimestamp"),
            Field::with_name(DataType::Int64, "inboxFeedItemAuthorInstagramFollowerCount"),
            Field::with_name(DataType::Int64, "inboxFeedItemAuthorTikTokFollowerCount"),
            Field::with_name(
                DataType::List(ListType::new(DataType::Varchar)),
                "attributes",
            ),
            Field::with_name(DataType::Varchar, "threadId"),
            Field::with_name(DataType::Vector(384), "vector"),
        ]);
        let attribute_indices = (2..schema.len()).collect_vec();
        let full_text_search_columns = parse_column_selection(
            Some(
                "body,noteContents,communityMemberHandle,communityMemberIdentifier,inboxFeedItemTitle",
            ),
            &schema,
            &attribute_indices,
        )
        .unwrap();
        let filterable_columns =
            parse_column_selection(Some("*"), &schema, &attribute_indices).unwrap();
        let generated_schema = build_turbopuffer_schema(
            &schema,
            &attribute_indices,
            &full_text_search_columns,
            &filterable_columns,
        )
        .unwrap();

        assert_eq!(
            generated_schema,
            json!({
                "inboxFeedItemId": {"type": "string", "filterable": true},
                "body": {"type": "string", "filterable": true, "full_text_search": true},
                "noteContents": {"type": "[]string", "filterable": true, "full_text_search": true},
                "communityMemberHandle": {"type": "string", "filterable": true, "full_text_search": true},
                "communityMemberIdentifier": {"type": "string", "filterable": true, "full_text_search": true},
                "inboxFeedItemTitle": {"type": "string", "filterable": true, "full_text_search": true},
                "isInboxFeedItemStarred": {"type": "bool", "filterable": true},
                "isInboxFeedItemAnswered": {"type": "bool", "filterable": true},
                "inboxFeedItemPreviewTimestamp": {"type": "datetime", "filterable": true},
                "inboxFeedItemPublishTimestamp": {"type": "datetime", "filterable": true},
                "inboxFeedItemAuthorInstagramFollowerCount": {"type": "int", "filterable": true},
                "inboxFeedItemAuthorTikTokFollowerCount": {"type": "int", "filterable": true},
                "attributes": {"type": "[]string", "filterable": true},
                "threadId": {"type": "string", "filterable": true},
                "vector": {"type": "[384]f32", "filterable": true, "ann": true}
            })
        );

        let config = TurbopufferConfig {
            base_url: "http://127.0.0.1:0".to_owned(),
            namespace: None,
            namespace_column: Some("workspaceId".to_owned()),
            api_key: "tpuf_test_key".to_owned(),
            distance_metric: Some("cosine_distance".to_owned()),
            disable_backpressure: Some(true),
            full_text_search_columns: Some("body,noteContents,communityMemberHandle,communityMemberIdentifier,inboxFeedItemTitle".to_owned()),
            filterable_columns: Some("*".to_owned()),
            r#type: "upsert".to_owned(),
        };
        let writer = TurbopufferSinkWriter::new(
            config,
            schema,
            false,
            0,
            TurbopufferNamespace::Dynamic { index: 1 },
            attribute_indices,
            generated_schema.clone(),
        )
        .unwrap();
        let vector =
            VectorVal::from_text(&format!("[{}]", vec!["0.25"; 384].join(",")), 384).unwrap();
        let row = OwnedRow::new(vec![
            Some(ScalarImpl::Utf8("doc-1".into())),
            Some(ScalarImpl::Utf8("workspace-1".into())),
            Some(ScalarImpl::Utf8("item-1".into())),
            Some(ScalarImpl::Utf8("body text".into())),
            Some(ScalarImpl::List(ListValue::from_iter(["note a", "note b"]))),
            Some(ScalarImpl::Utf8("@member".into())),
            Some(ScalarImpl::Utf8("member-1".into())),
            Some(ScalarImpl::Utf8("title".into())),
            Some(ScalarImpl::Bool(true)),
            Some(ScalarImpl::Bool(false)),
            Some(ScalarImpl::Timestamp(Timestamp::from_timestamp_uncheck(
                1_781_582_706,
                0,
            ))),
            Some(ScalarImpl::Timestamp(Timestamp::from_timestamp_uncheck(
                1_781_598_707,
                0,
            ))),
            Some(ScalarImpl::Int64(12345)),
            Some(ScalarImpl::Int64(67890)),
            Some(ScalarImpl::List(ListValue::from_iter(["important", "vip"]))),
            Some(ScalarImpl::Utf8("thread-1".into())),
            Some(ScalarImpl::Vector(vector)),
        ]);
        let id = writer.id_for_row(&row).unwrap();
        let upsert_row = writer.upsert_row(&row, id).unwrap();
        let body = writer.request_body(vec![upsert_row], Vec::new());

        assert_eq!(body["distance_metric"], json!("cosine_distance"));
        assert_eq!(body["disable_backpressure"], json!(true));
        assert_eq!(body["schema"], generated_schema);
        assert_eq!(body["upsert_rows"][0]["id"], json!("doc-1"));
        assert_eq!(body["upsert_rows"][0]["body"], json!("body text"));
        assert_eq!(
            body["upsert_rows"][0]["noteContents"],
            json!(["note a", "note b"])
        );
        assert_eq!(
            body["upsert_rows"][0]["isInboxFeedItemStarred"],
            json!(true)
        );
        assert_eq!(
            body["upsert_rows"][0]["isInboxFeedItemAnswered"],
            json!(false)
        );
        assert_eq!(
            body["upsert_rows"][0]["inboxFeedItemAuthorInstagramFollowerCount"],
            json!(12345)
        );
        assert_eq!(
            body["upsert_rows"][0]["inboxFeedItemPreviewTimestamp"],
            json!("2026-06-16 04:05:06.000000")
        );
        assert_eq!(
            body["upsert_rows"][0]["vector"].as_array().unwrap().len(),
            384
        );
        assert_eq!(body["upsert_rows"][0]["vector"][0], json!(0.25));
    }

    fn spawn_mock_http_server() -> (String, mpsc::Receiver<String>, thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let (request_tx, request_rx) = mpsc::channel();
        let server_thread = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut buf = Vec::new();
            let header_end = loop {
                let mut tmp = [0; 1024];
                let read = stream.read(&mut tmp).unwrap();
                assert_ne!(read, 0);
                buf.extend_from_slice(&tmp[..read]);
                if let Some(header_end) = find_header_end(&buf) {
                    break header_end;
                }
            };
            let headers = String::from_utf8_lossy(&buf[..header_end]);
            let content_length = headers
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse::<usize>().unwrap())
                })
                .unwrap();
            while buf.len() < header_end + 4 + content_length {
                let mut tmp = [0; 1024];
                let read = stream.read(&mut tmp).unwrap();
                assert_ne!(read, 0);
                buf.extend_from_slice(&tmp[..read]);
            }
            let request = String::from_utf8(buf[..header_end + 4 + content_length].to_vec())
                .expect("HTTP request should be utf8");
            request_tx.send(request.to_lowercase()).unwrap();
            stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                .unwrap();
        });
        (format!("http://{}", addr), request_rx, server_thread)
    }

    fn find_header_end(buf: &[u8]) -> Option<usize> {
        buf.windows(4).position(|window| window == b"\r\n\r\n")
    }
}
