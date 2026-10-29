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

use std::collections::{BTreeMap, HashMap};

use anyhow::anyhow;
use base64::Engine;
use base64::engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD};
use firestore::{FirestoreBatchWriter, FirestoreDb, FirestoreDbOptions};
use futures::StreamExt;
use gcloud_sdk::google::firestore::v1::value::ValueType;
use gcloud_sdk::google::firestore::v1::{ArrayValue, Document, MapValue, Value, Write, write};
use risingwave_common::array::{Op, RowRef, StreamChunk};
use risingwave_common::catalog::Schema;
use risingwave_common::row::Row as _;
use risingwave_common::types::{DataType, ScalarRefImpl, ToText};
use risingwave_common::util::iter_util::ZipEqDebug;
use serde::Deserialize;
use serde_json::Value as JsonValue;
use serde_with::{DisplayFromStr, serde_as};
use with_options::WithOptions;

use super::log_store::DeliveryFutureManagerAddFuture;
use super::writer::{
    AsyncTruncateLogSinkerOf, AsyncTruncateSinkWriter, AsyncTruncateSinkWriterExt,
};
use super::{Result, Sink, SinkError, SinkParam, SinkWriterParam};
use crate::enforce_secret::EnforceSecret;

pub const FIRESTORE_SINK: &str = "firestore";

const DEFAULT_MAX_BATCH_SIZE: usize = 500; // Firestore batch limit
const DEFAULT_MAX_FUTURE_SEND_NUMS: usize = 256;

#[serde_as]
#[derive(Deserialize, Debug, Clone, WithOptions)]
pub struct FirestoreConfig {
    /// Google Cloud project ID
    #[serde(rename = "firestore.project_id")]
    pub project_id: String,

    /// Firestore collection name
    #[serde(rename = "firestore.collection")]
    pub collection: String,

    /// Optional: Firestore database ID (for multi-database projects)
    #[serde(rename = "firestore.database_id")]
    pub database_id: Option<String>,

    /// A JSON string containing the service account credentials for authorization
    /// see the [service-account](https://developers.google.com/workspace/guides/create-credentials#create_credentials_for_a_service_account) credentials guide.
    #[serde(rename = "firestore.credentials")]
    pub credentials: Option<String>,

    /// Column whose value is used as the Firestore document ID. Values must be unique and valid
    /// Firestore document IDs. Duplicate values intentionally map multiple rows to the same
    /// Firestore document.
    #[serde(rename = "firestore.document_id_column")]
    pub document_id_column: Option<String>,

    /// Maximum batch size for batch writes (default: 500)
    #[serde(
        rename = "firestore.max_batch_size",
        default = "default_max_batch_size"
    )]
    #[serde_as(as = "DisplayFromStr")]
    pub max_batch_size: usize,

    /// Maximum concurrent batch write requests within one chunk (default: 256).
    /// Chunks are written sequentially to preserve per-document mutation order.
    #[serde(
        rename = "firestore.max_future_send_nums",
        default = "default_max_future_send_nums"
    )]
    #[serde_as(as = "DisplayFromStr")]
    pub max_future_send_nums: usize,
}

fn default_max_batch_size() -> usize {
    DEFAULT_MAX_BATCH_SIZE
}

fn default_max_future_send_nums() -> usize {
    DEFAULT_MAX_FUTURE_SEND_NUMS
}

impl EnforceSecret for FirestoreConfig {
    const ENFORCE_SECRET_PROPERTIES: phf::Set<&'static str> = phf::phf_set! {
        "firestore.credentials",
    };
}

impl FirestoreConfig {
    pub async fn build_client(&self) -> Result<FirestoreDb> {
        self.build_client_with_emulator(std::env::var("FIRESTORE_EMULATOR_HOST").ok())
            .await
    }

    async fn build_client_with_emulator(
        &self,
        emulator_host: Option<String>,
    ) -> Result<FirestoreDb> {
        let mut options = if let Some(ref database_id) = self.database_id {
            FirestoreDbOptions::new(self.project_id.clone()).with_database_id(database_id.clone())
        } else {
            FirestoreDbOptions::new(self.project_id.clone())
        };

        let db = if let Some(host) = emulator_host {
            // The SDK already supports the emulator endpoint, but still attempts ADC by default.
            // Use the emulator's owner token without fetching or forwarding cloud credentials.
            use gcloud_sdk::{ExternalJwtFunctionSource, Token, TokenSourceType};
            options.firebase_api_url = Some(format!("http://{host}"));
            let source = ExternalJwtFunctionSource::new(|| async {
                Ok(Token::new(
                    "Bearer".to_owned(),
                    "owner".to_owned().into(),
                    chrono::Utc::now() + chrono::Duration::hours(1),
                ))
            });
            FirestoreDb::with_options_token_source(
                options,
                vec![],
                TokenSourceType::ExternalSource(Box::new(source)),
            )
            .await
        } else if let Some(ref cred) = self.credentials {
            // Use JSON credentials string with gcloud-sdk's TokenSourceType
            // firestore internally uses gcloud-sdk, so we use TokenSourceType::Json
            use gcloud_sdk::{GCP_DEFAULT_SCOPES, TokenSourceType};

            FirestoreDb::with_options_token_source(
                options,
                GCP_DEFAULT_SCOPES.clone(),
                TokenSourceType::Json(cred.clone()),
            )
            .await
        } else {
            // Use default credentials (from environment, metadata server, etc.)
            FirestoreDb::with_options(options).await
        };

        db.map_err(|e| {
            SinkError::Firestore(anyhow!(e).context("failed to create Firestore client"))
        })
    }

    fn validate_schema(&self, schema: &Schema, pk_indices: &[usize]) -> Result<Option<usize>> {
        if !(1..=DEFAULT_MAX_BATCH_SIZE).contains(&self.max_batch_size) {
            return Err(SinkError::Config(anyhow!(
                "firestore.max_batch_size must be between 1 and {}",
                DEFAULT_MAX_BATCH_SIZE
            )));
        }
        if self.max_future_send_nums == 0 {
            return Err(SinkError::Config(anyhow!(
                "firestore.max_future_send_nums must be greater than 0"
            )));
        }
        if let Some(name) = &self.document_id_column {
            return schema
                .fields()
                .iter()
                .position(|f| &f.name == name)
                .map(Some)
                .ok_or_else(|| {
                    SinkError::Config(anyhow!("document_id_column '{}' not found in schema", name))
                });
        }
        if pk_indices.is_empty() {
            return Err(SinkError::Config(anyhow!(
                "Either document_id_column or primary key must be specified"
            )));
        }
        Ok(None)
    }

    fn from_btreemap(values: BTreeMap<String, String>) -> Result<Self> {
        serde_json::from_value::<FirestoreConfig>(serde_json::to_value(values).unwrap())
            .map_err(|e| SinkError::Config(anyhow!(e)))
    }
}

#[derive(Clone, Debug)]
pub struct FirestoreSink {
    pub config: FirestoreConfig,
    schema: Schema,
    pk_indices: Vec<usize>,
}

impl EnforceSecret for FirestoreSink {
    fn enforce_secret<'a>(
        prop_iter: impl Iterator<Item = &'a str>,
    ) -> crate::error::ConnectorResult<()> {
        for prop in prop_iter {
            FirestoreConfig::enforce_one(prop)?;
        }
        Ok(())
    }
}

impl Sink for FirestoreSink {
    type LogSinker = AsyncTruncateLogSinkerOf<FirestoreSinkWriter>;

    const SINK_NAME: &'static str = FIRESTORE_SINK;

    async fn validate(&self) -> Result<()> {
        self.config
            .validate_schema(&self.schema, &self.pk_indices)?;
        self.config.build_client().await?;

        Ok(())
    }

    async fn new_log_sinker(&self, _writer_param: SinkWriterParam) -> Result<Self::LogSinker> {
        Ok(FirestoreSinkWriter::new(
            self.config.clone(),
            self.schema.clone(),
            self.pk_indices.clone(),
        )
        .await?
        // Writes finish inline; request concurrency is bounded inside each chunk.
        .into_log_sinker(1))
    }
}

impl TryFrom<SinkParam> for FirestoreSink {
    type Error = SinkError;

    fn try_from(param: SinkParam) -> std::result::Result<Self, Self::Error> {
        let schema = param.schema();
        let pk_indices = param.downstream_pk_or_empty();
        let config = FirestoreConfig::from_btreemap(param.properties)?;

        Ok(Self {
            config,
            schema,
            pk_indices,
        })
    }
}

pub struct FirestoreSinkWriter {
    client: FirestoreDb,
    config: FirestoreConfig,
    schema: Schema,
    pk_indices: Vec<usize>,
    doc_id_col_idx: Option<usize>,
}

impl FirestoreSinkWriter {
    pub async fn new(
        config: FirestoreConfig,
        schema: Schema,
        pk_indices: Vec<usize>,
    ) -> Result<Self> {
        let doc_id_col_idx = config.validate_schema(&schema, &pk_indices)?;
        let client = config.build_client().await?;

        Ok(Self {
            client,
            config,
            schema,
            pk_indices,
            doc_id_col_idx,
        })
    }

    fn get_document_id(&self, row: RowRef<'_>) -> Result<String> {
        if let Some(idx) = self.doc_id_col_idx {
            // Use specified document ID column
            let datum = row.datum_at(idx);
            match datum {
                Some(scalar) => {
                    let doc_id = scalar.to_text();
                    validate_firestore_document_id(&doc_id)?;
                    Ok(doc_id)
                }
                None => Err(SinkError::Firestore(anyhow!(
                    "document_id_column value is null"
                ))),
            }
        } else {
            // Encode the PK tuple unambiguously. Joining text values is unsafe for
            // composite keys because values may contain the separator.
            let pk_values: Vec<JsonValue> = self
                .pk_indices
                .iter()
                .map(|&idx| {
                    let field = &self.schema.fields()[idx];
                    row.datum_at(idx)
                        .map(|s| JsonValue::String(s.to_text_with_type(&field.data_type)))
                        .unwrap_or(JsonValue::Null)
                })
                .collect();
            encode_pk_document_id(pk_values)
        }
    }

    fn format_row(&self, row: RowRef<'_>) -> Result<HashMap<String, Value>> {
        let mut map = HashMap::new();
        for (datum, field) in row.iter().zip_eq_debug(self.schema.fields()) {
            let value = format_datum(datum, &field.data_type)?;
            map.insert(field.name.clone(), value);
        }
        Ok(map)
    }

    fn write_chunk_inner(&self, chunk: StreamChunk) -> Result<Vec<FirestoreOperation>> {
        let mut operations = BTreeMap::new();

        for (op, row) in chunk.rows() {
            let doc_id = self.get_document_id(row)?;

            match op {
                Op::Insert | Op::UpdateInsert => {
                    let data = self.format_row(row)?;
                    operations.insert(doc_id.clone(), FirestoreOperation::Upsert { doc_id, data });
                }
                Op::Delete => {
                    operations.insert(doc_id.clone(), FirestoreOperation::Delete { doc_id });
                }
                Op::UpdateDelete => {
                    operations.insert(doc_id.clone(), FirestoreOperation::Delete { doc_id });
                }
            }
        }

        Ok(operations.into_values().collect())
    }

    async fn write_operations(&self, operations: Vec<FirestoreOperation>) -> Result<()> {
        let requests = operations.chunks(self.config.max_batch_size).map(|chunk| {
            let client = self.client.clone();
            let collection = self.config.collection.clone();
            let chunk = chunk.to_vec();
            async move {
                let document_ids = chunk
                    .iter()
                    .map(|op| op.document_id().to_owned())
                    .collect::<Vec<_>>();
                let writes = chunk
                    .into_iter()
                    .map(|op| {
                        let name = format!(
                            "{}/{}/{}",
                            client.get_documents_path(),
                            collection,
                            op.document_id()
                        );
                        let operation = match op {
                            FirestoreOperation::Upsert { data, .. } => {
                                write::Operation::Update(Document {
                                    name,
                                    fields: data,
                                    ..Default::default()
                                })
                            }
                            FirestoreOperation::Delete { .. } => write::Operation::Delete(name),
                        };
                        Write {
                            operation: Some(operation),
                            ..Default::default()
                        }
                    })
                    .collect::<Vec<_>>();
                let writer = client.create_simple_batch_writer().await.map_err(|e| {
                    SinkError::Firestore(
                        anyhow!(e).context("failed to create Firestore batch writer"),
                    )
                })?;
                let response = writer.write(writes).await.map_err(|e| {
                    SinkError::Firestore(
                        anyhow!(e).context("failed to execute Firestore batch write"),
                    )
                })?;
                check_batch_statuses(&response.statuses, &document_ids)
            }
        });

        // Each document occurs at most once per chunk. Only these independent requests may
        // run concurrently. Drain requests even on failure before allowing rewind/replay.
        futures::stream::iter(requests)
            .buffer_unordered(self.config.max_future_send_nums)
            .fold(Ok(()), |result, next| async move { result.and(next) })
            .await
    }
}

fn check_batch_statuses(
    statuses: &[gcloud_sdk::google::rpc::Status],
    document_ids: &[String],
) -> Result<()> {
    if statuses.len() != document_ids.len() {
        return Err(SinkError::Firestore(anyhow!(
            "Firestore batch write returned {} statuses for {} writes",
            statuses.len(),
            document_ids.len()
        )));
    }
    for (status, document_id) in statuses.iter().zip_eq_debug(document_ids) {
        if status.code != 0 {
            return Err(SinkError::Firestore(anyhow!(
                "Firestore batch write failed for document '{}': code={}, message={}",
                document_id,
                status.code,
                status.message
            )));
        }
    }
    Ok(())
}

#[derive(Clone)]
enum FirestoreOperation {
    Upsert {
        doc_id: String,
        data: HashMap<String, Value>,
    },
    Delete {
        doc_id: String,
    },
}

impl FirestoreOperation {
    fn document_id(&self) -> &str {
        match self {
            Self::Upsert { doc_id, .. } | Self::Delete { doc_id } => doc_id,
        }
    }
}

// Encode Firestore values directly: JSON cannot represent non-finite floats, and the
// SDK's serde serializer casts u64 to i64 without checking its range.
fn format_datum(datum: Option<ScalarRefImpl<'_>>, data_type: &DataType) -> Result<Value> {
    format_datum_at_depth(datum, data_type, 0)
}

fn check_container_depth(depth: usize) -> Result<()> {
    if depth >= 20 {
        return Err(SinkError::Firestore(anyhow!(
            "Firestore maps and arrays must not exceed 20 levels"
        )));
    }
    Ok(())
}

fn array_value(values: Vec<Value>) -> Result<ValueType> {
    if values
        .iter()
        .any(|v| matches!(v.value_type, Some(ValueType::ArrayValue(_))))
    {
        return Err(SinkError::Firestore(anyhow!(
            "Firestore arrays cannot directly contain arrays"
        )));
    }
    Ok(ValueType::ArrayValue(ArrayValue { values }))
}

fn format_datum_at_depth(
    datum: Option<ScalarRefImpl<'_>>,
    data_type: &DataType,
    depth: usize,
) -> Result<Value> {
    let Some(scalar) = datum else {
        return Ok(Value {
            value_type: Some(ValueType::NullValue(0)),
        });
    };
    let value_type = match data_type {
        DataType::Boolean => ValueType::BooleanValue(scalar.into_bool()),
        DataType::Int16 => ValueType::IntegerValue(scalar.into_int16().into()),
        DataType::Int32 => ValueType::IntegerValue(scalar.into_int32().into()),
        DataType::Int64 => ValueType::IntegerValue(scalar.into_int64()),
        DataType::Float32 => ValueType::DoubleValue(scalar.into_float32().0.into()),
        DataType::Float64 => ValueType::DoubleValue(scalar.into_float64().0),
        DataType::Decimal
        | DataType::Int256
        | DataType::Serial
        | DataType::Interval
        | DataType::Date
        | DataType::Time
        | DataType::Timestamp
        | DataType::Timestamptz => ValueType::StringValue(scalar.to_text_with_type(data_type)),
        DataType::Varchar => ValueType::StringValue(scalar.into_utf8().to_owned()),
        DataType::Bytea => ValueType::StringValue(STANDARD.encode(scalar.into_bytea())),
        DataType::Jsonb => return format_jsonb(scalar.into_jsonb().into(), depth),
        DataType::List(inner) => {
            check_container_depth(depth)?;
            let values = scalar
                .into_list()
                .iter()
                .map(|item| format_datum_at_depth(item, inner.elem(), depth + 1))
                .collect::<Result<Vec<_>>>()?;
            array_value(values)?
        }
        DataType::Struct(st) => {
            check_container_depth(depth)?;
            let mut fields = HashMap::new();
            for (datum, (name, dt)) in scalar
                .into_struct()
                .iter_fields_ref()
                .zip_eq_debug(st.iter())
            {
                fields.insert(
                    name.to_owned(),
                    format_datum_at_depth(datum, dt, depth + 1)?,
                );
            }
            ValueType::MapValue(MapValue { fields })
        }
        DataType::Map(_) | DataType::Vector(_) => {
            return Err(SinkError::Firestore(anyhow!(
                "{} is not supported by Firestore sink",
                data_type
            )));
        }
    };
    Ok(Value {
        value_type: Some(value_type),
    })
}

fn format_jsonb(value: jsonbb::ValueRef<'_>, depth: usize) -> Result<Value> {
    let value_type =
        match value {
            jsonbb::ValueRef::Null => ValueType::NullValue(0),
            jsonbb::ValueRef::Bool(v) => ValueType::BooleanValue(v),
            jsonbb::ValueRef::String(v) => ValueType::StringValue(v.as_str().to_owned()),
            jsonbb::ValueRef::Number(v) => {
                if let Some(v) = v.as_i64() {
                    ValueType::IntegerValue(v)
                } else if v.as_u64().is_some() {
                    return Err(SinkError::Firestore(anyhow!(
                        "JSONB integer exceeds Firestore signed 64-bit range"
                    )));
                } else {
                    ValueType::DoubleValue(v.as_f64().ok_or_else(|| {
                        SinkError::Firestore(anyhow!("JSONB number out of range"))
                    })?)
                }
            }
            jsonbb::ValueRef::Array(v) => {
                check_container_depth(depth)?;
                array_value(
                    v.iter()
                        .map(|v| format_jsonb(v, depth + 1))
                        .collect::<Result<_>>()?,
                )?
            }
            jsonbb::ValueRef::Object(v) => {
                check_container_depth(depth)?;
                let fields = v
                    .iter()
                    .map(|(k, v)| Ok((k.to_owned(), format_jsonb(v, depth + 1)?)))
                    .collect::<Result<_>>()?;
                ValueType::MapValue(MapValue { fields })
            }
        };
    Ok(Value {
        value_type: Some(value_type),
    })
}

fn encode_pk_document_id(pk_values: Vec<JsonValue>) -> Result<String> {
    let doc_id =
        URL_SAFE_NO_PAD.encode(serde_json::to_vec(&pk_values).map_err(|e| {
            SinkError::Firestore(anyhow!(e).context("failed to encode primary key"))
        })?);
    validate_firestore_document_id(&doc_id)?;
    Ok(doc_id)
}

fn validate_firestore_document_id(doc_id: &str) -> Result<()> {
    if doc_id.len() > 1500 || (doc_id.starts_with("__") && doc_id.ends_with("__")) {
        return Err(SinkError::Firestore(anyhow!(
            "Firestore document ID exceeds 1500 bytes or uses a reserved name"
        )));
    }
    if doc_id.is_empty() {
        return Err(SinkError::Firestore(anyhow!(
            "Firestore document ID must not be empty"
        )));
    }
    if doc_id.contains('/') {
        return Err(SinkError::Firestore(anyhow!(
            "Firestore document ID must not contain '/'"
        )));
    }
    if doc_id == "." || doc_id == ".." {
        return Err(SinkError::Firestore(anyhow!(
            "Firestore document ID must not be '.' or '..'"
        )));
    }
    Ok(())
}

impl AsyncTruncateSinkWriter for FirestoreSinkWriter {
    async fn write_chunk<'a>(
        &'a mut self,
        chunk: StreamChunk,
        _add_future: DeliveryFutureManagerAddFuture<'a, Self::DeliveryFuture>,
    ) -> Result<()> {
        let operations = self.write_chunk_inner(chunk)?;
        self.write_operations(operations).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests;
