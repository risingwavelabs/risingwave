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
use std::time::Duration;

use anyhow::anyhow;
use base64::Engine;
use base64::engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD};
use firestore::{FirestoreDb, FirestoreDbOptions};
use futures::StreamExt;
use gcloud_sdk::google::firestore::admin::v1::GetDatabaseRequest;
use gcloud_sdk::google::firestore::admin::v1::database::DatabaseEdition;
use gcloud_sdk::google::firestore::admin::v1::firestore_admin_client::FirestoreAdminClient;
use gcloud_sdk::google::firestore::v1::value::ValueType;
use gcloud_sdk::google::firestore::v1::{
    ArrayValue, BatchWriteRequest, Document, MapValue, Value, Write, write,
};
use prost::Message as _;
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
const MAX_DOCUMENT_SIZE: usize = 1_048_576;
// Leave room for the database name and protobuf framing below the 10 MiB RPC limit.
const MAX_BATCH_WRITE_BYTES: usize = 9 * 1024 * 1024;
const DATABASE_METADATA_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Deserialize, Debug, Clone, Copy, Default, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum FirestoreDecimalMode {
    #[default]
    Decimal128,
    Double,
}

impl crate::with_options::WithOptions for FirestoreDecimalMode {}

#[serde_as]
#[derive(Deserialize, Debug, Clone, WithOptions)]
pub struct FirestoreConfig {
    /// Google Cloud project ID
    #[serde(rename = "firestore.project_id")]
    pub project_id: String,

    /// Collection path. A parent document segment may reference a column, e.g. `users/{user_id}/orders`.
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
    /// Firestore document IDs. Duplicate values in the same resolved collection intentionally
    /// map multiple rows to the same Firestore document.
    #[serde(rename = "firestore.document_id_column")]
    pub document_id_column: Option<String>,

    /// Skip invalid rows and document-level `INVALID_ARGUMENT/OUT_OF_RANGE` rejections (default: false).
    /// Skipped mutations are not delivered; authentication and transient failures still fail the chunk.
    #[serde(rename = "firestore.skip_invalid_rows", default)]
    #[serde_as(as = "DisplayFromStr")]
    pub skip_invalid_rows: bool,

    /// DECIMAL encoding: `decimal128` (default, Enterprise only) or `double` (may lose precision).
    /// Database metadata is checked for Decimal128; unavailable metadata never selects double.
    #[serde(rename = "firestore.decimal_mode", default)]
    pub decimal_mode: FirestoreDecimalMode,

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

        if let Some(ref host) = emulator_host {
            options.firebase_api_url = Some(format!("http://{host}"));
        }
        let (scopes, token_source) = self.token_source(emulator_host.is_some());
        FirestoreDb::with_options_token_source(options, scopes, token_source)
            .await
            .map_err(|e| {
                SinkError::Firestore(anyhow!(e).context("failed to create Firestore client"))
            })
    }

    fn token_source(&self, emulator: bool) -> (Vec<String>, gcloud_sdk::TokenSourceType) {
        use gcloud_sdk::{ExternalJwtFunctionSource, GCP_DEFAULT_SCOPES, Token, TokenSourceType};
        if emulator {
            // Neither the data client nor the metadata client should fetch ADC or
            // forward configured cloud credentials to an emulator.
            let source = ExternalJwtFunctionSource::new(|| async {
                Ok(Token::new(
                    "Bearer".to_owned(),
                    "owner".to_owned().into(),
                    chrono::Utc::now() + chrono::Duration::hours(1),
                ))
            });
            (vec![], TokenSourceType::ExternalSource(Box::new(source)))
        } else if let Some(ref cred) = self.credentials {
            (
                GCP_DEFAULT_SCOPES.clone(),
                TokenSourceType::Json(cred.clone()),
            )
        } else {
            (GCP_DEFAULT_SCOPES.clone(), TokenSourceType::Default)
        }
    }

    async fn validate_decimal_mode(&self, schema: &Schema, client: &FirestoreDb) -> Result<()> {
        if self.decimal_mode == FirestoreDecimalMode::Double
            || !schema
                .fields()
                .iter()
                .any(|field| contains_decimal(&field.data_type))
        {
            return Ok(());
        }
        let edition =
            tokio::time::timeout(DATABASE_METADATA_TIMEOUT, self.database_edition(client))
                .await
                .map_err(|_| {
                    SinkError::Firestore(anyhow!("Firestore database metadata request timed out"))
                })??;
        match edition.and_then(|edition| DatabaseEdition::try_from(edition).ok()) {
            Some(DatabaseEdition::Standard) => Err(SinkError::Config(anyhow!(
                "firestore.decimal_mode='decimal128' requires Firestore Enterprise edition; \
                 this database is Standard. Set firestore.decimal_mode='double' to opt into \
                 floating-point conversion (which may lose precision), or cast DECIMAL to VARCHAR"
            ))),
            Some(DatabaseEdition::Enterprise) => Ok(()),
            _ => {
                tracing::warn!(database = %client.get_database_path(), "Firestore database edition is unknown; keeping firestore.decimal_mode='decimal128', without falling back to double");
                Ok(())
            }
        }
    }

    async fn database_edition(&self, client: &FirestoreDb) -> Result<Option<i32>> {
        let options = client.get_options();
        let (scopes, token_source) = self.token_source(options.firebase_api_url.is_some());
        let admin = gcloud_sdk::GoogleApi::from_function_with_token_source(
            FirestoreAdminClient::new,
            options
                .firebase_api_url
                .as_deref()
                .unwrap_or("https://firestore.googleapis.com"),
            Some(client.get_database_path().to_owned()),
            scopes,
            token_source,
        )
        .await
        .map_err(|error| {
            SinkError::Firestore(
                anyhow!(error).context("failed to create Firestore metadata client"),
            )
        })?;
        let mut request = tonic::Request::new(GetDatabaseRequest {
            name: client.get_database_path().to_owned(),
        });
        request.set_timeout(DATABASE_METADATA_TIMEOUT);
        match admin.get().get_database(request).await {
            Ok(response) => Ok(Some(response.into_inner().database_edition)),
            Err(error)
                if matches!(
                    error.code(),
                    tonic::Code::PermissionDenied | tonic::Code::Unimplemented
                ) =>
            {
                tracing::warn!(database = %client.get_database_path(), code = ?error.code(), "Cannot read Firestore database edition; metadata requires datastore.databases.getMetadata and may be unsupported by the emulator");
                Ok(None)
            }
            Err(error) => Err(SinkError::Firestore(
                anyhow!(error).context("failed to read Firestore database edition"),
            )),
        }
    }

    fn validate_schema(&self, schema: &Schema, pk_indices: &[usize]) -> Result<Option<usize>> {
        parse_collection_path(&self.collection, schema)?;
        for (name, value) in [
            ("project_id", Some(&self.project_id)),
            ("database_id", self.database_id.as_ref()),
        ] {
            if value.is_some_and(|value| value.is_empty() || value.contains('/')) {
                return Err(SinkError::Config(anyhow!(
                    "firestore.{} must be a nonempty resource ID without '/'",
                    name
                )));
            }
        }
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
        let client = self.config.build_client().await?;
        self.config
            .validate_decimal_mode(&self.schema, &client)
            .await?;

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
    collection_path: Vec<CollectionPathSegment>,
}

impl FirestoreSinkWriter {
    pub async fn new(
        config: FirestoreConfig,
        schema: Schema,
        pk_indices: Vec<usize>,
    ) -> Result<Self> {
        let doc_id_col_idx = config.validate_schema(&schema, &pk_indices)?;
        let collection_path = parse_collection_path(&config.collection, &schema)?;
        let client = config.build_client().await?;
        config.validate_decimal_mode(&schema, &client).await?;

        Ok(Self {
            client,
            config,
            schema,
            pk_indices,
            doc_id_col_idx,
            collection_path,
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
            let value = format_datum(datum, &field.data_type, self.config.decimal_mode)?;
            map.insert(field.name.clone(), value);
        }
        Ok(map)
    }

    fn get_document_path(&self, row: RowRef<'_>) -> Result<String> {
        let mut segments = Vec::with_capacity(self.collection_path.len() + 1);
        for segment in &self.collection_path {
            segments.push(match segment {
                CollectionPathSegment::Literal(value) => value.clone(),
                CollectionPathSegment::Column(index) => {
                    let value = row
                        .datum_at(*index)
                        .ok_or_else(|| {
                            SinkError::Firestore(anyhow!("Firestore parent document ID is null"))
                        })?
                        .to_text();
                    validate_firestore_document_id(&value)?;
                    value
                }
            });
        }
        segments.push(self.get_document_id(row)?);
        let path = segments.join("/");
        if self.client.get_documents_path().len() + 1 + path.len() > 6 * 1024 {
            return Err(SinkError::Firestore(anyhow!(
                "Firestore document name exceeds 6 KiB"
            )));
        }
        Ok(path)
    }

    fn write_chunk_inner(&self, chunk: StreamChunk) -> Result<Vec<FirestoreOperation>> {
        let mut operations = BTreeMap::new();

        for (op, row) in chunk.rows() {
            let operation = (|| {
                let path = self.get_document_path(row)?;
                Ok(match op {
                    Op::Insert | Op::UpdateInsert => {
                        let data = self.format_row(row)?;
                        validate_document_size(&path, &data)?;
                        FirestoreOperation::Upsert { path, data }
                    }
                    Op::Delete | Op::UpdateDelete => FirestoreOperation::Delete { path },
                })
            })();
            match operation {
                Ok(operation) => {
                    operations.insert(operation.document_path().to_owned(), operation);
                }
                Err(error) if self.config.skip_invalid_rows => {
                    tracing::warn!(error = %error, "skipping invalid Firestore row");
                }
                Err(error) => return Err(error),
            }
        }

        Ok(operations.into_values().collect())
    }

    async fn write_operations(&self, operations: Vec<FirestoreOperation>) -> Result<()> {
        let writes = operations
            .into_iter()
            .map(|op| {
                let name = format!(
                    "{}/{}",
                    self.client.get_documents_path(),
                    op.document_path()
                );
                let operation = match op {
                    FirestoreOperation::Upsert { data, .. } => write::Operation::Update(Document {
                        name,
                        fields: data,
                        ..Default::default()
                    }),
                    FirestoreOperation::Delete { .. } => write::Operation::Delete(name),
                };
                Write {
                    operation: Some(operation),
                    ..Default::default()
                }
            })
            .collect::<Vec<_>>();
        let batches = split_write_batches(writes, self.config.max_batch_size);
        let requests = batches.into_iter().map(|writes| self.write_batch(writes));

        // Each document occurs at most once per chunk. Only these independent requests may
        // run concurrently. Drain requests even on failure before allowing rewind/replay.
        futures::stream::iter(requests)
            .buffer_unordered(self.config.max_future_send_nums)
            .fold(Ok(()), |result, next| async move { result.and(next) })
            .await
    }

    async fn write_batch(&self, writes: Vec<Write>) -> Result<()> {
        let request = BatchWriteRequest {
            database: self.client.get_database_path().clone(),
            writes,
            ..Default::default()
        };
        let response = self
            .client
            .client()
            .get()
            .batch_write(request.clone())
            .await;
        match response {
            Ok(response) => {
                let paths = request
                    .writes
                    .iter()
                    .map(write_document_path)
                    .collect::<Vec<_>>();
                check_batch_statuses(
                    &response.into_inner().status,
                    &paths,
                    self.config.skip_invalid_rows,
                )
            }
            Err(error)
                if self.config.skip_invalid_rows && is_invalid_row_code(error.code() as i32) =>
            {
                // Some servers reject the whole RPC for one invalid document. Isolate it;
                // never discard the other writes in that batch or ignore transport failures.
                for write in request.writes {
                    let path = write_document_path(&write);
                    let response = self
                        .client
                        .client()
                        .get()
                        .batch_write(BatchWriteRequest {
                            database: request.database.clone(),
                            writes: vec![write],
                            ..Default::default()
                        })
                        .await;
                    match response {
                        Ok(response) => {
                            check_batch_statuses(&response.into_inner().status, &[path], true)?
                        }
                        Err(error) if is_invalid_row_code(error.code() as i32) => {
                            tracing::warn!(document = %path, error = %error, "skipping rejected Firestore row");
                        }
                        Err(error) => return Err(SinkError::Firestore(anyhow!(error))),
                    }
                }
                Ok(())
            }
            Err(error) => Err(SinkError::Firestore(
                anyhow!(error).context("failed to execute Firestore batch write"),
            )),
        }
    }
}

#[derive(Clone)]
enum CollectionPathSegment {
    Literal(String),
    Column(usize),
}

fn parse_collection_path(path: &str, schema: &Schema) -> Result<Vec<CollectionPathSegment>> {
    let segments = path.split('/').collect::<Vec<_>>();
    if segments.len() % 2 != 1 || segments.len() > 201 {
        return Err(SinkError::Config(anyhow!(
            "firestore.collection must be a collection path with at most 100 subcollections"
        )));
    }
    segments
        .into_iter()
        .enumerate()
        .map(|(position, segment)| {
            if let Some(column) = segment.strip_prefix('{').and_then(|s| s.strip_suffix('}')) {
                if position % 2 == 0 {
                    return Err(SinkError::Config(anyhow!(
                        "collection placeholders must be parent document segments"
                    )));
                }
                let index = schema
                    .fields()
                    .iter()
                    .position(|f| f.name == column)
                    .ok_or_else(|| {
                        SinkError::Config(anyhow!(
                            "collection path column '{}' not found in schema",
                            column
                        ))
                    })?;
                Ok(CollectionPathSegment::Column(index))
            } else {
                if segment.contains(['{', '}']) {
                    return Err(SinkError::Config(anyhow!(
                        "collection placeholders must occupy a complete path segment"
                    )));
                }
                validate_firestore_document_id(segment)
                    .map_err(|error| SinkError::Config(anyhow!(error)))?;
                Ok(CollectionPathSegment::Literal(segment.to_owned()))
            }
        })
        .collect()
}

fn is_invalid_row_code(code: i32) -> bool {
    matches!(code, 3 | 11) // INVALID_ARGUMENT and OUT_OF_RANGE only.
}

fn write_document_path(write: &Write) -> String {
    match write
        .operation
        .as_ref()
        .expect("Firestore write has an operation")
    {
        write::Operation::Update(document) => document.name.clone(),
        write::Operation::Delete(path) => path.clone(),
        write::Operation::Transform(_) => unreachable!("sink only emits upserts and deletes"),
    }
}

fn split_write_batches(writes: Vec<Write>, max_count: usize) -> Vec<Vec<Write>> {
    let mut batches = Vec::new();
    let mut batch = Vec::new();
    let mut bytes = 0;
    for write in writes {
        let size = write.encoded_len() + 5;
        if !batch.is_empty() && (batch.len() >= max_count || bytes + size > MAX_BATCH_WRITE_BYTES) {
            batches.push(std::mem::take(&mut batch));
            bytes = 0;
        }
        bytes += size;
        batch.push(write);
    }
    if !batch.is_empty() {
        batches.push(batch);
    }
    batches
}

// Firestore storage size differs from protobuf size. See:
// https://firebase.google.com/docs/firestore/storage-size
fn firestore_fields_size(fields: &HashMap<String, Value>) -> usize {
    fields
        .iter()
        .map(|(name, value)| name.len() + 1 + firestore_value_size(value))
        .sum()
}

fn firestore_value_size(value: &Value) -> usize {
    match value.value_type.as_ref().expect("encoded Firestore value") {
        ValueType::NullValue(_) | ValueType::BooleanValue(_) => 1,
        ValueType::IntegerValue(_) | ValueType::DoubleValue(_) | ValueType::TimestampValue(_) => 8,
        ValueType::StringValue(value) => value.len() + 1,
        ValueType::BytesValue(value) => value.len(),
        ValueType::ArrayValue(value) => value.values.iter().map(firestore_value_size).sum(),
        ValueType::MapValue(value) => firestore_fields_size(&value.fields) + 32,
        ValueType::GeoPointValue(_) => 16,
        ValueType::ReferenceValue(value) => value.len() + 16,
        ValueType::FieldReferenceValue(_)
        | ValueType::FunctionValue(_)
        | ValueType::PipelineValue(_) => {
            unreachable!("query-only Firestore values are never emitted by the row encoder")
        }
    }
}

fn validate_document_size(path: &str, fields: &HashMap<String, Value>) -> Result<()> {
    let name_size = path.split('/').map(|part| part.len() + 1).sum::<usize>() + 16;
    if name_size + firestore_fields_size(fields) + 32 > MAX_DOCUMENT_SIZE {
        return Err(SinkError::Firestore(anyhow!(
            "Firestore document exceeds 1 MiB"
        )));
    }
    Ok(())
}

fn check_batch_statuses(
    statuses: &[gcloud_sdk::google::rpc::Status],
    document_ids: &[String],
    skip_invalid_rows: bool,
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
            if skip_invalid_rows && is_invalid_row_code(status.code) {
                tracing::warn!(document = %document_id, code = status.code, message = %status.message, "skipping rejected Firestore row");
                continue;
            }
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
        path: String,
        data: HashMap<String, Value>,
    },
    Delete {
        path: String,
    },
}

impl FirestoreOperation {
    fn document_path(&self) -> &str {
        match self {
            Self::Upsert { path, .. } | Self::Delete { path } => path,
        }
    }
}

// Encode Firestore values directly: JSON cannot represent non-finite floats, and the
// SDK's serde serializer casts u64 to i64 without checking its range.
fn format_datum(
    datum: Option<ScalarRefImpl<'_>>,
    data_type: &DataType,
    decimal_mode: FirestoreDecimalMode,
) -> Result<Value> {
    format_datum_at_depth(datum, data_type, decimal_mode, 0)
}

fn contains_decimal(data_type: &DataType) -> bool {
    match data_type {
        DataType::Decimal => true,
        DataType::List(list) => contains_decimal(list.elem()),
        DataType::Struct(struct_type) => struct_type.types().any(contains_decimal),
        _ => false,
    }
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
    decimal_mode: FirestoreDecimalMode,
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
        DataType::Decimal => match decimal_mode {
            // Firestore's wire representation for Decimal128 is a reserved map,
            // not a string field. RisingWave decimals have at most 29 significant
            // digits, within Decimal128's 34, so no intermediate f64 is needed.
            // https://cloud.google.com/firestore/mongodb-compatibility/docs/supported-data-types-drivers
            FirestoreDecimalMode::Decimal128 => ValueType::MapValue(MapValue {
                fields: HashMap::from([(
                    "__decimal128__".to_owned(),
                    Value {
                        value_type: Some(ValueType::StringValue(scalar.into_decimal().to_string())),
                    },
                )]),
            }),
            FirestoreDecimalMode::Double => ValueType::DoubleValue(
                f64::try_from(scalar.into_decimal())
                    .map_err(|error| SinkError::Firestore(anyhow!(error)))?,
            ),
        },
        DataType::Timestamp => {
            // A timestamp without a time zone is interpreted as UTC, independently of
            // the session/process time zone. Firestore stores microsecond precision.
            let time = scalar.into_timestamp().0.and_utc();
            firestore_timestamp(time.timestamp(), time.timestamp_subsec_nanos())?
        }
        DataType::Timestamptz => {
            let time = scalar.into_timestamptz();
            firestore_timestamp(time.timestamp(), time.timestamp_subsec_nanos())?
        }
        DataType::Int256
        | DataType::Serial
        | DataType::Interval
        | DataType::Date
        | DataType::Time => ValueType::StringValue(scalar.to_text_with_type(data_type)),
        DataType::Varchar => ValueType::StringValue(scalar.into_utf8().to_owned()),
        DataType::Bytea => ValueType::StringValue(STANDARD.encode(scalar.into_bytea())),
        DataType::Jsonb => return format_jsonb(scalar.into_jsonb().into(), depth),
        DataType::List(inner) => {
            check_container_depth(depth)?;
            let values = scalar
                .into_list()
                .iter()
                .map(|item| format_datum_at_depth(item, inner.elem(), decimal_mode, depth + 1))
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
                    format_datum_at_depth(datum, dt, decimal_mode, depth + 1)?,
                );
            }
            ValueType::MapValue(MapValue { fields })
        }
        DataType::Map(_) | DataType::Vector(_) | DataType::Variant => {
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

fn firestore_timestamp(seconds: i64, nanos: u32) -> Result<ValueType> {
    if !(-62_135_596_800..=253_402_300_799).contains(&seconds) || nanos >= 1_000_000_000 {
        return Err(SinkError::Firestore(anyhow!(
            "Firestore timestamp must be between years 0001 and 9999"
        )));
    }
    Ok(ValueType::TimestampValue(prost_types::Timestamp {
        seconds,
        nanos: (nanos / 1000 * 1000) as i32,
    }))
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
