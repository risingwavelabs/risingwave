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

//! Qdrant sink. Each row is written as a point to a collection over gRPC. A missing collection is
//! created at `CREATE SINK`: one vector column becomes an unnamed vector, and several become named
//! vectors.
//!
//! - Point id: a single integer key is used as a `u64`, and a single varchar key holding a
//!   canonical UUID is used as is. Any other key becomes a `UUIDv5` (`NAMESPACE_OID`) of the
//!   varchar value, or of the JSON array of the key columns' text, e.g. `["1","a"]`.
//! - Vectors: `vector(n)` columns. An unnamed collection vector takes the only vector column, and
//!   a null value deletes the point. Named vectors are matched by column name, and nulls are
//!   omitted.
//! - Payload: all other columns.

use std::collections::{BTreeMap, HashMap};
use std::sync::LazyLock;
use std::time::Duration;

use anyhow::{Context, anyhow};
use async_trait::async_trait;
use itertools::{Either, Itertools};
use prost::Message;
use qdrant_client::Qdrant;
use qdrant_client::qdrant::points_update_operation::{DeletePoints, Operation, PointStructList};
use qdrant_client::qdrant::value::Kind;
use qdrant_client::qdrant::{
    CreateCollectionBuilder, Distance, ListValue, PointId, PointStruct, PointsUpdateOperation,
    Struct, UpdateBatchPointsBuilder, Value as QdrantValue, VectorParams, VectorParamsMap, Vectors,
    VectorsConfig, vectors_config,
};
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::catalog::Schema;
use risingwave_common::log::LogSuppressor;
use risingwave_common::row::Row;
use risingwave_common::types::{DataType, ScalarRefImpl, ToText};
use serde::Deserialize;
use serde_json::Value;
use serde_with::{DisplayFromStr, serde_as};
use uuid::Uuid;
use with_options::WithOptions;

use crate::enforce_secret::EnforceSecret;
use crate::sink::batching_log_sink::{BatchingLogSinker, BatchingSinkWriter};
use crate::sink::encoder::{JsonEncoder, RowEncoder};
use crate::sink::{Result, Sink, SinkError, SinkParam, SinkWriterParam};

pub const QDRANT_SINK: &str = "qdrant";

const DEFAULT_WRITE_BATCH_SIZE: usize = 500;
// Half of Qdrant's default request size limit.
const MAX_REQUEST_BYTES: usize = 16 * 1024 * 1024;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(300);

static LOG_SUPPRESSOR: LazyLock<LogSuppressor> = LazyLock::new(LogSuppressor::default);

fn default_write_batch_size() -> usize {
    DEFAULT_WRITE_BATCH_SIZE
}

#[serde_as]
#[derive(Clone, Debug, Deserialize, WithOptions)]
pub struct QdrantConfig {
    /// gRPC endpoint of Qdrant, e.g. `http://localhost:6334`.
    pub url: String,
    /// Name of the collection to write points to. It is created if it does not exist.
    pub collection_name: String,
    pub api_key: Option<String>,
    /// Distance of the vectors: `cosine`, `dot`, `euclid` or `manhattan`. Used to create the
    /// collection, `cosine` by default. If set, an existing collection must use it.
    pub distance: Option<String>,
    /// Maximum number of points buffered before they are written. Buffered points are also
    /// written at every barrier.
    #[serde(default = "default_write_batch_size")]
    #[serde_as(as = "DisplayFromStr")]
    #[with_option(allow_alter_on_fly)]
    pub write_batch_size: usize,
    pub r#type: String, // accept "append-only" or "upsert"

    #[serde(flatten)]
    pub unknown_fields: HashMap<String, String>,
}

crate::impl_sink_unknown_fields!(QdrantConfig);

impl EnforceSecret for QdrantConfig {
    const ENFORCE_SECRET_PROPERTIES: phf::Set<&'static str> = phf::phf_set! {
        "api_key",
    };
}

impl QdrantConfig {
    fn from_btreemap(values: BTreeMap<String, String>) -> Result<Self> {
        let config = serde_json::from_value::<QdrantConfig>(
            serde_json::to_value(values).expect("serialize sink properties"),
        )
        .map_err(|e| SinkError::Config(anyhow!(e)))?;
        if config.write_batch_size == 0 {
            return Err(SinkError::Config(anyhow!(
                "`write_batch_size` must be greater than 0"
            )));
        }
        config.distance()?;
        Ok(config)
    }

    fn distance(&self) -> Result<Option<Distance>> {
        let parse = |distance: &String| match distance.to_lowercase().as_str() {
            "cosine" => Ok(Distance::Cosine),
            "dot" => Ok(Distance::Dot),
            "euclid" => Ok(Distance::Euclid),
            "manhattan" => Ok(Distance::Manhattan),
            _ => Err(SinkError::Config(anyhow!(
                "invalid `distance` '{}', expected one of cosine, dot, euclid, manhattan",
                distance
            ))),
        };
        self.distance.as_ref().map(parse).transpose()
    }

    fn build_client(&self) -> Result<Qdrant> {
        Qdrant::from_url(&self.url)
            .api_key(self.api_key.clone())
            .connect_timeout(CONNECT_TIMEOUT)
            .timeout(REQUEST_TIMEOUT)
            .skip_compatibility_check()
            .build()
            .context("failed to build qdrant client")
            .map_err(SinkError::Config)
    }
}

#[derive(Clone, Debug, PartialEq)]
enum VectorLayout {
    /// The collection has a single unnamed vector, written from the column at `index`.
    Unnamed { index: usize },
    /// Each vector column is written to the named vector with the same name.
    Named { indices: Vec<usize> },
}

fn vector_size(schema: &Schema, index: usize) -> u64 {
    match schema[index].data_type {
        DataType::Vector(size) => size as u64,
        _ => unreachable!("not a vector column"),
    }
}

fn check_vector(
    params: &VectorParams,
    name: &str,
    size: u64,
    distance: Option<Distance>,
) -> Result<()> {
    if params.size != size {
        return Err(SinkError::Config(anyhow!(
            "Qdrant vector size is {}, but column '{}' is vector({})",
            params.size,
            name,
            size
        )));
    }
    if let Some(distance) = distance
        && params.distance != distance as i32
    {
        return Err(SinkError::Config(anyhow!(
            "Qdrant collection uses distance {}, but the sink specifies {}",
            params.distance().as_str_name(),
            distance.as_str_name()
        )));
    }
    Ok(())
}

fn resolve_vector_layout(
    vectors: Option<vectors_config::Config>,
    schema: &Schema,
    vector_indices: &[usize],
    distance: Option<Distance>,
) -> Result<VectorLayout> {
    let named = match vectors {
        Some(vectors_config::Config::Params(params)) => {
            let [index] = vector_indices else {
                return Err(SinkError::Config(anyhow!(
                    "Qdrant collection has a single unnamed vector, so the sink requires exactly one vector column, got {}",
                    vector_indices.len()
                )));
            };
            let name = &schema[*index].name;
            check_vector(&params, name, vector_size(schema, *index), distance)?;
            return Ok(VectorLayout::Unnamed { index: *index });
        }
        Some(vectors_config::Config::ParamsMap(named)) => named.map,
        None => HashMap::new(),
    };
    for index in vector_indices {
        let name = &schema[*index].name;
        let params = named.get(name).ok_or_else(|| {
            SinkError::Config(anyhow!(
                "Qdrant collection has no vector named '{}', available: [{}]",
                name,
                named.keys().sorted().join(", ")
            ))
        })?;
        check_vector(params, name, vector_size(schema, *index), distance)?;
    }
    Ok(VectorLayout::Named {
        indices: vector_indices.to_vec(),
    })
}

/// One vector column becomes an unnamed vector, and otherwise each becomes a named vector.
fn new_vectors_config(
    schema: &Schema,
    vector_indices: &[usize],
    distance: Distance,
) -> VectorsConfig {
    let params = |index: usize| VectorParams {
        size: vector_size(schema, index),
        distance: distance as i32,
        ..Default::default()
    };
    let config = match vector_indices {
        [index] => vectors_config::Config::Params(params(*index)),
        _ => vectors_config::Config::ParamsMap(VectorParamsMap {
            map: vector_indices
                .iter()
                .map(|index| (schema[*index].name.clone(), params(*index)))
                .collect(),
        }),
    };
    VectorsConfig {
        config: Some(config),
    }
}

/// Whether the JSON encoder can encode the type.
fn is_supported_payload_type(data_type: &DataType) -> bool {
    match data_type {
        DataType::Int256 | DataType::Map(_) => false,
        DataType::List(list_type) => is_supported_payload_type(list_type.elem()),
        DataType::Struct(struct_type) => struct_type
            .iter()
            .all(|(_, field_type)| is_supported_payload_type(field_type)),
        _ => true,
    }
}

/// Same as `qdrant-client`'s `serde` feature, which needs a newer `serde_json`.
fn json_to_qdrant(value: Value) -> QdrantValue {
    let kind = match value {
        Value::Null => Kind::NullValue(0),
        Value::Bool(v) => Kind::BoolValue(v),
        Value::Number(v) => match v.as_i64() {
            Some(v) => Kind::IntegerValue(v),
            None => Kind::DoubleValue(v.as_f64().unwrap_or(f64::NAN)),
        },
        Value::String(v) => Kind::StringValue(v),
        Value::Array(v) => Kind::ListValue(ListValue {
            values: v.into_iter().map(json_to_qdrant).collect(),
        }),
        Value::Object(v) => Kind::StructValue(Struct {
            fields: v.into_iter().map(|(k, v)| (k, json_to_qdrant(v))).collect(),
        }),
    };
    QdrantValue { kind: Some(kind) }
}

#[derive(Clone, Debug)]
pub struct QdrantSink {
    config: QdrantConfig,
    schema: Schema,
    pk_indices: Vec<usize>,
    vector_indices: Vec<usize>,
    payload_indices: Vec<usize>,
}

impl EnforceSecret for QdrantSink {
    fn enforce_secret<'a>(
        prop_iter: impl Iterator<Item = &'a str>,
    ) -> crate::error::ConnectorResult<()> {
        for prop in prop_iter {
            QdrantConfig::enforce_one(prop)?;
        }
        Ok(())
    }
}

impl TryFrom<SinkParam> for QdrantSink {
    type Error = SinkError;

    fn try_from(param: SinkParam) -> std::result::Result<Self, Self::Error> {
        let schema = param.schema();
        let pk_indices = param.downstream_pk_or_empty();
        if pk_indices.is_empty() {
            return Err(SinkError::Config(anyhow!(
                "Qdrant sink requires primary_key to derive point ids"
            )));
        }
        for index in &pk_indices {
            if matches!(schema[*index].data_type, DataType::Vector(_)) {
                return Err(SinkError::Config(anyhow!(
                    "Qdrant primary key column '{}' must not be a vector",
                    schema[*index].name
                )));
            }
        }
        let config = QdrantConfig::from_btreemap(param.properties)?;
        let (vector_indices, payload_indices): (Vec<_>, Vec<_>) = (0..schema.len())
            .partition(|index| matches!(schema[*index].data_type, DataType::Vector(_)));
        for index in &payload_indices {
            let field = &schema[*index];
            if !is_supported_payload_type(&field.data_type) {
                return Err(SinkError::Config(anyhow!(
                    "Qdrant sink does not support column '{}' of type {}",
                    field.name,
                    field.data_type
                )));
            }
        }

        Ok(Self {
            config,
            schema,
            pk_indices,
            vector_indices,
            payload_indices,
        })
    }
}

impl QdrantSink {
    fn collection_error(&self, action: &str) -> String {
        format!(
            "failed to {} Qdrant collection '{}'",
            action, self.config.collection_name
        )
    }

    async fn collection_exists(&self, client: &Qdrant) -> Result<bool> {
        client
            .collection_exists(&self.config.collection_name)
            .await
            .with_context(|| self.collection_error("check"))
            .map_err(SinkError::Config)
    }

    /// Only called at `CREATE SINK`, so that a collection deleted later is not silently
    /// recreated empty.
    async fn create_collection_if_missing(&self, client: &Qdrant) -> Result<()> {
        if self.collection_exists(client).await? {
            return Ok(());
        }
        let distance = self.config.distance()?.unwrap_or(Distance::Cosine);
        let vectors = new_vectors_config(&self.schema, &self.vector_indices, distance);
        let request =
            CreateCollectionBuilder::new(&self.config.collection_name).vectors_config(vectors);
        if let Err(error) = client.create_collection(request).await
            // Another sink may have created it concurrently.
            && !self.collection_exists(client).await?
        {
            return Err(SinkError::Config(
                anyhow!(error).context(self.collection_error("create")),
            ));
        }
        Ok(())
    }

    async fn vector_layout(&self, client: &Qdrant) -> Result<VectorLayout> {
        let info = client
            .collection_info(&self.config.collection_name)
            .await
            .with_context(|| self.collection_error("fetch"))
            .map_err(SinkError::Config)?;
        let vectors = info
            .result
            .and_then(|info| info.config)
            .and_then(|config| config.params)
            .and_then(|params| params.vectors_config)
            .and_then(|vectors| vectors.config);
        resolve_vector_layout(
            vectors,
            &self.schema,
            &self.vector_indices,
            self.config.distance()?,
        )
    }
}

impl Sink for QdrantSink {
    type LogSinker = BatchingLogSinker<QdrantSinkWriter>;

    const SINK_NAME: &'static str = QDRANT_SINK;

    crate::impl_validate_sink_unknown_fields!();

    fn validate_alter_config(config: &BTreeMap<String, String>) -> Result<()> {
        QdrantConfig::from_btreemap(config.clone())?;
        Ok(())
    }

    async fn validate(&self) -> Result<()> {
        let client = self.config.build_client()?;
        self.create_collection_if_missing(&client).await?;
        self.vector_layout(&client).await?;
        Ok(())
    }

    async fn new_log_sinker(&self, _writer_param: SinkWriterParam) -> Result<Self::LogSinker> {
        let client = self.config.build_client()?;
        let vector_layout = self.vector_layout(&client).await?;
        let writer = QdrantSinkWriter::new(
            &self.config,
            client,
            self.schema.clone(),
            self.pk_indices.clone(),
            vector_layout,
            self.payload_indices.clone(),
        );
        Ok(BatchingLogSinker::new(writer))
    }
}

fn vector_value(row: &impl Row, index: usize) -> Option<Vec<f32>> {
    match row.datum_at(index) {
        Some(ScalarRefImpl::Vector(vector)) => Some(vector.as_raw_slice().to_vec()),
        _ => None,
    }
}

fn uuid_v5(name: &str) -> PointId {
    Uuid::new_v5(&Uuid::NAMESPACE_OID, name.as_bytes())
        .to_string()
        .into()
}

enum CompactedOp {
    Upsert(PointStruct),
    Delete,
}

pub struct QdrantSinkWriter {
    client: Qdrant,
    collection_name: String,
    schema: Schema,
    pk_indices: Vec<usize>,
    vector_layout: VectorLayout,
    payload_encoder: JsonEncoder,
    write_batch_size: usize,
    /// The latest operation of each point.
    pending: HashMap<PointId, CompactedOp>,
}

impl QdrantSinkWriter {
    fn new(
        config: &QdrantConfig,
        client: Qdrant,
        schema: Schema,
        pk_indices: Vec<usize>,
        vector_layout: VectorLayout,
        payload_indices: Vec<usize>,
    ) -> Self {
        let payload_encoder = JsonEncoder::new_with_qdrant(schema.clone(), Some(payload_indices));
        Self {
            client,
            collection_name: config.collection_name.clone(),
            schema,
            pk_indices,
            vector_layout,
            payload_encoder,
            write_batch_size: config.write_batch_size,
            pending: HashMap::new(),
        }
    }

    /// Returns `None` if the primary key is null.
    fn point_id(&self, row: &impl Row) -> Option<PointId> {
        if let [index] = self.pk_indices.as_slice() {
            match row.datum_at(*index)? {
                ScalarRefImpl::Int16(v) => return Some((v as u64).into()),
                ScalarRefImpl::Int32(v) => return Some((v as u64).into()),
                ScalarRefImpl::Int64(v) => return Some((v as u64).into()),
                ScalarRefImpl::Serial(v) => return Some((v.into_inner() as u64).into()),
                ScalarRefImpl::Utf8(v) => {
                    return Some(match Uuid::try_parse(v) {
                        Ok(uuid) if uuid.to_string() == v => v.into(),
                        _ => uuid_v5(v),
                    });
                }
                _ => {}
            }
        }
        let key: Vec<String> = self
            .pk_indices
            .iter()
            .map(|index| Some(row.datum_at(*index)?.to_text()))
            .collect::<Option<_>>()?;
        Some(uuid_v5(
            &serde_json::to_string(&key).expect("serialize key"),
        ))
    }

    /// Returns `None` if the unnamed vector is null.
    fn point(&self, row: &impl Row, id: PointId) -> Result<Option<PointStruct>> {
        let vectors: Vectors = match &self.vector_layout {
            VectorLayout::Unnamed { index } => match vector_value(row, *index) {
                Some(vector) => vector.into(),
                None => return Ok(None),
            },
            VectorLayout::Named { indices } => indices
                .iter()
                .filter_map(|index| {
                    let value = vector_value(row, *index)?;
                    Some((self.schema[*index].name.clone(), value))
                })
                .collect::<HashMap<_, _>>()
                .into(),
        };
        let payload: HashMap<String, QdrantValue> = self
            .payload_encoder
            .encode(row)?
            .into_iter()
            .map(|(k, v)| (k, json_to_qdrant(v)))
            .collect();
        Ok(Some(PointStruct::new(id, vectors, payload)))
    }

    fn absorb(&mut self, chunk: StreamChunk) -> Result<()> {
        for (op, row) in chunk.rows() {
            let Some(id) = self.point_id(&row) else {
                if let Ok(suppressed_count) = LOG_SUPPRESSOR.check() {
                    tracing::warn!(
                        suppressed_count,
                        ?row,
                        "skip qdrant row with null primary key"
                    );
                }
                continue;
            };
            let compacted_op = match op {
                Op::Insert | Op::UpdateInsert => match self.point(&row, id.clone())? {
                    Some(point) => CompactedOp::Upsert(point),
                    None => CompactedOp::Delete,
                },
                Op::Delete | Op::UpdateDelete => CompactedOp::Delete,
            };
            self.pending.insert(id, compacted_op);
        }
        Ok(())
    }

    fn take_operations(&mut self) -> Vec<Operation> {
        let (upserts, deletes): (Vec<_>, Vec<_>) =
            self.pending.drain().partition_map(|(id, op)| match op {
                CompactedOp::Upsert(point) => Either::Left(point),
                CompactedOp::Delete => Either::Right(id),
            });
        let upserts = batches(upserts, self.write_batch_size)
            .into_iter()
            .map(|points| {
                Operation::Upsert(PointStructList {
                    points,
                    ..Default::default()
                })
            });
        let deletes = batches(deletes, self.write_batch_size)
            .into_iter()
            .map(|ids| {
                Operation::DeletePoints(DeletePoints {
                    points: Some(ids.into()),
                    ..Default::default()
                })
            });
        upserts.chain(deletes).collect()
    }

    async fn flush(&mut self) -> Result<()> {
        for operation in self.take_operations() {
            let request = UpdateBatchPointsBuilder::new(
                &self.collection_name,
                vec![PointsUpdateOperation {
                    operation: Some(operation),
                }],
            )
            // The log store is truncated once this returns.
            .wait(true);
            self.client
                .update_points_batch(request)
                .await
                .context("failed to write to qdrant")
                .map_err(SinkError::Http)?;
        }
        Ok(())
    }
}

fn batches<T: Message>(items: Vec<T>, max_items: usize) -> Vec<Vec<T>> {
    let mut batches = Vec::new();
    let mut batch = Vec::new();
    let mut batch_bytes = 0;
    for item in items {
        let item_bytes = item.encoded_len();
        if !batch.is_empty()
            && (batch.len() >= max_items || batch_bytes + item_bytes > MAX_REQUEST_BYTES)
        {
            batches.push(std::mem::take(&mut batch));
            batch_bytes = 0;
        }
        batch_bytes += item_bytes;
        batch.push(item);
    }
    if !batch.is_empty() {
        batches.push(batch);
    }
    batches
}

#[async_trait]
impl BatchingSinkWriter for QdrantSinkWriter {
    async fn write_batch(&mut self, chunk: StreamChunk) -> Result<()> {
        self.absorb(chunk)
    }

    async fn try_commit(&mut self) -> Result<bool> {
        if self.pending.len() >= self.write_batch_size {
            self.flush().await?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    async fn commit_on_barrier(&mut self) -> Result<bool> {
        self.flush().await?;
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::array::VectorVal;
    use risingwave_common::catalog::Field;
    use risingwave_common::row::OwnedRow;
    use risingwave_common::types::{ListType, MapType, ScalarImpl, StructType};
    use serde_json::json;

    use super::*;

    fn writer(
        fields: &[(DataType, &str)],
        pk_indices: Vec<usize>,
        vector_layout: VectorLayout,
    ) -> QdrantSinkWriter {
        let config = QdrantConfig {
            url: "http://localhost:6334".to_owned(),
            collection_name: "items".to_owned(),
            api_key: None,
            distance: None,
            write_batch_size: 10,
            r#type: "upsert".to_owned(),
            unknown_fields: Default::default(),
        };
        let schema = Schema::new(
            fields
                .iter()
                .map(|(data_type, name)| Field::with_name(data_type.clone(), *name))
                .collect(),
        );
        let payload_indices = (0..schema.len())
            .filter(|index| !matches!(schema[*index].data_type, DataType::Vector(_)))
            .collect();
        let client = config.build_client().unwrap();
        QdrantSinkWriter::new(
            &config,
            client,
            schema,
            pk_indices,
            vector_layout,
            payload_indices,
        )
    }

    fn vector(text: &str) -> ScalarImpl {
        ScalarImpl::Vector(VectorVal::from_text(text, 2).unwrap())
    }

    #[test]
    fn test_vector_layout() {
        let schema = Schema::new(vec![
            Field::with_name(DataType::Int64, "id"),
            Field::with_name(DataType::Vector(3), "text"),
            Field::with_name(DataType::Vector(2), "image"),
        ]);
        // Created collections resolve to the layout they were created for.
        let created = |indices: &[usize]| {
            let vectors = new_vectors_config(&schema, indices, Distance::Dot).config;
            resolve_vector_layout(vectors, &schema, indices, Some(Distance::Dot)).unwrap()
        };
        assert_eq!(created(&[]), VectorLayout::Named { indices: vec![] });
        assert_eq!(created(&[1]), VectorLayout::Unnamed { index: 1 });
        assert_eq!(
            created(&[1, 2]),
            VectorLayout::Named {
                indices: vec![1, 2]
            }
        );

        let error = |vectors, indices: &[usize]| {
            let err = resolve_vector_layout(vectors, &schema, indices, None).unwrap_err();
            err.to_string()
        };
        let unnamed = new_vectors_config(&schema, &[1], Distance::Dot).config;
        assert!(error(unnamed, &[1, 2]).contains("exactly one vector column, got 2"));
        assert!(error(None, &[1]).contains("no vector named 'text', available: []"));
    }

    #[test]
    fn test_point_id() {
        let id = |data_types: &[DataType], datums: Vec<Option<ScalarImpl>>| {
            let fields = data_types.iter().map(|t| (t.clone(), "k")).collect_vec();
            let pk_indices = (0..datums.len()).collect();
            let layout = VectorLayout::Named { indices: vec![] };
            writer(&fields, pk_indices, layout).point_id(&OwnedRow::new(datums))
        };
        let int = |v| id(&[DataType::Int32], vec![Some(ScalarImpl::Int32(v))]);
        let text = |v: &str| id(&[DataType::Varchar], vec![Some(ScalarImpl::Utf8(v.into()))]);

        assert_eq!(int(42), Some(42.into()));
        assert_eq!(int(-1), Some(u64::MAX.into()));
        assert_eq!(id(&[DataType::Int32], vec![None]), None);

        let canonical = "936da01f-9abd-4d9d-80c7-02af85c822a8";
        assert_eq!(text(canonical), Some(canonical.into()));
        let upper = canonical.to_uppercase();
        assert_eq!(text(&upper), Some(uuid_v5(&upper)));
        assert_eq!(text("doc-1"), Some(uuid_v5("doc-1")));

        let composite = |b: Option<&str>| {
            id(
                &[DataType::Int32, DataType::Varchar],
                vec![
                    Some(ScalarImpl::Int32(1)),
                    b.map(|b| ScalarImpl::Utf8(b.into())),
                ],
            )
        };
        assert_eq!(composite(Some("a")), Some(uuid_v5(r#"["1","a"]"#)));
        assert_ne!(composite(Some(r#"a","b"#)), composite(Some("a")));
        assert_eq!(composite(None), None);
    }

    #[test]
    fn test_payload() {
        let big = json_to_qdrant(json!(u64::MAX)).kind;
        assert_eq!(big, Some(Kind::DoubleValue(u64::MAX as f64)));

        assert!(!is_supported_payload_type(&DataType::Struct(
            StructType::new([("a", DataType::Int32), ("b", DataType::Int256)])
        )));
        assert!(!is_supported_payload_type(&DataType::List(ListType::new(
            DataType::Map(MapType::from_kv(DataType::Varchar, DataType::Int32))
        ))));
    }

    #[test]
    fn test_batches() {
        fn sizes<T>(batches: Vec<Vec<T>>) -> Vec<usize> {
            batches.iter().map(Vec::len).collect()
        }
        let ids = (0..7).map(PointId::from).collect_vec();
        assert_eq!(sizes(batches(ids, 3)), [3, 3, 1]);

        let point = |len| PointStruct::new(0, vec![1.0], [("body", "x".repeat(len).into())]);
        let third = MAX_REQUEST_BYTES / 3;
        assert_eq!(
            sizes(batches(vec![point(third), point(third), point(third)], 10)),
            [2, 1]
        );
        // A point larger than the limit is sent on its own.
        let huge = point(MAX_REQUEST_BYTES);
        assert_eq!(sizes(batches(vec![huge, point(0)], 10)), [1, 1]);
    }

    #[tokio::test]
    async fn test_compaction() {
        let fields = [(DataType::Int64, "id"), (DataType::Vector(2), "v")];
        let mut writer = writer(&fields, vec![0], VectorLayout::Unnamed { index: 1 });
        let row =
            |id, v: Option<&str>| OwnedRow::new(vec![Some(ScalarImpl::Int64(id)), v.map(vector)]);
        let chunk = StreamChunk::from_rows(
            &[
                (Op::Insert, row(1, Some("[1,2]"))),
                (Op::UpdateDelete, row(1, Some("[1,2]"))),
                (Op::UpdateInsert, row(1, Some("[5,6]"))),
                (Op::Insert, row(2, Some("[3,4]"))),
                // A null unnamed vector deletes the point.
                (Op::UpdateDelete, row(2, Some("[3,4]"))),
                (Op::UpdateInsert, row(2, None)),
            ],
            &[DataType::Int64, DataType::Vector(2)],
        );
        writer.write_batch(chunk).await.unwrap();

        let operations = writer.take_operations();
        let [Operation::Upsert(upserts), Operation::DeletePoints(deletes)] = &operations[..] else {
            panic!("unexpected operations: {operations:?}");
        };
        assert_eq!(upserts.points.len(), 1);
        assert_eq!(upserts.points[0].vectors, Some(vec![5.0, 6.0].into()));
        assert_eq!(deletes.points, Some(vec![PointId::from(2)].into()));
    }
}
