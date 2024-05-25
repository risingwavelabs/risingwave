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
use std::ops::Deref;
use std::sync::LazyLock;

use anyhow::anyhow;
use mongodb::bson::{doc, Array, Bson, DateTime, Decimal128, Document};
use mongodb::{Client, Database};
use risingwave_common::array::{RowRef, StreamChunk};
use risingwave_common::bail;
use risingwave_common::catalog::Schema;
use risingwave_common::log::LogSuppresser;
use risingwave_common::session_config::sink_decouple::SinkDecouple;
use risingwave_common::types::{JsonbVal, ScalarRefImpl, SelfAsScalarRef};
use serde_derive::Deserialize;
use serde_with::serde_as;
use tonic::async_trait;
use with_options::WithOptions;

use super::catalog::desc::SinkDesc;
use crate::connector_common::MongodbCommon;
use crate::deserialize_bool_from_string;
use crate::sink::writer::{LogSinkerOf, SinkWriter, SinkWriterExt};
use crate::sink::{
    DummySinkCommitCoordinator, Result, Sink, SinkError, SinkParam, SinkWriterParam,
    SINK_TYPE_APPEND_ONLY, SINK_TYPE_OPTION, SINK_TYPE_UPSERT,
};

pub const MONGODB_SINK: &str = "mongodb";

static LOG_SUPPERSSER: LazyLock<LogSuppresser> = LazyLock::new(LogSuppresser::default);

#[serde_as]
#[derive(Clone, Debug, Deserialize, WithOptions)]
pub struct MongodbConfig {
    #[serde(flatten)]
    pub common: MongodbCommon,

    pub r#type: String, // accept "append-only" or "upsert"

    /// The dynamic collection name where data should be sunk to. If specified, the field value will be used
    /// as the collection name. The collection name format is same as `collection.name`. If the field value is
    /// null or an empty string, then the `collection.name` will be used as a fallback destination, if both
    /// `collection.name.field` and `collection.name` are empty, then an error is printed in the log and the
    /// current sinking record is dropped.
    #[serde(rename = "collection.name.field")]
    pub collection_name_field: Option<String>,

    /// Controls whether the field value of `collection.name.field` should be dropped when sinking.
    /// If this option set to true, the sink must have at least two fields, i.e., primary key (_id)
    /// and the `collection.name.field` field.
    #[serde(
        default,
        deserialize_with = "deserialize_bool_from_string",
        rename = "collection.name.field.drop"
    )]
    pub drop_collection_name_field: bool,
}

impl MongodbConfig {
    pub fn from_hashmap(properties: HashMap<String, String>) -> crate::sink::Result<Self> {
        let config =
            serde_json::from_value::<MongodbConfig>(serde_json::to_value(properties).unwrap())
                .map_err(|e| SinkError::Config(anyhow!(e)))?;
        if config.r#type != SINK_TYPE_APPEND_ONLY && config.r#type != SINK_TYPE_UPSERT {
            return Err(SinkError::Config(anyhow!(
                "`{}` must be {}, or {}",
                SINK_TYPE_OPTION,
                SINK_TYPE_APPEND_ONLY,
                SINK_TYPE_UPSERT
            )));
        }
        Ok(config)
    }
}

/// Avoid using `mongodb::Client` directly, use this `ClientGuard` to hold a client.
/// The `client::shutdown` is called in an async manner when `ClientGuard` is dropped.
/// Please be aware this is a "best effort" style shutdown, which may not be successful if the
/// tokio runtime is in the process of terminating. However, the server-side resources will be
/// cleaned up eventually due to the session expiration.
/// see [this issue](https://github.com/mongodb/mongo-rust-driver/issues/719 for more information)
struct ClientGuard {
    tx: tokio::sync::oneshot::Sender<()>,
    client: Client,
}

impl ClientGuard {
    fn new(name: String, client: Client) -> Self {
        let client_copy = client.clone();
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        tokio::spawn(async move {
            tracing::debug!(%name, "waiting for client to shut down");
            let _ = rx.await;
            tracing::debug!(%name, "sender dropped now calling client's shutdown");
            // shutdown may stuck if the resources created by client are not dropped at this point.
            // As recommended by [shutdown](https://docs.rs/mongodb/2.8.2/mongodb/struct.Client.html#method.shutdown)
            // documentation, we should make our resources usage shorter-lived than the client. So if this happens,
            // there are some programming error in our code.
            client_copy.shutdown().await;
            tracing::debug!(%name, "client shutdown succeeded");
        });
        Self { tx, client }
    }
}

impl Deref for ClientGuard {
    type Target = Client;

    fn deref(&self) -> &Self::Target {
        &self.client
    }
}

#[derive(Debug)]
pub struct MongodbSink {
    pub config: MongodbConfig,
    param: SinkParam,
    schema: Schema,
    pk_indices: Vec<usize>,
}

impl MongodbSink {
    pub fn new(param: SinkParam) -> Result<Self> {
        let config = MongodbConfig::from_hashmap(param.properties.clone())?;
        let pk_indices = param.downstream_pk.clone();
        let schema = param.schema();
        Ok(Self {
            config,
            param,
            schema,
            pk_indices,
        })
    }
}

impl TryFrom<SinkParam> for MongodbSink {
    type Error = SinkError;

    fn try_from(param: SinkParam) -> std::result::Result<Self, Self::Error> {
        MongodbSink::new(param)
    }
}

impl Sink for MongodbSink {
    type Coordinator = DummySinkCommitCoordinator;
    type LogSinker = LogSinkerOf<MongodbSinkWriter>;

    const SINK_NAME: &'static str = MONGODB_SINK;

    fn is_sink_decouple(desc: &SinkDesc, user_specified: &SinkDecouple) -> Result<bool> {
        match user_specified {
            SinkDecouple::Default => Ok(desc.sink_type.is_append_only()),
            SinkDecouple::Disable => Ok(false),
            SinkDecouple::Enable => Ok(true),
        }
    }

    async fn validate(&self) -> Result<()> {
        if self.pk_indices.len() != 1 {
            bail!("mongodb sink requires exactly one primary field as the _id field")
        }

        // checking reachability
        let client = self.config.common.build_client().await?;
        let client = ClientGuard::new(self.param.sink_name.clone(), client);
        client
            .database("admin")
            .run_command(doc! {"hello":1}, None)
            .await
            .map_err(|err| {
                SinkError::Mongodb(anyhow!(err).context("failed to send hello command to mongodb"))
            })?;

        if self.config.drop_collection_name_field && self.config.collection_name_field.is_none() {
            bail!("collection.name.field must be specified when collection.name.field.drop is enabled")
        }

        if let Some(coll_field) = &self.config.collection_name_field {
            let fields = self.schema.fields();

            let coll_field_index = fields
                .iter()
                .enumerate()
                .find_map(|(index, field)| {
                    if &field.name == coll_field {
                        Some(index)
                    } else {
                        None
                    }
                })
                .ok_or(anyhow!("collection.name.field {} not found", coll_field))?;

            if fields[coll_field_index].data_type() != risingwave_common::types::DataType::Varchar {
                bail!("the type of collection.name.field must be varchar")
            }

            if self.pk_indices[0] == coll_field_index {
                bail!("collection.name.field must not be equal to the primary key field")
            }
        }

        Ok(())
    }

    async fn new_log_sinker(&self, writer_param: SinkWriterParam) -> Result<Self::LogSinker> {
        Ok(MongodbSinkWriter::new(
            format!("{}-{}", writer_param.executor_id, self.param.sink_name),
            self.config.clone(),
            self.schema.clone(),
            self.pk_indices.clone(),
        )
        .await?
        .into_log_sinker(writer_param.sink_metrics))
    }
}

pub struct MongodbSinkWriter {
    pub config: MongodbConfig,
    schema: Schema,
    pk_indices: Vec<usize>,
    client: ClientGuard,
}

impl MongodbSinkWriter {
    pub async fn new(
        name: String,
        config: MongodbConfig,
        schema: Schema,
        pk_indices: Vec<usize>,
    ) -> Result<Self> {
        let client = config.common.build_client().await?;
        let client = ClientGuard::new(name, client);
        Ok(Self {
            config,
            schema,
            pk_indices,
            client,
        })
    }
}

#[async_trait]
impl SinkWriter for MongodbSinkWriter {
    async fn begin_epoch(&mut self, _epoch: u64) -> Result<()> {
        Ok(())
    }

    async fn write_batch(&mut self, chunk: StreamChunk) -> Result<()> {
        chunk.row
    }

    async fn barrier(&mut self, is_checkpoint: bool) -> Result<Self::CommitMetadata> {
        todo!()
    }
}

fn bson_from_scalar_ref<'a>(data: Option<ScalarRefImpl<'a>>) -> Bson {
    match data {
        Some(scalar) => match scalar {
            ScalarRefImpl::Int16(v) => Bson::Int32(v as i32),
            ScalarRefImpl::Int32(v) => Bson::Int32(v),
            ScalarRefImpl::Int64(v) => Bson::Int64(v),
            ScalarRefImpl::Int256(v) => {
                if let Ok(suppressed_count) = LOG_SUPPERSSER.check() {
                    tracing::warn!(
                        suppressed_count,
                        "mongodb sink does not support int256 type {}",
                        v,
                    );
                }
                Bson::Null
            }
            ScalarRefImpl::Float32(v) => Bson::Double(v.into_inner() as f64),
            ScalarRefImpl::Float64(v) => Bson::Double(v.into_inner()),
            ScalarRefImpl::Utf8(v) => Bson::String(v.to_string()),
            ScalarRefImpl::Bool(v) => Bson::Boolean(v),
            ScalarRefImpl::Decimal(v) => {
                let decimal_str = v.to_string();
                let converted = decimal_str.parse();
                match converted {
                    Ok(v) => Bson::Decimal128(v),
                    Err(err) => {
                        if let Ok(suppressed_count) = LOG_SUPPERSSER.check() {
                            tracing::warn!(
                                suppressed_count,
                                ?err,
                                "risingwave decimal {} convert to bson decimal128 failed",
                                decimal_str,
                            );
                        }
                        Bson::Null
                    }
                }
            }
            ScalarRefImpl::Interval(v) => {
                if let Ok(suppressed_count) = LOG_SUPPERSSER.check() {
                    tracing::warn!(
                        suppressed_count,
                        "mongodb sink does not support interval type {}",
                        v,
                    );
                }
                Bson::Null
            }
            ScalarRefImpl::Date(v) => {
                if let Ok(suppressed_count) = LOG_SUPPERSSER.check() {
                    tracing::warn!(suppressed_count, "mongodb sink does not support date type {}", v,);
                }
                Bson::Null
            }
            ScalarRefImpl::Time(v) => {
                if let Ok(suppressed_count) = LOG_SUPPERSSER.check() {
                    tracing::warn!(suppressed_count, "mongodb sink does not support time type {}", v,);
                }
                Bson::Null
            }
            ScalarRefImpl::Timestamp(v) => {
                Bson::DateTime(DateTime::from_millis(v.0.timestamp_millis()))
            }
            ScalarRefImpl::Timestamptz(v) => {
                Bson::DateTime(DateTime::from_millis(v.timestamp_millis()))
            }
            ScalarRefImpl::Jsonb(v) => {
                let jsonb_val: JsonbVal = v.into();
                match jsonb_val.take().try_into() {
                    Ok(doc) => doc,
                    Err(err) => {
                        if let Ok(suppressed_count) = LOG_SUPPERSSER.check() {
                            tracing::warn!(
                                suppressed_count,
                                error = %err,
                                "convert jsonb to mongodb bson failed",
                            );
                        }
                        Bson::Null
                    }
                }
            }
            ScalarRefImpl::Serial(v) => Bson::Int64(v),
            ScalarRefImpl::Struct(v) => {

            },
            ScalarRefImpl::List(v) => {

            },
            ScalarRefImpl::Bytea(_) => {
                if let Ok(suppressed_count) = LOG_SUPPERSSER.check() {
                    tracing::warn!(
                        suppressed_count,
                        "mongodb sink does not support bytea type",
                    );
                }
                Bson::Null
            },
        },
        None => Bson::Null,
    }
}

struct UpsertCommandBuilder {
    cmd: Document,
    upserts: Array,
}

impl UpsertCommandBuilder {
    fn new(coll: String, capacity: usize) -> Self {
        Self {
            cmd: doc! {
                "update": coll,
                "ordered": true,
            },
            upserts: Array::with_capacity(capacity),
        }
    }

    fn append(&mut self, row: RowRef) {
        self.upserts.push(doc! {
            "q": {"_id":}
        })
    }
}

struct MongodbPayloadWriter {
    db: Database,
    coll: String,
}

impl MongodbPayloadWriter {
    fn new(db: Database, coll: String) -> Self {
        Self { db, coll }
    }
}
