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

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::ops::Deref;
use std::sync::LazyLock;

use anyhow::anyhow;
use itertools::Itertools;
use mongodb::bson::{bson, doc, Array, Bson, Document};
use mongodb::{Client, Namespace};
use risingwave_common::array::{Op, RowRef, StreamChunk};
use risingwave_common::catalog::Schema;
use risingwave_common::log::LogSuppresser;
use risingwave_common::row::Row;
use risingwave_common::session_config::sink_decouple::SinkDecouple;
use risingwave_common::types::ScalarRefImpl;
use serde_derive::Deserialize;
use serde_with::{serde_as, DisplayFromStr};
use thiserror_ext::AsReport;
use tonic::async_trait;
use with_options::WithOptions;

use super::catalog::desc::SinkDesc;
use super::encoder::BsonEncoder;
use crate::connector_common::MongodbCommon;
use crate::deserialize_bool_from_string;
use crate::sink::encoder::RowEncoder;
use crate::sink::writer::{LogSinkerOf, SinkWriter, SinkWriterExt};
use crate::sink::{
    DummySinkCommitCoordinator, Result, Sink, SinkError, SinkParam, SinkWriterParam,
    SINK_TYPE_APPEND_ONLY, SINK_TYPE_OPTION, SINK_TYPE_UPSERT,
};

pub const MONGODB_SINK: &str = "mongodb";
pub const MONGODB_BULK_WRITE_SIZE_LIMIT: usize = 65536;
pub const MONGODB_PK_NAME: &str = "_id";

static LOG_SUPPERSSER: LazyLock<LogSuppresser> = LazyLock::new(LogSuppresser::default);

const fn _default_bulk_write_max_entries() -> usize {
    1024
}

#[serde_as]
#[derive(Clone, Debug, Deserialize, WithOptions)]
pub struct MongodbConfig {
    #[serde(flatten)]
    pub common: MongodbCommon,

    pub r#type: String, // accept "append-only" or "upsert"

    /// The dynamic collection name where data should be sunk to. If specified, the field value will be used
    /// as the collection name. The collection name format is same as `collection.name`. If the field value is
    /// null or an empty string, then the `collection.name` will be used as a fallback destination.
    #[serde(rename = "collection.name.field")]
    pub collection_name_field: Option<String>,

    /// Controls whether the field value of `collection.name.field` should be dropped when sinking.
    /// Set this option to true to avoid the duplicate values of `collection.name.field` being written to the
    /// result collection.
    #[serde(
        default,
        deserialize_with = "deserialize_bool_from_string",
        rename = "collection.name.field.drop"
    )]
    pub drop_collection_name_field: bool,

    /// The maximum entries will accumulate before performing the bulk write, defaults to 1024.
    #[serde(
        rename = "mongodb.bulk_write.max_entries",
        default = "_default_bulk_write_max_entries"
    )]
    #[serde_as(as = "DisplayFromStr")]
    pub bulk_write_max_entries: usize,
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

/// An async-drop style `Guard` for `mongodb::Client`. Use this guard to hold a client,
/// the `client::shutdown` is called in an async manner when the guard is dropped.
/// Please be aware this is a "best effort" style shutdown, which may not be successful if the
/// tokio runtime is in the process of terminating. However, the server-side resources will be
/// cleaned up eventually due to the session expiration.
/// see [this issue](https://github.com/mongodb/mongo-rust-driver/issues/719) for more information
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
    is_append_only: bool,
}

impl MongodbSink {
    pub fn new(param: SinkParam) -> Result<Self> {
        let config = MongodbConfig::from_hashmap(param.properties.clone())?;
        let pk_indices = param.downstream_pk.clone();
        let is_append_only = param.sink_type.is_append_only();
        let schema = param.schema();
        Ok(Self {
            config,
            param,
            schema,
            pk_indices,
            is_append_only,
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

    fn is_sink_decouple(_desc: &SinkDesc, user_specified: &SinkDecouple) -> Result<bool> {
        match user_specified {
            // Set default sink decouple to false, because mongodb sink writer only ensure delivery on checkpoint barrier
            SinkDecouple::Default => Ok(false),
            SinkDecouple::Disable => Ok(false),
            SinkDecouple::Enable => Ok(true),
        }
    }

    async fn validate(&self) -> Result<()> {
        if !self.is_append_only {
            if self.pk_indices.is_empty() {
                return Err(SinkError::Config(anyhow!(
                    "Primary key not defined for upsert mongodb sink (please define in `primary_key` field)")));
            }

            // checking if there is a non-pk field's name is `_id`
            if self
                .schema
                .fields
                .iter()
                .enumerate()
                .any(|(i, field)| !self.pk_indices.contains(&i) && field.name == MONGODB_PK_NAME)
            {
                return Err(SinkError::Config(anyhow!(
                    "_id field must be the sink's primary key, but a non primary key field name is _id",
                )));
            }

            // assume the sink's pk is (a, b) and then the data written to mongodb will be
            // { "_id": {"a": 1, "b": 2}, "a": 1, "b": 2, ... }
            // you can see that the compound pk (a, b) is turned into an Object {"a": 1, "b": 2}
            // and the each pk field is become as a field of the document
            // but if the sink's pk is (_id, b) and the data will be:
            // { "_id": {"_id": 1, "b": 2}, "b": 2, ... }
            // in this case, the original _id field of the compound pk has been overridden
            // we should consider this is a schema error
            if self.pk_indices.len() > 1
                && self
                    .pk_indices
                    .iter()
                    .map(|&idx| self.schema.fields[idx].name.as_str())
                    .any(|field| field == MONGODB_PK_NAME)
            {
                return Err(SinkError::Config(anyhow!(
                    "primary key fields must not contain a field named _id"
                )));
            }
        }

        if self.config.bulk_write_max_entries > MONGODB_BULK_WRITE_SIZE_LIMIT {
            return Err(SinkError::Config(anyhow!(
                "mongodb.bulk_write.max_entries {} exceeds the limit {}",
                self.config.bulk_write_max_entries,
                MONGODB_BULK_WRITE_SIZE_LIMIT
            )));
        }

        if let Err(err) = self.config.common.collection_name.parse::<Namespace>() {
            return Err(SinkError::Config(anyhow!(err).context(format!(
                "invalid collection.name {}",
                self.config.common.collection_name
            ))));
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
            return Err(SinkError::Config(anyhow!(
                    "collection.name.field must be specified when collection.name.field.drop is enabled"
                )));
        }

        // checking dynamic collection name settings
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
                .ok_or(SinkError::Config(anyhow!(
                    "collection.name.field {} not found",
                    coll_field
                )))?;

            if fields[coll_field_index].data_type() != risingwave_common::types::DataType::Varchar {
                return Err(SinkError::Config(anyhow!(
                    "the type of collection.name.field {} must be varchar",
                    coll_field
                )));
            }

            if !self.is_append_only && self.pk_indices.iter().any(|idx| *idx == coll_field_index) {
                return Err(SinkError::Config(anyhow!(
                    "collection.name.field {} must not be equal to the primary key field",
                    coll_field
                )));
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
            self.is_append_only,
        )
        .await?
        .into_log_sinker(writer_param.sink_metrics))
    }
}

pub struct MongodbSinkWriter {
    pub config: MongodbConfig,
    client: ClientGuard,
    payload_writer: MongodbPayloadWriter,
}

impl MongodbSinkWriter {
    pub async fn new(
        name: String,
        config: MongodbConfig,
        schema: Schema,
        pk_indices: Vec<usize>,
        is_append_only: bool,
    ) -> Result<Self> {
        let client = config.common.build_client().await?;

        let default_namespace =
            config
                .common
                .collection_name
                .parse()
                .map_err(|err: mongodb::error::Error| {
                    SinkError::Mongodb(anyhow!(err).context("parsing default namespace failed"))
                })?;

        let coll_name_field_index =
            config
                .collection_name_field
                .as_ref()
                .and_then(|coll_name_field| {
                    schema
                        .names_str()
                        .iter()
                        .position(|&name| coll_name_field == name)
                });

        let col_indices = if let Some(coll_name_field_index) = coll_name_field_index
            && config.drop_collection_name_field
        {
            (0..schema.fields.len())
                .filter(|idx| *idx != coll_name_field_index)
                .collect_vec()
        } else {
            (0..schema.fields.len()).collect_vec()
        };

        let row_encoder = BsonEncoder::new(schema.clone(), Some(col_indices), pk_indices.clone());

        let command_builder = if is_append_only {
            CommandBuilder::AppendOnly(HashMap::new())
        } else {
            CommandBuilder::Upsert(HashMap::new())
        };

        let payload_writer = MongodbPayloadWriter::new(
            schema,
            pk_indices,
            default_namespace,
            coll_name_field_index,
            client.clone(),
            config.bulk_write_max_entries,
            row_encoder,
            command_builder,
        );

        Ok(Self {
            config,
            client: ClientGuard::new(name, client),
            payload_writer,
        })
    }
}

#[async_trait]
impl SinkWriter for MongodbSinkWriter {
    async fn begin_epoch(&mut self, _epoch: u64) -> Result<()> {
        Ok(())
    }

    async fn write_batch(&mut self, chunk: StreamChunk) -> Result<()> {
        for (op, row) in chunk.rows() {
            self.payload_writer.write(op, row).await?;
        }
        Ok(())
    }

    async fn barrier(&mut self, is_checkpoint: bool) -> Result<Self::CommitMetadata> {
        if is_checkpoint {
            self.payload_writer.flush().await?;
        }
        Ok(())
    }
}

struct InsertCommandBuilder {
    coll: String,
    inserts: Array,
}

impl InsertCommandBuilder {
    fn new(coll: String, capacity: usize) -> Self {
        Self {
            coll,
            inserts: Array::with_capacity(capacity),
        }
    }

    fn append(&mut self, row: Document) {
        self.inserts.push(Bson::Document(row));
    }

    fn build(self) -> Document {
        doc! {
            "insert": self.coll,
            "ordered": true,
            "documents": self.inserts,
        }
    }
}

struct UpsertCommandBuilder {
    coll: String,
    updates: Array,
    deletes: HashMap<Vec<u8>, Document>,
}

impl UpsertCommandBuilder {
    fn new(coll: String, capacity: usize) -> Self {
        Self {
            coll,
            updates: Array::with_capacity(capacity),
            deletes: HashMap::with_capacity(capacity),
        }
    }

    fn add_upsert(&mut self, pk: Document, row: Document) -> Result<()> {
        let pk_data = mongodb::bson::to_vec(&pk)
            .map_err(|err| anyhow!(err).context("cannot serialize primary key"))?;
        // under same pk, if the record currently being upserted was marked for deletion previously, we should
        // revert the deletion, otherwise, the upserting record may be accidentally deleted.
        // see https://github.com/risingwavelabs/risingwave/pull/17102#discussion_r1630684160 for more information.
        self.deletes.remove(&pk_data);

        self.updates.push(bson!( {
            "q": pk,
            "u": row,
            "upsert": true,
            "multi": false,
        }));

        Ok(())
    }

    fn add_delete(&mut self, pk: Document) -> Result<()> {
        let pk_data = mongodb::bson::to_vec(&pk)
            .map_err(|err| anyhow!(err).context("cannot serialize primary key"))?;
        self.deletes.insert(pk_data, pk);
        Ok(())
    }

    fn build(self) -> (Option<Document>, Option<Document>) {
        let (mut upsert_document, mut delete_document) = (None, None);
        if !self.updates.is_empty() {
            upsert_document = Some(doc! {
                "update": self.coll.clone(),
                "ordered": true,
                "updates": self.updates,
            });
        }
        if !self.deletes.is_empty() {
            let deletes = self
                .deletes
                .into_values()
                .map(|pk| {
                    bson!({
                        "q": pk,
                        "limit": 1,
                    })
                })
                .collect::<Array>();

            delete_document = Some(doc! {
                "delete": self.coll,
                "ordered": true,
                "deletes": deletes,
            });
        }
        (upsert_document, delete_document)
    }
}

enum CommandBuilder {
    AppendOnly(HashMap<MongodbNamespace, InsertCommandBuilder>),
    Upsert(HashMap<MongodbNamespace, UpsertCommandBuilder>),
}

type MongodbNamespace = (String, String);

// In the future, we may build the payload into RawBSON to gain a better performance.
// The current API (mongodb-2.8.2) lacks the support of writing RawBSON.
struct MongodbPayloadWriter {
    schema: Schema,
    pk_indices: Vec<usize>,
    default_namespace: Namespace,
    coll_name_field_index: Option<usize>,
    client: Client,
    buffered_entries: usize,
    max_entries: usize,
    row_encoder: BsonEncoder,
    // TODO switching to bulk write API when mongodb driver supports it
    command_builder: Option<CommandBuilder>,
}

impl MongodbPayloadWriter {
    fn new(
        schema: Schema,
        pk_indices: Vec<usize>,
        default_namespace: Namespace,
        coll_name_field_index: Option<usize>,
        client: Client,
        max_entries: usize,
        row_encoder: BsonEncoder,
        command_builder: CommandBuilder,
    ) -> Self {
        Self {
            schema,
            pk_indices,
            default_namespace,
            coll_name_field_index,
            client,
            buffered_entries: 0,
            max_entries,
            row_encoder,
            command_builder: Some(command_builder),
        }
    }

    fn extract_namespace_from_row_ref(&self, row: RowRef<'_>) -> MongodbNamespace {
        let ns = self.coll_name_field_index.and_then(|coll_name_field_index| {
            match row.datum_at(coll_name_field_index) {
                Some(ScalarRefImpl::Utf8(v)) => match v.parse::<Namespace>() {
                    Ok(ns) => Some(ns),
                    Err(err) => {
                        if let Ok(suppressed_count) = LOG_SUPPERSSER.check() {
                            tracing::warn!(
                                suppressed_count,
                                error = %err.as_report(),
                                collection_name = %v,
                                "parsing collection name failed, fallback to use default collection.name"
                            );
                        }
                        None
                    }
                },
                _ => {
                    if let Ok(suppressed_count) = LOG_SUPPERSSER.check() {
                        tracing::warn!(
                            suppressed_count,
                            "the value of collection.name.field is null, fallback to use default collection.name"
                        );
                    }
                    None
                }
            }
        });
        match ns {
            Some(ns) => (ns.db, ns.coll),
            None => (
                self.default_namespace.db.clone(),
                self.default_namespace.coll.clone(),
            ),
        }
    }

    fn append(
        &self,
        insert_builder: &mut HashMap<MongodbNamespace, InsertCommandBuilder>,
        row: RowRef<'_>,
    ) -> Result<()> {
        let document = self.row_encoder.encode(row)?;
        let ns = self.extract_namespace_from_row_ref(row);
        let coll = ns.1.clone();
        match insert_builder.entry(ns) {
            Entry::Occupied(mut entry) => entry.get_mut().append(document),
            Entry::Vacant(entry) => {
                let mut builder = InsertCommandBuilder::new(coll, self.max_entries);
                builder.append(document);
                entry.insert(builder);
            }
        }
        Ok(())
    }

    fn upsert(
        &self,
        upsert_builder: &mut HashMap<MongodbNamespace, UpsertCommandBuilder>,
        op: Op,
        row: RowRef<'_>,
    ) -> Result<()> {
        let mut document = self.row_encoder.encode(row)?;
        let ns = self.extract_namespace_from_row_ref(row);
        let coll = ns.1.clone();

        let pk = self.row_encoder.construct_pk(row);

        // Specify the primary key (_id) for the MongoDB collection if the user does not provide one.
        if self.pk_indices.len() > 1
            || self.schema.fields[self.pk_indices[0]].name != MONGODB_PK_NAME
        {
            // compound pk should not have a field named `_id`
            document.insert(MONGODB_PK_NAME, pk.clone());
        }

        let pk = doc! {MONGODB_PK_NAME: pk};
        match op {
            Op::Insert | Op::UpdateInsert => match upsert_builder.entry(ns) {
                Entry::Occupied(mut entry) => entry.get_mut().add_upsert(pk, document),
                Entry::Vacant(entry) => {
                    let mut builder = UpsertCommandBuilder::new(coll, self.max_entries);
                    builder.add_upsert(pk, document)?;
                    entry.insert(builder);
                    Ok(())
                }
            },
            Op::UpdateDelete => Ok(()),
            Op::Delete => match upsert_builder.entry(ns) {
                Entry::Occupied(mut entry) => entry.get_mut().add_delete(pk),
                Entry::Vacant(entry) => {
                    let mut builder = UpsertCommandBuilder::new(coll, self.max_entries);
                    builder.add_delete(pk)?;
                    entry.insert(builder);
                    Ok(())
                }
            },
        }
    }

    async fn write(&mut self, op: Op, row: RowRef<'_>) -> Result<()> {
        let mut command_builder = self.command_builder.take();
        match command_builder {
            Some(CommandBuilder::AppendOnly(ref mut insert_builder)) => {
                if op != Op::Insert {
                    return Ok(());
                }
                self.append(insert_builder, row)?;
            }
            Some(CommandBuilder::Upsert(ref mut upsert_builder)) => {
                if op == Op::UpdateDelete {
                    // we should ignore the `UpdateDelete` in upsert mode
                    return Ok(());
                }
                self.upsert(upsert_builder, op, row)?;
            }
            _ => unreachable!(),
        }
        self.command_builder = command_builder;

        self.buffered_entries += 1;
        if self.buffered_entries >= self.max_entries {
            self.flush().await?;
        }

        Ok(())
    }

    async fn flush(&mut self) -> Result<()> {
        let mut command_builder = self.command_builder.take();
        match command_builder {
            Some(CommandBuilder::AppendOnly(ref mut insert_builder)) => {
                for (ns, builder) in insert_builder.drain() {
                    self.send_bulk_write_command(&ns.0, builder.build()).await?;
                }
            }
            Some(CommandBuilder::Upsert(ref mut upsert_builder)) => {
                for (ns, builder) in upsert_builder.drain() {
                    let (upsert, delete) = builder.build();
                    // we are sending the bulk upsert first because, under same pk, the `Insert` and `UpdateInsert`
                    // should always appear before `Delete`. we have already ignored the `UpdateDelete`
                    // which is useless in upsert mode.
                    if let Some(upsert) = upsert {
                        self.send_bulk_write_command(&ns.0, upsert).await?;
                    }
                    if let Some(delete) = delete {
                        self.send_bulk_write_command(&ns.0, delete).await?;
                    }
                }
            }
            _ => unreachable!(),
        }
        self.command_builder = command_builder;

        self.buffered_entries = 0;
        Ok(())
    }

    async fn send_bulk_write_command(&self, database: &str, command: Document) -> Result<()> {
        let db = self.client.database(database);

        let result = db.run_command(command, None).await.map_err(|err| {
            SinkError::Mongodb(anyhow!(err).context(format!(
                "sending bulk write command failed, database: {}",
                database
            )))
        })?;

        if let Ok(write_errors) = result.get_array("writeErrors") {
            return Err(SinkError::Mongodb(anyhow!(
                "bulk write respond with write errors: {:?}",
                write_errors,
            )));
        }

        let n = result.get_i32("n").map_err(|err| {
            SinkError::Mongodb(
                anyhow!(err).context("can't extract field n from bulk write response"),
            )
        })?;
        if n < 1 {
            return Err(SinkError::Mongodb(anyhow!(
                "bulk write respond with an abnormal state, n = {}",
                n
            )));
        }

        Ok(())
    }
}
