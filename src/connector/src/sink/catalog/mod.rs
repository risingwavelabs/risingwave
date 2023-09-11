// Copyright 2023 RisingWave Labs
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

pub mod desc;

use std::collections::{BTreeMap, HashMap};

use anyhow::anyhow;
use itertools::Itertools;
use risingwave_common::catalog::{
    ColumnCatalog, ConnectionId, DatabaseId, Field, Schema, SchemaId, TableId, UserId,
};
use risingwave_common::util::epoch::Epoch;
use risingwave_common::util::sort_util::ColumnOrder;
use risingwave_pb::catalog::{PbSink, PbSinkFormatDesc, PbSinkType, PbStreamJobStatus};

use super::{
    SinkError, CONNECTOR_TYPE_KEY, SINK_TYPE_APPEND_ONLY, SINK_TYPE_DEBEZIUM, SINK_TYPE_OPTION,
    SINK_TYPE_UPSERT,
};

#[derive(Clone, Copy, Debug, Default, Hash, PartialOrd, PartialEq, Eq)]
pub struct SinkId {
    pub sink_id: u32,
}

impl SinkId {
    pub const fn new(sink_id: u32) -> Self {
        SinkId { sink_id }
    }

    /// Sometimes the id field is filled later, we use this value for better debugging.
    pub const fn placeholder() -> Self {
        SinkId {
            sink_id: u32::MAX - 1,
        }
    }

    pub fn sink_id(&self) -> u32 {
        self.sink_id
    }
}

impl std::fmt::Display for SinkId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.sink_id)
    }
}

impl From<u32> for SinkId {
    fn from(id: u32) -> Self {
        Self::new(id)
    }
}
impl From<SinkId> for u32 {
    fn from(id: SinkId) -> Self {
        id.sink_id
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum SinkType {
    /// The data written into the sink connector can only be INSERT. No UPDATE or DELETE is
    /// allowed.
    AppendOnly,
    /// The input of the sink operator can be INSERT, UPDATE, or DELETE, but it must drop any
    /// UPDATE or DELETE and write only INSERT into the sink connector.
    ForceAppendOnly,
    /// The data written into the sink connector can be INSERT, UPDATE, or DELETE.
    Upsert,
}

impl SinkType {
    pub fn is_append_only(&self) -> bool {
        self == &Self::AppendOnly || self == &Self::ForceAppendOnly
    }

    pub fn is_upsert(&self) -> bool {
        self == &Self::Upsert
    }

    pub fn to_proto(self) -> PbSinkType {
        match self {
            SinkType::AppendOnly => PbSinkType::AppendOnly,
            SinkType::ForceAppendOnly => PbSinkType::ForceAppendOnly,
            SinkType::Upsert => PbSinkType::Upsert,
        }
    }

    pub fn from_proto(pb: PbSinkType) -> Self {
        match pb {
            PbSinkType::AppendOnly => SinkType::AppendOnly,
            PbSinkType::ForceAppendOnly => SinkType::ForceAppendOnly,
            PbSinkType::Upsert => SinkType::Upsert,
            PbSinkType::Unspecified => unreachable!(),
        }
    }
}

/// May replace [`SinkType`].
///
/// TODO: consolidate with [`crate::source::SourceStruct`] and [`crate::parser::SpecificParserConfig`].
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SinkFormatDesc {
    pub format: SinkFormat,
    pub encode: SinkEncode,
    pub options: BTreeMap<String, String>,
}

/// TODO: consolidate with [`crate::source::SourceFormat`] and [`crate::parser::ProtocolProperties`].
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SinkFormat {
    AppendOnly,
    Upsert,
    Debezium,
}

/// TODO: consolidate with [`crate::source::SourceEncode`] and [`crate::parser::EncodingProperties`].
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SinkEncode {
    Json,
    Protobuf,
    Avro,
    Template,
}

impl SinkFormatDesc {
    pub fn from_legacy_type(connector: &str, r#type: &str) -> Result<Option<Self>, SinkError> {
        use crate::sink::kafka::KafkaSink;
        use crate::sink::kinesis::KinesisSink;
        use crate::sink::pulsar::PulsarSink;
        use crate::sink::Sink as _;

        let format = match r#type {
            SINK_TYPE_APPEND_ONLY => SinkFormat::AppendOnly,
            SINK_TYPE_UPSERT => SinkFormat::Upsert,
            SINK_TYPE_DEBEZIUM => SinkFormat::Debezium,
            _ => {
                return Err(SinkError::Config(anyhow!(
                    "sink type unsupported: {}",
                    r#type
                )))
            }
        };
        let encode = match connector {
            KafkaSink::SINK_NAME | KinesisSink::SINK_NAME | PulsarSink::SINK_NAME => {
                SinkEncode::Json
            }
            _ => return Ok(None),
        };
        Ok(Some(Self {
            format,
            encode,
            options: Default::default(),
        }))
    }

    pub fn to_proto(&self) -> PbSinkFormatDesc {
        use risingwave_pb::plan_common::{EncodeType as E, FormatType as F};

        let format = match self.format {
            SinkFormat::AppendOnly => F::Plain,
            SinkFormat::Upsert => F::Upsert,
            SinkFormat::Debezium => F::Debezium,
        };
        let encode = match self.encode {
            SinkEncode::Json => E::Json,
            SinkEncode::Protobuf => E::Protobuf,
            SinkEncode::Avro => E::Avro,
            SinkEncode::Template => E::Template,
        };
        let options = self
            .options
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        PbSinkFormatDesc {
            format: format.into(),
            encode: encode.into(),
            options,
        }
    }
}

impl TryFrom<PbSinkFormatDesc> for SinkFormatDesc {
    type Error = SinkError;

    fn try_from(value: PbSinkFormatDesc) -> Result<Self, Self::Error> {
        use risingwave_pb::plan_common::{EncodeType as E, FormatType as F};

        let format = match value.format() {
            F::Plain => SinkFormat::AppendOnly,
            F::Upsert => SinkFormat::Upsert,
            F::Debezium => SinkFormat::Debezium,
            f @ (F::Unspecified | F::Native | F::DebeziumMongo | F::Maxwell | F::Canal) => {
                return Err(SinkError::Config(anyhow!(
                    "sink format unsupported: {}",
                    f.as_str_name()
                )))
            }
        };
        let encode = match value.encode() {
            E::Json => SinkEncode::Json,
            E::Protobuf => SinkEncode::Protobuf,
            E::Template => SinkEncode::Template,
            E::Avro => SinkEncode::Avro,
            e @ (E::Unspecified | E::Native | E::Csv | E::Bytes) => {
                return Err(SinkError::Config(anyhow!(
                    "sink encode unsupported: {}",
                    e.as_str_name()
                )))
            }
        };
        let options = value.options.into_iter().collect();

        Ok(Self {
            format,
            encode,
            options,
        })
    }
}

/// the catalog of the sink. There are two kind of schema here. The full schema is all columns
/// stored in the `column` which is the sink executor/fragment's output schema. The visible
/// schema contains the columns whose `is_hidden` is false, which is the columns sink out to the
/// external system. The distribution key and all other keys are indexed in the full schema.
#[derive(Clone, Debug)]
pub struct SinkCatalog {
    /// Id of the sink.
    pub id: SinkId,

    /// Schema of the sink.
    pub schema_id: SchemaId,

    /// Database of the sink.
    pub database_id: DatabaseId,

    /// Name of the sink.
    pub name: String,

    /// The full `CREATE SINK` definition of the sink.
    pub definition: String,

    /// All columns of the sink. Note that this is NOT sorted by columnId in the vector.
    columns: Vec<ColumnCatalog>,

    /// Primary keys of the sink. Derived by the frontend.
    pub plan_pk: Vec<ColumnOrder>,

    /// User-defined primary key indices for upsert sink.
    pub downstream_pk: Vec<usize>,

    /// Distribution key indices of the sink. For example, if `distribution_key = [1, 2]`, then the
    /// distribution keys will be `columns[1]` and `columns[2]`.
    pub distribution_key: Vec<usize>,

    /// The properties of the sink.
    pub properties: HashMap<String, String>,

    /// Owner of the sink.
    pub owner: UserId,

    // Relations on which the sink depends.
    pub dependent_relations: Vec<TableId>,

    // The append-only behavior of the physical sink connector. Frontend will determine `sink_type`
    // based on both its own derivation on the append-only attribute and other user-specified
    // options in `properties`.
    pub sink_type: SinkType,

    // The format and encode of the sink.
    pub format_desc: Option<SinkFormatDesc>,

    /// Sink may use a privatelink connection to connect to the downstream system.
    pub connection_id: Option<ConnectionId>,

    pub created_at_epoch: Option<Epoch>,

    pub initialized_at_epoch: Option<Epoch>,

    /// Name of the database
    pub db_name: String,

    /// Name for the table info for Debezium sink
    pub sink_from_name: String,

    pub sink_into_name: Option<String>,
}

impl SinkCatalog {
    pub fn to_proto(&self) -> PbSink {
        PbSink {
            id: self.id.into(),
            schema_id: self.schema_id.schema_id,
            database_id: self.database_id.database_id,
            name: self.name.clone(),
            definition: self.definition.clone(),
            columns: self.columns.iter().map(|c| c.to_protobuf()).collect_vec(),
            plan_pk: self.plan_pk.iter().map(|o| o.to_protobuf()).collect(),
            downstream_pk: self
                .downstream_pk
                .iter()
                .map(|idx| *idx as i32)
                .collect_vec(),
            dependent_relations: self
                .dependent_relations
                .iter()
                .map(|id| id.table_id)
                .collect_vec(),
            distribution_key: self
                .distribution_key
                .iter()
                .map(|k| *k as i32)
                .collect_vec(),
            owner: self.owner.into(),
            properties: self.properties.clone(),
            sink_type: self.sink_type.to_proto() as i32,
            format_desc: self.format_desc.as_ref().map(|f| f.to_proto()),
            connection_id: self.connection_id.map(|id| id.into()),
            initialized_at_epoch: self.initialized_at_epoch.map(|e| e.0),
            created_at_epoch: self.created_at_epoch.map(|e| e.0),
            db_name: self.db_name.clone(),
            sink_from_name: self.sink_from_name.clone(),
            stream_job_status: PbStreamJobStatus::Creating.into(),
            sink_into_name: self.sink_into_name.clone(),
        }
    }

    /// Returns the SQL statement that can be used to create this sink.
    pub fn create_sql(&self) -> String {
        self.definition.clone()
    }

    pub fn visible_columns(&self) -> impl Iterator<Item = &ColumnCatalog> {
        self.columns.iter().filter(|c| !c.is_hidden)
    }

    pub fn full_columns(&self) -> &[ColumnCatalog] {
        &self.columns
    }

    pub fn full_schema(&self) -> Schema {
        let fields = self
            .full_columns()
            .iter()
            .map(|column| Field::from(column.column_desc.clone()))
            .collect_vec();
        Schema { fields }
    }

    pub fn visible_schema(&self) -> Schema {
        let fields = self
            .visible_columns()
            .map(|column| Field::from(column.column_desc.clone()))
            .collect_vec();
        Schema { fields }
    }

    pub fn downstream_pk_indices(&self) -> Vec<usize> {
        self.downstream_pk.clone()
    }
}

impl From<PbSink> for SinkCatalog {
    fn from(pb: PbSink) -> Self {
        let sink_type = pb.get_sink_type().unwrap();
        let format_desc = match pb.format_desc {
            Some(f) => f.try_into().ok(),
            None => {
                let connector = pb.properties.get(CONNECTOR_TYPE_KEY);
                let r#type = pb.properties.get(SINK_TYPE_OPTION);
                match (connector, r#type) {
                    (Some(c), Some(t)) => SinkFormatDesc::from_legacy_type(c, t).ok().flatten(),
                    _ => None,
                }
            }
        };
        SinkCatalog {
            id: pb.id.into(),
            name: pb.name,
            schema_id: pb.schema_id.into(),
            database_id: pb.database_id.into(),
            definition: pb.definition,
            columns: pb
                .columns
                .into_iter()
                .map(ColumnCatalog::from)
                .collect_vec(),
            plan_pk: pb
                .plan_pk
                .iter()
                .map(ColumnOrder::from_protobuf)
                .collect_vec(),
            downstream_pk: pb.downstream_pk.into_iter().map(|k| k as _).collect_vec(),
            distribution_key: pb
                .distribution_key
                .into_iter()
                .map(|k| k as _)
                .collect_vec(),
            properties: pb.properties,
            owner: pb.owner.into(),
            dependent_relations: pb
                .dependent_relations
                .into_iter()
                .map(TableId::from)
                .collect_vec(),
            sink_type: SinkType::from_proto(sink_type),
            format_desc,
            connection_id: pb.connection_id.map(ConnectionId),
            created_at_epoch: pb.created_at_epoch.map(Epoch::from),
            initialized_at_epoch: pb.initialized_at_epoch.map(Epoch::from),
            db_name: pb.db_name,
            sink_from_name: pb.sink_from_name,
            sink_into_name: pb.sink_into_name,
        }
    }
}

impl From<&PbSink> for SinkCatalog {
    fn from(pb: &PbSink) -> Self {
        pb.clone().into()
    }
}
