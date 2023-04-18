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

use std::collections::HashMap;

use itertools::Itertools;
use risingwave_common::catalog::{
    ColumnCatalog, ConnectionId, DatabaseId, Field, Schema, SchemaId, TableId, UserId,
};
use risingwave_common::util::sort_util::ColumnOrder;
use risingwave_pb::catalog::{PbSink, PbSinkType};

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
    pub columns: Vec<ColumnCatalog>,

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

    /// Sink may use a privatelink connection to connect to the downstream system.
    pub connection_id: Option<ConnectionId>,
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
            connection_id: self.connection_id.map(|id| id.into()),
        }
    }

    /// Returns the SQL statement that can be used to create this sink.
    pub fn create_sql(&self) -> String {
        self.definition.clone()
    }

    pub fn schema(&self) -> Schema {
        let fields = self
            .columns
            .iter()
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
            connection_id: pb.connection_id.map(|conn_id| ConnectionId(conn_id)),
        }
    }
}

impl From<&PbSink> for SinkCatalog {
    fn from(pb: &PbSink) -> Self {
        pb.clone().into()
    }
}
