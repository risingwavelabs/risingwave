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

use std::collections::BTreeMap;

use itertools::Itertools;
use risingwave_common::catalog::{
    ColumnCatalog, ConnectionId, DatabaseId, SchemaId, TableId, UserId,
};
use risingwave_common::util::sort_util::ColumnOrder;
use risingwave_pb::plan_common::PbColumnDesc;
use risingwave_pb::stream_plan::PbSinkDesc;

use super::{SinkCatalog, SinkId, SinkType};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SinkDesc {
    /// Id of the sink. For debug now.
    pub id: SinkId,

    /// Name of the sink. For debug now.
    pub name: String,

    /// Full SQL definition of the sink. For debug now.
    pub definition: String,

    /// All columns of the sink. Note that this is NOT sorted by columnId in the vector.
    pub columns: Vec<ColumnCatalog>,

    /// Primiary keys of the sink. Derived by the frontend.
    pub plan_pk: Vec<ColumnOrder>,

    /// User-defined primary key indices for upsert sink.
    pub downstream_pk: Vec<usize>,

    /// Distribution key indices of the sink. For example, if `distribution_key = [1, 2]`, then the
    /// distribution keys will be `columns[1]` and `columns[2]`.
    pub distribution_key: Vec<usize>,

    /// The properties of the sink.
    pub properties: BTreeMap<String, String>,

    // The append-only behavior of the physical sink connector. Frontend will determine `sink_type`
    // based on both its own derivation on the append-only attribute and other user-specified
    // options in `properties`.
    pub sink_type: SinkType,
}

impl SinkDesc {
    pub fn into_catalog(
        self,
        schema_id: SchemaId,
        database_id: DatabaseId,
        owner: UserId,
        connection_id: Option<ConnectionId>,
        dependent_relations: Vec<TableId>,
    ) -> SinkCatalog {
        SinkCatalog {
            id: self.id,
            schema_id,
            database_id,
            name: self.name,
            definition: self.definition,
            columns: self.columns,
            plan_pk: self.plan_pk,
            downstream_pk: self.downstream_pk,
            distribution_key: self.distribution_key,
            owner,
            dependent_relations,
            properties: self.properties.into_iter().collect(),
            sink_type: self.sink_type,
            connection_id,
        }
    }

    pub fn to_proto(&self) -> PbSinkDesc {
        PbSinkDesc {
            id: self.id.sink_id,
            name: self.name.clone(),
            definition: self.definition.clone(),
            columns: self
                .columns
                .iter()
                .map(|column| Into::<PbColumnDesc>::into(&column.column_desc))
                .collect_vec(),
            plan_pk: self.plan_pk.iter().map(|k| k.to_protobuf()).collect_vec(),
            downstream_pk: self.downstream_pk.iter().map(|idx| *idx as _).collect_vec(),
            distribution_key: self.distribution_key.iter().map(|k| *k as _).collect_vec(),
            properties: self.properties.clone().into_iter().collect(),
            sink_type: self.sink_type.to_proto() as i32,
        }
    }
}
