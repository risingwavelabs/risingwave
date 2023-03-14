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
use risingwave_common::catalog::{ColumnCatalog, DatabaseId, SchemaId, TableId, UserId};
use risingwave_common::util::sort_util::ColumnOrder;
use risingwave_pb::plan_common::ColumnDesc as ProstColumnDesc;
use risingwave_pb::stream_plan::SinkDesc as ProstSinkDesc;

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

    /// Primiary keys of the sink (connector). Now the sink does not care about a field's
    /// order (ASC/DESC).
    pub pk: Vec<ColumnOrder>,

    /// Primary key indices of the corresponding sink operator's output.
    pub stream_key: Vec<usize>,

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
        dependent_relations: Vec<TableId>,
    ) -> SinkCatalog {
        SinkCatalog {
            id: self.id,
            schema_id,
            database_id,
            name: self.name,
            definition: self.definition,
            columns: self.columns,
            pk: self.pk,
            stream_key: self.stream_key,
            distribution_key: self.distribution_key,
            owner,
            dependent_relations,
            properties: self.properties.into_iter().collect(),
            sink_type: self.sink_type,
        }
    }

    pub fn to_proto(&self) -> ProstSinkDesc {
        ProstSinkDesc {
            id: self.id.sink_id,
            name: self.name.clone(),
            definition: self.definition.clone(),
            columns: self
                .columns
                .iter()
                .map(|column| Into::<ProstColumnDesc>::into(&column.column_desc))
                .collect_vec(),
            pk: self.pk.iter().map(|k| k.to_protobuf()).collect_vec(),
            stream_key: self.stream_key.iter().map(|idx| *idx as _).collect_vec(),
            distribution_key: self.distribution_key.iter().map(|k| *k as _).collect_vec(),
            properties: self.properties.clone().into_iter().collect(),
            sink_type: self.sink_type.to_proto() as i32,
        }
    }
}
