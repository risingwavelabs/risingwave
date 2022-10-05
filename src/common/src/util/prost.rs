// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use risingwave_pb::stream_plan::source_node::Info as SourceInfo;
use risingwave_pb::stream_plan::SourceNode;
use risingwave_pb::{batch_plan, data};

pub trait TypeUrl {
    fn type_url() -> &'static str;
}

impl TypeUrl for batch_plan::ExchangeNode {
    fn type_url() -> &'static str {
        "type.googleapis.com/plan.ExchangeNode"
    }
}

impl TypeUrl for data::Column {
    fn type_url() -> &'static str {
        "type.googleapis.com/data.Column"
    }
}

#[inline(always)]
pub fn is_stream_source(source_node: &SourceNode) -> bool {
    matches!(source_node.info.as_ref(), Some(SourceInfo::StreamSource(_)))
}

use risingwave_pb::catalog::{ColumnIndex, TableSourceInfo};
use risingwave_pb::plan_common::ColumnCatalog;
use risingwave_pb::stream_plan::source_node::Info as ProstSourceInfo;

use crate::catalog::{ColumnDesc, ColumnId, Schema};

pub fn create_table_info(
    schema: &Schema,
    row_id_index: Option<u64>,
    pk_column_ids: Vec<i32>,
) -> ProstSourceInfo {
    ProstSourceInfo::TableSource(TableSourceInfo {
        row_id_index: row_id_index.map(|index| ColumnIndex { index }),
        columns: schema
            .fields
            .iter()
            .enumerate()
            .map(|(i, f)| ColumnCatalog {
                column_desc: Some(
                    ColumnDesc {
                        data_type: f.data_type.clone(),
                        column_id: ColumnId::from(i as i32), // use column index as column id
                        name: f.name.clone(),
                        field_descs: vec![],
                        type_name: "".to_string(),
                    }
                    .to_protobuf(),
                ),
                is_hidden: false,
            })
            .collect(),
        pk_column_ids,
        properties: Default::default(),
    })
}
