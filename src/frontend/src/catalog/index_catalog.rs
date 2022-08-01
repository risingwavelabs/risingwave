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

use itertools::Itertools;
use risingwave_common::catalog::IndexId;
use risingwave_common::types::DataType;
use risingwave_pb::catalog::Index as ProstIndex;
use risingwave_pb::expr::expr_node::RexNode;

use crate::catalog::{DatabaseId, SchemaId, TableId};
use crate::expr::{Expr, InputRef};

#[derive(Clone, Debug)]
pub struct IndexCatalog {
    pub id: IndexId,
    pub schema_id: SchemaId,
    pub database_id: DatabaseId,
    pub name: String,
    pub table_id: TableId,
    pub indexed_table_id: TableId,
    // Only InputRef type index is supported.
    pub index_columns: Vec<InputRef>,
    pub include_columns: Vec<InputRef>,
}

impl From<&ProstIndex> for IndexCatalog {
    fn from(index: &ProstIndex) -> Self {
        IndexCatalog {
            id: index.id.into(),
            schema_id: index.schema_id,
            database_id: index.database_id,
            name: index.name.clone(),
            table_id: index.table_id.into(),
            indexed_table_id: index.indexed_table_id.into(),
            index_columns: index
                .index_item
                .iter()
                .map(|x| match x.rex_node.as_ref().unwrap() {
                    RexNode::InputRef(input_ref_expr) => InputRef {
                        index: input_ref_expr.column_idx as usize,
                        data_type: DataType::from(x.return_type.as_ref().unwrap()),
                    },
                    RexNode::FuncCall(_) => unimplemented!(),
                    _ => unreachable!(),
                })
                .collect_vec(),
            include_columns: index
                .include_item
                .iter()
                .map(|x| match x.rex_node.as_ref().unwrap() {
                    RexNode::InputRef(input_ref_expr) => InputRef {
                        index: input_ref_expr.column_idx as usize,
                        data_type: DataType::from(x.return_type.as_ref().unwrap()),
                    },
                    _ => unreachable!(),
                })
                .collect_vec(),
        }
    }
}

impl IndexCatalog {
    pub fn to_prost(&self, schema_id: SchemaId, database_id: DatabaseId) -> ProstIndex {
        ProstIndex {
            id: self.id.index_id,
            schema_id,
            database_id,
            name: self.name.clone(),
            table_id: self.table_id.table_id,
            indexed_table_id: self.indexed_table_id.table_id,
            index_item: self
                .index_columns
                .iter()
                .map(InputRef::to_expr_proto)
                .collect_vec(),
            include_item: self
                .include_columns
                .iter()
                .map(InputRef::to_expr_proto)
                .collect_vec(),
        }
    }
}
