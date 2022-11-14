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

use std::collections::HashMap;
use std::sync::Arc;

use itertools::Itertools;
use risingwave_common::catalog::IndexId;
use risingwave_common::types::DataType;
use risingwave_pb::catalog::Index as ProstIndex;
use risingwave_pb::expr::expr_node::RexNode;

use crate::catalog::{DatabaseId, SchemaId, TableCatalog};
use crate::expr::{Expr, InputRef};
use crate::optimizer::property::FieldOrder;

#[derive(Clone, Debug)]
pub struct IndexCatalog {
    pub id: IndexId,

    pub name: String,

    /// Only `InputRef` type index is supported Now.
    /// The index of `InputRef` is the column index of the primary table.
    /// index_item size is equal to index table columns size
    pub index_item: Vec<InputRef>,

    pub index_table: Arc<TableCatalog>,

    pub primary_table: Arc<TableCatalog>,

    pub primary_to_secondary_mapping: HashMap<usize, usize>,

    pub secondary_to_primary_mapping: HashMap<usize, usize>,
}

impl IndexCatalog {
    pub fn build_from(
        index_prost: &ProstIndex,
        index_table: &TableCatalog,
        primary_table: &TableCatalog,
    ) -> Self {
        let index_item = index_prost
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
            .collect_vec();

        let primary_to_secondary_mapping = index_item
            .iter()
            .enumerate()
            .map(|(i, input_ref)| (input_ref.index, i))
            .collect();

        let secondary_to_primary_mapping = index_item
            .iter()
            .enumerate()
            .map(|(i, input_ref)| (i, input_ref.index))
            .collect();

        IndexCatalog {
            id: index_prost.id.into(),
            name: index_prost.name.clone(),
            index_item,
            index_table: Arc::new(index_table.clone()),
            primary_table: Arc::new(primary_table.clone()),
            primary_to_secondary_mapping,
            secondary_to_primary_mapping,
        }
    }

    pub fn primary_table_pk_ref_to_index_table(&self) -> Vec<FieldOrder> {
        let mapping = self.primary_to_secondary_mapping();

        self.primary_table
            .pk
            .iter()
            .map(|x| FieldOrder {
                index: *mapping.get(&x.index).unwrap(),
                direct: x.direct,
            })
            .collect_vec()
    }

    pub fn primary_table_distribute_key_ref_to_index_table(&self) -> Vec<usize> {
        let mapping = self.primary_to_secondary_mapping();

        self.primary_table
            .distribution_key
            .iter()
            .map(|x| *mapping.get(x).unwrap())
            .collect_vec()
    }

    pub fn full_covering(&self) -> bool {
        self.index_table.columns.len() == self.primary_table.columns.len()
    }

    /// a mapping maps column index of secondary index to column index of primary table
    pub fn secondary_to_primary_mapping(&self) -> &HashMap<usize, usize> {
        &self.secondary_to_primary_mapping
    }

    /// a mapping maps column index of primary table to column index of secondary index
    pub fn primary_to_secondary_mapping(&self) -> &HashMap<usize, usize> {
        &self.primary_to_secondary_mapping
    }

    pub fn to_prost(&self, schema_id: SchemaId, database_id: DatabaseId) -> ProstIndex {
        ProstIndex {
            id: self.id.index_id,
            schema_id,
            database_id,
            name: self.name.clone(),
            owner: self.index_table.owner,
            index_table_id: self.index_table.id.table_id,
            primary_table_id: self.primary_table.id.table_id,
            index_item: self
                .index_item
                .iter()
                .map(InputRef::to_expr_proto)
                .collect_vec(),
        }
    }
}
