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
use risingwave_common::util::sort_util::OrderPair;
use risingwave_pb::catalog::Index as ProstIndex;
use risingwave_pb::expr::expr_node::RexNode;

use crate::catalog::{DatabaseId, SchemaId, TableId};
use crate::expr::{Expr, InputRef};
use crate::optimizer::property::FieldOrder;

/// # `IndexCatalog` Example:
/// create table t (a int, b int, c int, d int);
/// create index idx on t(a,b,a) include(a,c,d,b);
///
/// Table t (called indexed table)
/// columns:
/// 0: `_row_id`
/// 1: a
/// 2: b
/// 3: c
/// 4: d
///
/// index idx (called index table)
/// columns:
/// 0: a
/// 1: b
/// 2: c
/// 3: d
/// 4: `t._row_id`
///
/// `TableDesc` columns of index table
/// | index columns | include columns | hidden(pk columns) |
/// If index columns and include columns contain pk columns of the indexed table,
/// then the hidden pk columns can be removed.
///
/// The index of `InputRef` is the index of the indexed `TableDesc` columns.
/// `index_columns`: InputRef(1),InputRef(2)
/// `include_columns`: InputRef(3),InputRef(4)

#[derive(Clone, Debug)]
pub struct IndexCatalog {
    pub id: IndexId,

    pub schema_id: SchemaId,

    pub database_id: DatabaseId,

    /// index name
    pub name: String,

    pub owner: u32,

    /// The id of the index table
    pub table_id: TableId,

    /// The id of the indexed table
    pub indexed_table_id: TableId,

    /// Only `InputRef` type index is supported Now.
    /// The index of `InputRef` is the index of the indexed `TableDesc` columns.
    /// `index_columns` and `include_columns` are in canonical form.
    ///
    /// example-1:
    /// create table t (a int, b int, c int, d int)
    /// create index idx on t(a,b) include(c,d)
    /// index_columns stand for (a,b)
    /// include_columns stand for (c,d)
    ///
    /// example-2: illustration of canonical form
    /// create index idx on t(a,b,a) include(a,c,d,b)
    /// `index_columns` stand for (a,b)
    /// `include_columns` stand for (c,d)
    pub index_columns: Vec<InputRef>,

    pub include_columns: Vec<InputRef>,

    /// mapping the index columns to the order key of the indexed table, used by table lookup
    pub indexed_table_order_key: Vec<OrderPair>,
}

impl From<&ProstIndex> for IndexCatalog {
    fn from(index: &ProstIndex) -> Self {
        IndexCatalog {
            id: index.id.into(),
            schema_id: index.schema_id,
            database_id: index.database_id,
            name: index.name.clone(),
            owner: index.owner,
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
                    RexNode::FuncCall(_) => unimplemented!(),
                    _ => unreachable!(),
                })
                .collect_vec(),
            indexed_table_order_key: index
                .indexed_table_order_key
                .iter()
                .map(FieldOrder::from_protobuf)
                .map(|x| x.to_order_pair())
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
            owner: self.owner,
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
            indexed_table_order_key: self
                .indexed_table_order_key
                .iter()
                .map(|o| o.to_protobuf())
                .collect(),
        }
    }
}
