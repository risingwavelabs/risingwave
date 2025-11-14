// Copyright 2025 RisingWave Labs
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

use std::collections::{BTreeMap, HashMap, HashSet};
use std::ops::Deref;
use std::sync::Arc;

use educe::Educe;
use itertools::Itertools;
use risingwave_common::catalog::{ColumnDesc, Field, IndexId, Schema};
use risingwave_common::session_config::SessionConfig;
use risingwave_common::util::epoch::Epoch;
use risingwave_common::util::sort_util::ColumnOrder;
use risingwave_pb::catalog::{
    PbIndex, PbIndexColumnProperties, PbVectorIndexInfo, vector_index_info,
};

use crate::catalog::table_catalog::TableType;
use crate::catalog::{OwnedByUserCatalog, TableCatalog};
use crate::expr::{ExprDisplay, ExprImpl, ExprRewriter as _, FunctionCall};
use crate::user::UserId;

#[derive(Clone, Debug, Educe)]
#[educe(PartialEq, Eq, Hash)]
pub struct TableIndex {
    pub name: String,
    /// The properties of the index columns.
    /// <https://www.postgresql.org/docs/current/functions-info.html#FUNCTIONS-INFO-INDEX-COLUMN-PROPS>
    pub index_column_properties: Vec<PbIndexColumnProperties>,

    pub index_table: Arc<TableCatalog>,

    pub primary_table: Arc<TableCatalog>,

    pub primary_to_secondary_mapping: BTreeMap<usize, usize>,

    pub secondary_to_primary_mapping: BTreeMap<usize, usize>,

    /// Map function call from the primary table to the index table.
    /// Use `HashMap` instead of `BTreeMap`, because `FunctionCall` can't be used as the key for
    /// `BTreeMap`. BTW, the trait `std::hash::Hash` is not implemented for
    /// `HashMap<function_call::FunctionCall, usize>`, so we need to ignore it. It will not
    /// affect the correctness, since it can be derived by `index_item`.
    #[educe(PartialEq(ignore))]
    #[educe(Hash(ignore))]
    pub function_mapping: HashMap<FunctionCall, usize>,

    pub index_columns_len: u32,
}

#[derive(Clone, Debug, PartialEq, Eq, Educe)]
#[educe(Hash)]
pub struct VectorIndex {
    pub index_table: Arc<TableCatalog>,
    pub vector_expr: ExprImpl,
    #[educe(Hash(ignore))]
    pub primary_to_included_info_column_mapping: HashMap<usize, usize>,
    pub primary_key_idx_in_info_columns: Vec<usize>,
    pub included_info_columns: Vec<usize>,
    pub vector_index_info: PbVectorIndexInfo,
}

impl VectorIndex {
    pub fn info_column_desc(&self) -> Vec<ColumnDesc> {
        self.index_table.columns[1..=self.included_info_columns.len()]
            .iter()
            .map(|col| col.column_desc.clone())
            .collect()
    }

    pub fn resolve_hnsw_ef_search(&self, config: &SessionConfig) -> Option<usize> {
        match self.vector_index_info.config.as_ref().unwrap() {
            vector_index_info::Config::Flat(_) => None,
            vector_index_info::Config::HnswFlat(_) => Some(config.batch_hnsw_ef_search()),
        }
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub enum IndexType {
    Table(Arc<TableIndex>),
    Vector(Arc<VectorIndex>),
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct IndexCatalog {
    pub id: IndexId,

    pub name: String,

    /// Only `InputRef` and `FuncCall` type index is supported Now.
    /// The index of `InputRef` is the column index of the primary table.
    /// The `index_item` size is equal to the index table columns size
    /// The input args of `FuncCall` is also the column index of the primary table.
    pub index_item: Vec<ExprImpl>,

    pub index_type: IndexType,

    pub primary_table: Arc<TableCatalog>,

    pub created_at_epoch: Option<Epoch>,

    pub initialized_at_epoch: Option<Epoch>,

    pub created_at_cluster_version: Option<String>,

    pub initialized_at_cluster_version: Option<String>,
}

impl IndexCatalog {
    pub fn build_from(
        index_prost: &PbIndex,
        index_table: &Arc<TableCatalog>,
        primary_table: &Arc<TableCatalog>,
    ) -> Self {
        let index_item: Vec<ExprImpl> = index_prost
            .index_item
            .iter()
            .map(|expr| ExprImpl::from_expr_proto(expr).unwrap())
            .map(|expr| item_rewriter::CompositeCastEliminator.rewrite_expr(expr))
            .collect();

        let index_type = match index_table.table_type {
            TableType::Index => {
                let primary_to_secondary_mapping: BTreeMap<usize, usize> = index_item
                    .iter()
                    .enumerate()
                    .filter_map(|(i, expr)| match expr {
                        ExprImpl::InputRef(input_ref) => Some((input_ref.index, i)),
                        ExprImpl::FunctionCall(_) => None,
                        _ => unreachable!(),
                    })
                    .collect();

                let secondary_to_primary_mapping = BTreeMap::from_iter(
                    primary_to_secondary_mapping
                        .clone()
                        .into_iter()
                        .map(|(x, y)| (y, x)),
                );

                let function_mapping: HashMap<FunctionCall, usize> = index_item
                    .iter()
                    .enumerate()
                    .filter_map(|(i, expr)| match expr {
                        ExprImpl::InputRef(_) => None,
                        ExprImpl::FunctionCall(func) => Some((func.deref().clone(), i)),
                        _ => unreachable!(),
                    })
                    .collect();
                IndexType::Table(Arc::new(TableIndex {
                    name: index_prost.name.clone(),
                    index_column_properties: index_prost.index_column_properties.clone(),
                    index_columns_len: index_prost.index_columns_len,
                    index_table: index_table.clone(),
                    primary_table: primary_table.clone(),
                    primary_to_secondary_mapping,
                    secondary_to_primary_mapping,
                    function_mapping,
                }))
            }
            TableType::VectorIndex => {
                assert_eq!(index_prost.index_columns_len, 1);
                let included_info_columns = index_item[1..].iter().map(|item| {
                    let ExprImpl::InputRef(input) = item else {
                        panic!("vector index included columns must be from direct input column, but got: {:?}", item);
                    };
                    input.index
                }).collect_vec();
                let primary_to_included_info_column_mapping: HashMap<_, _> = included_info_columns
                    .iter()
                    .enumerate()
                    .map(|(included_info_column_idx, primary_column_idx)| {
                        (*primary_column_idx, included_info_column_idx)
                    })
                    .collect();
                let primary_key_idx_in_info_columns = primary_table
                    .pk()
                    .iter()
                    .map(|order| primary_to_included_info_column_mapping[&order.column_index])
                    .collect();
                IndexType::Vector(Arc::new(VectorIndex {
                    index_table: index_table.clone(),
                    vector_expr: index_item[0].clone(),
                    primary_to_included_info_column_mapping,
                    primary_key_idx_in_info_columns,
                    included_info_columns,
                    vector_index_info: index_table
                        .vector_index_info
                        .expect("should exist for vector index"),
                }))
            }
            TableType::Table | TableType::MaterializedView | TableType::Internal => {
                unreachable!()
            }
        };

        IndexCatalog {
            id: index_prost.id,
            name: index_prost.name.clone(),
            index_item,
            index_type,
            primary_table: primary_table.clone(),
            created_at_epoch: index_prost.created_at_epoch.map(Epoch::from),
            initialized_at_epoch: index_prost.initialized_at_epoch.map(Epoch::from),
            created_at_cluster_version: index_prost.created_at_cluster_version.clone(),
            initialized_at_cluster_version: index_prost.initialized_at_cluster_version.clone(),
        }
    }
}

impl TableIndex {
    pub fn primary_table_pk_ref_to_index_table(&self) -> Vec<ColumnOrder> {
        let mapping = self.primary_to_secondary_mapping();

        self.primary_table
            .pk
            .iter()
            .map(|x| ColumnOrder::new(*mapping.get(&x.column_index).unwrap(), x.order_type))
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

    /// A mapping maps the column index of the secondary index to the column index of the primary
    /// table.
    pub fn secondary_to_primary_mapping(&self) -> &BTreeMap<usize, usize> {
        &self.secondary_to_primary_mapping
    }

    /// A mapping maps the column index of the primary table to the column index of the secondary
    /// index.
    pub fn primary_to_secondary_mapping(&self) -> &BTreeMap<usize, usize> {
        &self.primary_to_secondary_mapping
    }

    pub fn function_mapping(&self) -> &HashMap<FunctionCall, usize> {
        &self.function_mapping
    }
}

impl IndexCatalog {
    pub fn index_table(&self) -> &Arc<TableCatalog> {
        match &self.index_type {
            IndexType::Table(index) => &index.index_table,
            IndexType::Vector(index) => &index.index_table,
        }
    }

    /// Get the column properties of the index column.
    pub fn get_column_properties(&self, column_idx: usize) -> Option<PbIndexColumnProperties> {
        match &self.index_type {
            IndexType::Table(index) => index.index_column_properties.get(column_idx).cloned(),
            IndexType::Vector { .. } => {
                if column_idx == 0 {
                    // return with the default value defined in [https://www.postgresql.org/docs/current/sql-createindex.html]
                    Some(PbIndexColumnProperties {
                        is_desc: false,
                        nulls_first: false,
                    })
                } else {
                    None
                }
            }
        }
    }

    pub fn get_column_def(&self, column_idx: usize) -> Option<String> {
        if let Some(col) = self.index_table().columns.get(column_idx) {
            if col.is_hidden {
                return None;
            }
        } else {
            return None;
        }
        let expr_display = ExprDisplay {
            expr: &self.index_item[column_idx],
            input_schema: &Schema::new(
                self.primary_table
                    .columns
                    .iter()
                    .map(|col| Field::from(&col.column_desc))
                    .collect_vec(),
            ),
        };

        // TODO(Kexiang): Currently, extra info like ":Int32" introduced by `ExprDisplay` is kept for simplity.
        // We'd better remove it in the future.
        Some(expr_display.to_string())
    }

    pub fn display(&self) -> IndexDisplay {
        let index_table = self.index_table().clone();
        let index_columns_with_ordering = index_table
            .pk
            .iter()
            .filter(|x| !index_table.columns[x.column_index].is_hidden)
            .map(|x| {
                let index_column_name = index_table.columns[x.column_index].name().to_owned();
                format!("{} {}", index_column_name, x.order_type)
            })
            .collect_vec();

        let pk_column_index_set = index_table
            .pk
            .iter()
            .map(|x| x.column_index)
            .collect::<HashSet<_>>();

        let include_columns = index_table
            .columns
            .iter()
            .enumerate()
            .filter(|(i, _)| !pk_column_index_set.contains(i))
            .filter(|(_, x)| !x.is_hidden)
            .map(|(_, x)| x.name().to_owned())
            .collect_vec();

        let distributed_by_columns = index_table
            .distribution_key
            .iter()
            .map(|&x| index_table.columns[x].name().to_owned())
            .collect_vec();

        IndexDisplay {
            index_columns_with_ordering,
            include_columns,
            distributed_by_columns,
        }
    }

    pub fn is_created(&self) -> bool {
        self.index_table().is_created()
    }
}

pub struct IndexDisplay {
    pub index_columns_with_ordering: Vec<String>,
    pub include_columns: Vec<String>,
    pub distributed_by_columns: Vec<String>,
}

impl OwnedByUserCatalog for IndexCatalog {
    fn owner(&self) -> UserId {
        self.index_table().owner
    }
}

mod item_rewriter {
    use risingwave_pb::expr::expr_node;

    use crate::expr::{Expr, ExprImpl, ExprRewriter, FunctionCall};

    /// Rewrite the expression of index item to eliminate `CompositeCast`, if any. This is needed
    /// if the type of a column was changed and there's functional index on it.
    ///
    /// # Example
    ///
    /// Imagine there's a table created with `CREATE TABLE t (v struct<a int, b int>)`.
    /// Then we create an index on it with `CREATE INDEX idx ON t ((v).a)`, which will create an
    /// index item `Field(InputRef(0), 0)`.
    ///
    /// If we alter the column with `ALTER TABLE t ALTER COLUMN v TYPE struct<x varchar, a int>`,
    /// the meta service will wrap the `InputRef(0)` with a `CompositeCast` to maintain the correct
    /// return type. The index item will now become `Field(CompositeCast(InputRef(0)), 0)`.
    ///
    /// `CompositeCast` is for internal use only, and cannot be constructed or executed. To allow
    /// this functional index to work and be matched with user queries, we need to eliminate it
    /// here. By comparing the input and output types of `CompositeCast` and matching the field id,
    /// we can find the real `Field` index and rewrite it to `Field(InputRef(0), 1)`.
    ///
    /// Note that if the field is dropped, we will leave the index item as is. This makes the index
    /// item invalid, and it will never be matched and used.
    pub struct CompositeCastEliminator;

    impl ExprRewriter for CompositeCastEliminator {
        fn rewrite_function_call(&mut self, func_call: FunctionCall) -> ExprImpl {
            let (func_type, inputs, ret) = func_call.decompose();

            // Flatten consecutive `CompositeCast`.
            if func_type == expr_node::Type::CompositeCast {
                let child = inputs[0].clone();

                if let Some(child) = child.as_function_call()
                    && child.func_type() == expr_node::Type::CompositeCast
                {
                    let new_child = child.inputs()[0].clone();

                    // If the type already matches, no need to wrap again.
                    // Recursively eliminate more composite cast by calling rewrite again.
                    if new_child.return_type() == ret {
                        return self.rewrite_expr(new_child);
                    } else {
                        let new_composite_cast =
                            FunctionCall::new_unchecked(func_type, vec![new_child], ret);
                        return self.rewrite_function_call(new_composite_cast);
                    }
                }
            }
            // Rewrite `Field(CompositeCast(x), y)` to `Field(x, y')`.
            // TODO: also support rewriting `ArrayAccess` and `MapAccess`.
            else if func_type == expr_node::Type::Field {
                let child = inputs[0].clone();

                if let Some(child) = child.as_function_call()
                    && child.func_type() == expr_node::Type::CompositeCast
                {
                    let index = (inputs[1].clone().into_literal().unwrap())
                        .get_data()
                        .clone()
                        .unwrap()
                        .into_int32();

                    let struct_type = child.return_type().into_struct();
                    let field_id = struct_type
                        .id_at(index as usize)
                        .expect("ids should be set");

                    // Unwrap the composite cast.
                    let new_child = child.inputs()[0].clone();
                    let new_struct_type = new_child.return_type().into_struct();

                    let Some(new_index) = new_struct_type
                        .ids()
                        .expect("ids should be set")
                        .position(|x| x == field_id)
                    else {
                        // Previously we have index on this field, but now it's dropped.
                        // As a result, this entire index item becomes invalid.
                        // Simply leave it as is. Users cannot construct a `CompositeCast` (which is
                        // not user-facing), thus this index item will never be matched and used.
                        return FunctionCall::new_unchecked(func_type, inputs, ret).into();
                    };
                    let new_index = ExprImpl::literal_int(new_index as i32);

                    let new_inputs = vec![new_child, new_index];
                    let new_field_call = FunctionCall::new_unchecked(func_type, new_inputs, ret);

                    // Recursively eliminate more composite cast by calling rewrite again.
                    return self.rewrite_function_call(new_field_call);
                }
            }

            let inputs = inputs
                .into_iter()
                .map(|expr| self.rewrite_expr(expr))
                .collect();
            FunctionCall::new_unchecked(func_type, inputs, ret).into()
        }
    }
}
