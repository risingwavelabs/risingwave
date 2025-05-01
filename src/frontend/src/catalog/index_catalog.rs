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
use risingwave_common::catalog::{Field, IndexId, Schema};
use risingwave_common::util::epoch::Epoch;
use risingwave_common::util::sort_util::ColumnOrder;
use risingwave_pb::catalog::{PbIndex, PbIndexColumnProperties, PbStreamJobStatus};

use crate::catalog::{DatabaseId, OwnedByUserCatalog, SchemaId, TableCatalog};
use crate::expr::{Expr, ExprDisplay, ExprImpl, ExprRewriter as _, FunctionCall};
use crate::user::UserId;

#[derive(Clone, Debug, Educe)]
#[educe(PartialEq, Eq, Hash)]
pub struct IndexCatalog {
    pub id: IndexId,

    pub name: String,

    /// Only `InputRef` and `FuncCall` type index is supported Now.
    /// The index of `InputRef` is the column index of the primary table.
    /// The `index_item` size is equal to the index table columns size
    /// The input args of `FuncCall` is also the column index of the primary table.
    pub index_item: Vec<ExprImpl>,

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

    pub created_at_epoch: Option<Epoch>,

    pub initialized_at_epoch: Option<Epoch>,

    pub created_at_cluster_version: Option<String>,

    pub initialized_at_cluster_version: Option<String>,
}

impl IndexCatalog {
    pub fn build_from(
        index_prost: &PbIndex,
        index_table: &TableCatalog,
        primary_table: &TableCatalog,
    ) -> Self {
        let index_item: Vec<ExprImpl> = index_prost
            .index_item
            .iter()
            .map(|expr| ExprImpl::from_expr_proto(expr).unwrap())
            .map(|expr| rewriter::CompositeCastEliminator.rewrite_expr(expr))
            .collect();

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

        IndexCatalog {
            id: index_prost.id.into(),
            name: index_prost.name.clone(),
            index_item,
            index_column_properties: index_prost.index_column_properties.clone(),
            index_table: Arc::new(index_table.clone()),
            primary_table: Arc::new(primary_table.clone()),
            primary_to_secondary_mapping,
            secondary_to_primary_mapping,
            function_mapping,
            index_columns_len: index_prost.index_columns_len,
            created_at_epoch: index_prost.created_at_epoch.map(Epoch::from),
            initialized_at_epoch: index_prost.initialized_at_epoch.map(Epoch::from),
            created_at_cluster_version: index_prost.created_at_cluster_version.clone(),
            initialized_at_cluster_version: index_prost.initialized_at_cluster_version.clone(),
        }
    }

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

    pub fn to_prost(&self, schema_id: SchemaId, database_id: DatabaseId) -> PbIndex {
        PbIndex {
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
                .map(|expr| expr.to_expr_proto())
                .collect_vec(),
            index_column_properties: self.index_column_properties.clone(),
            index_columns_len: self.index_columns_len,
            initialized_at_epoch: self.initialized_at_epoch.map(|e| e.0),
            created_at_epoch: self.created_at_epoch.map(|e| e.0),
            stream_job_status: PbStreamJobStatus::Creating.into(),
            initialized_at_cluster_version: self.initialized_at_cluster_version.clone(),
            created_at_cluster_version: self.created_at_cluster_version.clone(),
        }
    }

    /// Get the column properties of the index column.
    pub fn get_column_properties(&self, column_idx: usize) -> Option<PbIndexColumnProperties> {
        self.index_column_properties.get(column_idx).cloned()
    }

    pub fn get_column_def(&self, column_idx: usize) -> Option<String> {
        if let Some(col) = self.index_table.columns.get(column_idx) {
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
        let index_table = self.index_table.clone();
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
}

pub struct IndexDisplay {
    pub index_columns_with_ordering: Vec<String>,
    pub include_columns: Vec<String>,
    pub distributed_by_columns: Vec<String>,
}

impl OwnedByUserCatalog for IndexCatalog {
    fn owner(&self) -> UserId {
        self.index_table.owner
    }
}

mod rewriter {
    use risingwave_pb::expr::expr_node;

    use crate::expr::{Expr, ExprImpl, ExprRewriter, FunctionCall};

    pub struct CompositeCastEliminator;

    impl ExprRewriter for CompositeCastEliminator {
        fn rewrite_function_call(&mut self, func_call: FunctionCall) -> ExprImpl {
            let (func_type, inputs, ret) = func_call.decompose();

            // TODO: support passthroughing `ArrayAccess` and `MapAccess`.
            if func_type == expr_node::Type::Field {
                let child = inputs[0].clone();
                let index = (inputs[1].clone().into_literal().unwrap())
                    .get_data()
                    .clone()
                    .unwrap()
                    .into_int32();

                if let Some(cast) = child.as_function_call()
                    && cast.func_type() == expr_node::Type::CompositeCast
                {
                    let struct_type = cast.return_type().into_struct();
                    let field_id = struct_type
                        .id_at(index as usize)
                        .expect("ids should be set");

                    let new_child = cast.inputs()[0].clone();
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

                    // Recursively eliminate more composite cast.
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
