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

use std::collections::{BTreeMap, HashMap, HashSet};
use std::ops::Deref;
use std::sync::Arc;

use educe::Educe;
use itertools::Itertools;
use risingwave_common::catalog::IndexId;
use risingwave_common::util::epoch::Epoch;
use risingwave_common::util::sort_util::ColumnOrder;
use risingwave_pb::catalog::{PbIndex, PbStreamJobStatus};

use super::ColumnId;
use crate::catalog::{DatabaseId, OwnedByUserCatalog, SchemaId, TableCatalog};
use crate::expr::{Expr, ExprImpl, FunctionCall};
use crate::user::UserId;

#[derive(Clone, Debug, Educe)]
#[educe(PartialEq, Eq, Hash)]
pub struct IndexCatalog {
    pub id: IndexId,

    pub name: String,

    /// Only `InputRef` and `FuncCall` type index is supported Now.
    /// The index of `InputRef` is the column index of the primary table.
    /// The index_item size is equal to the index table columns size
    /// The input args of `FuncCall` is also the column index of the primary table.
    pub index_item: Vec<ExprImpl>,

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

    pub original_columns: Vec<ColumnId>,

    pub created_at_epoch: Option<Epoch>,

    pub initialized_at_epoch: Option<Epoch>,

    pub description: Option<String>,
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
            .map(ExprImpl::from_expr_proto)
            .try_collect()
            .unwrap();

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

        let original_columns = index_prost
            .original_columns
            .clone()
            .into_iter()
            .map(Into::into)
            .collect();

        IndexCatalog {
            id: index_prost.id.into(),
            name: index_prost.name.clone(),
            index_item,
            index_table: Arc::new(index_table.clone()),
            primary_table: Arc::new(primary_table.clone()),
            primary_to_secondary_mapping,
            secondary_to_primary_mapping,
            function_mapping,
            original_columns,
            created_at_epoch: index_prost.created_at_epoch.map(Epoch::from),
            initialized_at_epoch: index_prost.initialized_at_epoch.map(Epoch::from),
            description: index_prost.description.clone(),
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
            original_columns: self.original_columns.iter().map(Into::into).collect_vec(),
            initialized_at_epoch: self.initialized_at_epoch.map(|e| e.0),
            created_at_epoch: self.created_at_epoch.map(|e| e.0),
            stream_job_status: PbStreamJobStatus::Creating.into(),
            description: self.description.clone(),
        }
    }

    pub fn display(&self) -> IndexDisplay {
        let index_table = self.index_table.clone();
        let index_columns_with_ordering = index_table
            .pk
            .iter()
            .filter(|x| !index_table.columns[x.column_index].is_hidden)
            .map(|x| {
                let index_column_name = index_table.columns[x.column_index].name().to_string();
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
            .map(|(_, x)| x.name().to_string())
            .collect_vec();

        let distributed_by_columns = index_table
            .distribution_key
            .iter()
            .map(|&x| index_table.columns[x].name().to_string())
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
