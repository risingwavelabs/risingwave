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

use std::collections::HashMap;
use std::sync::Arc;

use educe::Educe;
use pretty_xmlish::Pretty;
use risingwave_common::catalog::{ColumnCatalog, ColumnDesc, Field, Schema};
use risingwave_common::util::column_index_mapping::ColIndexMapping;

use super::GenericPlanNode;
use crate::catalog::ColumnId;
use crate::catalog::system_catalog::SystemTableCatalog;
use crate::optimizer::optimizer_context::OptimizerContextRef;
use crate::optimizer::property::{Cardinality, FunctionalDependencySet};

/// [`SysScan`] returns contents of a table or other equivalent object
#[derive(Debug, Clone, Educe)]
#[educe(PartialEq, Eq, Hash)]
pub struct SysScan {
    pub output_col_idx: Vec<usize>,
    pub table: Arc<SystemTableCatalog>,
    /// Help `RowSeqSysScan` executor use a better chunk size
    pub chunk_size: Option<u32>,
    /// The cardinality of the table **without** applying the predicate.
    pub table_cardinality: Cardinality,
    #[educe(PartialEq(ignore))]
    #[educe(Hash(ignore))]
    pub ctx: OptimizerContextRef,
}

impl SysScan {
    pub(crate) fn column_names_with_table_prefix(&self) -> Vec<String> {
        self.output_col_idx
            .iter()
            .map(|&i| format!("{}.{}", self.table.name, self.get_table_columns()[i].name))
            .collect()
    }

    pub(crate) fn column_names(&self) -> Vec<String> {
        self.output_col_idx
            .iter()
            .map(|&i| self.get_table_columns()[i].name.clone())
            .collect()
    }

    /// get the Mapping of columnIndex from internal column index to output column index
    pub fn i2o_col_mapping(&self) -> ColIndexMapping {
        ColIndexMapping::with_remaining_columns(
            &self.output_col_idx,
            self.get_table_columns().len(),
        )
    }

    /// Create a `LogicalSysScan` node. Used internally by optimizer.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        output_col_idx: Vec<usize>, // the column index in the table
        table: Arc<SystemTableCatalog>,
        ctx: OptimizerContextRef,
        table_cardinality: Cardinality,
    ) -> Self {
        Self::new_inner(output_col_idx, table, ctx, table_cardinality)
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new_inner(
        output_col_idx: Vec<usize>, // the column index in the table
        table: Arc<SystemTableCatalog>,
        ctx: OptimizerContextRef,
        table_cardinality: Cardinality,
    ) -> Self {
        Self {
            output_col_idx,
            table,
            chunk_size: None,
            ctx,
            table_cardinality,
        }
    }

    pub(crate) fn columns_pretty<'a>(&self, verbose: bool) -> Pretty<'a> {
        Pretty::Array(
            match verbose {
                true => self.column_names_with_table_prefix(),
                false => self.column_names(),
            }
            .into_iter()
            .map(Pretty::from)
            .collect(),
        )
    }
}

impl GenericPlanNode for SysScan {
    fn schema(&self) -> Schema {
        let fields = self
            .output_col_idx
            .iter()
            .map(|tb_idx| {
                let col = &self.get_table_columns()[*tb_idx];
                Field::from_with_table_name_prefix(col, &self.table.name)
            })
            .collect();
        Schema { fields }
    }

    fn stream_key(&self) -> Option<Vec<usize>> {
        let id_to_op_idx = Self::get_id_to_op_idx_mapping(&self.output_col_idx, &self.table);
        self.table
            .pk
            .iter()
            .map(|&c| id_to_op_idx.get(&self.table.columns[c].column_id).copied())
            .collect::<Option<Vec<_>>>()
    }

    fn ctx(&self) -> OptimizerContextRef {
        self.ctx.clone()
    }

    fn functional_dependency(&self) -> FunctionalDependencySet {
        let pk_indices = self.stream_key();
        let col_num = self.output_col_idx.len();
        match &pk_indices {
            Some(pk_indices) => FunctionalDependencySet::with_key(col_num, pk_indices),
            None => FunctionalDependencySet::new(col_num),
        }
    }
}

impl SysScan {
    pub fn get_table_columns(&self) -> &[ColumnCatalog] {
        &self.table.columns
    }

    /// Get the descs of the output columns.
    pub fn column_descs(&self) -> Vec<ColumnDesc> {
        self.output_col_idx
            .iter()
            .map(|&i| self.get_table_columns()[i].column_desc.clone())
            .collect()
    }

    /// Helper function to create a mapping from `column_id` to `operator_idx`
    pub fn get_id_to_op_idx_mapping(
        output_col_idx: &[usize],
        table: &SystemTableCatalog,
    ) -> HashMap<ColumnId, usize> {
        let mut id_to_op_idx = HashMap::new();
        output_col_idx
            .iter()
            .enumerate()
            .for_each(|(op_idx, tb_idx)| {
                let col = &table.columns[*tb_idx];
                id_to_op_idx.insert(col.column_id, op_idx);
            });
        id_to_op_idx
    }
}
