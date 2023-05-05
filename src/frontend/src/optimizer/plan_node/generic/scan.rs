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

use std::collections::HashMap;
use std::rc::Rc;

use educe::Educe;
use risingwave_common::catalog::{ColumnDesc, Field, Schema, TableDesc};

use super::GenericPlanNode;
use crate::catalog::{ColumnId, IndexCatalog};
use crate::expr::ExprRewriter;
use crate::optimizer::optimizer_context::OptimizerContextRef;
use crate::optimizer::property::FunctionalDependencySet;
use crate::utils::Condition;

/// [`Scan`] returns contents of a table or other equivalent object
#[derive(Debug, Clone, Derivative)]
#[educe(PartialEq, Eq, Hash)]
pub struct Scan {
    pub table_name: String,
    pub is_sys_table: bool,
    /// Include `output_col_idx` and columns required in `predicate`
    pub required_col_idx: Vec<usize>,
    pub output_col_idx: Vec<usize>,
    // Descriptor of the table
    pub table_desc: Rc<TableDesc>,
    // Descriptors of all indexes on this table
    pub indexes: Vec<Rc<IndexCatalog>>,
    /// The pushed down predicates. It refers to column indexes of the table.
    pub predicate: Condition,
    /// Help RowSeqScan executor use a better chunk size
    pub chunk_size: Option<u32>,
    /// syntax `FOR SYSTEM_TIME AS OF PROCTIME()` is used for temporal join.
    pub for_system_time_as_of_proctime: bool,
    #[educe(PartialEq = "ignore")]
    #[educe(Hash = "ignore")]
    pub ctx: OptimizerContextRef,
}

impl Scan {
    pub(crate) fn rewrite_exprs(&mut self, r: &mut dyn ExprRewriter) {
        self.predicate = self.predicate.clone().rewrite_expr(r);
    }
}

impl GenericPlanNode for Scan {
    fn schema(&self) -> Schema {
        let fields = self
            .output_col_idx
            .iter()
            .map(|tb_idx| {
                let col = &self.table_desc.columns[*tb_idx];
                Field::from_with_table_name_prefix(col, &self.table_name)
            })
            .collect();
        Schema { fields }
    }

    fn logical_pk(&self) -> Option<Vec<usize>> {
        let id_to_op_idx = Self::get_id_to_op_idx_mapping(&self.output_col_idx, &self.table_desc);
        self.table_desc
            .stream_key
            .iter()
            .map(|&c| {
                id_to_op_idx
                    .get(&self.table_desc.columns[c].column_id)
                    .copied()
            })
            .collect::<Option<Vec<_>>>()
    }

    fn ctx(&self) -> OptimizerContextRef {
        self.ctx.clone()
    }

    fn functional_dependency(&self) -> FunctionalDependencySet {
        let pk_indices = self.logical_pk();
        let col_num = self.output_col_idx.len();
        match &pk_indices {
            Some(pk_indices) => FunctionalDependencySet::with_key(col_num, pk_indices),
            None => FunctionalDependencySet::new(col_num),
        }
    }
}

impl Scan {
    /// Get the descs of the output columns.
    pub fn column_descs(&self) -> Vec<ColumnDesc> {
        self.output_col_idx
            .iter()
            .map(|&i| self.table_desc.columns[i].clone())
            .collect()
    }

    /// Helper function to create a mapping from `column_id` to `operator_idx`
    pub fn get_id_to_op_idx_mapping(
        output_col_idx: &[usize],
        table_desc: &Rc<TableDesc>,
    ) -> HashMap<ColumnId, usize> {
        let mut id_to_op_idx = HashMap::new();
        output_col_idx
            .iter()
            .enumerate()
            .for_each(|(op_idx, tb_idx)| {
                let col = &table_desc.columns[*tb_idx];
                id_to_op_idx.insert(col.column_id, op_idx);
            });
        id_to_op_idx
    }
}
