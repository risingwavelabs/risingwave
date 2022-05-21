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

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::rc::Rc;

use itertools::Itertools;
use risingwave_common::catalog::{ColumnDesc, Schema, TableDesc};
use risingwave_common::error::Result;

use super::{
    ColPrunable, LogicalFilter, PlanBase, PlanRef, PredicatePushdown, StreamTableScan, ToBatch,
    ToStream,
};
use crate::optimizer::plan_node::BatchSeqScan;
use crate::session::OptimizerContextRef;
use crate::utils::{ColIndexMapping, Condition};

/// `LogicalScan` returns contents of a table or other equivalent object
#[derive(Debug, Clone)]
pub struct LogicalScan {
    pub base: PlanBase,
    table_name: String, // explain-only
    required_col_idx: Vec<usize>,
    // Descriptor of the table
    table_desc: Rc<TableDesc>,
    // Descriptors of all indexes on this table
    indexes: Vec<(String, Rc<TableDesc>)>,
}

impl LogicalScan {
    /// Create a `LogicalScan` node. Used internally by optimizer.
    pub fn new(
        table_name: String,           // explain-only
        required_col_idx: Vec<usize>, // the column index in the table
        table_desc: Rc<TableDesc>,
        indexes: Vec<(String, Rc<TableDesc>)>,
        ctx: OptimizerContextRef,
    ) -> Self {
        // here we have 3 concepts
        // 1. column_id: ColumnId, stored in catalog and a ID to access data from storage.
        // 2. table_idx: usize, column index in the TableDesc or tableCatalog.
        // 3. operator_idx: usize,  column index in the ScanOperator's schema.
        // in a query we get the same version of catalog, so the mapping from column_id and
        // table_idx will not changes. and the `required_col_idx is the `table_idx` of the
        // required columns, in other word, is the mapping from operator_idx to table_idx.

        let mut id_to_op_idx = HashMap::new();

        let fields = required_col_idx
            .iter()
            .enumerate()
            .map(|(op_idx, tb_idx)| {
                let col = &table_desc.columns[*tb_idx];
                id_to_op_idx.insert(col.column_id, op_idx);
                col.into()
            })
            .collect();

        let pk_indices = table_desc
            .pks
            .iter()
            .map(|&c| id_to_op_idx.get(&table_desc.columns[c].column_id).copied())
            .collect::<Option<Vec<_>>>()
            .unwrap_or_default();

        let schema = Schema { fields };
        let base = PlanBase::new_logical(ctx, schema, pk_indices);
        Self {
            base,
            table_name,
            required_col_idx,
            table_desc,
            indexes,
        }
    }

    /// Create a [`LogicalScan`] node. Used by planner.
    pub fn create(
        table_name: String, // explain-only
        table_desc: Rc<TableDesc>,
        indexes: Vec<(String, Rc<TableDesc>)>,
        ctx: OptimizerContextRef,
    ) -> Result<PlanRef> {
        Ok(Self::new(
            table_name,
            (0..table_desc.columns.len()).into_iter().collect(),
            table_desc,
            indexes,
            ctx,
        )
        .into())
    }

    pub(super) fn column_names(&self) -> Vec<String> {
        self.required_col_idx
            .iter()
            .map(|i| self.table_desc.columns[*i].name.clone())
            .collect()
    }

    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    /// Get a reference to the logical scan's table desc.
    #[must_use]
    pub fn table_desc(&self) -> &TableDesc {
        self.table_desc.as_ref()
    }

    /// Get a reference to the logical scan's table desc. The columns are not exactly the same as
    /// the original table as being pruned, but will have the same order as the required input.
    #[must_use]
    pub fn column_descs(&self) -> Vec<ColumnDesc> {
        self.required_col_idx
            .iter()
            .map(|i| self.table_desc.columns[*i].clone())
            .collect()
    }

    /// Get all indexes on this table
    #[must_use]
    pub fn indexes(&self) -> &[(String, Rc<TableDesc>)] {
        &self.indexes
    }

    /// distribution keys stored in catalog only contains column index of the table (`table_idx`),
    /// so we need to convert it to `operator_idx` when filling distributions.
    pub fn map_distribution_keys(&self) -> Vec<usize> {
        let tb_idx_to_op_idx = self
            .required_col_idx
            .iter()
            .enumerate()
            .map(|(op_idx, tb_idx)| (*tb_idx, op_idx))
            .collect::<HashMap<_, _>>();
        self.table_desc
            .distribution_keys
            .iter()
            .map(|&tb_idx| tb_idx_to_op_idx[&tb_idx])
            .collect()
    }

    pub fn to_index_scan(&self, index_name: &str, index: &Rc<TableDesc>) -> LogicalScan {
        let mut new_required_col_idx = Vec::with_capacity(self.required_col_idx.len());
        let all_columns = index
            .columns
            .iter()
            .enumerate()
            .map(|(idx, desc)| (desc.column_id, idx))
            .collect::<HashMap<_, _>>();

        // create index scan plan to match the output order of the current table scan
        for &col_idx in &self.required_col_idx {
            let column_idx_in_index = all_columns[&self.table_desc.columns[col_idx].column_id];
            new_required_col_idx.push(column_idx_in_index);
        }

        Self::new(
            index_name.to_string(),
            new_required_col_idx,
            index.clone(),
            vec![],
            self.ctx(),
        )
    }
}

impl_plan_tree_node_for_leaf! {LogicalScan}

impl fmt::Display for LogicalScan {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "LogicalScan {{ table: {}, columns: [{}] }}",
            self.table_name,
            self.column_names().join(", ")
        )
    }
}

impl ColPrunable for LogicalScan {
    fn prune_col(&self, required_cols: &[usize]) -> PlanRef {
        let required_col_idx = required_cols
            .iter()
            .map(|i| self.required_col_idx[*i])
            .collect();

        Self::new(
            self.table_name.clone(),
            required_col_idx,
            self.table_desc.clone(),
            self.indexes.clone(),
            self.base.ctx.clone(),
        )
        .into()
    }
}

impl PredicatePushdown for LogicalScan {
    fn predicate_pushdown(&self, predicate: Condition) -> PlanRef {
        LogicalFilter::create(self.clone().into(), predicate)
    }
}

impl ToBatch for LogicalScan {
    fn to_batch(&self) -> Result<PlanRef> {
        Ok(BatchSeqScan::new(self.clone()).into())
    }
}

impl ToStream for LogicalScan {
    fn to_stream(&self) -> Result<PlanRef> {
        Ok(StreamTableScan::new(self.clone()).into())
    }

    fn logical_rewrite_for_stream(&self) -> Result<(PlanRef, ColIndexMapping)> {
        match self.base.pk_indices.is_empty() {
            true => {
                let mut col_ids = HashSet::new();

                for idx in &self.required_col_idx {
                    col_ids.insert(self.table_desc.columns[*idx].column_id);
                }
                let mut col_id_to_tb_idx = HashMap::new();
                for (tb_idx, c) in self.table_desc().columns.iter().enumerate() {
                    col_id_to_tb_idx.insert(c.column_id, tb_idx);
                }
                let col_need_to_add = self
                    .table_desc
                    .order_desc
                    .iter()
                    .filter(|c| !col_ids.contains(&c.column_desc.column_id))
                    .map(|c| col_id_to_tb_idx.get(&c.column_desc.column_id).unwrap())
                    .collect_vec();

                let mut required_col_idx = self.required_col_idx.clone();
                required_col_idx.extend(col_need_to_add);
                let new_len = required_col_idx.len();
                Ok((
                    Self::new(
                        self.table_name.clone(),
                        required_col_idx,
                        self.table_desc.clone(),
                        self.indexes.clone(),
                        self.base.ctx.clone(),
                    )
                    .into(),
                    ColIndexMapping::identity_or_none(self.schema().len(), new_len),
                ))
            }
            false => Ok((
                self.clone().into(),
                ColIndexMapping::identity(self.schema().len()),
            )),
        }
    }
}
