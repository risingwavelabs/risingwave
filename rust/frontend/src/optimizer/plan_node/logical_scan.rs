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
use std::fmt;

use std::rc::Rc;

use fixedbitset::FixedBitSet;
use risingwave_common::catalog::{ColumnDesc, Field, Schema, TableDesc};
use risingwave_common::error::Result;

use super::{ColPrunable, PlanBase, PlanRef, StreamTableScan, ToBatch, ToStream};
use crate::catalog::{ColumnId};
use crate::optimizer::plan_node::BatchSeqScan;
use crate::optimizer::property::WithSchema;
use crate::session::QueryContextRef;
use crate::utils::ColIndexMapping;

/// `LogicalScan` returns contents of a table or other equivalent object
#[derive(Debug, Clone)]
pub struct LogicalScan {
    pub base: PlanBase,
    table_name: String, // explain-only
    required_col_idx: Vec<usize>,
    table_desc: Rc<TableDesc>,
}

impl LogicalScan {
    /// Create a LogicalScan node. Used internally by optimizer.
    pub fn new(
        table_name: String,           // explain-only
        required_col_idx: Vec<usize>, // the column index in the table
        table_desc: Rc<TableDesc>,
        ctx: QueryContextRef,
    ) -> Self {
        let (fields, col_ids): (Vec<Field>, Vec<ColumnId>) = required_col_idx
            .iter()
            .map(|idx| {
                let col = &table_desc.columns[*idx];
                (col.into(), col.column_id)
            })
            .unzip();
        let id_to_idx: HashMap<_, _> =
            HashMap::from_iter(col_ids.into_iter().enumerate().map(|(idx, id)| (id, idx)));
        let pk_indices = table_desc
            .pk
            .iter()
            .map(|c| id_to_idx.get(&c.column_desc.column_id).copied())
            .collect::<Option<Vec<_>>>()
            .unwrap_or_default();
        let schema = Schema { fields };
        let base = PlanBase::new_logical(ctx, schema, pk_indices);
        Self {
            base,
            table_name,
            required_col_idx,
            table_desc,
        }
    }

    /// Create a LogicalScan node. Used by planner.
    pub fn create(
        table_name: String, // explain-only
        table_desc: Rc<TableDesc>,
        ctx: QueryContextRef,
    ) -> Result<PlanRef> {
        Ok(Self::new(
            table_name,
            (0..table_desc.columns.len()).into_iter().collect(),
            table_desc,
            ctx,
        )
        .into())
    }

    pub(super) fn column_names(&self) -> Vec<String> {
        self.schema()
            .fields()
            .iter()
            .map(|f| f.name.clone())
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

    /// Get a reference to the logical scan's table desc.
    #[must_use]
    pub fn column_descs(&self) -> Vec<ColumnDesc> {
        self.required_col_idx
            .iter()
            .map(|i| self.table_desc.columns[*i].clone())
            .collect()
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
    fn prune_col(&self, required_cols: &FixedBitSet) -> PlanRef {
        self.must_contain_columns(required_cols);
        let required_col_idx = required_cols
            .ones()
            .map(|i| self.required_col_idx[i])
            .collect();

        Self::new(
            self.table_name.clone(),
            required_col_idx,
            self.table_desc.clone(),
            self.base.ctx.clone(),
        )
        .into()
    }
}

impl ToBatch for LogicalScan {
    fn to_batch(&self) -> PlanRef {
        BatchSeqScan::new(self.clone()).into()
    }
}

impl ToStream for LogicalScan {
    fn to_stream(&self) -> PlanRef {
        StreamTableScan::new(self.clone()).into()
    }

    fn logical_rewrite_for_stream(&self) -> (PlanRef, ColIndexMapping) {
        // TODO: add pk here
        (
            self.clone().into(),
            ColIndexMapping::identical_map(self.schema().len()),
        )
    }
}
