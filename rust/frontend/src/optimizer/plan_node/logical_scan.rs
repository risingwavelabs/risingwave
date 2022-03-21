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
//
use std::fmt;

use fixedbitset::FixedBitSet;
use risingwave_common::catalog::Schema;
use risingwave_common::error::Result;

use super::{ColPrunable, PlanBase, PlanRef, StreamTableScan, ToBatch, ToStream};
use crate::catalog::{ColumnId, TableId};
use crate::optimizer::plan_node::BatchSeqScan;
use crate::optimizer::property::WithSchema;
use crate::session::QueryContextRef;

/// `LogicalScan` returns contents of a table or other equivalent object
#[derive(Debug, Clone)]
pub struct LogicalScan {
    pub base: PlanBase,
    table_name: String,
    table_id: TableId,
    columns: Vec<ColumnId>,
}

impl LogicalScan {
    /// Create a LogicalScan node. Used internally by optimizer.
    pub fn new(
        table_name: String,
        table_id: TableId,
        columns: Vec<ColumnId>,
        schema: Schema,
        ctx: QueryContextRef,
    ) -> Self {
        // TODO: check pk
        let base = PlanBase::new_logical(ctx, schema, vec![]);
        Self {
            table_name,
            table_id,
            columns,
            base,
        }
    }

    /// Create a LogicalScan node. Used by planner.
    pub fn create(
        table_name: String,
        table_id: TableId,
        columns: Vec<ColumnId>,
        schema: Schema,
        ctx: QueryContextRef,
    ) -> Result<PlanRef> {
        Ok(Self::new(table_name, table_id, columns, schema, ctx).into())
    }

    pub(super) fn column_names(&self) -> Vec<String> {
        self.schema()
            .fields()
            .iter()
            .map(|f| f.name.clone())
            .collect()
    }

    pub fn table_id(&self) -> u32 {
        self.table_id.table_id
    }

    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    pub fn columns(&self) -> &[ColumnId] {
        &self.columns
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

        let (columns, fields) = required_cols
            .ones()
            .map(|id| (self.columns[id], self.schema().fields[id].clone()))
            .unzip();

        Self::new(
            self.table_name.clone(),
            self.table_id,
            columns,
            Schema { fields },
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
}
