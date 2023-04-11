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

use std::fmt;

use risingwave_common::catalog::{Field, Schema, TableVersionId};
use risingwave_common::error::Result;
use risingwave_common::types::DataType;

use super::{
    gen_filter_and_pushdown, BatchInsert, ColPrunable, ExprRewritable, PlanBase, PlanRef,
    PlanTreeNodeUnary, PredicatePushdown, ToBatch, ToStream,
};
use crate::catalog::TableId;
use crate::optimizer::plan_node::{
    ColumnPruningContext, PredicatePushdownContext, RewriteStreamContext, ToStreamContext,
};
use crate::optimizer::property::FunctionalDependencySet;
use crate::utils::{ColIndexMapping, Condition};

/// `LogicalInsert` iterates on input relation and insert the data into specified table.
///
/// It corresponds to the `INSERT` statements in SQL. Especially, for `INSERT ... VALUES`
/// statements, the input relation would be [`super::LogicalValues`].
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LogicalInsert {
    pub base: PlanBase,
    table_name: String, // explain-only
    table_id: TableId,
    table_version_id: TableVersionId,
    input: PlanRef,
    column_indices: Vec<usize>, // columns in which to insert
    row_id_index: Option<usize>,
    returning: bool,
}

impl LogicalInsert {
    /// Create a [`LogicalInsert`] node. Used internally by optimizer.
    pub fn new(
        input: PlanRef,
        table_name: String,
        table_id: TableId,
        table_version_id: TableVersionId,
        column_indices: Vec<usize>,
        row_id_index: Option<usize>,
        returning: bool,
        returning_schema: Option<Schema>,
    ) -> Self {
        let ctx = input.ctx();
        let schema = if returning {
            returning_schema.expect("returning schema should be set with returning")
        } else {
            Schema::new(vec![Field::unnamed(DataType::Int64)])
        };
        let functional_dependency = FunctionalDependencySet::new(schema.len());
        let base = PlanBase::new_logical(ctx, schema, vec![], functional_dependency);
        Self {
            base,
            table_name,
            table_id,
            table_version_id,
            input,
            column_indices,
            row_id_index,
            returning,
        }
    }

    /// Create a [`LogicalInsert`] node. Used by planner.
    pub fn create(
        input: PlanRef,
        table_name: String,
        table_id: TableId,
        table_version_id: TableVersionId,
        column_indices: Vec<usize>,
        row_id_index: Option<usize>,
        returning: bool,
        returning_schema: Option<Schema>,
    ) -> Result<Self> {
        Ok(Self::new(
            input,
            table_name,
            table_id,
            table_version_id,
            column_indices,
            row_id_index,
            returning,
            returning_schema,
        ))
    }

    pub(super) fn fmt_with_name(&self, f: &mut fmt::Formatter<'_>, name: &str) -> fmt::Result {
        write!(
            f,
            "{} {{ table: {}{} }}",
            name,
            self.table_name,
            if self.returning {
                ", returning: true"
            } else {
                ""
            }
        )
    }

    // Get the column indexes in which to insert to
    #[must_use]
    pub fn column_indices(&self) -> Vec<usize> {
        self.column_indices.clone()
    }

    #[must_use]
    pub fn table_id(&self) -> TableId {
        self.table_id
    }

    #[must_use]
    pub fn row_id_index(&self) -> Option<usize> {
        self.row_id_index
    }

    pub fn has_returning(&self) -> bool {
        self.returning
    }

    pub fn table_version_id(&self) -> TableVersionId {
        self.table_version_id
    }
}

impl PlanTreeNodeUnary for LogicalInsert {
    fn input(&self) -> PlanRef {
        self.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self::new(
            input,
            self.table_name.clone(),
            self.table_id,
            self.table_version_id,
            self.column_indices.clone(),
            self.row_id_index,
            self.returning,
            if self.returning {
                Some(self.schema().clone())
            } else {
                None
            },
        )
    }
}

impl_plan_tree_node_for_unary! {LogicalInsert}

impl fmt::Display for LogicalInsert {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.fmt_with_name(f, "LogicalInsert")
    }
}

impl ColPrunable for LogicalInsert {
    fn prune_col(&self, _required_cols: &[usize], ctx: &mut ColumnPruningContext) -> PlanRef {
        if self.has_returning() {
            self.clone().into()
        } else {
            let required_cols: Vec<_> = (0..self.input.schema().len()).collect();
            self.clone_with_input(self.input.prune_col(&required_cols, ctx))
                .into()
        }
    }
}

impl ExprRewritable for LogicalInsert {}

impl PredicatePushdown for LogicalInsert {
    fn predicate_pushdown(
        &self,
        predicate: Condition,
        ctx: &mut PredicatePushdownContext,
    ) -> PlanRef {
        gen_filter_and_pushdown(self, predicate, Condition::true_cond(), ctx)
    }
}

impl ToBatch for LogicalInsert {
    fn to_batch(&self) -> Result<PlanRef> {
        let new_input = self.input().to_batch()?;
        let new_logical = self.clone_with_input(new_input);
        Ok(BatchInsert::new(new_logical).into())
    }
}

impl ToStream for LogicalInsert {
    fn to_stream(&self, _ctx: &mut ToStreamContext) -> Result<PlanRef> {
        unreachable!("insert should always be converted to batch plan");
    }

    fn logical_rewrite_for_stream(
        &self,
        _ctx: &mut RewriteStreamContext,
    ) -> Result<(PlanRef, ColIndexMapping)> {
        unreachable!("insert should always be converted to batch plan");
    }
}
