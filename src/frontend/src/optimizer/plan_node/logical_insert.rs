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
    gen_filter_and_pushdown, generic, BatchInsert, ColPrunable, ExprRewritable, PlanBase, PlanRef,
    PlanTreeNodeUnary, PredicatePushdown, ToBatch, ToStream,
};
use crate::catalog::TableId;
use crate::expr::ExprImpl;
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
    core: generic::Insert<PlanRef>,
}

impl LogicalInsert {
    pub fn new(core: generic::Insert<PlanRef>) -> Self {
        let ctx = core.ctx();
        let schema = if core.returning {
            core.input.schema().clone()
        } else {
            Schema::new(vec![Field::unnamed(DataType::Int64)])
        };
        let functional_dependency = FunctionalDependencySet::new(schema.len());
        let base = PlanBase::new_logical(ctx, schema, vec![], functional_dependency);
        Self { base, core }
    }

    pub(super) fn fmt_with_name(&self, f: &mut fmt::Formatter<'_>, name: &str) -> fmt::Result {
        self.core.fmt_with_name(f, name)
    }

    // Get the column indexes in which to insert to
    #[must_use]
    pub fn column_indices(&self) -> Vec<usize> {
        self.core.column_indices.clone()
    }

    #[must_use]
    pub fn default_columns(&self) -> Vec<(usize, ExprImpl)> {
        self.core.default_columns.clone()
    }

    #[must_use]
    pub fn table_id(&self) -> TableId {
        self.core.table_id
    }

    #[must_use]
    pub fn row_id_index(&self) -> Option<usize> {
        self.core.row_id_index
    }

    pub fn has_returning(&self) -> bool {
        self.core.returning
    }

    pub fn table_version_id(&self) -> TableVersionId {
        self.core.table_version_id
    }
}

impl PlanTreeNodeUnary for LogicalInsert {
    fn input(&self) -> PlanRef {
        self.core.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        let mut core = self.core.clone();
        core.input = input;
        Self::new(core)
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
        let input = &self.core.input;
        let required_cols: Vec<_> = (0..input.schema().len()).collect();
        self.clone_with_input(input.prune_col(&required_cols, ctx))
            .into()
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
        let mut logical = self.core.clone();
        logical.input = new_input;
        Ok(BatchInsert::new(logical).into())
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
