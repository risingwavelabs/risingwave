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

use std::{fmt, vec};

use risingwave_common::catalog::{Field, Schema, TableVersionId};
use risingwave_common::error::Result;
use risingwave_common::types::DataType;

use super::{
    gen_filter_and_pushdown, BatchUpdate, ColPrunable, ExprRewritable, PlanBase, PlanRef,
    PlanTreeNodeUnary, PredicatePushdown, ToBatch, ToStream,
};
use crate::catalog::TableId;
use crate::expr::{ExprImpl, ExprRewriter};
use crate::optimizer::plan_node::{
    ColumnPruningContext, PredicatePushdownContext, RewriteStreamContext, ToStreamContext,
};
use crate::optimizer::property::FunctionalDependencySet;
use crate::utils::{ColIndexMapping, Condition};

/// [`LogicalUpdate`] iterates on input relation, set some columns, and inject update records into
/// specified table.
///
/// It corresponds to the `UPDATE` statements in SQL.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LogicalUpdate {
    pub base: PlanBase,
    table_name: String, // explain-only
    table_id: TableId,
    table_version_id: TableVersionId,
    input: PlanRef,
    exprs: Vec<ExprImpl>,
    returning: bool,
}

impl LogicalUpdate {
    /// Create a [`LogicalUpdate`] node. Used internally by optimizer.
    pub fn new(
        input: PlanRef,
        table_name: String,
        table_id: TableId,
        table_version_id: TableVersionId,
        exprs: Vec<ExprImpl>,
        returning: bool,
    ) -> Self {
        let ctx = input.ctx();
        let schema = if returning {
            input.schema().clone()
        } else {
            Schema::new(vec![Field::unnamed(DataType::Int64)])
        };
        let fd_set = FunctionalDependencySet::new(schema.len());
        let base = PlanBase::new_logical(ctx, schema, vec![], fd_set);
        Self {
            base,
            table_name,
            table_id,
            table_version_id,
            input,
            exprs,
            returning,
        }
    }

    /// Create a [`LogicalUpdate`] node. Used by planner.
    pub fn create(
        input: PlanRef,
        table_name: String,
        table_id: TableId,
        table_version_id: TableVersionId,
        exprs: Vec<ExprImpl>,
        returning: bool,
    ) -> Result<Self> {
        Ok(Self::new(
            input,
            table_name,
            table_id,
            table_version_id,
            exprs,
            returning,
        ))
    }

    pub(super) fn fmt_with_name(&self, f: &mut fmt::Formatter<'_>, name: &str) -> fmt::Result {
        write!(
            f,
            "{} {{ table: {}, exprs: {:?}{} }}",
            name,
            self.table_name,
            self.exprs,
            if self.returning {
                ", returning: true"
            } else {
                ""
            }
        )
    }

    #[must_use]
    pub fn table_id(&self) -> TableId {
        self.table_id
    }

    pub fn exprs(&self) -> &[ExprImpl] {
        self.exprs.as_ref()
    }

    pub fn has_returning(&self) -> bool {
        self.returning
    }

    pub fn table_version_id(&self) -> TableVersionId {
        self.table_version_id
    }
}

impl PlanTreeNodeUnary for LogicalUpdate {
    fn input(&self) -> PlanRef {
        self.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self::new(
            input,
            self.table_name.clone(),
            self.table_id,
            self.table_version_id,
            self.exprs.clone(),
            self.returning,
        )
    }
}

impl_plan_tree_node_for_unary! { LogicalUpdate }

impl fmt::Display for LogicalUpdate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.fmt_with_name(f, "LogicalUpdate")
    }
}

impl ExprRewritable for LogicalUpdate {
    fn has_rewritable_expr(&self) -> bool {
        true
    }

    fn rewrite_exprs(&self, r: &mut dyn ExprRewriter) -> PlanRef {
        let mut new = self.clone();
        new.exprs = new.exprs.into_iter().map(|e| r.rewrite_expr(e)).collect();
        new.base = new.base.clone_with_new_plan_id();
        new.into()
    }
}

impl ColPrunable for LogicalUpdate {
    fn prune_col(&self, _required_cols: &[usize], ctx: &mut ColumnPruningContext) -> PlanRef {
        let required_cols: Vec<_> = (0..self.input.schema().len()).collect();
        self.clone_with_input(self.input.prune_col(&required_cols, ctx))
            .into()
    }
}

impl PredicatePushdown for LogicalUpdate {
    fn predicate_pushdown(
        &self,
        predicate: Condition,
        ctx: &mut PredicatePushdownContext,
    ) -> PlanRef {
        gen_filter_and_pushdown(self, predicate, Condition::true_cond(), ctx)
    }
}

impl ToBatch for LogicalUpdate {
    fn to_batch(&self) -> Result<PlanRef> {
        let new_input = self.input().to_batch()?;
        let new_logical = self.clone_with_input(new_input);
        Ok(BatchUpdate::new(new_logical).into())
    }
}

impl ToStream for LogicalUpdate {
    fn to_stream(&self, _ctx: &mut ToStreamContext) -> Result<PlanRef> {
        unreachable!("update should always be converted to batch plan");
    }

    fn logical_rewrite_for_stream(
        &self,
        _ctx: &mut RewriteStreamContext,
    ) -> Result<(PlanRef, ColIndexMapping)> {
        unreachable!("update should always be converted to batch plan");
    }
}
