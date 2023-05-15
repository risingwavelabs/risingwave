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
    gen_filter_and_pushdown, generic, BatchUpdate, ColPrunable, ExprRewritable, PlanBase, PlanRef,
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
    core: generic::Update<PlanRef>,
}

impl From<generic::Update<PlanRef>> for LogicalUpdate {
    fn from(core: generic::Update<PlanRef>) -> Self {
        let ctx = core.input.ctx();
        let schema = if core.returning {
            core.input.schema().clone()
        } else {
            Schema::new(vec![Field::unnamed(DataType::Int64)])
        };
        let fd_set = FunctionalDependencySet::new(schema.len());
        let base = PlanBase::new_logical(ctx, schema, vec![], fd_set);
        Self { base, core }
    }
}

impl LogicalUpdate {
    #[must_use]
    pub fn table_id(&self) -> TableId {
        self.core.table_id
    }

    pub fn exprs(&self) -> &[ExprImpl] {
        self.core.exprs.as_ref()
    }

    pub fn has_returning(&self) -> bool {
        self.core.returning
    }

    pub fn table_version_id(&self) -> TableVersionId {
        self.core.table_version_id
    }
}

impl PlanTreeNodeUnary for LogicalUpdate {
    fn input(&self) -> PlanRef {
        self.core.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        let mut core = self.core.clone();
        core.input = input;
        core.into()
    }
}

impl_plan_tree_node_for_unary! { LogicalUpdate }

impl fmt::Display for LogicalUpdate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.core.fmt_with_name(f, "LogicalUpdate")
    }
}

impl ExprRewritable for LogicalUpdate {
    fn has_rewritable_expr(&self) -> bool {
        true
    }

    fn rewrite_exprs(&self, r: &mut dyn ExprRewriter) -> PlanRef {
        let mut new = self.core.clone();
        new.exprs = new.exprs.into_iter().map(|e| r.rewrite_expr(e)).collect();
        Self::from(new).into()
    }
}

impl ColPrunable for LogicalUpdate {
    fn prune_col(&self, _required_cols: &[usize], ctx: &mut ColumnPruningContext) -> PlanRef {
        let input = self.input();
        let required_cols: Vec<_> = (0..input.schema().len()).collect();
        self.clone_with_input(input.prune_col(&required_cols, ctx))
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
        let mut new_logical = self.core.clone();
        new_logical.input = new_input;
        Ok(BatchUpdate::new(new_logical, self.schema().clone()).into())
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
