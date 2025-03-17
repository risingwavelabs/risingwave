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

use pretty_xmlish::XmlNode;
use risingwave_common::catalog::TableVersionId;

use super::generic::GenericPlanRef;
use super::utils::{Distill, childless_record};
use super::{
    BatchInsert, ColPrunable, ExprRewritable, Logical, LogicalProject, PlanBase, PlanRef,
    PlanTreeNodeUnary, PredicatePushdown, ToBatch, ToStream, gen_filter_and_pushdown, generic,
};
use crate::catalog::TableId;
use crate::error::Result;
use crate::expr::{ExprImpl, ExprRewriter, ExprVisitor};
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::plan_node::{
    ColumnPruningContext, PredicatePushdownContext, RewriteStreamContext, ToStreamContext,
};
use crate::utils::{ColIndexMapping, Condition};

/// `LogicalInsert` iterates on input relation and insert the data into specified table.
///
/// It corresponds to the `INSERT` statements in SQL. Especially, for `INSERT ... VALUES`
/// statements, the input relation would be [`super::LogicalValues`].
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LogicalInsert {
    pub base: PlanBase<Logical>,
    core: generic::Insert<PlanRef>,
}

impl LogicalInsert {
    pub fn new(core: generic::Insert<PlanRef>) -> Self {
        let base = PlanBase::new_logical_with_core(&core);
        Self { base, core }
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

impl Distill for LogicalInsert {
    fn distill<'a>(&self) -> XmlNode<'a> {
        let vec = self
            .core
            .fields_pretty(self.base.ctx().is_explain_verbose());
        childless_record("LogicalInsert", vec)
    }
}

impl ColPrunable for LogicalInsert {
    fn prune_col(&self, required_cols: &[usize], ctx: &mut ColumnPruningContext) -> PlanRef {
        let pruned_input = {
            let input = &self.core.input;
            let required_cols: Vec<_> = (0..input.schema().len()).collect();
            input.prune_col(&required_cols, ctx)
        };

        // No pruning.
        LogicalProject::with_out_col_idx(
            self.clone_with_input(pruned_input).into(),
            required_cols.iter().copied(),
        )
        .into()
    }
}

impl ExprRewritable for LogicalInsert {
    fn has_rewritable_expr(&self) -> bool {
        true
    }

    fn rewrite_exprs(&self, r: &mut dyn ExprRewriter) -> PlanRef {
        let mut new = self.clone();
        new.core.default_columns = new
            .core
            .default_columns
            .into_iter()
            .map(|(c, e)| (c, r.rewrite_expr(e)))
            .collect();
        new.into()
    }
}

impl ExprVisitable for LogicalInsert {
    fn visit_exprs(&self, v: &mut dyn ExprVisitor) {
        self.core
            .default_columns
            .iter()
            .for_each(|(_, e)| v.visit_expr(e));
    }
}

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
        let mut core = self.core.clone();
        core.input = new_input;
        Ok(BatchInsert::new(core).into())
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
