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

use risingwave_common::error::{ErrorCode, Result};
use risingwave_common::util::column_index_mapping::ColIndexMapping;

use super::generic::Sort;
use super::{
    gen_filter_and_pushdown, ColPrunable, ColumnPruningContext, ExprRewritable, PlanBase, PlanRef,
    PlanTreeNodeUnary, PredicatePushdown, PredicatePushdownContext, RewriteStreamContext, ToBatch,
    ToStream, ToStreamContext,
};
use crate::utils::Condition;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LogicalSort {
    pub base: PlanBase,
    core: Sort<PlanRef>,
}

impl LogicalSort {
    pub fn new(input: PlanRef, sort_column_index: usize) -> Self {
        let core = Sort::new(input, sort_column_index);
        let base = PlanBase::new_logical_with_core(&core);
        Self { base, core }
    }
}

impl PlanTreeNodeUnary for LogicalSort {
    fn input(&self) -> PlanRef {
        self.core.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self::new(input, self.core.sort_column_index)
    }
}

impl_plan_tree_node_for_unary! { LogicalSort }

impl fmt::Display for LogicalSort {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.core.fmt_with_name(f, "LogicalSort")
    }
}

impl ColPrunable for LogicalSort {
    fn prune_col(&self, required_cols: &[usize], ctx: &mut ColumnPruningContext) -> PlanRef {
        let mapping =
            ColIndexMapping::with_remaining_columns(required_cols, self.input().schema().len());
        let new_sort_column_index = mapping.map(self.core.sort_column_index);
        let new_input = self.input().prune_col(required_cols, ctx);
        Self::new(new_input, new_sort_column_index).into()
    }
}

impl ExprRewritable for LogicalSort {}

impl PredicatePushdown for LogicalSort {
    fn predicate_pushdown(
        &self,
        predicate: Condition,
        ctx: &mut PredicatePushdownContext,
    ) -> PlanRef {
        gen_filter_and_pushdown(self, Condition::true_cond(), predicate, ctx)
    }
}

impl ToBatch for LogicalSort {
    fn to_batch(&self) -> Result<PlanRef> {
        Err(ErrorCode::NotImplemented("LogicalSort to batch".to_string(), None.into()).into())
    }
}

impl ToStream for LogicalSort {
    fn logical_rewrite_for_stream(
        &self,
        ctx: &mut RewriteStreamContext,
    ) -> Result<(PlanRef, ColIndexMapping)> {
        // TODO()
        todo!()
    }

    fn to_stream(&self, ctx: &mut ToStreamContext) -> Result<PlanRef> {
        // TODO()
        todo!()
    }
}
