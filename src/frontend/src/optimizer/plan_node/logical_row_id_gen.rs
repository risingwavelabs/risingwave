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

use risingwave_common::bail;
use risingwave_common::error::Result;

use super::{
    ColPrunable, ExprRewritable, LogicalFilter, PlanBase, PlanRef, PlanTreeNodeUnary,
    PredicatePushdown, StreamRowIdGen, ToBatch, ToStream,
};
use crate::optimizer::plan_node::{
    ColumnPruningContext, PredicatePushdownContext, RewriteStreamContext, ToStreamContext,
};
use crate::optimizer::property::FunctionalDependencySet;
use crate::utils::{ColIndexMapping, Condition};

/// `LogicalRowIdGen` builds rows according to a list of expressions
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LogicalRowIdGen {
    pub base: PlanBase,
    input: PlanRef,
    row_id_index: usize,
}

impl LogicalRowIdGen {
    pub fn new(input: PlanRef, row_id: usize) -> Self {
        let schema = input.schema().clone();
        let functional_dependency = FunctionalDependencySet::new(schema.len());
        let base = PlanBase::new_logical(input.ctx(), schema, vec![row_id], functional_dependency);
        Self {
            base,
            input,
            row_id_index: row_id,
        }
    }

    pub fn create(input: PlanRef, row_id: usize) -> Result<Self> {
        Ok(Self::new(input, row_id))
    }

    pub fn row_id_index(&self) -> usize {
        self.row_id_index
    }
}

impl fmt::Display for LogicalRowIdGen {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "LogicalRowIdGen {{ row_id_index: {} }}",
            self.row_id_index
        )
    }
}

impl ExprRewritable for LogicalRowIdGen {}

impl PlanTreeNodeUnary for LogicalRowIdGen {
    fn input(&self) -> PlanRef {
        self.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self::new(input, self.row_id_index)
    }
}

impl_plan_tree_node_for_unary! {LogicalRowIdGen}

impl PredicatePushdown for LogicalRowIdGen {
    fn predicate_pushdown(
        &self,
        predicate: Condition,
        _ctx: &mut PredicatePushdownContext,
    ) -> PlanRef {
        LogicalFilter::create(self.clone().into(), predicate)
    }
}

impl ColPrunable for LogicalRowIdGen {
    fn prune_col(&self, _required_cols: &[usize], _ctx: &mut ColumnPruningContext) -> PlanRef {
        self.clone().into()
    }
}

impl ToBatch for LogicalRowIdGen {
    fn to_batch(&self) -> Result<PlanRef> {
        bail!("`LogicalRowIdGen` can only be converted to stream")
    }
}

impl ToStream for LogicalRowIdGen {
    fn to_stream(&self, ctx: &mut ToStreamContext) -> Result<PlanRef> {
        let stream_input = self.input().to_stream(ctx)?;
        let new_logical = self.clone_with_input(stream_input);
        Ok(StreamRowIdGen::new(new_logical).into())
    }

    fn logical_rewrite_for_stream(
        &self,
        ctx: &mut RewriteStreamContext,
    ) -> Result<(PlanRef, ColIndexMapping)> {
        let (input, input_col_change) = self.input().logical_rewrite_for_stream(ctx)?;
        Ok((self.clone_with_input(input).into(), input_col_change))
    }
}
