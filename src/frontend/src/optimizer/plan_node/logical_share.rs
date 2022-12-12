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

use std::cell::RefCell;
use std::fmt;

use risingwave_common::error::Result;

use super::generic::{self, GenericPlanNode};
use super::{
    ColPrunableImpl, PlanBase, PlanRef, PlanTreeNodeUnary, PredicatePushdownImpl, ToBatch, ToStream,
};
use crate::optimizer::plan_node::generic::GenericPlanRef;
use crate::optimizer::plan_node::{ColumnPruningCtx, PredicatePushdownCtx, StreamShare};
use crate::utils::{ColIndexMapping, Condition};

#[derive(Debug, Clone)]
pub struct LogicalShare {
    pub base: PlanBase,
    core: generic::Share<PlanRef>,
}

impl LogicalShare {
    pub fn new(input: PlanRef) -> Self {
        let ctx = input.ctx();
        let functional_dependency = input.functional_dependency().clone();
        let core = generic::Share {
            parent_num: RefCell::new(1),
            input: RefCell::new(input),
        };
        let schema = core.schema();
        let pk_indices = core.logical_pk();
        let base = PlanBase::new_logical(
            ctx,
            schema,
            pk_indices.unwrap_or_default(),
            functional_dependency,
        );
        LogicalShare { base, core }
    }

    pub fn create(input: PlanRef) -> PlanRef {
        LogicalShare::new(input).into()
    }

    pub(super) fn fmt_with_name(&self, f: &mut fmt::Formatter<'_>, name: &str) -> fmt::Result {
        write!(f, "{} id = {}", name, &self.base.id.0)
    }
}

impl PlanTreeNodeUnary for LogicalShare {
    fn input(&self) -> PlanRef {
        self.core.input.borrow().clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self::new(input)
    }

    #[must_use]
    fn rewrite_with_input(
        &self,
        input: PlanRef,
        input_col_change: ColIndexMapping,
    ) -> (Self, ColIndexMapping) {
        (Self::new(input), input_col_change)
    }
}

impl_plan_tree_node_for_unary! {LogicalShare}

impl LogicalShare {
    pub fn replace_input(&self, plan: PlanRef) {
        *self.core.input.borrow_mut() = plan;
    }

    pub fn parent_num(&self) -> usize {
        *self.core.parent_num.borrow()
    }

    pub fn inc_parent_num(&self) {
        *self.core.parent_num.borrow_mut() += 1;
    }

    pub fn dec_parent_num(&self) {
        *self.core.parent_num.borrow_mut() -= 1;
    }
}

impl fmt::Display for LogicalShare {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.fmt_with_name(f, "LogicalShare")
    }
}

impl ColPrunableImpl for LogicalShare {
    fn prune_col_impl(&self, _required_cols: &[usize], _ctx: &mut ColumnPruningCtx) -> PlanRef {
        unimplemented!()
    }
}

impl PredicatePushdownImpl for LogicalShare {
    fn predicate_pushdown_impl(
        &self,
        _predicate: Condition,
        _ctx: &mut PredicatePushdownCtx,
    ) -> PlanRef {
        unimplemented!()
    }
}

impl ToBatch for LogicalShare {
    fn to_batch(&self) -> Result<PlanRef> {
        unimplemented!()
    }
}

impl ToStream for LogicalShare {
    fn to_stream(&self) -> Result<PlanRef> {
        let new_input = self.input().to_stream()?;
        let new_logical = self.clone_with_input(new_input);
        Ok(StreamShare::new(new_logical).into())
    }

    fn logical_rewrite_for_stream(&self) -> Result<(PlanRef, ColIndexMapping)> {
        // let (input, input_col_change) = self.input().logical_rewrite_for_stream()?;
        todo!()
    }
}
