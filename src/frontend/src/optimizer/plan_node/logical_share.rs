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
    ColPrunable, PlanBase, PlanRef, PlanTreeNodeUnary, PredicatePushdown, ToBatch, ToStream,
};
use crate::optimizer::plan_node::generic::GenericPlanRef;
use crate::optimizer::plan_node::{
    ColumnPruningContext, PredicatePushdownContext, RewriteStreamContext, StreamShare,
};
use crate::utils::{ColIndexMapping, Condition};

#[derive(Debug, Clone)]
pub struct LogicalShare {
    pub base: PlanBase,
    core: generic::Share<PlanRef>,
}

impl LogicalShare {
    pub fn new(input: PlanRef) -> Self {
        Self::new_with_parent_num(input, 1)
    }

    pub fn new_with_parent_num(input: PlanRef, parent_num: usize) -> Self {
        let ctx = input.ctx();
        let functional_dependency = input.functional_dependency().clone();
        let core = generic::Share {
            parent_num: RefCell::new(parent_num),
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
        write!(
            f,
            "{} id = {} parent_num = {}",
            name,
            &self.base.id.0,
            self.parent_num()
        )
    }
}

impl PlanTreeNodeUnary for LogicalShare {
    fn input(&self) -> PlanRef {
        self.core.input.borrow().clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self::new_with_parent_num(input, self.parent_num())
    }

    #[must_use]
    fn rewrite_with_input(
        &self,
        input: PlanRef,
        input_col_change: ColIndexMapping,
    ) -> (Self, ColIndexMapping) {
        (
            Self::new_with_parent_num(input, self.parent_num()),
            input_col_change,
        )
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

impl ColPrunable for LogicalShare {
    fn prune_col(&self, _required_cols: &[usize], _ctx: &mut ColumnPruningContext) -> PlanRef {
        unimplemented!("call prune_col of the PlanRef instead of calling directly on LogicalShare")
    }
}

impl PredicatePushdown for LogicalShare {
    fn predicate_pushdown(
        &self,
        _predicate: Condition,
        _ctx: &mut PredicatePushdownContext,
    ) -> PlanRef {
        unimplemented!(
            "call predicate_pushdown of the PlanRef instead of calling directly on LogicalShare"
        )
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

    fn logical_rewrite_for_stream(
        &self,
        ctx: &mut RewriteStreamContext,
    ) -> Result<(PlanRef, ColIndexMapping)> {
        match ctx.get_rewrite_result(self.base.id) {
            None => {
                let (new_input, col_change) = self.input().logical_rewrite_for_stream(ctx)?;
                let new_share: PlanRef = self.clone_with_input(new_input).into();
                ctx.add_rewrite_result(self.base.id, new_share.clone(), col_change.clone());
                Ok((new_share, col_change))
            }
            Some(cache) => Ok(cache.clone()),
        }
    }
}
