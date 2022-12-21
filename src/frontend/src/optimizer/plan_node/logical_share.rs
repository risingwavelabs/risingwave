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

use itertools::Itertools;
use risingwave_common::error::Result;

use super::generic::{self, GenericPlanNode};
use super::{
    ColPrunable, PlanBase, PlanRef, PlanTreeNodeUnary, PredicatePushdown, ToBatch, ToStream,
};
use crate::expr::{ExprImpl, InputRef};
use crate::optimizer::plan_node::generic::GenericPlanRef;
use crate::optimizer::plan_node::{
    ColumnPruningContext, LogicalProject, PredicatePushdownContext, RewriteStreamContext,
    StreamShare, ToStreamContext,
};
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
        write!(f, "{} id = {}", name, &self.base.id.0,)
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
    fn to_stream(&self, ctx: &mut ToStreamContext) -> Result<PlanRef> {
        match ctx.get_to_stream_result(self.id()) {
            None => {
                let new_input = self.input().to_stream(ctx)?;
                let new_logical = self.clone_with_input(new_input);
                let stream_share_ref: PlanRef = StreamShare::new(new_logical).into();
                ctx.add_to_stream_result(self.id(), stream_share_ref.clone());
                Ok(stream_share_ref)
            }
            Some(cache) => Ok(cache.clone()),
        }
    }

    fn logical_rewrite_for_stream(
        &self,
        ctx: &mut RewriteStreamContext,
    ) -> Result<(PlanRef, ColIndexMapping)> {
        match ctx.get_rewrite_result(self.id()) {
            None => {
                let (new_input, col_change) = self.input().logical_rewrite_for_stream(ctx)?;
                let new_share: PlanRef = self.clone_with_input(new_input).into();

                // FIXME: Add an identity project here to avoid parent exchange connecting directly
                // to the share operator.
                let exprs = new_share
                    .schema()
                    .fields
                    .iter()
                    .enumerate()
                    .map(|(i, field)| {
                        ExprImpl::InputRef(Box::new(InputRef::new(i, field.data_type.clone())))
                    })
                    .collect_vec();
                let project = LogicalProject::create(new_share, exprs);

                ctx.add_rewrite_result(self.id(), project.clone(), col_change.clone());
                Ok((project, col_change))
            }
            Some(cache) => Ok(cache.clone()),
        }
    }
}
