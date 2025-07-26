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

use itertools::Itertools;
use pretty_xmlish::{Pretty, XmlNode};
use risingwave_common::bail;
use risingwave_common::util::column_index_mapping::ColIndexMapping;
use risingwave_common::util::sort_util::{ColumnOrder, OrderType};
use risingwave_pb::common::PbDistanceType;

use crate::PlanRef;
use crate::expr::{
    Expr, ExprImpl, ExprRewriter, ExprType, ExprVisitor, FunctionCall, InputRef, collect_input_refs,
};
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::plan_node::generic::{GenericPlanRef, TopNLimit, VectorSearch};
use crate::optimizer::plan_node::utils::{Distill, childless_record};
use crate::optimizer::plan_node::{
    BatchProject, BatchTopN, ColPrunable, ColumnPruningContext, ExprRewritable, Logical, PlanBase,
    PlanTreeNodeUnary, PredicatePushdown, PredicatePushdownContext, RewriteStreamContext, ToBatch,
    ToStream, ToStreamContext, gen_filter_and_pushdown, generic,
};
use crate::optimizer::property::Order;
use crate::utils::Condition;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LogicalVectorSearch {
    pub base: PlanBase<Logical>,
    core: VectorSearch<PlanRef>,
}

impl LogicalVectorSearch {
    pub(crate) fn with_core(core: VectorSearch<PlanRef>) -> Self {
        let base = PlanBase::new_logical_with_core(&core);
        Self { base, core }
    }
}

impl_plan_tree_node_for_unary! { LogicalVectorSearch }

impl PlanTreeNodeUnary for LogicalVectorSearch {
    fn input(&self) -> PlanRef {
        self.core.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        let core = self.core.clone_with_input(input);
        Self::with_core(core)
    }
}

impl Distill for LogicalVectorSearch {
    fn distill<'a>(&self) -> XmlNode<'a> {
        let verbose = self.base.ctx().is_explain_verbose();
        let mut vec = Vec::with_capacity(if verbose { 4 } else { 6 });
        vec.push(("distance_type", Pretty::debug(&self.core.distance_type)));
        vec.push(("top_n", Pretty::debug(&self.core.top_n)));
        vec.push(("left", Pretty::debug(&self.core.left)));
        vec.push(("right", Pretty::debug(&self.core.right)));

        if verbose {
            vec.push((
                "non_distance_columns",
                Pretty::Array(
                    self.core
                        .non_distance_columns
                        .iter()
                        .map(Pretty::debug)
                        .collect(),
                ),
            ));
            vec.push((
                "non_distance_columns",
                Pretty::debug(&self.core.include_distance),
            ));
        }

        childless_record("LogicalVectorSearch", vec)
    }
}

impl ColPrunable for LogicalVectorSearch {
    fn prune_col(&self, required_cols: &[usize], ctx: &mut ColumnPruningContext) -> PlanRef {
        let input_col_num: usize = self.input().schema().len();
        let mut output_non_distance_col = vec![];
        let mut output_has_distance_col = false;
        let output_has_distance_col = &mut output_has_distance_col;
        for &orig_col_idx in required_cols {
            if orig_col_idx == self.core.non_distance_columns.len() {
                assert!(!*output_has_distance_col);
                *output_has_distance_col = true;
            } else {
                output_non_distance_col.push(orig_col_idx);
            }
        }
        let input_required_cols = collect_input_refs(
            input_col_num,
            required_cols
                .iter()
                .filter_map(|i| self.core.non_distance_columns.get(*i))
                .chain([&self.core.left, &self.core.right]),
        )
        .ones()
        .collect_vec();
        let new_input = self.input().prune_col(&input_required_cols, ctx);
        let mut mapping = ColIndexMapping::with_remaining_columns(
            &input_required_cols,
            self.input().schema().len(),
        );

        let mut new_core = self.core.clone_with_input(new_input);
        new_core.left = mapping.rewrite_expr(new_core.left);
        new_core.right = mapping.rewrite_expr(new_core.right);
        new_core.non_distance_columns = output_non_distance_col
            .iter()
            .map(|i| mapping.rewrite_expr(self.core.non_distance_columns[*i].clone()))
            .collect();
        new_core.include_distance = *output_has_distance_col;
        let vector_search = Self::with_core(new_core);
        vector_search.into()
    }
}

impl ExprRewritable for LogicalVectorSearch {
    fn has_rewritable_expr(&self) -> bool {
        true
    }

    fn rewrite_exprs(&self, r: &mut dyn ExprRewriter) -> PlanRef {
        let mut core = self.core.clone();
        core.rewrite_exprs(r);
        Self::with_core(core).into()
    }
}

impl ExprVisitable for LogicalVectorSearch {
    fn visit_exprs(&self, v: &mut dyn ExprVisitor) {
        self.core.visit_exprs(v);
    }
}

impl PredicatePushdown for LogicalVectorSearch {
    fn predicate_pushdown(
        &self,
        predicate: Condition,
        ctx: &mut PredicatePushdownContext,
    ) -> PlanRef {
        gen_filter_and_pushdown(self, predicate, Condition::true_cond(), ctx)
    }
}

impl ToStream for LogicalVectorSearch {
    fn logical_rewrite_for_stream(
        &self,
        _ctx: &mut RewriteStreamContext,
    ) -> crate::error::Result<(PlanRef, ColIndexMapping)> {
        bail!("LogicalVectorSearch can only for batch plan, not stream plan");
    }

    fn to_stream(&self, _ctx: &mut ToStreamContext) -> crate::error::Result<PlanRef> {
        bail!("LogicalVectorSearch can only for batch plan, not stream plan");
    }
}

impl ToBatch for LogicalVectorSearch {
    fn to_batch(&self) -> crate::error::Result<PlanRef> {
        let input = self.input().to_batch()?;
        let mut exprs = self.core.non_distance_columns.clone();
        let expr_type = match self.core.distance_type {
            PbDistanceType::Unspecified => {
                unreachable!()
            }
            PbDistanceType::L2 => ExprType::L2Distance,
            PbDistanceType::L1 | PbDistanceType::Cosine | PbDistanceType::InnerProduct => {
                todo!("VECTOR_PLACEHOLDER")
            }
        };
        exprs.push(ExprImpl::FunctionCall(Box::new(FunctionCall::new(
            expr_type,
            vec![self.core.left.clone(), self.core.right.clone()],
        )?)));

        let project = generic::Project::new(exprs, input);
        let input = BatchProject::new(project).into();
        let top_n = generic::TopN::without_group(
            input,
            TopNLimit::Simple(self.core.top_n),
            0,
            Order::new(vec![ColumnOrder::new(
                self.core.non_distance_columns.len(),
                OrderType::ascending(),
            )]),
        );
        let mut plan = BatchTopN::new(top_n).into();
        if !self.core.include_distance {
            plan = BatchProject::new(generic::Project::new(
                (0..self.core.non_distance_columns.len())
                    .map(|idx| {
                        ExprImpl::InputRef(
                            InputRef {
                                index: idx,
                                data_type: self.core.non_distance_columns[idx].return_type(),
                            }
                            .into(),
                        )
                    })
                    .collect(),
                plan,
            ))
            .into();
        }
        Ok(plan)
    }
}
