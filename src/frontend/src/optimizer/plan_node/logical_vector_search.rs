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
    BatchProject, BatchTopN, ColPrunable, ColumnPruningContext, ExprRewritable, Logical,
    LogicalProject, PlanBase, PlanTreeNodeUnary, PredicatePushdown, PredicatePushdownContext,
    RewriteStreamContext, ToBatch, ToStream, ToStreamContext, gen_filter_and_pushdown, generic,
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
                "cols_before_vector_distance",
                Pretty::Array(
                    self.core
                        .cols_before_vector_distance
                        .iter()
                        .map(Pretty::debug)
                        .collect(),
                ),
            ));
            vec.push((
                "cols_after_vector_distance",
                Pretty::Array(
                    self.core
                        .cols_after_vector_distance
                        .iter()
                        .map(Pretty::debug)
                        .collect(),
                ),
            ));
        }

        childless_record("LogicalVectorSearch", vec)
    }
}

impl ColPrunable for LogicalVectorSearch {
    fn prune_col(&self, required_cols: &[usize], ctx: &mut ColumnPruningContext) -> PlanRef {
        let input_col_num: usize = self.input().schema().len();
        let mut output_col_before_distance = vec![];
        let mut output_has_distance_col = false;
        let output_has_distance_col = &mut output_has_distance_col;
        let mut output_col_after_distance = vec![];
        for &orig_col_idx in required_cols {
            if orig_col_idx == self.core.cols_before_vector_distance.len() {
                assert!(!*output_has_distance_col);
                *output_has_distance_col = true;
            } else if *output_has_distance_col {
                output_col_after_distance.push(orig_col_idx);
            } else {
                output_col_before_distance.push(orig_col_idx);
            }
        }
        let input_required_cols = collect_input_refs(
            input_col_num,
            required_cols
                .iter()
                .filter_map(|i| self.core.non_distance_col(*i))
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
        new_core.cols_before_vector_distance = output_col_before_distance
            .iter()
            .map(|i| {
                self.core
                    .non_distance_col(*i)
                    .expect("must not be distance col")
            })
            .cloned()
            .collect();
        new_core.cols_after_vector_distance = output_col_after_distance
            .iter()
            .map(|i| {
                self.core
                    .non_distance_col(*i)
                    .expect("must not be distance col")
            })
            .cloned()
            .collect();
        let vector_search = Self::with_core(new_core);
        if *output_has_distance_col {
            vector_search.into()
        } else {
            assert!(output_col_after_distance.is_empty());
            let exprs = (0..output_col_before_distance.len())
                .map(|i| {
                    ExprImpl::InputRef(Box::new(InputRef::new(
                        i,
                        vector_search.core.cols_before_vector_distance[i].return_type(),
                    )))
                })
                .collect();
            // only keep the columns before distance column
            LogicalProject::new(vector_search.into(), exprs).into()
        }
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
        let mut exprs = self.core.cols_before_vector_distance.clone();
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
        exprs.extend(self.core.cols_after_vector_distance.iter().cloned());

        let project = generic::Project::new(exprs, input);
        let input = BatchProject::new(project).into();
        let top_n = generic::TopN::without_group(
            input,
            TopNLimit::Simple(self.core.top_n),
            0,
            Order::new(vec![ColumnOrder::new(
                self.core.cols_before_vector_distance.len(),
                OrderType::ascending(),
            )]),
        );
        Ok(BatchTopN::new(top_n).into())
    }
}
