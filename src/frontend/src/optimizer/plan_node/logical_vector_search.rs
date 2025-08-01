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
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::types::DataType;
use risingwave_common::util::column_index_mapping::ColIndexMapping;
use risingwave_common::util::sort_util::{ColumnOrder, OrderType};
use risingwave_pb::common::PbDistanceType;

use crate::expr::{
    ExprImpl, ExprRewriter, ExprType, ExprVisitor, FunctionCall, collect_input_refs,
};
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::plan_node::generic::{GenericPlanNode, GenericPlanRef, TopNLimit};
use crate::optimizer::plan_node::utils::{Distill, childless_record};
use crate::optimizer::plan_node::{
    BatchProject, BatchTopN, ColPrunable, ColumnPruningContext, ExprRewritable, Logical,
    LogicalProject, PlanBase, PlanTreeNodeUnary, PredicatePushdown, PredicatePushdownContext,
    RewriteStreamContext, ToBatch, ToStream, ToStreamContext, gen_filter_and_pushdown, generic,
};
use crate::optimizer::property::{FunctionalDependencySet, Order};
use crate::utils::{ColIndexMappingRewriteExt, Condition};
use crate::{OptimizerContextRef, PlanRef};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct VectorSearchCore {
    top_n: u64,
    distance_type: PbDistanceType,
    left: ExprImpl,
    right: ExprImpl,
    output_input_idx: Vec<usize>,
    input: PlanRef,
}

impl VectorSearchCore {
    pub(crate) fn clone_with_input(&self, input: PlanRef) -> Self {
        Self {
            top_n: self.top_n,
            distance_type: self.distance_type,
            left: self.left.clone(),
            right: self.right.clone(),
            output_input_idx: self.output_input_idx.clone(),
            input,
        }
    }

    pub(crate) fn rewrite_exprs(&mut self, r: &mut dyn ExprRewriter) {
        self.left = r.rewrite_expr(self.left.clone());
        self.right = r.rewrite_expr(self.right.clone());
    }

    pub(crate) fn visit_exprs(&self, v: &mut dyn ExprVisitor) {
        v.visit_expr(&self.left);
        v.visit_expr(&self.right);
    }

    pub(crate) fn i2o_mapping(&self) -> ColIndexMapping {
        let mut mapping = vec![None; self.input.schema().len()];
        for (output_idx, input_idx) in self.output_input_idx.iter().enumerate() {
            mapping[*input_idx] = Some(output_idx);
        }
        ColIndexMapping::new(mapping, self.output_input_idx.len() + 1)
    }
}

impl GenericPlanNode for VectorSearchCore {
    fn functional_dependency(&self) -> FunctionalDependencySet {
        self.i2o_mapping()
            .rewrite_functional_dependency_set(self.input.functional_dependency().clone())
    }

    fn schema(&self) -> Schema {
        let fields = self
            .output_input_idx
            .iter()
            .map(|idx| self.input.schema()[*idx].clone())
            .chain([Field::new("vector_distance", DataType::Float64)])
            .collect();
        Schema { fields }
    }

    fn stream_key(&self) -> Option<Vec<usize>> {
        self.input.stream_key().and_then(|v| {
            let i2o_mapping = self.i2o_mapping();
            v.iter().map(|idx| i2o_mapping.try_map(*idx)).collect()
        })
    }

    fn ctx(&self) -> OptimizerContextRef {
        self.input.ctx()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LogicalVectorSearch {
    pub base: PlanBase<Logical>,
    core: VectorSearchCore,
}

impl LogicalVectorSearch {
    pub(crate) fn new(
        top_n: u64,
        distance_type: PbDistanceType,
        left: ExprImpl,
        right: ExprImpl,
        output_input_idx: Vec<usize>,
        input: PlanRef,
    ) -> Self {
        let core = VectorSearchCore {
            top_n,
            distance_type,
            left,
            right,
            output_input_idx,
            input,
        };
        Self::with_core(core)
    }

    fn with_core(core: VectorSearchCore) -> Self {
        let base = PlanBase::new_logical_with_core(&core);
        Self { base, core }
    }

    pub(crate) fn i2o_mapping(&self) -> ColIndexMapping {
        self.core.i2o_mapping()
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
                "output_columns",
                Pretty::Array(
                    self.core
                        .output_input_idx
                        .iter()
                        .map(|input_idx| {
                            Pretty::debug(&self.core.input.schema().fields()[*input_idx])
                        })
                        .collect(),
                ),
            ));
        }

        childless_record("LogicalVectorSearch", vec)
    }
}

impl ColPrunable for LogicalVectorSearch {
    fn prune_col(&self, required_cols: &[usize], ctx: &mut ColumnPruningContext) -> PlanRef {
        let input_schema = self.core.input.schema();
        let distance_required_input_idx =
            collect_input_refs(input_schema.len(), [&self.core.left, &self.core.right]);
        let mut required_input_idx_bitset = distance_required_input_idx.clone();
        let mut non_distance_required_input_idx = Vec::new();
        let mut required_cols_new_output_mapping = vec![None; required_cols.len()];
        for (original_output_idx, input_idx) in self.core.output_input_idx.iter().enumerate() {
            if let Some(new_output_idx) =
                required_cols
                    .iter()
                    .position(|required_original_output_idx| {
                        required_original_output_idx == &original_output_idx
                    })
            {
                assert_eq!(
                    required_cols_new_output_mapping[new_output_idx]
                        .replace(non_distance_required_input_idx.len()),
                    None
                );
                non_distance_required_input_idx.push(*input_idx);
                required_input_idx_bitset.set(*input_idx, true);
            }
        }
        if let Some(distance_new_output_idx) =
            required_cols
                .iter()
                .position(|required_original_output_idx| {
                    *required_original_output_idx == self.core.output_input_idx.len()
                })
        {
            assert_eq!(
                required_cols_new_output_mapping[distance_new_output_idx]
                    .replace(non_distance_required_input_idx.len()),
                None
            );
        }
        let required_cols_new_output_idx = required_cols_new_output_mapping
            .into_iter()
            .map(Option::unwrap)
            .collect_vec();
        let input_required_idx = required_input_idx_bitset.ones().collect_vec();

        let new_input = self.input().prune_col(&input_required_idx, ctx);
        // mapping from idx of original input to new input
        let mut mapping = ColIndexMapping::with_remaining_columns(
            &input_required_idx,
            self.input().schema().len(),
        );

        let mut new_core = self.core.clone_with_input(new_input);
        new_core.left = mapping.rewrite_expr(new_core.left);
        new_core.right = mapping.rewrite_expr(new_core.right);
        new_core.output_input_idx = non_distance_required_input_idx
            .iter()
            .map(|input_idx| mapping.map(*input_idx))
            .collect();
        let vector_search = Self::with_core(new_core);
        let plan: PlanRef = vector_search.into();
        if required_cols_new_output_idx.len() == plan.schema().len()
            && required_cols_new_output_idx.is_sorted()
        {
            // the current plan output has match the required column order.
            plan
        } else {
            LogicalProject::with_out_col_idx(plan, required_cols_new_output_idx.iter().copied())
                .into()
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
        let (neg, expr_type) = match self.core.distance_type {
            PbDistanceType::Unspecified => {
                unreachable!()
            }
            PbDistanceType::L1 => (false, ExprType::L1Distance),
            PbDistanceType::L2 => (false, ExprType::L2Distance),
            PbDistanceType::Cosine => (false, ExprType::CosineDistance),
            PbDistanceType::InnerProduct => (true, ExprType::InnerProduct),
        };
        let mut expr = ExprImpl::FunctionCall(Box::new(FunctionCall::new(
            expr_type,
            vec![self.core.left.clone(), self.core.right.clone()],
        )?));
        if neg {
            expr = ExprImpl::FunctionCall(Box::new(FunctionCall::new(ExprType::Neg, vec![expr])?));
        }
        let exprs = generic::Project::out_col_idx_exprs(
            &self.core.input,
            self.core.output_input_idx.iter().copied(),
        )
        .chain([expr])
        .collect();

        let project = generic::Project::new(exprs, input);
        let input = BatchProject::new(project).into();
        let top_n = generic::TopN::without_group(
            input,
            TopNLimit::Simple(self.core.top_n),
            0,
            Order::new(vec![ColumnOrder::new(
                self.core.output_input_idx.len(),
                OrderType::ascending(),
            )]),
        );
        Ok(BatchTopN::new(top_n).into())
    }
}
