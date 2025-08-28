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

use crate::OptimizerContextRef;
use crate::expr::{
    ExprImpl, ExprRewriter, ExprType, ExprVisitor, FunctionCall, collect_input_refs,
};
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::plan_node::generic::{GenericPlanNode, GenericPlanRef, TopNLimit};
use crate::optimizer::plan_node::utils::{Distill, childless_record};
use crate::optimizer::plan_node::{
    BatchPlanRef, BatchProject, BatchTopN, ColPrunable, ColumnPruningContext, ExprRewritable,
    Logical, LogicalPlanRef as PlanRef, LogicalProject, PlanBase, PlanTreeNodeUnary,
    PredicatePushdown, PredicatePushdownContext, RewriteStreamContext, StreamPlanRef, ToBatch,
    ToStream, ToStreamContext, gen_filter_and_pushdown, generic,
};
use crate::optimizer::property::{FunctionalDependencySet, Order};
use crate::utils::{ColIndexMappingRewriteExt, Condition};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct VectorSearchCore {
    top_n: u64,
    distance_type: PbDistanceType,
    left: ExprImpl,
    right: ExprImpl,
    /// The indices of input that will be included in the output.
    /// The index of distance column is `output_indices.len()`
    output_indices: Vec<usize>,
    input: PlanRef,
}

impl VectorSearchCore {
    pub(crate) fn clone_with_input(&self, input: PlanRef) -> Self {
        Self {
            top_n: self.top_n,
            distance_type: self.distance_type,
            left: self.left.clone(),
            right: self.right.clone(),
            output_indices: self.output_indices.clone(),
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
        for (output_idx, input_idx) in self.output_indices.iter().enumerate() {
            mapping[*input_idx] = Some(output_idx);
        }
        ColIndexMapping::new(mapping, self.output_indices.len() + 1)
    }
}

impl GenericPlanNode for VectorSearchCore {
    fn functional_dependency(&self) -> FunctionalDependencySet {
        self.i2o_mapping()
            .rewrite_functional_dependency_set(self.input.functional_dependency().clone())
    }

    fn schema(&self) -> Schema {
        let fields = self
            .output_indices
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
        output_indices: Vec<usize>,
        input: PlanRef,
    ) -> Self {
        let core = VectorSearchCore {
            top_n,
            distance_type,
            left,
            right,
            output_indices,
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

impl_plan_tree_node_for_unary! { Logical, LogicalVectorSearch }

impl PlanTreeNodeUnary<Logical> for LogicalVectorSearch {
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
                        .output_indices
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
        let mut required_input_idx_bitset =
            collect_input_refs(input_schema.len(), [&self.core.left, &self.core.right]);
        let mut non_distance_required_input_idx = Vec::new();
        let mut distance_col_idx_in_required_cols = None;
        for (new_output_idx, &required_col_idx) in required_cols.iter().enumerate() {
            if required_col_idx == self.core.output_indices.len() {
                distance_col_idx_in_required_cols = Some(new_output_idx);
            } else {
                let required_input_idx = self.core.output_indices[required_col_idx];
                non_distance_required_input_idx.push(required_input_idx);
                required_input_idx_bitset.set(required_col_idx, true);
            }
        }
        let input_required_idx = required_input_idx_bitset.ones().collect_vec();

        let new_input = self.input().prune_col(&input_required_idx, ctx);
        // mapping from idx of original input to new input
        let mut mapping = ColIndexMapping::with_remaining_columns(
            &input_required_idx,
            self.input().schema().len(),
        );

        let vector_search = {
            let mut new_core = self.core.clone_with_input(new_input);
            new_core.left = mapping.rewrite_expr(new_core.left);
            new_core.right = mapping.rewrite_expr(new_core.right);
            new_core.output_indices = non_distance_required_input_idx
                .iter()
                .map(|input_idx| mapping.map(*input_idx))
                .collect();
            Self::with_core(new_core)
        };
        if let Some(distance_col_idx_in_required_cols) = distance_col_idx_in_required_cols {
            assert_eq!(required_cols.len(), vector_search.schema().len());
            // distance column is at the end of vector search
            let distance_col_idx_in_vector_search = vector_search.schema().len() - 1;
            // recompose output col idx by inserting distance column in the middle
            let output_col_idx = (0..distance_col_idx_in_required_cols)
                .chain([distance_col_idx_in_vector_search])
                .chain(distance_col_idx_in_required_cols..distance_col_idx_in_vector_search);
            LogicalProject::with_out_col_idx(vector_search.into(), output_col_idx).into()
        } else {
            LogicalProject::with_out_col_idx(
                vector_search.into(),
                0..non_distance_required_input_idx.len(),
            )
            .into()
        }
    }
}

impl ExprRewritable<Logical> for LogicalVectorSearch {
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

    fn to_stream(&self, _ctx: &mut ToStreamContext) -> crate::error::Result<StreamPlanRef> {
        bail!("LogicalVectorSearch can only for batch plan, not stream plan");
    }
}

impl ToBatch for LogicalVectorSearch {
    fn to_batch(&self) -> crate::error::Result<BatchPlanRef> {
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
            self.core.output_indices.iter().copied(),
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
                self.core.output_indices.len(),
                OrderType::ascending(),
            )]),
        );
        Ok(BatchTopN::new(top_n).into())
    }
}
