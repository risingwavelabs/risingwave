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

use risingwave_common::catalog::{Field, Schema};
use risingwave_common::types::DataType;
use risingwave_common::util::column_index_mapping::ColIndexMapping;
use risingwave_pb::common::PbDistanceType;

use crate::OptimizerContextRef;
use crate::expr::{ExprImpl, ExprRewriter, ExprVisitor};
use crate::optimizer::plan_node::generic::{GenericPlanNode, GenericPlanRef};
use crate::optimizer::property::FunctionalDependencySet;
use crate::utils::ColIndexMappingRewriteExt;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct VectorSearch<PlanRef> {
    pub top_n: u64,
    pub distance_type: PbDistanceType,
    pub left: ExprImpl,
    pub right: ExprImpl,

    pub output_input_idx: Vec<usize>,

    pub input: PlanRef,
}

impl<PlanRef: GenericPlanRef> VectorSearch<PlanRef> {
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

impl<PlanRef: GenericPlanRef> GenericPlanNode for VectorSearch<PlanRef> {
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
