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

use std::cmp::Ordering;

use risingwave_common::catalog::{Field, Schema};
use risingwave_common::types::DataType;
use risingwave_common::util::column_index_mapping::ColIndexMapping;
use risingwave_pb::common::PbDistanceType;

use crate::OptimizerContextRef;
use crate::expr::{Expr, ExprDisplay, ExprImpl, ExprRewriter, ExprVisitor};
use crate::optimizer::plan_node::generic::{GenericPlanNode, GenericPlanRef};
use crate::optimizer::property::FunctionalDependencySet;
use crate::utils::ColIndexMappingRewriteExt;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct VectorSearch<PlanRef> {
    pub top_n: u64,
    pub left: ExprImpl,
    pub right: ExprImpl,

    pub cols_before_vector_distance: Vec<ExprImpl>,
    pub cols_after_vector_distance: Vec<ExprImpl>,

    pub input: PlanRef,
    pub distance_type: PbDistanceType,
}

impl<PlanRef: GenericPlanRef> VectorSearch<PlanRef> {
    pub(crate) fn clone_with_input(&self, input: PlanRef) -> Self {
        Self {
            top_n: self.top_n,
            left: self.left.clone(),
            right: self.right.clone(),
            cols_before_vector_distance: self.cols_before_vector_distance.clone(),
            cols_after_vector_distance: self.cols_after_vector_distance.clone(),
            input,
            distance_type: self.distance_type,
        }
    }

    pub(crate) fn rewrite_exprs(&mut self, r: &mut dyn ExprRewriter) {
        self.left = r.rewrite_expr(self.left.clone());
        self.right = r.rewrite_expr(self.right.clone());
        self.cols_before_vector_distance = self
            .cols_before_vector_distance
            .iter()
            .map(|expr| r.rewrite_expr(expr.clone()))
            .collect();
        self.cols_after_vector_distance = self
            .cols_after_vector_distance
            .iter()
            .map(|expr| r.rewrite_expr(expr.clone()))
            .collect();
    }

    pub(crate) fn visit_exprs(&self, v: &mut dyn ExprVisitor) {
        [&self.left, &self.right]
            .into_iter()
            .chain(&self.cols_before_vector_distance)
            .chain(&self.cols_after_vector_distance)
            .for_each(|expr| {
                v.visit_expr(expr);
            });
    }

    pub fn o2i_col_mapping(&self) -> ColIndexMapping {
        let input_len = self.input.schema().len();
        let mut map = vec![
            None;
            self.cols_before_vector_distance.len()
                + self.cols_after_vector_distance.len()
        ];
        for (i, expr) in self.cols_before_vector_distance.iter().enumerate().chain(
            self.cols_after_vector_distance
                .iter()
                .enumerate()
                .map(|(idx, expr)| (idx + 1, expr)),
        ) {
            if let ExprImpl::InputRef(input) = expr {
                map[i] = Some(input.index())
            }
        }
        ColIndexMapping::new(map, input_len)
    }

    /// get the Mapping of columnIndex from input column index to output column index,if a input
    /// column corresponds more than one out columns, mapping to any one
    pub fn i2o_col_mapping(&self) -> ColIndexMapping {
        let input_len = self.input.schema().len();
        let mut map = vec![None; input_len];
        for (i, expr) in self.cols_before_vector_distance.iter().enumerate().chain(
            self.cols_after_vector_distance
                .iter()
                .enumerate()
                .map(|(idx, expr)| (idx + 1, expr)),
        ) {
            if let ExprImpl::InputRef(input) = expr {
                map[input.index()] = Some(i)
            }
        }
        ColIndexMapping::new(
            map,
            self.cols_before_vector_distance.len() + self.cols_after_vector_distance.len() + 1,
        )
    }

    pub fn non_distance_col(&self, idx: usize) -> Option<&ExprImpl> {
        match idx.cmp(&self.cols_before_vector_distance.len()) {
            Ordering::Less => Some(&self.cols_before_vector_distance[idx]),
            Ordering::Equal => None,
            Ordering::Greater => Some(
                &self.cols_after_vector_distance[idx - 1 - self.cols_after_vector_distance.len()],
            ),
        }
    }
}

impl<PlanRef: GenericPlanRef> GenericPlanNode for VectorSearch<PlanRef> {
    fn functional_dependency(&self) -> FunctionalDependencySet {
        let i2o = self.i2o_col_mapping();
        i2o.rewrite_functional_dependency_set(self.input.functional_dependency().clone())
    }

    fn schema(&self) -> Schema {
        let o2i = self.o2i_col_mapping();
        let input_schema = self.input.schema();
        let ctx = self.ctx();
        let to_field = |(i, expr): (usize, &ExprImpl)| {
            // Get field info from o2i.
            let name = match o2i.try_map(i) {
                Some(input_idx) => input_schema.fields()[input_idx].name.clone(),
                None => match expr {
                    ExprImpl::InputRef(_) | ExprImpl::Literal(_) => {
                        format!("{:?}", ExprDisplay { expr, input_schema })
                    }
                    _ => {
                        format!("$expr{}", ctx.next_expr_display_id())
                    }
                },
            };
            Field::with_name(expr.return_type(), name)
        };
        let fields = self
            .cols_before_vector_distance
            .iter()
            .enumerate()
            .map(to_field)
            .chain([Field::new("vector_distance", DataType::Float64)])
            .chain(
                self.cols_after_vector_distance
                    .iter()
                    .enumerate()
                    .map(|(idx, expr)| to_field((idx + 1, expr))),
            )
            .collect();
        Schema { fields }
    }

    fn stream_key(&self) -> Option<Vec<usize>> {
        let i2o = self.i2o_col_mapping();
        self.input
            .stream_key()?
            .iter()
            .map(|pk_col| i2o.try_map(*pk_col))
            .collect::<Option<Vec<_>>>()
    }

    fn ctx(&self) -> OptimizerContextRef {
        self.input.ctx()
    }
}
