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

use risingwave_common::catalog::Schema;
use risingwave_pb::plan_common::JoinType;

use super::{EqJoinPredicate, GenericPlanNode, GenericPlanRef};
use crate::expr::ExprRewriter;
use crate::optimizer::optimizer_context::OptimizerContextRef;
use crate::utils::{ColIndexMapping, Condition};

/// [`Join`] combines two relations according to some condition.
///
/// Each output row has fields from the left and right inputs. The set of output rows is a subset
/// of the cartesian product of the two inputs; precisely which subset depends on the join
/// condition. In addition, the output columns are a subset of the columns of the left and
/// right columns, dependent on the output indices provided. A repeat output index is illegal.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Join<PlanRef> {
    pub left: PlanRef,
    pub right: PlanRef,
    pub on: Condition,
    pub join_type: JoinType,
    pub output_indices: Vec<usize>,
}

impl<PlanRef> Join<PlanRef> {
    pub(crate) fn rewrite_exprs(&mut self, r: &mut dyn ExprRewriter) {
        self.on = self.on.clone().rewrite_expr(r);
    }
}

impl<PlanRef: GenericPlanRef> GenericPlanNode for Join<PlanRef> {
    fn schema(&self) -> Schema {
        let left_schema = self.left.schema();
        let right_schema = self.right.schema();
        let i2l = self.i2l_col_mapping();
        let i2r = self.i2r_col_mapping();
        let fields = self
            .output_indices
            .iter()
            .map(|&i| match (i2l.try_map(i), i2r.try_map(i)) {
                (Some(l_i), None) => left_schema.fields()[l_i].clone(),
                (None, Some(r_i)) => right_schema.fields()[r_i].clone(),
                _ => panic!(
                    "left len {}, right len {}, i {}, lmap {:?}, rmap {:?}",
                    left_schema.len(),
                    right_schema.len(),
                    i,
                    i2l,
                    i2r
                ),
            })
            .collect();
        Schema { fields }
    }

    fn logical_pk(&self) -> Option<Vec<usize>> {
        let _left_len = self.left.schema().len();
        let _right_len = self.right.schema().len();
        let left_pk = self.left.logical_pk();
        let right_pk = self.right.logical_pk();
        let l2i = self.l2i_col_mapping();
        let r2i = self.r2i_col_mapping();
        let full_out_col_num = self.internal_column_num();
        let i2o = ColIndexMapping::with_remaining_columns(&self.output_indices, full_out_col_num);

        let pk_indices = left_pk
            .iter()
            .map(|index| l2i.try_map(*index))
            .chain(right_pk.iter().map(|index| r2i.try_map(*index)))
            .flatten()
            .map(|index| i2o.try_map(index))
            .collect::<Option<Vec<_>>>();

        // NOTE(st1page): add join keys in the pk_indices a work around before we really have stream
        // key.
        pk_indices.and_then(|mut pk_indices| {
            let left_len = self.left.schema().len();
            let right_len = self.right.schema().len();
            let eq_predicate = EqJoinPredicate::create(left_len, right_len, self.on.clone());

            let l2i = self.l2i_col_mapping();
            let r2i = self.r2i_col_mapping();
            let full_out_col_num = self.internal_column_num();
            let i2o =
                ColIndexMapping::with_remaining_columns(&self.output_indices, full_out_col_num);

            for (lk, rk) in eq_predicate.eq_indexes() {
                if let Some(lk) = l2i.try_map(lk) {
                    if let Some(out_k) = i2o.try_map(lk) {
                        if pk_indices.contains(&out_k) {
                            continue;
                        }
                    }
                }
                if let Some(rk) = r2i.try_map(rk) {
                    if let Some(out_k) = i2o.try_map(rk) {
                        if pk_indices.contains(&out_k) {
                            continue;
                        }
                    }
                }
                // Just add the left join key is enough, as it is equal with the right one.
                if let Some(lk) = l2i.try_map(lk) {
                    let out_k = i2o.try_map(lk)?;
                    if !pk_indices.contains(&out_k) {
                        pk_indices.push(out_k);
                    }
                }
            }
            Some(pk_indices)
        })
    }

    fn ctx(&self) -> OptimizerContextRef {
        self.left.ctx()
    }
}

impl<PlanRef> Join<PlanRef> {
    pub fn decompose(self) -> (PlanRef, PlanRef, Condition, JoinType, Vec<usize>) {
        (
            self.left,
            self.right,
            self.on,
            self.join_type,
            self.output_indices,
        )
    }

    pub fn full_out_col_num(left_len: usize, right_len: usize, join_type: JoinType) -> usize {
        match join_type {
            JoinType::Inner | JoinType::LeftOuter | JoinType::RightOuter | JoinType::FullOuter => {
                left_len + right_len
            }
            JoinType::LeftSemi | JoinType::LeftAnti => left_len,
            JoinType::RightSemi | JoinType::RightAnti => right_len,
            JoinType::Unspecified => unreachable!(),
        }
    }
}

impl<PlanRef: GenericPlanRef> Join<PlanRef> {
    pub fn with_full_output(
        left: PlanRef,
        right: PlanRef,
        join_type: JoinType,
        on: Condition,
    ) -> Self {
        let out_column_num =
            Self::full_out_col_num(left.schema().len(), right.schema().len(), join_type);
        Self {
            left,
            right,
            join_type,
            on,
            output_indices: (0..out_column_num).collect(),
        }
    }

    pub fn internal_column_num(&self) -> usize {
        Self::full_out_col_num(
            self.left.schema().len(),
            self.right.schema().len(),
            self.join_type,
        )
    }

    /// Get the Mapping of columnIndex from internal column index to left column index.
    pub fn i2l_col_mapping(&self) -> ColIndexMapping {
        let left_len = self.left.schema().len();
        let right_len = self.right.schema().len();

        match self.join_type {
            JoinType::Inner | JoinType::LeftOuter | JoinType::RightOuter | JoinType::FullOuter => {
                ColIndexMapping::identity_or_none(left_len + right_len, left_len)
            }

            JoinType::LeftSemi | JoinType::LeftAnti => ColIndexMapping::identity(left_len),
            JoinType::RightSemi | JoinType::RightAnti => {
                ColIndexMapping::empty(right_len, left_len)
            }
            JoinType::Unspecified => unreachable!(),
        }
    }

    /// Get the Mapping of columnIndex from internal column index to right column index.
    pub fn i2r_col_mapping(&self) -> ColIndexMapping {
        let left_len = self.left.schema().len();
        let right_len = self.right.schema().len();

        match self.join_type {
            JoinType::Inner | JoinType::LeftOuter | JoinType::RightOuter | JoinType::FullOuter => {
                ColIndexMapping::with_shift_offset(left_len + right_len, -(left_len as isize))
            }
            JoinType::LeftSemi | JoinType::LeftAnti => ColIndexMapping::empty(left_len, right_len),
            JoinType::RightSemi | JoinType::RightAnti => ColIndexMapping::identity(right_len),
            JoinType::Unspecified => unreachable!(),
        }
    }

    /// Get the Mapping of columnIndex from left column index to internal column index.
    pub fn l2i_col_mapping(&self) -> ColIndexMapping {
        self.i2l_col_mapping().inverse()
    }

    /// Get the Mapping of columnIndex from right column index to internal column index.
    pub fn r2i_col_mapping(&self) -> ColIndexMapping {
        self.i2r_col_mapping().inverse()
    }
}
