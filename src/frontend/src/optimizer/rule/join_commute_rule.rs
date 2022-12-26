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

use itertools::Itertools;
use risingwave_pb::plan_common::JoinType;

use super::{BoxedRule, Rule};
use crate::expr::{Expr, ExprImpl, ExprRewriter, InputRef};
use crate::optimizer::plan_node::LogicalJoin;
use crate::optimizer::PlanRef;

/// Convert right join type to left join type:
///
/// `RightOuter` => `LeftOuter`
///
/// `RightSemi` => `LeftSemi`
///
/// `RightAnti` => `LeftAnti`
pub struct JoinCommuteRule {}
impl Rule for JoinCommuteRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let join: &LogicalJoin = plan.as_logical_join()?;
        let join_type = join.join_type();
        match join_type {
            JoinType::RightOuter | JoinType::RightSemi | JoinType::RightAnti => {
                let (left, right, on, join_type, output_indices) = join.clone().decompose();

                let left_len = left.schema().len();
                let right_len = right.schema().len();

                let new_output_indices = output_indices
                    .into_iter()
                    .map(|i| {
                        if i < left_len {
                            i + right_len
                        } else {
                            i - left_len
                        }
                    })
                    .collect_vec();

                let mut condition_rewriter = Rewriter {
                    join_left_len: left_len,
                    join_left_offset: right_len as isize,
                    join_right_offset: -(left_len as isize),
                };
                let new_on = on.rewrite_expr(&mut condition_rewriter);

                let new_join = LogicalJoin::with_output_indices(
                    right,
                    left,
                    Self::inverse_join_type(join_type),
                    new_on,
                    new_output_indices,
                );

                Some(new_join.into())
            }
            JoinType::Inner
            | JoinType::LeftOuter
            | JoinType::LeftSemi
            | JoinType::LeftAnti
            | JoinType::FullOuter
            | JoinType::Unspecified => None,
        }
    }
}

struct Rewriter {
    join_left_len: usize,
    join_left_offset: isize,
    join_right_offset: isize,
}
impl ExprRewriter for Rewriter {
    fn rewrite_input_ref(&mut self, input_ref: InputRef) -> ExprImpl {
        if input_ref.index < self.join_left_len {
            InputRef::new(
                (input_ref.index() as isize + self.join_left_offset) as usize,
                input_ref.return_type(),
            )
            .into()
        } else {
            InputRef::new(
                (input_ref.index() as isize + self.join_right_offset) as usize,
                input_ref.return_type(),
            )
            .into()
        }
    }
}

impl JoinCommuteRule {
    pub fn create() -> BoxedRule {
        Box::new(JoinCommuteRule {})
    }

    fn inverse_join_type(join_type: JoinType) -> JoinType {
        match join_type {
            JoinType::Unspecified => JoinType::Unspecified,
            JoinType::Inner => JoinType::Inner,
            JoinType::LeftOuter => JoinType::RightOuter,
            JoinType::RightOuter => JoinType::LeftOuter,
            JoinType::FullOuter => JoinType::FullOuter,
            JoinType::LeftSemi => JoinType::RightSemi,
            JoinType::LeftAnti => JoinType::RightAnti,
            JoinType::RightSemi => JoinType::LeftSemi,
            JoinType::RightAnti => JoinType::LeftAnti,
        }
    }
}
