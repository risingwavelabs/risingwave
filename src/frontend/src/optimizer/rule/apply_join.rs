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
use risingwave_common::types::DataType;
use risingwave_common::types::DataType::Boolean;
use risingwave_pb::plan_common::JoinType;

use super::{BoxedRule, Rule};
use crate::expr::{
    CorrelatedId, CorrelatedInputRef, Expr, ExprImpl, ExprRewriter, ExprType, FunctionCall,
    InputRef,
};
use crate::optimizer::plan_node::{LogicalApply, LogicalJoin, LogicalProject, PlanTreeNodeBinary};
use crate::optimizer::PlanRef;
use crate::utils::{ColIndexMapping, Condition};

/// Push `LogicalJoin` down `LogicalApply`
pub struct ApplyJoinRule {}
impl Rule for ApplyJoinRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let apply: &LogicalApply = plan.as_logical_apply()?;
        let (left, right, on, join_type, correlated_id, correlated_indices) =
            apply.clone().decompose();
        assert_eq!(join_type, JoinType::Inner);
        let join: &LogicalJoin = right.as_logical_join()?;

        let join_left_len = join.left().schema().len();
        let join_left_offset = left.schema().len();
        let join_right_offset = 2 * left.schema().len();
        let mut rewriter = Rewriter {
            join_left_len,
            join_left_offset,
            join_right_offset,
            index_mapping: ColIndexMapping::new(
                correlated_indices
                    .clone()
                    .into_iter()
                    .map(Some)
                    .collect_vec(),
            )
            .inverse(),
            has_correlated_input_ref: false,
            correlated_id,
        };

        // rewrite join on condition and add natural join condition
        let natural_conjunctions = left
            .schema()
            .fields
            .iter()
            .enumerate()
            .map(|(i, field)| {
                Self::create_equal_expr(
                    i,
                    field.data_type.clone(),
                    i + join_left_len + join_left_offset,
                    field.data_type.clone(),
                )
            })
            .collect_vec();
        let new_join_condition = Condition {
            conjunctions: join
                .on()
                .clone()
                .into_iter()
                .map(|expr| rewriter.rewrite_expr(expr))
                .chain(natural_conjunctions.into_iter())
                .collect_vec(),
        };
        let new_join_left = LogicalApply::create(
            left.clone(),
            join.left().clone(),
            join_type,
            on.clone(),
            correlated_id,
            correlated_indices.clone(),
        );
        let new_join_right = LogicalApply::create(
            left.clone(),
            join.right().clone(),
            join_type,
            on,
            correlated_id,
            correlated_indices,
        );
        let new_join = LogicalJoin::new(
            new_join_left.clone(),
            new_join_right.clone(),
            join.join_type(),
            new_join_condition,
        );

        match join.join_type() {
            JoinType::LeftSemi | JoinType::LeftAnti | JoinType::RightSemi | JoinType::RightAnti => {
                Some(new_join.into())
            }
            JoinType::Inner | JoinType::LeftOuter | JoinType::RightOuter | JoinType::FullOuter => {
                // use to provide a natural join
                let mut project_exprs: Vec<ExprImpl> = vec![];
                project_exprs.extend(
                    new_join_left
                        .schema()
                        .fields
                        .iter()
                        .enumerate()
                        .map(|(i, field)| {
                            ExprImpl::InputRef(Box::new(InputRef::new(i, field.data_type.clone())))
                        })
                        .collect_vec(),
                );
                project_exprs.extend(
                    new_join_right
                        .schema()
                        .fields
                        .iter()
                        .enumerate()
                        .skip(join_left_offset)
                        .map(|(i, field)| {
                            ExprImpl::InputRef(Box::new(InputRef::new(
                                i + new_join_left.schema().fields.len(),
                                field.data_type.clone(),
                            )))
                        })
                        .collect_vec(),
                );

                let new_project = LogicalProject::new(new_join.into(), project_exprs);
                Some(new_project.into())
            }
        }
    }
}

impl ApplyJoinRule {
    fn create_equal_expr(
        left: usize,
        left_data_type: DataType,
        right: usize,
        right_data_type: DataType,
    ) -> ExprImpl {
        ExprImpl::FunctionCall(Box::new(FunctionCall::new_unchecked(
            ExprType::Equal,
            vec![
                ExprImpl::InputRef(Box::new(InputRef::new(left, left_data_type))),
                ExprImpl::InputRef(Box::new(InputRef::new(right, right_data_type))),
            ],
            Boolean,
        )))
    }
}

impl ApplyJoinRule {
    pub fn create() -> BoxedRule {
        Box::new(ApplyJoinRule {})
    }
}

/// Convert `CorrelatedInputRef` to `InputRef` and shift `InputRef` with offset.
struct Rewriter {
    join_left_len: usize,
    join_left_offset: usize,
    join_right_offset: usize,
    index_mapping: ColIndexMapping,
    has_correlated_input_ref: bool,
    correlated_id: CorrelatedId,
}
impl ExprRewriter for Rewriter {
    fn rewrite_correlated_input_ref(
        &mut self,
        correlated_input_ref: CorrelatedInputRef,
    ) -> ExprImpl {
        let found = correlated_input_ref.get_correlated_id() == self.correlated_id;
        self.has_correlated_input_ref |= found;
        if found {
            InputRef::new(
                self.index_mapping.map(correlated_input_ref.index()),
                correlated_input_ref.return_type(),
            )
            .into()
        } else {
            correlated_input_ref.into()
        }
    }

    fn rewrite_input_ref(&mut self, input_ref: InputRef) -> ExprImpl {
        if input_ref.index < self.join_left_len {
            InputRef::new(
                input_ref.index() + self.join_left_offset,
                input_ref.return_type(),
            )
            .into()
        } else {
            InputRef::new(
                input_ref.index() + self.join_right_offset,
                input_ref.return_type(),
            )
            .into()
        }
    }
}
