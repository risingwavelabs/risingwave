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

use risingwave_pb::plan_common::JoinType;

use super::super::plan_node::*;
use super::Rule;
use crate::expr::{ExprImpl, ExprType};
use crate::optimizer::rule::BoxedRule;
use crate::utils::{ColIndexMapping, Condition};

/// Pushes predicates above and within a join node into the join node and/or its children nodes.
///
/// # Which predicates can be pushed
///
/// For inner join, we can do all kinds of pushdown.
///
/// For left/right semi join, we can push filter to left/right and on-clause,
/// and push on-clause to left/right.
///
/// For left/right anti join, we can push filter to left/right, but on-clause can not be pushed
///
/// ## Outer Join
///
/// Preserved Row table
/// : The table in an Outer Join that must return all rows.
///
/// Null Supplying table
/// : This is the table that has nulls filled in for its columns in unmatched rows.
///
/// |                          | Preserved Row table | Null Supplying table |
/// |--------------------------|---------------------|----------------------|
/// | Join predicate (on)      | Not Pushed          | Pushed               |
/// | Where predicate (filter) | Pushed              | Not Pushed           |
pub struct FilterJoinRule {}

impl Rule for FilterJoinRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let (filter_predicate, join) = match plan.as_logical_filter() {
            Some(filter) => {
                let input = filter.input();
                let join = input.as_logical_join()?;
                (filter.predicate().clone(), join.clone())
            }
            // This rule also handles the case where there's no filter above the join.
            // We pushdown the predicates within the on-clause.
            None => {
                let join = plan.as_logical_join()?;
                (Condition::true_cond(), join.clone())
            }
        };

        let join_type = join.join_type();
        let left_col_num = join.left().schema().len();
        let right_col_num = join.right().schema().len();

        let mut new_filter_predicate = filter_predicate.clone();

        let join_type = self.simplify_outer(&filter_predicate, left_col_num, join_type);

        let (left_from_filter, right_from_filter, on) = self.push_down(
            &mut new_filter_predicate,
            left_col_num,
            right_col_num,
            self.can_push_left_from_filter(join_type),
            self.can_push_right_from_filter(join_type),
            self.can_push_on_from_filter(join_type),
        );

        let mut new_on = join.on().clone().and(on);

        let (left_from_on, right_from_on, on) = self.push_down(
            &mut new_on,
            left_col_num,
            right_col_num,
            self.can_push_left_from_on(join_type),
            self.can_push_right_from_on(join_type),
            false,
        );
        assert!(
            on.always_true(),
            "On-clause should not be pushed to on-clause."
        );

        let left_predicate = left_from_filter.and(left_from_on);
        let right_predicate = right_from_filter.and(right_from_on);

        let new_left = LogicalFilter::create(join.left(), left_predicate);
        let new_right = LogicalFilter::create(join.right(), right_predicate);
        let new_join = LogicalJoin::new(new_left, new_right, join_type, new_on);

        Some(LogicalFilter::create(new_join.into(), new_filter_predicate))
    }
}

impl FilterJoinRule {
    pub fn create() -> BoxedRule {
        Box::new(FilterJoinRule {})
    }

    /// Try to split and pushdown `predicate` into a join's left/right child or the on clause.
    /// Returns the pushed predicates. The pushed part will be removed from the original predicate.
    ///
    /// `InputRef`s in the right `Condition` are shifted by `-left_col_num`.
    fn push_down(
        &self,
        predicate: &mut Condition,
        left_col_num: usize,
        right_col_num: usize,
        push_left: bool,
        push_right: bool,
        push_on: bool,
    ) -> (Condition, Condition, Condition) {
        let conjunctions = std::mem::take(&mut predicate.conjunctions);
        let (mut left, right, mut others) =
            Condition { conjunctions }.split(left_col_num, right_col_num);

        let mut cannot_push = vec![];

        if !push_left {
            cannot_push.extend(left);
            left = Condition::true_cond();
        };

        let right = if push_right {
            let mut mapping = ColIndexMapping::with_shift_offset(
                left_col_num + right_col_num,
                -(left_col_num as isize),
            );
            right.rewrite_expr(&mut mapping)
        } else {
            cannot_push.extend(right);
            Condition::true_cond()
        };

        let on = if push_on {
            others.conjunctions.extend(std::mem::take(&mut cannot_push));
            others
        } else {
            cannot_push.extend(others);
            Condition::true_cond()
        };

        predicate.conjunctions = cannot_push;

        (left, right, on)
    }

    fn can_push_left_from_filter(&self, ty: JoinType) -> bool {
        matches!(
            ty,
            JoinType::Inner | JoinType::LeftOuter | JoinType::LeftSemi | JoinType::LeftAnti
        )
    }

    fn can_push_right_from_filter(&self, ty: JoinType) -> bool {
        matches!(
            ty,
            JoinType::Inner | JoinType::RightOuter | JoinType::RightSemi | JoinType::RightAnti
        )
    }

    fn can_push_on_from_filter(&self, ty: JoinType) -> bool {
        matches!(
            ty,
            JoinType::Inner | JoinType::LeftSemi | JoinType::RightSemi
        )
    }

    fn can_push_left_from_on(&self, ty: JoinType) -> bool {
        matches!(
            ty,
            JoinType::Inner | JoinType::RightOuter | JoinType::LeftSemi
        )
    }

    fn can_push_right_from_on(&self, ty: JoinType) -> bool {
        matches!(
            ty,
            JoinType::Inner | JoinType::LeftOuter | JoinType::RightSemi
        )
    }

    /// Try to simplify the outer join with the predicate on the top of the join
    ///
    /// now it is just a naive implementation for comparison expression, we can give a more general
    /// implementation with constant folding in future
    fn simplify_outer(
        &self,
        predicate: &Condition,
        left_col_num: usize,
        join_type: JoinType,
    ) -> JoinType {
        let (mut gen_null_in_left, mut gen_null_in_right) = match join_type {
            JoinType::LeftOuter => (false, true),
            JoinType::RightOuter => (true, false),
            JoinType::FullOuter => (true, true),
            _ => return join_type,
        };

        for expr in &predicate.conjunctions {
            if let ExprImpl::FunctionCall(func) = expr {
                match func.get_expr_type() {
                    ExprType::Equal
                    | ExprType::NotEqual
                    | ExprType::LessThan
                    | ExprType::LessThanOrEqual
                    | ExprType::GreaterThan
                    | ExprType::GreaterThanOrEqual => {
                        for input in func.inputs() {
                            if let ExprImpl::InputRef(input) = input {
                                let idx = input.index;
                                if idx < left_col_num {
                                    gen_null_in_left = false;
                                } else {
                                    gen_null_in_right = false;
                                }
                            }
                        }
                    }
                    _ => {}
                };
            }
        }

        match (gen_null_in_left, gen_null_in_right) {
            (true, true) => JoinType::FullOuter,
            (true, false) => JoinType::RightOuter,
            (false, true) => JoinType::LeftOuter,
            (false, false) => JoinType::Inner,
        }
    }
}

#[cfg(test)]
mod tests {
    use rand::Rng;
    use risingwave_common::types::DataType;

    use super::*;
    use crate::expr::{Expr, ExprImpl, ExprType, FunctionCall, InputRef};

    #[test]
    fn test_push_down() {
        let rule = FilterJoinRule {};

        let left_col_num = 3;
        let right_col_num = 2;

        let ty = DataType::Int32;

        let mut rng = rand::thread_rng();
        let left: ExprImpl = FunctionCall::new(
            ExprType::Equal,
            vec![
                InputRef::new(rng.gen_range(0..left_col_num), ty.clone()).into(),
                InputRef::new(rng.gen_range(0..left_col_num), ty.clone()).into(),
            ],
        )
        .unwrap()
        .into();
        let right_inputs = vec![
            InputRef::new(
                rng.gen_range(left_col_num..left_col_num + right_col_num),
                ty.clone(),
            )
            .into(),
            InputRef::new(
                rng.gen_range(left_col_num..left_col_num + right_col_num),
                ty.clone(),
            )
            .into(),
        ];
        let right: ExprImpl = FunctionCall::new(ExprType::LessThan, right_inputs.clone())
            .unwrap()
            .into();
        let right_inputs_shifted = right_inputs
            .iter()
            .map(|input| match input {
                ExprImpl::InputRef(i) => {
                    InputRef::new(i.index() - left_col_num, i.return_type()).into()
                }
                _ => panic!("Expect InputRef, got {:?}", input),
            })
            .collect();
        let right_shifted: ExprImpl = FunctionCall::new(ExprType::LessThan, right_inputs_shifted)
            .unwrap()
            .into();
        let other: ExprImpl = FunctionCall::new(
            ExprType::GreaterThan,
            vec![
                InputRef::new(rng.gen_range(0..left_col_num), ty.clone()).into(),
                InputRef::new(
                    rng.gen_range(left_col_num..left_col_num + right_col_num),
                    ty,
                )
                .into(),
            ],
        )
        .unwrap()
        .into();

        let predicate = Condition::with_expr(other.clone())
            .and(Condition::with_expr(right.clone()))
            .and(Condition::with_expr(left.clone()));

        // Only push to left
        let mut predicate_push_left = predicate.clone();
        let (left_pushed, right_pushed, on_pushed) = rule.push_down(
            &mut predicate_push_left,
            left_col_num,
            right_col_num,
            true,
            false,
            false,
        );

        assert_eq!(left_pushed.conjunctions, vec![left.clone()]);
        assert!(right_pushed.always_true());
        assert!(on_pushed.always_true());
        if predicate_push_left.conjunctions[0] != other {
            assert_eq!(
                predicate_push_left.conjunctions,
                vec![right.clone(), other.clone()]
            );
        } else {
            assert_eq!(
                predicate_push_left.conjunctions,
                vec![other.clone(), right.clone()]
            );
        }

        // Only push to right
        let mut predicate_push_right = predicate.clone();
        let (left_pushed, right_pushed, on_pushed) = rule.push_down(
            &mut predicate_push_right,
            left_col_num,
            right_col_num,
            false,
            true,
            false,
        );

        assert!(left_pushed.always_true());
        assert_eq!(right_pushed.conjunctions, vec![right_shifted.clone()]);
        assert!(on_pushed.always_true());
        if predicate_push_right.conjunctions[0] != other {
            assert_eq!(
                predicate_push_right.conjunctions,
                vec![left.clone(), other.clone()]
            );
        } else {
            assert_eq!(
                predicate_push_right.conjunctions,
                vec![other.clone(), left.clone()]
            );
        }

        // Push to left, and on
        let mut predicate_push_left_on = predicate.clone();
        let (left_pushed, right_pushed, on_pushed) = rule.push_down(
            &mut predicate_push_left_on,
            left_col_num,
            right_col_num,
            true,
            false,
            true,
        );

        assert_eq!(left_pushed.conjunctions, vec![left.clone()]);
        assert!(right_pushed.always_true());
        if on_pushed.conjunctions[0] != other {
            assert_eq!(on_pushed.conjunctions, vec![right, other.clone()]);
        } else {
            assert_eq!(on_pushed.conjunctions, vec![other.clone(), right]);
        }
        assert_eq!(predicate_push_left_on.conjunctions, vec![]);

        // Push to left, right and on
        let mut predicate_push_all = predicate;
        let (left_pushed, right_pushed, on_pushed) = rule.push_down(
            &mut predicate_push_all,
            left_col_num,
            right_col_num,
            true,
            true,
            true,
        );

        assert_eq!(left_pushed.conjunctions, vec![left]);
        assert_eq!(right_pushed.conjunctions, vec![right_shifted]);
        assert_eq!(on_pushed.conjunctions, vec![other]);
        assert_eq!(predicate_push_all.conjunctions, vec![]);
    }
}
