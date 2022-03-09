use risingwave_pb::plan::JoinType;

use super::super::plan_node::*;
use super::Rule;
use crate::utils::{ColIndexMapping, Condition};

/// Pushes predicates above and within a join node into the join node and/or its children nodes.
///
/// # Which predicates can be pushed
///
/// For inner join, we can do all kinds of pushdown.
///
/// For left/right semi join, we can push filter to left/right and on, and push on to left/right.
///
/// For left/right anti join, we can push left/right to left/right
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
struct FilterJoinRule {}
impl Rule for FilterJoinRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let filter = plan.as_logical_filter()?;
        let input = filter.input();
        let join = input.as_logical_join()?;

        let join_type = join.join_type();
        let left_col_num = join.left().schema().len();
        let right_col_num = join.right().schema().len();

        let mut new_filter_predicate = filter.predicate().clone();

        let (left_from_filter, right_from_filter, on) = self.push_down(
            &mut new_filter_predicate,
            left_col_num,
            right_col_num,
            self.can_push_left_from_filter(join_type),
            self.can_push_right_from_filter(join_type),
            self.can_push_on_from_filter(join_type),
        );

        let mut new_on = join.on().clone();
        if let Some(on) = on {
            new_on = new_on.and(on);
        }

        let (left_from_on, right_from_on, on) = self.push_down(
            &mut new_on,
            left_col_num,
            right_col_num,
            self.can_push_left_from_on(join_type),
            self.can_push_right_from_on(join_type),
            false,
        );
        assert!(on.is_none());

        let left_predicate = left_from_filter.and_then(|c1| left_from_on.map(|c2| c1.and(c2)));
        let right_predicate = right_from_filter.and_then(|c1| right_from_on.map(|c2| c1.and(c2)));

        let new_left: PlanRef = if let Some(predicate) = left_predicate {
            LogicalFilter::new(join.left(), predicate).into()
        } else {
            join.left()
        };
        let new_right: PlanRef = if let Some(predicate) = right_predicate {
            LogicalFilter::new(join.right(), predicate).into()
        } else {
            join.right()
        };
        let new_join = LogicalJoin::new(new_left, new_right, join_type, new_on);

        if new_filter_predicate.always_true() {
            Some(new_join.into())
        } else {
            Some(LogicalFilter::new(new_join.into(), new_filter_predicate).into())
        }
    }
}

impl FilterJoinRule {
    /// Try to split and pushdown `predicate` into a join's left/right child or the on clause.
    /// Returns the pushed predicates. Original predicate will be modified.
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
    ) -> (Option<Condition>, Option<Condition>, Option<Condition>) {
        let conjunctions = std::mem::take(&mut predicate.conjunctions);
        let (left, right, mut others) =
            Condition { conjunctions }.split(left_col_num, right_col_num);

        let mut cannot_pushed = vec![];

        let left = if push_left {
            Some(Condition { conjunctions: left })
        } else {
            cannot_pushed.extend(left);
            None
        };

        let right = if push_right {
            let cond = Condition {
                conjunctions: right,
            };
            let mut mapping = ColIndexMapping::with_shift_offset(
                left_col_num + right_col_num,
                -(left_col_num as isize),
            );
            Some(cond.rewrite_expr(&mut mapping))
        } else {
            cannot_pushed.extend(right);
            None
        };

        let on = if push_on {
            others.extend(std::mem::take(&mut cannot_pushed));
            Some(Condition {
                conjunctions: others,
            })
        } else {
            cannot_pushed.extend(others);
            None
        };

        if !cannot_pushed.is_empty() {
            predicate.conjunctions = cannot_pushed;
        }

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
}
