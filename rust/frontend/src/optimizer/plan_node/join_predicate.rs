use crate::expr::{to_conjunctions, Expr, ExprImpl, ExprType, FunctionCall, InputRef};
use crate::utils::Condition;
#[derive(Debug, Clone)]
/// the join predicate used in optimizer
pub struct JoinPredicate {
    /// the conditions that all columns in the left side,
    left_cond: Condition,
    /// the conditions that all columns in the right side,
    right_cond: Condition,
    /// other conditions, linked with AND conjunction.
    other_cond: Condition,

    /// the equal columns indexes(in the input schema) both sides,
    /// the first is from the left table and the second is from the right table.
    /// now all are normal equal(not null-safe-equal),
    eq_keys: Vec<(InputRef, InputRef)>,
}
#[allow(dead_code)]
impl JoinPredicate {
    /// the new method for `JoinPredicate` without any analysis, check or rewrite.
    pub fn new(
        left_cond: Condition,
        right_cond: Condition,
        other_cond: Condition,
        eq_keys: Vec<(InputRef, InputRef)>,
    ) -> Self {
        Self {
            left_cond,
            right_cond,
            other_cond,
            eq_keys,
        }
    }

    /// Construct a empty predicate. Condition always true -- do not filter rows.
    pub fn true_predicate() -> Self {
        JoinPredicate {
            left_cond: Condition::true_cond(),
            right_cond: Condition::true_cond(),
            other_cond: Condition::true_cond(),
            eq_keys: vec![],
        }
    }

    /// `create` will analyze the on clause condition and construct a `JoinPredicate`.
    /// e.g.
    /// ```sql
    ///   select a.v1, a.v2, b.v1, b.v2 from a,b on a.v1 = a.v2 and a.v1 = b.v1 and a.v2 > b.v2
    /// ```
    /// will call the `create` function with left_colsnum = 2 and on_clause is (supposed input_ref
    /// count start from 0)
    /// ```sql
    /// input_ref(0) = input_ref(1) and input_ref(0) = input_ref(2) and input_ref(1) > input_ref(3)
    /// ```
    /// And the `create funcitons` should return `JoinPredicate`
    /// ```sql
    ///   other_conds = Vec[input_ref(1) = input_ref(1), input_ref(1) > input_ref(3)],
    ///   keys= Vec[(1,1)]
    /// ```
    #[allow(unused_variables)]
    pub fn create(left_cols_num: usize, on_clause: ExprImpl) -> Self {
        let conds = to_conjunctions(on_clause);
        for cond in conds {}
        todo!()
    }

    /// Get join predicate's eq conds.
    pub fn eq_conds(&self) -> Vec<ExprImpl> {
        self.eq_keys
            .iter()
            .cloned()
            .map(|(l, r)| {
                FunctionCall::new(ExprType::Equal, vec![l.bound_expr(), r.bound_expr()])
                    .unwrap()
                    .bound_expr()
            })
            .collect()
    }

    /// Get a reference to the join predicate's left cond.
    pub fn left_cond(&self) -> &Condition {
        &self.left_cond
    }

    /// Get a reference to the join predicate's right cond.
    pub fn right_cond(&self) -> &Condition {
        &self.right_cond
    }

    /// Get a reference to the join predicate's other cond.
    pub fn other_cond(&self) -> &Condition {
        &self.other_cond
    }
    /// Get a reference to the join predicate's eq keys.
    pub fn eq_keys(&self) -> &[(InputRef, InputRef)] {
        self.eq_keys.as_ref()
    }
    pub fn eq_indexes(&self) -> Vec<(usize, usize)> {
        self.eq_keys
            .iter()
            .map(|(left, right)| (left.index(), right.index()))
            .collect()
    }

    pub fn left_eq_indexes(&self) -> Vec<usize> {
        self.eq_keys.iter().map(|(left, _)| left.index()).collect()
    }
    pub fn right_eq_indexes(&self) -> Vec<usize> {
        self.eq_keys
            .iter()
            .map(|(_, right)| right.index())
            .collect()
    }
}
