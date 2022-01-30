use crate::expr::{get_col_refs, to_conjunctions, ExprImpl};
#[derive(Debug, Clone)]
/// the join predicate used in optimizer
pub struct JoinPredicate {
    /// other conditions, linked with AND conjunction.
    /// 1. the non equal conditons
    /// 2. the conditions in the same side,
    other_conds: Vec<ExprImpl>,
    /// the equal columns indexes(in the input schema) both sides, now all are normal equal(not
    /// null-safe-equal),
    keys: Vec<(usize, usize)>,
}
#[allow(dead_code)]
impl JoinPredicate {
    /// the new method for `JoinPredicate` without any analysis, check or rewrite.
    pub fn new(other_conds: Vec<ExprImpl>, keys: Vec<(usize, usize)>) -> Self {
        JoinPredicate { other_conds, keys }
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
        for cond in conds {
            let cols = get_col_refs(&cond);
        }
        todo!()
    }

    /// check if the `JoinPredicate` only include equal conditions, because now our hash join and
    /// stream join executor just support join predicate only with equi-conditions
    pub fn is_equal_cond(&self) -> bool {
        self.other_conds.is_empty()
    }
    pub fn equal_keys(&self) -> Vec<(usize, usize)> {
        self.keys.clone()
    }
    pub fn left_keys(&self) -> Vec<usize> {
        self.keys.iter().map(|(left, _)| *left).collect()
    }
    pub fn right_keys(&self) -> Vec<usize> {
        self.keys.iter().map(|(_, right)| *right).collect()
    }

    /// Get a reference to the join predicate's other conds.
    pub fn other_conds(&self) -> &[ExprImpl] {
        self.other_conds.as_ref()
    }
    // TODO: our backend not support some equal columns and sonm NonEqual condition
    //   fn has_equal_cond(&self) ->bool{
    //     todo!()
    //   }
}
