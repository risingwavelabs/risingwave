use std::fmt;

use fixedbitset::FixedBitSet;

use crate::expr::{get_inputs_col_index, ExprImpl, ExprType, FunctionCall, InputRef};
use crate::utils::Condition;

/// The join predicate used in optimizer
#[derive(Debug, Clone)]
pub struct EqJoinPredicate {
    /// Other conditions, linked with `AND` conjunction.
    other_cond: Condition,

    /// The equal columns indexes(in the input schema) both sides,
    /// the first is from the left table and the second is from the right table.
    /// now all are normal equal(not null-safe-equal),
    eq_keys: Vec<(InputRef, InputRef)>,
}

impl fmt::Display for EqJoinPredicate {
    fn fmt(&self, f: &mut fmt::Formatter) -> std::fmt::Result {
        let mut eq_keys = self.eq_keys().iter();
        if let Some((k1, k2)) = eq_keys.next() {
            write!(f, "{} = {}", k1, k2)?;
        }
        for (k1, k2) in eq_keys {
            write!(f, "AND {} = {}", k1, k2)?;
        }
        if !self.other_cond.always_true() {
            write!(f, "AND {}", self.other_cond)?;
        }

        Ok(())
    }
}

impl EqJoinPredicate {
    /// The new method for `JoinPredicate` without any analysis, check or rewrite.
    pub fn new(other_cond: Condition, eq_keys: Vec<(InputRef, InputRef)>) -> Self {
        Self {
            other_cond,
            eq_keys,
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
    pub fn create(left_cols_num: usize, right_cols_num: usize, on_clause: Condition) -> Self {
        let mut other_cond = Condition::true_cond();
        let mut eq_keys = vec![];

        for cond_expr in on_clause.conjunctions {
            let mut cols = FixedBitSet::with_capacity(left_cols_num + right_cols_num);
            get_inputs_col_index(&cond_expr, &mut cols);
            let from_left = cols
                .ones()
                .min()
                .map(|mx| mx < left_cols_num)
                .unwrap_or(false);
            let from_right = cols
                .ones()
                .max()
                .map(|mx| mx >= left_cols_num)
                .unwrap_or(false);
            match (from_left, from_right) {
                (true, true) => {
                    // TODO: refactor with if_chain
                    let mut is_eq_cond = false;
                    if let ExprImpl::FunctionCall(function_call) = cond_expr.clone() {
                        if function_call.get_expr_type() == ExprType::Equal {
                            if let (_, ExprImpl::InputRef(x), ExprImpl::InputRef(y)) =
                                function_call.decompose_as_binary()
                            {
                                is_eq_cond = true;
                                if x.index() < y.index() {
                                    eq_keys.push((*x, *y));
                                } else {
                                    eq_keys.push((*y, *x));
                                }
                            }
                        }
                    }
                    if !is_eq_cond {
                        other_cond.conjunctions.push(cond_expr)
                    }
                }
                (true, false) => other_cond.conjunctions.push(cond_expr),
                (false, true) => other_cond.conjunctions.push(cond_expr),
                (false, false) => other_cond.conjunctions.push(cond_expr),
            }
        }
        Self::new(other_cond, eq_keys)
    }

    /// Get join predicate's eq conds.
    pub fn eq_cond(&self) -> Condition {
        Condition {
            conjunctions: self
                .eq_keys
                .iter()
                .cloned()
                .map(|(l, r)| {
                    FunctionCall::new(ExprType::Equal, vec![l.into(), r.into()])
                        .unwrap()
                        .into()
                })
                .collect(),
        }
    }

    pub fn non_eq_cond(&self) -> Condition {
        self.other_cond.clone()
    }

    pub fn all_cond(&self) -> Condition {
        let cond = self.eq_cond();
        cond.and(self.non_eq_cond())
    }

    pub fn has_eq(&self) -> bool {
        !self.eq_keys.is_empty()
    }

    pub fn has_non_eq(&self) -> bool {
        !self.other_cond.always_true()
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
