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

use std::fmt;

use fixedbitset::FixedBitSet;
use itertools::Itertools;
use risingwave_common::types::{DataType, ScalarImpl};

use crate::expr::{
    fold_boolean_constant, to_conjunctions, try_get_bool_constant, ExprImpl, ExprRewriter,
    ExprType, ExprVisitor, FunctionCall, InputRef, Literal,
};

#[derive(Debug, Clone)]
pub struct Condition {
    /// Condition expressions in conjunction form (combined with `AND`)
    pub conjunctions: Vec<ExprImpl>,
}

impl IntoIterator for Condition {
    type Item = ExprImpl;
    type IntoIter = std::vec::IntoIter<ExprImpl>;

    fn into_iter(self) -> Self::IntoIter {
        self.conjunctions.into_iter()
    }
}

impl fmt::Display for Condition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut conjunctions = self.conjunctions.iter();
        if let Some(expr) = conjunctions.next() {
            write!(f, "{:?}", expr)?;
        }
        if self.always_true() {
            write!(f, "always")?;
        } else {
            for expr in conjunctions {
                write!(f, " AND {:?}", expr)?;
            }
        }

        Ok(())
    }
}

impl Condition {
    pub fn with_expr(expr: ExprImpl) -> Self {
        let conjunctions = to_conjunctions(expr);

        Self { conjunctions }.simplify()
    }

    pub fn true_cond() -> Self {
        Self {
            conjunctions: vec![],
        }
    }

    pub fn always_true(&self) -> bool {
        self.conjunctions.is_empty()
    }

    pub fn to_expr(self) -> ExprImpl {
        let mut iter = self.conjunctions.into_iter();
        if let Some(mut ret) = iter.next() {
            for expr in iter {
                ret = FunctionCall::new(ExprType::And, vec![ret, expr])
                    .unwrap()
                    .into();
            }
            ret
        } else {
            Literal::new(Some(ScalarImpl::Bool(true)), DataType::Boolean).into()
        }
    }

    // TODO(TaoWu): We might also use `Vec<ExprImpl>` form of predicates in compute node,
    // rather than using `AND` to combine them.
    pub fn as_expr(&self) -> ExprImpl {
        let mut iter = self.conjunctions.iter();
        if let Some(e) = iter.next() {
            let mut ret = e.clone();
            for expr in iter {
                ret = FunctionCall::new(ExprType::And, vec![ret, expr.clone()])
                    .unwrap()
                    .into();
            }
            ret
        } else {
            Literal::new(Some(ScalarImpl::Bool(true)), DataType::Boolean).into()
        }
    }

    /// Convert condition to an expression. If always true, return `None`.
    pub fn as_expr_unless_true(&self) -> Option<ExprImpl> {
        let mut iter = self.conjunctions.iter();
        if let Some(e) = iter.next() {
            let mut ret = e.clone();
            for expr in iter {
                ret = FunctionCall::new(ExprType::And, vec![ret, expr.clone()])
                    .unwrap()
                    .into();
            }
            Some(ret)
        } else {
            None
        }
    }

    #[must_use]
    pub fn and(self, other: Self) -> Self {
        let mut ret = self;
        ret.conjunctions.extend(other.conjunctions);
        ret.simplify()
    }

    #[must_use]
    /// Split the condition expressions into 3 groups: left, right and others
    pub fn split(self, left_col_num: usize, right_col_num: usize) -> (Self, Self, Self) {
        let left_bit_map = FixedBitSet::from_iter(0..left_col_num);
        let right_bit_map = FixedBitSet::from_iter(left_col_num..left_col_num + right_col_num);

        self.group_by::<_, 3>(|expr| {
            let input_bits = expr.collect_input_refs(left_col_num + right_col_num);
            if input_bits.is_subset(&left_bit_map) {
                0
            } else if input_bits.is_subset(&right_bit_map) {
                1
            } else {
                2
            }
        })
        .into_iter()
        .next_tuple()
        .unwrap()
    }

    #[must_use]
    /// For [`EqJoinPredicate`], separate equality conditions which connect left columns and right
    /// columns from other conditions.
    ///
    /// The equality conditions are transformed into `(left_col_id, right_col_id)` pairs.
    ///
    /// [`EqJoinPredicate`]: crate::optimizer::plan_node::EqJoinPredicate
    pub fn split_eq_keys(
        self,
        left_col_num: usize,
        right_col_num: usize,
    ) -> (Vec<(InputRef, InputRef)>, Self) {
        let left_bit_map = FixedBitSet::from_iter(0..left_col_num);
        let right_bit_map = FixedBitSet::from_iter(left_col_num..left_col_num + right_col_num);

        let (mut eq_keys, mut others) = (vec![], vec![]);
        self.conjunctions.into_iter().for_each(|expr| {
            let input_bits = expr.collect_input_refs(left_col_num + right_col_num);
            if input_bits.is_disjoint(&left_bit_map) || input_bits.is_disjoint(&right_bit_map) {
                others.push(expr)
            } else {
                let mut is_eq_cond = false;
                if let ExprImpl::FunctionCall(function_call) = expr.clone()
                    && function_call.get_expr_type() == ExprType::Equal
                    && let (_, ExprImpl::InputRef(x), ExprImpl::InputRef(y)) =
                            function_call.decompose_as_binary()
                    {
                        is_eq_cond = true;
                        if x.index() < y.index() {
                            eq_keys.push((*x, *y));
                        } else {
                            eq_keys.push((*y, *x));
                        }
                    }
                if !is_eq_cond {
                    others.push(expr)
                }
            }
        });

        (
            eq_keys,
            Condition {
                conjunctions: others,
            },
        )
    }

    #[must_use]
    /// Split the condition expressions into 2 groups: those referencing `columns` and others which
    /// are disjoint with columns.
    pub fn split_disjoint(self, columns: &FixedBitSet) -> (Self, Self) {
        self.group_by::<_, 2>(|expr| {
            let input_bits = expr.collect_input_refs(columns.len());
            if input_bits.is_disjoint(columns) {
                1
            } else {
                0
            }
        })
        .into_iter()
        .next_tuple()
        .unwrap()
    }

    #[must_use]
    /// Split the condition expressions into `N` groups.
    /// An expression `expr` is in the `i`-th group if `f(expr)==i`.
    ///
    /// # Panics
    /// Panics if `f(expr)>=N`.
    pub fn group_by<F, const N: usize>(self, f: F) -> [Self; N]
    where
        F: Fn(&ExprImpl) -> usize,
    {
        const EMPTY: Vec<ExprImpl> = vec![];
        let mut groups = [EMPTY; N];
        for (key, group) in &self.conjunctions.into_iter().group_by(|expr| {
            // i-th group
            let i = f(expr);
            assert!(i < N);
            i
        }) {
            groups[key].extend(group);
        }

        groups.map(|group| Condition {
            conjunctions: group,
        })
    }

    #[must_use]
    pub fn rewrite_expr(self, rewriter: &mut impl ExprRewriter) -> Self {
        Self {
            conjunctions: self
                .conjunctions
                .into_iter()
                .map(|expr| rewriter.rewrite_expr(expr))
                .collect(),
        }
    }

    pub fn visit_expr(&self, visitor: &mut impl ExprVisitor) {
        self.conjunctions
            .iter()
            .for_each(|expr| visitor.visit_expr(expr))
    }

    /// Simplify conditions
    /// It simplify conditions by applying constant folding and removing unnecessary conjunctions
    fn simplify(self) -> Self {
        // boolean constant folding
        let conjunctions: Vec<_> = self
            .conjunctions
            .into_iter()
            .map(fold_boolean_constant)
            .flat_map(to_conjunctions)
            .collect();

        let mut res: Vec<ExprImpl> = Vec::new();
        for i in conjunctions {
            if let Some(v) = try_get_bool_constant(&i) {
                if !v {
                    // if there is a `false` in conjunctions, the whole condition will be `false`
                    res.clear();
                    res.push(ExprImpl::literal_bool(false));
                    break;
                }
            } else {
                res.push(i);
            }
        }
        Self { conjunctions: res }
    }
}

#[cfg(test)]
mod tests {
    use rand::Rng;

    use super::*;
    use crate::expr::InputRef;

    #[test]
    fn test_split() {
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
        let right: ExprImpl = FunctionCall::new(
            ExprType::LessThan,
            vec![
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
            ],
        )
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

        let cond = Condition::with_expr(other.clone())
            .and(Condition::with_expr(right.clone()))
            .and(Condition::with_expr(left.clone()));
        let res = cond.split(left_col_num, right_col_num);
        assert_eq!(res.0.conjunctions, vec![left]);
        assert_eq!(res.1.conjunctions, vec![right]);
        assert_eq!(res.2.conjunctions, vec![other]);
    }
}
