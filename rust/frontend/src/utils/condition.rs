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
use risingwave_common::types::{DataType, ScalarImpl};

use crate::expr::{
    to_conjunctions, ExprImpl, ExprRewriter, ExprType, ExprVisitor, FunctionCall, Literal,
};
use crate::optimizer::plan_node::CollectInputRef;

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
        for expr in conjunctions {
            write!(f, " AND {:?}", expr)?;
        }
        Ok(())
    }
}

impl Condition {
    pub fn with_expr(expr: ExprImpl) -> Self {
        Self {
            conjunctions: to_conjunctions(expr),
        }
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

    #[must_use]
    pub fn and(self, other: Self) -> Self {
        let mut ret = self;
        ret.conjunctions
            .reserve(ret.conjunctions.len() + other.conjunctions.len());
        for expr in other.conjunctions {
            ret.conjunctions.push(expr);
        }
        ret
    }

    #[must_use]
    /// Split the condition expressions into 3 groups: left, right and others
    pub fn split(self, left_col_num: usize, right_col_num: usize) -> (Self, Self, Self) {
        let left_bit_map = FixedBitSet::from_iter(0..left_col_num);
        let right_bit_map = FixedBitSet::from_iter(left_col_num..left_col_num + right_col_num);

        let (mut left, mut right, mut others) = (vec![], vec![], vec![]);
        self.conjunctions.into_iter().for_each(|expr| {
            let input_bits = CollectInputRef::collect(&expr, left_col_num + right_col_num);
            if input_bits.is_subset(&left_bit_map) {
                left.push(expr)
            } else if input_bits.is_subset(&right_bit_map) {
                right.push(expr)
            } else {
                others.push(expr)
            }
        });

        (
            Condition { conjunctions: left },
            Condition {
                conjunctions: right,
            },
            Condition {
                conjunctions: others,
            },
        )
    }

    #[must_use]
    /// Split the condition expressions into 2 groups: those referencing `columns` and others which
    /// are disjoint with columns.
    pub fn split_disjoint(self, columns: &FixedBitSet, capacity: usize) -> (Self, Self) {
        let (mut referencing, mut disjoint) = (vec![], vec![]);
        self.conjunctions.into_iter().for_each(|expr| {
            let input_bits = CollectInputRef::collect(&expr, capacity);
            if input_bits.is_disjoint(columns) {
                disjoint.push(expr)
            } else {
                referencing.push(expr)
            }
        });

        (
            Condition {
                conjunctions: referencing,
            },
            Condition {
                conjunctions: disjoint,
            },
        )
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
