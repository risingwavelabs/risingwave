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

impl fmt::Display for Condition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut conjunctions = self.conjunctions.iter();
        if let Some(expr) = conjunctions.next() {
            write!(f, "{:?}", expr)?;
        }
        for expr in conjunctions {
            write!(f, "AND {:?}", expr)?;
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
    pub fn split(
        self,
        left_col_num: usize,
        right_col_num: usize,
    ) -> (Vec<ExprImpl>, Vec<ExprImpl>, Vec<ExprImpl>) {
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

        (left, right, others)
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
        assert_eq!(res.0, vec![left]);
        assert_eq!(res.1, vec![right]);
        assert_eq!(res.2, vec![other]);
    }
}
