use risingwave_common::types::{DataType, ScalarImpl};

use crate::expr::{to_conjunctions, Expr, ExprImpl, ExprRewriter, ExprType, FunctionCall, Literal};

#[derive(Debug, Clone)]
pub struct Condition {
    /// condition bool expressions, linked with AND conjunction
    pub conjunctions: Vec<ExprImpl>,
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
        self.conjunctions.len() == 0
    }
    pub fn to_expr(self) -> ExprImpl {
        let mut iter = self.conjunctions.into_iter();
        if let Some(mut ret) = iter.next() {
            for expr in iter {
                ret = FunctionCall::new(ExprType::And, vec![ret, expr])
                    .unwrap()
                    .bound_expr();
            }
            ret
        } else {
            Literal::new(Some(ScalarImpl::Bool(true)), DataType::Boolean).bound_expr()
        }
    }

    pub fn and(&mut self, other: Self) {
        self.conjunctions
            .reserve(self.conjunctions.len() + other.conjunctions.len());
        for expr in other.conjunctions {
            self.conjunctions.push(expr);
        }
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
}
