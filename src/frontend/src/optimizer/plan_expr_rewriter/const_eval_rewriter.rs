// Copyright 2025 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::error::RwError;
use crate::expr::{Expr, ExprImpl, ExprRewriter, Literal, default_rewrite_expr};

pub(crate) struct ConstEvalRewriter {
    pub(crate) error: Option<RwError>,
}
impl ExprRewriter for ConstEvalRewriter {
    fn rewrite_expr(&mut self, expr: ExprImpl) -> ExprImpl {
        if self.error.is_some() {
            return expr;
        }
        if let Some(result) = expr.try_fold_const() {
            match result {
                Ok(datum) => Literal::new(datum, expr.return_type()).into(),
                Err(e) => {
                    self.error = Some(e);
                    expr
                }
            }
        } else if let ExprImpl::Parameter(_) = expr {
            // Parameters should be handled gracefully, as they may appear before replacement
            // in some code paths (e.g., during index creation). Return the parameter as-is.
            expr
        } else {
            default_rewrite_expr(self, expr)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expr::Parameter;
    use crate::binder::ParameterTypes;
    use risingwave_common::types::DataType;

    #[test]
    fn test_const_eval_rewriter_handles_parameter() {
        // Create a parameter expression
        let param_types = ParameterTypes::new();
        let param = Parameter::new(1, param_types);
        let param_expr: ExprImpl = param.into();
        
        // Create a const eval rewriter
        let mut rewriter = ConstEvalRewriter { error: None };
        
        // This should not panic and should return the parameter as-is
        let result = rewriter.rewrite_expr(param_expr.clone());
        
        // The result should be the same parameter (not rewritten)
        assert!(matches!(result, ExprImpl::Parameter(_)));
        
        // No error should occur
        assert!(rewriter.error.is_none());
    }
    
    #[test]
    fn test_const_eval_rewriter_handles_constant() {
        // Create a literal expression  
        let literal = Literal::new(Some(42i32.into()), DataType::Int32);
        let literal_expr: ExprImpl = literal.into();
        
        // Create a const eval rewriter
        let mut rewriter = ConstEvalRewriter { error: None };
        
        // This should return the same literal (already constant)
        let result = rewriter.rewrite_expr(literal_expr.clone());
        
        // The result should be a literal
        assert!(matches!(result, ExprImpl::Literal(_)));
        
        // No error should occur
        assert!(rewriter.error.is_none());
    }
}
