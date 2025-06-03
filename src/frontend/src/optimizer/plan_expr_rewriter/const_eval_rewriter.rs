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

use crate::error::{ErrorCode, RwError};
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
            self.error = Some((ErrorCode::InvalidInputSyntax(
                "Parameter should not appear here. It will be replaced by a literal before this step.".to_owned(),
            )).into());
            expr
        } else {
            default_rewrite_expr(self, expr)
        }
    }
}
