// Copyright 2023 RisingWave Labs
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
// limitations under the License.use std::collections::HashMap;

use std::collections::HashMap;

use crate::expr::{Expr, ExprImpl, ExprRewriter, FunctionCall, InputRef};
use crate::optimizer::plan_expr_visitor::CseExprCounter;

#[derive(Default)]
pub struct CseRewriter {
    pub expr_counter: CseExprCounter,
    pub cse_input_ref_offset: usize,
    pub cse_mapping: HashMap<FunctionCall, InputRef>,
}

impl CseRewriter {
    pub fn new(expr_counter: CseExprCounter, cse_input_ref_offset: usize) -> Self {
        Self {
            expr_counter,
            cse_input_ref_offset,
            cse_mapping: HashMap::default(),
        }
    }
}

impl ExprRewriter for CseRewriter {
    fn rewrite_function_call(&mut self, func_call: FunctionCall) -> ExprImpl {
        if let Some(count) = self.expr_counter.counter.get(&func_call) && *count > 1 {
            if let Some(expr) = self.cse_mapping.get(&func_call) {
                let expr: ExprImpl = ExprImpl::InputRef(expr.clone().into());
                return expr;
            }
            let input_ref = InputRef::new(self.cse_input_ref_offset, func_call.return_type());
            self.cse_input_ref_offset += 1;
            self.cse_mapping.insert(func_call, input_ref.clone());
            return ExprImpl::InputRef(input_ref.into());
        }

        let (func_type, inputs, ret) = func_call.decompose();
        let inputs = inputs
            .into_iter()
            .map(|expr| self.rewrite_expr(expr))
            .collect();
        FunctionCall::new_unchecked(func_type, inputs, ret).into()
    }
}
