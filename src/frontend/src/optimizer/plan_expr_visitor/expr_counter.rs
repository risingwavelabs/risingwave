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

use std::collections::HashMap;

use crate::expr::{ExprImpl, ExprType, ExprVisitor, FunctionCall, default_visit_expr};

/// `ExprCounter` is used by `CseRewriter`.
#[derive(Default)]
pub struct CseExprCounter {
    // Only count pure function call and not const.
    pub counter: HashMap<FunctionCall, usize>,
}

impl ExprVisitor for CseExprCounter {
    fn visit_expr(&mut self, expr: &ExprImpl) {
        // Considering this sql, `In` expression needs to ensure its in-clauses to be const.
        // If we extract it into a common sub-expression (finally be a `InputRef`) which will
        // violate this assumption, so ban this case. SELECT x,
        //        tand(x) IN ('-Infinity'::float8,-1,0,1,'Infinity'::float8) AS tand_exact,
        //        cotd(x) IN ('-Infinity'::float8,-1,0,1,'Infinity'::float8) AS cotd_exact
        // FROM (VALUES (0), (45), (90), (135), (180),(225), (270), (315), (360)) AS t(x);
        if expr.is_const() {
            return;
        }

        default_visit_expr(self, expr);
    }

    fn visit_function_call(&mut self, func_call: &FunctionCall) {
        if func_call.is_pure() {
            self.counter
                .entry(func_call.clone())
                .and_modify(|counter| *counter += 1)
                .or_insert(1);
        }

        match func_call.func_type() {
            // Short cut semantic func type cannot be extracted into common sub-expression.
            // E.g. select case when a = 0 then 0 when a < 10 then 1 + 1 / a else 1 / a end from x
            // If a is zero, common sub-expression (1 / a) would lead to division by zero error.
            //
            // Also note `AND`/`OR` is not guaranteed this semantic.
            // E.g. select * from a, b where a1 > b1*b1 and 3 / a1 < 5;
            // Optimizer is allowed to filter with `3 / a1 < 5` before join on `a1 > b1*b1`.
            // This can lead to division by zero error not observed without optimization.
            ExprType::Case | ExprType::Coalesce => {
                return;
            }
            // `some` and `all` cannot be separated from their inner binary boolean operator #11766
            // We could still visit the lhs scalar and rhs array, but keeping it simple here.
            // E.g. `v not like some(arr)` is bound as `Some(Not(Like(v, arr)))`
            // It is invalid to extract `Like(v, arr)` or `Not(Like(v, arr))`. `v` and `arr` are ok.
            ExprType::Some | ExprType::All => {
                return;
            }
            _ => {}
        };

        func_call
            .inputs()
            .iter()
            .for_each(|expr| self.visit_expr(expr));
    }
}
