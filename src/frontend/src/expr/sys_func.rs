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
// limitations under the License.

use super::{ExprImpl, ExprVisitor};
use crate::expr::expr_visitor::default_visit_function_call;

struct SysFuncAnalyzer;

impl ExprVisitor for SysFuncAnalyzer {
    type Result = bool;

    fn merge(a: bool, b: bool) -> bool {
        a || b
    }

    fn visit_function_call(&mut self, func_call: &super::FunctionCall) -> bool {
        use risingwave_pb::expr::expr_node::Type::*;

        match func_call.func_type() {
            ColDescription | CastRegclass => true,
            _ => default_visit_function_call(self, func_call),
        }
    }
}

impl ExprImpl {
    #[expect(dead_code)]
    pub(crate) fn has_sys_func(&self) -> bool {
        let mut analyzer = SysFuncAnalyzer;
        analyzer.visit_expr(self)
    }
}

#[cfg(test)]
mod tests {
    use crate::binder::test_utils;

    #[test]
    fn test_has_sys_func() {
        let cases = [
            ("'foo'::regclass", true),
            ("'foo'::regclass::varchar || 'bar'", true),
            ("1 + 2", false),
        ];
        for (expr, expected) in cases {
            let expr = test_utils::must_parse_and_bind_expr(expr);
            let output = expr.has_sys_func();
            assert_eq!(output, expected, "{:?}", expr);
        }
    }
}
