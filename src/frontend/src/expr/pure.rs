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

use risingwave_pb::expr::expr_node;

use super::{ExprImpl, ExprVisitor};
struct ImpureAnalyzer {}

impl ExprVisitor<bool> for ImpureAnalyzer {
    fn merge(a: bool, b: bool) -> bool {
        // the expr will be impure if any of its input is impure
        a || b
    }

    fn visit_user_defined_function(&mut self, _func_call: &super::UserDefinedFunction) -> bool {
        true
    }

    fn visit_function_call(&mut self, func_call: &super::FunctionCall) -> bool {
        matches!(
            func_call.get_expr_type(),
            expr_node::Type::Now | expr_node::Type::Udf
        )
    }
}

pub fn is_pure(expr: &ExprImpl) -> bool {
    !is_impure(expr)
}
pub fn is_impure(expr: &ExprImpl) -> bool {
    let mut a = ImpureAnalyzer {};
    a.visit_expr(expr)
}
