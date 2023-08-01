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

use std::collections::HashMap;

use crate::expr::{ExprVisitor, FunctionCall};

#[derive(Default)]
pub struct ExprCounter {
    // Only count pure function call right now.
    pub counter: HashMap<FunctionCall, usize>,
}

impl ExprVisitor<()> for ExprCounter {
    fn merge(_: (), _: ()) {}

    fn visit_function_call(&mut self, func_call: &FunctionCall) {
        if func_call.is_pure() {
            self.counter
                .entry(func_call.clone())
                .and_modify(|counter| *counter += 1)
                .or_insert(1);
        }

        func_call
            .inputs()
            .iter()
            .map(|expr| self.visit_expr(expr))
            .reduce(Self::merge)
            .unwrap_or_default()
    }
}
