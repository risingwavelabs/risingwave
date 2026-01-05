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

use crate::expr::{ExprVisitor, InputRef};

#[derive(Default)]
pub struct InputRefCounter {
    // `input_ref` index -> count
    pub counter: HashMap<usize, usize>,
}

impl ExprVisitor for InputRefCounter {
    fn visit_input_ref(&mut self, input_ref: &InputRef) {
        self.counter
            .entry(input_ref.index)
            .and_modify(|counter| *counter += 1)
            .or_insert(1);
    }
}
