// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

mod column_index_mapping;
pub use column_index_mapping::*;
mod condition;
pub use condition::*;
mod connected_components;
pub(crate) use connected_components::*;
mod with_options;
pub use with_options::*;

use crate::expr::{Expr, ExprImpl, ExprRewriter, InputRef};

/// Substitute `InputRef` with corresponding `ExprImpl`.
pub struct Substitute {
    pub mapping: Vec<ExprImpl>,
}

impl ExprRewriter for Substitute {
    fn rewrite_input_ref(&mut self, input_ref: InputRef) -> ExprImpl {
        assert_eq!(
            input_ref.return_type(),
            self.mapping[input_ref.index()].return_type(),
            "Type mismatch when substituting {:?} with {:?}",
            input_ref,
            self.mapping[input_ref.index()],
        );
        self.mapping[input_ref.index()].clone()
    }
}
