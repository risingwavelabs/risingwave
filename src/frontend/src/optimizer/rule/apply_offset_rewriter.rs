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

use itertools::Itertools;

use crate::expr::{CorrelatedId, CorrelatedInputRef, Expr, ExprImpl, ExprRewriter, InputRef};
use crate::utils::ColIndexMapping;

/// Convert `CorrelatedInputRef` to `InputRef` and shift `InputRef` with offset.
pub struct ApplyOffsetRewriter {
    offset: usize,
    index_mapping: ColIndexMapping,
    has_correlated_input_ref: bool,
    correlated_id: CorrelatedId,
}

impl ExprRewriter for ApplyOffsetRewriter {
    fn rewrite_correlated_input_ref(
        &mut self,
        correlated_input_ref: CorrelatedInputRef,
    ) -> ExprImpl {
        let found = correlated_input_ref.correlated_id() == self.correlated_id;
        self.has_correlated_input_ref |= found;
        if found {
            InputRef::new(
                self.index_mapping.map(correlated_input_ref.index()),
                correlated_input_ref.return_type(),
            )
            .into()
        } else {
            correlated_input_ref.into()
        }
    }

    fn rewrite_input_ref(&mut self, input_ref: InputRef) -> ExprImpl {
        InputRef::new(input_ref.index() + self.offset, input_ref.return_type()).into()
    }
}

impl ApplyOffsetRewriter {
    pub fn new(offset: usize, correlated_indices: &[usize], correlated_id: CorrelatedId) -> Self {
        Self {
            offset,
            index_mapping: ColIndexMapping::new(
                correlated_indices.iter().copied().map(Some).collect_vec(),
            )
            .inverse(),
            has_correlated_input_ref: false,
            correlated_id,
        }
    }

    pub fn has_correlated_input_ref(&self) -> bool {
        self.has_correlated_input_ref
    }

    pub fn reset_state(&mut self) {
        self.has_correlated_input_ref = false;
    }
}
