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

use crate::expr::{CorrelatedId, CorrelatedInputRef, Expr, ExprImpl, ExprRewriter, InputRef};

/// Base rewriter for pulling up correlated expressions.
///
/// Provides common functionality for rewriting correlated input references to regular input references.
/// Different rules can extend this by implementing their own `rewrite_input_ref` behavior.
pub struct CorrelatedExprRewriter {
    pub correlated_id: CorrelatedId,
}

impl CorrelatedExprRewriter {
    pub fn new(correlated_id: CorrelatedId) -> Self {
        Self { correlated_id }
    }

    /// Common logic for rewriting correlated input references to input references.
    pub fn rewrite_correlated_input_ref_impl(
        &mut self,
        correlated_input_ref: CorrelatedInputRef,
    ) -> ExprImpl {
        // Convert correlated_input_ref to input_ref.
        // only rewrite the correlated_input_ref with the same correlated_id
        if correlated_input_ref.correlated_id() == self.correlated_id {
            InputRef::new(
                correlated_input_ref.index(),
                correlated_input_ref.return_type(),
            )
            .into()
        } else {
            correlated_input_ref.into()
        }
    }
}

/// Rewriter for pulling up correlated predicates.
///
/// Collects all `InputRef`s and shifts their indices for use in join conditions.
pub struct PredicateRewriter {
    pub base: CorrelatedExprRewriter,
    // All uncorrelated `InputRef`s in the expression.
    pub input_refs: Vec<InputRef>,
    pub index: usize,
}

impl PredicateRewriter {
    pub fn new(correlated_id: CorrelatedId, index: usize) -> Self {
        Self {
            base: CorrelatedExprRewriter::new(correlated_id),
            input_refs: vec![],
            index,
        }
    }
}

impl ExprRewriter for PredicateRewriter {
    fn rewrite_correlated_input_ref(
        &mut self,
        correlated_input_ref: CorrelatedInputRef,
    ) -> ExprImpl {
        self.base
            .rewrite_correlated_input_ref_impl(correlated_input_ref)
    }

    fn rewrite_input_ref(&mut self, input_ref: InputRef) -> ExprImpl {
        let data_type = input_ref.return_type();

        // It will be appended to exprs in LogicalProject, so its index remain the same.
        self.input_refs.push(input_ref);

        // Rewrite input_ref's index to its new location.
        let input_ref = InputRef::new(self.index, data_type);
        self.index += 1;
        input_ref.into()
    }
}

/// Rewriter for pulling up correlated project expressions with values.
///
/// For inlining scalar subqueries, we don't need to collect or shift input references.
pub struct ProjectValueRewriter {
    pub base: CorrelatedExprRewriter,
}

impl ProjectValueRewriter {
    pub fn new(correlated_id: CorrelatedId) -> Self {
        Self {
            base: CorrelatedExprRewriter::new(correlated_id),
        }
    }
}

impl ExprRewriter for ProjectValueRewriter {
    fn rewrite_correlated_input_ref(
        &mut self,
        correlated_input_ref: CorrelatedInputRef,
    ) -> ExprImpl {
        self.base
            .rewrite_correlated_input_ref_impl(correlated_input_ref)
    }

    fn rewrite_input_ref(&mut self, input_ref: InputRef) -> ExprImpl {
        // For project value rule, input_refs in the project should reference Values
        // Since we're inlining, we don't need to preserve these references
        // They should be constant-folded or handled separately
        input_ref.into()
    }
}
