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

use fixedbitset::FixedBitSet;

use super::{ExprImpl, ExprVisitor, InputRef};
use crate::expr::ExprType;

fn to_conjunctions_inner(expr: ExprImpl, rets: &mut Vec<ExprImpl>) {
    match expr {
        ExprImpl::FunctionCall(func_call) if func_call.get_expr_type() == ExprType::And => {
            let (_, exprs, _) = func_call.decompose();
            for expr in exprs {
                to_conjunctions_inner(expr, rets);
            }
        }
        _ => rets.push(expr),
    }
}

/// give a bool expression, and transform it to Conjunctive form. e.g. given expression is
/// (expr1 AND expr2 AND expr3) the function will return Vec[expr1, expr2, expr3].
pub fn to_conjunctions(expr: ExprImpl) -> Vec<ExprImpl> {
    let mut rets = vec![];
    to_conjunctions_inner(expr, &mut rets);
    // TODO: extract common factors fromm the conjunctions with OR expression.
    // e.g: transform (a AND ((b AND c) OR (b AND d))) to (a AND b AND (c OR d))
    rets
}

/// give a expression, and check all columns in its `input_ref` expressions less than the input
/// column number.
macro_rules! assert_input_ref {
    ($expr:expr, $input_col_num:expr) => {
        let _ = CollectInputRef::collect($expr, $input_col_num);
    };
}
pub(crate) use assert_input_ref;

/// Collect all `InputRef`s' indexes in the expression.
///
/// # Panics
/// Panics if an `InputRef`'s index is out of bounds of the [`FixedBitSet`].
pub struct CollectInputRef {
    /// All `InputRef`s' indexes are inserted into the [`FixedBitSet`].
    pub input_bits: FixedBitSet,
}

impl ExprVisitor for CollectInputRef {
    fn visit_input_ref(&mut self, expr: &InputRef) {
        self.input_bits.insert(expr.index());
    }
}

impl CollectInputRef {
    /// Creates a `CollectInputRef` with an initial `input_bits`.
    pub fn new(initial_input_bits: FixedBitSet) -> Self {
        CollectInputRef {
            input_bits: initial_input_bits,
        }
    }

    /// Creates an empty `CollectInputRef` with the given capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        CollectInputRef {
            input_bits: FixedBitSet::with_capacity(capacity),
        }
    }

    /// Collect all `InputRef`s' indexes in the expression.
    ///
    /// # Panics
    /// Panics if `input_ref >= capacity`.
    pub fn collect(expr: &ExprImpl, capacity: usize) -> FixedBitSet {
        let mut visitor = Self::with_capacity(capacity);
        visitor.visit_expr(expr);
        visitor.input_bits
    }
}
