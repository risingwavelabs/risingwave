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

use std::sync::Arc;

use risingwave_common::array::{ArrayRef, DataChunk, Row};
use risingwave_common::types::Datum;
use risingwave_expr::expr::Expression;
use risingwave_expr::ExprError;
use static_assertions::const_assert;

pub trait InfallibleExpression: Expression {
    fn eval_infallible(&self, input: &DataChunk, on_err: impl Fn(ExprError)) -> ArrayRef {
        const_assert!(!crate::STRICT_MODE);

        #[expect(clippy::disallowed_methods)]
        self.eval(input).unwrap_or_else(|_err| {
            // When eval failed, recompute in row-based execution
            // and pad with NULL for each failed row.
            let mut array_builder = self.return_type().create_array_builder(input.cardinality());
            for row in input.rows_with_holes() {
                if let Some(row) = row {
                    let datum = self.eval_row_infallible(&row.to_owned_row(), &on_err);
                    array_builder.append_datum(&datum).unwrap();
                } else {
                    array_builder.append_null().unwrap();
                }
            }
            Arc::new(array_builder.finish())
        })
    }

    fn eval_row_infallible(&self, input: &Row, on_err: impl Fn(ExprError)) -> Datum {
        const_assert!(!crate::STRICT_MODE);

        #[expect(clippy::disallowed_methods)]
        self.eval_row(input).unwrap_or_else(|err| {
            on_err(err);
            None
        })
    }
}

impl<E: Expression + ?Sized> InfallibleExpression for E {}
