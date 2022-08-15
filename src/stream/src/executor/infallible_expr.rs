use std::sync::Arc;

use risingwave_common::array::{ArrayRef, DataChunk};
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
                    match self.eval_row(&row.to_owned_row()) {
                        Ok(datum) => array_builder.append_datum(&datum).unwrap(),
                        Err(err) => {
                            on_err(err);
                            array_builder.append_null().unwrap();
                        }
                    }
                } else {
                    array_builder.append_null().unwrap();
                }
            }
            Arc::new(array_builder.finish().unwrap())
        })
    }
}

impl InfallibleExpression for dyn Expression {}
