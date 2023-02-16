use num_traits::Float;
use risingwave_common::types::OrderedF64;

use crate::{ExprError, Result};

pub fn exp_f64(input: OrderedF64) -> Result<OrderedF64> {
    let res = input.exp();
    if res.is_infinite() {
        Err(ExprError::NumericOutOfRange)
    } else {
        Ok(res)
    }
}
