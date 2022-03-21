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
//
use std::sync::Arc;

use log::debug;

use crate::array::{ArrayBuilder, ArrayImpl, ArrayRef, DataChunk, I32ArrayBuilder};
use crate::error::Result;
use crate::expr::{BoxedExpression, Expression};
use crate::types::DataType;

/// `PG_SLEEP` sleeps on current session for given duration (double precision in seconds),
/// and returns `NULL` for all inputs.
///
/// Note that currently `PG_SLEEP` accepts decimals as arguments, which is not compatible
/// with Postgres. The reason for this is that Calcite always converts float/double to
/// decimal, but not vice versa.
#[derive(Debug)]
pub struct PgSleepExpression {
    child_expr: BoxedExpression,
    return_type: DataType,
}

impl PgSleepExpression {
    pub fn new(child_expr: BoxedExpression) -> Self {
        PgSleepExpression {
            child_expr,
            return_type: DataType::Int32,
        }
    }
}

impl Expression for PgSleepExpression {
    fn return_type(&self) -> DataType {
        self.return_type.clone()
    }

    fn eval(&mut self, input: &DataChunk) -> Result<ArrayRef> {
        use std::time::Duration;

        use num_traits::ToPrimitive;

        let child_result = self.child_expr.eval(input)?;
        let mut array_builder = I32ArrayBuilder::new(input.cardinality())?;
        for datum in child_result.iter() {
            if let Some(duration) = datum {
                // Postgres accepts double precisions, but Calcite likes decimals
                let duration_secs = duration.into_decimal().to_f64().unwrap();
                if duration_secs > 0.0 {
                    let duration_ms = (duration_secs * 1000.0) as u64;
                    debug!("pg_sleep() for {} ms", duration_ms);
                    std::thread::sleep(Duration::from_millis(duration_ms));
                }
            }
            array_builder.append_null()?;
        }
        let array = array_builder.finish()?;
        Ok(Arc::new(ArrayImpl::from(array)))
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;
    use crate::array::column::Column;
    use crate::array::DecimalArrayBuilder;
    use crate::expr::InputRefExpression;
    use crate::types::Decimal;

    #[test]
    fn test_pg_sleep() -> Result<()> {
        let mut expr =
            PgSleepExpression::new(Box::new(InputRefExpression::new(DataType::Decimal, 0)));

        let input_array = {
            let mut builder = DecimalArrayBuilder::new(3)?;
            builder.append(Some(Decimal::from_str("0.1").unwrap()))?;
            builder.append(Some(Decimal::from_str("-0.1").unwrap()))?;
            builder.append(None)?;
            builder.finish()?
        };

        let input_chunk = DataChunk::builder()
            .columns(vec![Column::new(Arc::new(ArrayImpl::Decimal(input_array)))])
            .build();
        let result_array = expr.eval(&input_chunk).unwrap();
        assert_eq!(3, result_array.len());
        for i in 0..3 {
            assert!(result_array.value_at(i).is_none());
        }
        Ok(())
    }
}
