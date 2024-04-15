// Copyright 2024 RisingWave Labs
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

//! This is for arrow dependency named `arrow-xxx-deltalake` such as `arrow-array-deltalake`
//! in the cargo workspace.
//!
//! The corresponding version of arrow is currently used by `deltalake` sink.

use std::ops::{Div, Mul};
use std::sync::Arc;

use arrow_array::ArrayRef;
use itertools::Itertools;
use num_traits::abs;
use {
    arrow_array_deltalake as arrow_array, arrow_buffer_deltalake as arrow_buffer,
    arrow_cast_deltalake as arrow_cast, arrow_schema_deltalake as arrow_schema,
};

use self::arrow_impl::ToArrowArrayWithTypeConvert;
use crate::array::{Array, ArrayError, DataChunk, DecimalArray};
use crate::util::iter_util::ZipEqFast;
#[expect(clippy::duplicate_mod)]
#[path = "./arrow_impl.rs"]
mod arrow_impl;

struct DeltaLakeConvert;

impl arrow_impl::ToArrowArrayWithTypeConvert for DeltaLakeConvert {
    fn decimal_to_arrow(
        &self,
        data_type: &arrow_schema::DataType,
        array: &DecimalArray,
    ) -> Result<arrow_array::ArrayRef, ArrayError> {
        let (precision, max_scale) = match data_type {
            arrow_schema::DataType::Decimal128(precision, scale) => (*precision, *scale),
            _ => return Err(ArrayError::to_arrow("Invalid decimal type")),
        };

        // Convert Decimal to i128:
        let values: Vec<Option<i128>> = array
            .iter()
            .map(|e| {
                e.and_then(|e| match e {
                    crate::array::Decimal::Normalized(e) => {
                        let value = e.mantissa();
                        let scale = e.scale() as i8;
                        let diff_scale = abs(max_scale - scale);
                        let value = match scale {
                            _ if scale < max_scale => {
                                value.mul(10_i32.pow(diff_scale as u32) as i128)
                            }
                            _ if scale > max_scale => {
                                value.div(10_i32.pow(diff_scale as u32) as i128)
                            }
                            _ => value,
                        };
                        Some(value)
                    }
                    // For Inf, we replace them with the max/min value within the precision.
                    crate::array::Decimal::PositiveInf => {
                        let max_value = 10_i128.pow(precision as u32) - 1;
                        Some(max_value)
                    }
                    crate::array::Decimal::NegativeInf => {
                        let max_value = 10_i128.pow(precision as u32) - 1;
                        Some(-max_value)
                    }
                    crate::array::Decimal::NaN => None,
                })
            })
            .collect();

        let array = arrow_array::Decimal128Array::from(values)
            .with_precision_and_scale(precision, max_scale)
            .map_err(ArrayError::from_arrow)?;
        Ok(Arc::new(array) as ArrayRef)
    }
}

/// Converts RisingWave array to Arrow array with the schema.
/// This function will try to convert the array if the type is not same with the schema.
pub fn to_deltalake_record_batch_with_schema(
    schema: arrow_schema::SchemaRef,
    chunk: &DataChunk,
) -> Result<arrow_array::RecordBatch, ArrayError> {
    if !chunk.is_compacted() {
        let c = chunk.clone();
        return to_deltalake_record_batch_with_schema(schema, &c.compact());
    }
    let columns: Vec<_> = chunk
        .columns()
        .iter()
        .zip_eq_fast(schema.fields().iter())
        .map(|(column, field)| {
            let column: arrow_array::ArrayRef =
                DeltaLakeConvert.to_arrow_with_type(field.data_type(), column)?;
            if column.data_type() == field.data_type() {
                Ok(column)
            } else {
                arrow_cast::cast(&column, field.data_type()).map_err(ArrayError::from_arrow)
            }
        })
        .try_collect::<_, _, ArrayError>()?;

    let opts = arrow_array::RecordBatchOptions::default().with_row_count(Some(chunk.capacity()));
    arrow_array::RecordBatch::try_new_with_options(schema, columns, &opts)
        .map_err(ArrayError::to_arrow)
}
