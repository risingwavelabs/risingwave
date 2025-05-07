// Copyright 2025 RisingWave Labs
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
use num_traits::abs;

pub use super::arrow_53::{
    FromArrow, ToArrow, arrow_array, arrow_buffer, arrow_cast, arrow_schema,
};
use crate::array::{Array, ArrayError, DataChunk, Decimal, DecimalArray};

pub struct DeltaLakeConvert;

impl DeltaLakeConvert {
    pub fn to_record_batch(
        &self,
        schema: arrow_schema::SchemaRef,
        chunk: &DataChunk,
    ) -> Result<arrow_array::RecordBatch, ArrayError> {
        ToArrow::to_record_batch(self, schema, chunk)
    }

    fn decimal_to_i128(decimal: Decimal, precision: u8, max_scale: i8) -> Option<i128> {
        match decimal {
            crate::array::Decimal::Normalized(e) => {
                let value = e.mantissa();
                let scale = e.scale() as i8;
                let diff_scale = abs(max_scale - scale);
                let value = match scale {
                    _ if scale < max_scale => value.mul(10_i32.pow(diff_scale as u32) as i128),
                    _ if scale > max_scale => value.div(10_i32.pow(diff_scale as u32) as i128),
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
        }
    }
}

impl ToArrow for DeltaLakeConvert {
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
            .map(|e| e.and_then(|e| DeltaLakeConvert::decimal_to_i128(e, precision, max_scale)))
            .collect();

        let array = arrow_array::Decimal128Array::from(values)
            .with_precision_and_scale(precision, max_scale)
            .map_err(ArrayError::from_arrow)?;
        Ok(Arc::new(array) as ArrayRef)
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow_array::ArrayRef;
    use arrow_array::cast::AsArray;
    use arrow_schema::Field;

    use super::*;
    use crate::array::arrow::arrow_deltalake::DeltaLakeConvert;
    use crate::array::{ArrayImpl, Decimal, DecimalArray, ListArray, ListValue};
    use crate::bitmap::Bitmap;

    #[test]
    fn test_decimal_list_chunk() {
        let value = ListValue::new(crate::array::ArrayImpl::Decimal(DecimalArray::from_iter([
            None,
            Some(Decimal::NaN),
            Some(Decimal::PositiveInf),
            Some(Decimal::NegativeInf),
            Some(Decimal::Normalized("1".parse().unwrap())),
            Some(Decimal::Normalized("123.456".parse().unwrap())),
        ])));
        let array = Arc::new(ArrayImpl::List(ListArray::from_iter(vec![value])));
        let chunk = crate::array::DataChunk::new(vec![array], Bitmap::ones(1));

        let schema = arrow_schema::Schema::new(vec![Field::new(
            "test",
            arrow_schema::DataType::List(Arc::new(Field::new(
                "test",
                arrow_schema::DataType::Decimal128(10, 0),
                true,
            ))),
            false,
        )]);

        let record_batch = DeltaLakeConvert
            .to_record_batch(Arc::new(schema), &chunk)
            .unwrap();
        let expect_array = Arc::new(
            arrow_array::Decimal128Array::from(vec![
                None,
                None,
                Some(9999999999),
                Some(-9999999999),
                Some(1),
                Some(123),
            ])
            .with_precision_and_scale(10, 0)
            .unwrap(),
        ) as ArrayRef;

        assert_eq!(
            &record_batch.column(0).as_list::<i32>().value(0),
            &expect_array
        );
    }
}
