// Copyright 2024 RisingWave Labs
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

use std::ops::{Div, Mul};
use std::sync::Arc;

use arrow_array::ArrayRef;
use num_traits::abs;

use super::{FromArrow, ToArrow};
use crate::array::{Array, ArrayError, DecimalArray};

pub struct IcebergArrowConvert;

impl ToArrow for IcebergArrowConvert {
    #[inline]
    fn decimal_type_to_arrow(&self, name: &str) -> arrow_schema::Field {
        let data_type =
            arrow_schema::DataType::Decimal128(arrow_schema::DECIMAL128_MAX_PRECISION, 0);
        arrow_schema::Field::new(name, data_type, true)
    }

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

impl FromArrow for IcebergArrowConvert {}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow_array::ArrayRef;

    use crate::array::arrow::arrow_iceberg::IcebergArrowConvert;
    use crate::array::arrow::ToArrow;
    use crate::array::{Decimal, DecimalArray};

    #[test]
    fn decimal() {
        let array = DecimalArray::from_iter([
            None,
            Some(Decimal::NaN),
            Some(Decimal::PositiveInf),
            Some(Decimal::NegativeInf),
            Some(Decimal::Normalized("123.4".parse().unwrap())),
            Some(Decimal::Normalized("123.456".parse().unwrap())),
        ]);
        let ty = arrow_schema::DataType::Decimal128(6, 3);
        let arrow_array = IcebergArrowConvert.decimal_to_arrow(&ty, &array).unwrap();
        let expect_array = Arc::new(
            arrow_array::Decimal128Array::from(vec![
                None,
                None,
                Some(999999),
                Some(-999999),
                Some(123400),
                Some(123456),
            ])
            .with_data_type(ty),
        ) as ArrayRef;
        assert_eq!(&arrow_array, &expect_array);
    }
}
