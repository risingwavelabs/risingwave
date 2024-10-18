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

use std::ops::{Div, Mul};
use std::sync::Arc;

use arrow_array_iceberg::{self as arrow_array, ArrayRef};
use arrow_buffer_iceberg::IntervalMonthDayNano as ArrowIntervalType;
use num_traits::abs;
use {
    arrow_buffer_iceberg as arrow_buffer, arrow_cast_iceberg as arrow_cast,
    arrow_schema_iceberg as arrow_schema,
};

use crate::array::{Array, ArrayError, ArrayImpl, DataChunk, DataType, DecimalArray};
use crate::types::{Interval, StructType};

impl ArrowIntervalTypeTrait for ArrowIntervalType {
    fn to_interval(self) -> Interval {
        // XXX: the arrow-rs decoding is incorrect
        // let (months, days, ns) = arrow_array::types::IntervalMonthDayNanoType::to_parts(value);
        Interval::from_month_day_usec(self.months, self.days, self.nanoseconds / 1000)
    }

    fn from_interval(value: Interval) -> Self {
        // XXX: the arrow-rs encoding is incorrect
        // arrow_array::types::IntervalMonthDayNanoType::make_value(
        //     self.months(),
        //     self.days(),
        //     // TODO: this may overflow and we need `try_into`
        //     self.usecs() * 1000,
        // )
        Self {
            months: value.months(),
            days: value.days(),
            nanoseconds: value.usecs() * 1000,
        }
    }
}

#[path = "./arrow_impl.rs"]
mod arrow_impl;

use arrow_impl::{FromArrow, ToArrow};

use crate::array::arrow::ArrowIntervalTypeTrait;

pub struct IcebergArrowConvert;

impl IcebergArrowConvert {
    pub fn to_record_batch(
        &self,
        schema: arrow_schema::SchemaRef,
        chunk: &DataChunk,
    ) -> Result<arrow_array::RecordBatch, ArrayError> {
        ToArrow::to_record_batch(self, schema, chunk)
    }

    pub fn chunk_from_record_batch(
        &self,
        batch: &arrow_array::RecordBatch,
    ) -> Result<DataChunk, ArrayError> {
        FromArrow::from_record_batch(self, batch)
    }

    pub fn to_arrow_field(
        &self,
        name: &str,
        data_type: &DataType,
    ) -> Result<arrow_schema::Field, ArrayError> {
        ToArrow::to_arrow_field(self, name, data_type)
    }

    pub fn type_from_field(&self, field: &arrow_schema::Field) -> Result<DataType, ArrayError> {
        FromArrow::from_field(self, field)
    }

    pub fn struct_from_fields(
        &self,
        fields: &arrow_schema::Fields,
    ) -> Result<StructType, ArrayError> {
        FromArrow::from_fields(self, fields)
    }

    pub fn to_arrow_array(
        &self,
        data_type: &arrow_schema::DataType,
        array: &ArrayImpl,
    ) -> Result<arrow_array::ArrayRef, ArrayError> {
        ToArrow::to_array(self, data_type, array)
    }

    pub fn array_from_arrow_array(
        &self,
        field: &arrow_schema::Field,
        array: &arrow_array::ArrayRef,
    ) -> Result<ArrayImpl, ArrayError> {
        FromArrow::from_array(self, field, array)
    }
}

impl ToArrow for IcebergArrowConvert {
    #[inline]
    fn decimal_type_to_arrow(&self, name: &str) -> arrow_schema::Field {
        // Fixed-point decimal; precision P, scale S Scale is fixed, precision must be less than 38.
        let data_type = arrow_schema::DataType::Decimal128(28, 10);
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

    use arrow_array_iceberg::{ArrayRef, Decimal128Array};
    use arrow_schema_iceberg::DataType;

    use super::arrow_impl::ToArrow;
    use super::IcebergArrowConvert;
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
        let ty = DataType::Decimal128(6, 3);
        let arrow_array = IcebergArrowConvert.decimal_to_arrow(&ty, &array).unwrap();
        let expect_array = Arc::new(
            Decimal128Array::from(vec![
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
