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
use arrow_schema::DataType;
use itertools::Itertools;
use num_traits::abs;
use {
    arrow_array_deltalake as arrow_array, arrow_buffer_deltalake as arrow_buffer,
    arrow_cast_deltalake as arrow_cast, arrow_schema_deltalake as arrow_schema,
};

use self::arrow_impl::ToArrowArrayWithTypeConvert;
use crate::array::arrow::arrow_deltalake::arrow_impl::FromIntoArrow;
use crate::array::{Array, ArrayError, ArrayImpl, DataChunk, Decimal, DecimalArray, ListArray};
use crate::util::iter_util::ZipEqFast;
#[expect(clippy::duplicate_mod)]
#[path = "./arrow_impl.rs"]
mod arrow_impl;

struct DeltaLakeConvert;

impl DeltaLakeConvert {
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
            .map(|e| e.and_then(|e| DeltaLakeConvert::decimal_to_i128(e, precision, max_scale)))
            .collect();

        let array = arrow_array::Decimal128Array::from(values)
            .with_precision_and_scale(precision, max_scale)
            .map_err(ArrayError::from_arrow)?;
        Ok(Arc::new(array) as ArrayRef)
    }

    #[inline]
    fn list_to_arrow(
        &self,
        data_type: &arrow_schema::DataType,
        array: &ListArray,
    ) -> Result<arrow_array::ArrayRef, ArrayError> {
        use arrow_array::builder::*;
        fn build<A, B, F>(
            array: &ListArray,
            a: &A,
            builder: B,
            mut append: F,
        ) -> arrow_array::ListArray
        where
            A: Array,
            B: arrow_array::builder::ArrayBuilder,
            F: FnMut(&mut B, Option<A::RefItem<'_>>),
        {
            let mut builder = ListBuilder::with_capacity(builder, a.len());
            for i in 0..array.len() {
                for j in array.offsets[i]..array.offsets[i + 1] {
                    append(builder.values(), a.value_at(j as usize));
                }
                builder.append(!array.is_null(i));
            }
            builder.finish()
        }
        let inner_type = match data_type {
            arrow_schema::DataType::List(inner) => inner.data_type(),
            _ => return Err(ArrayError::to_arrow("Invalid list type")),
        };
        let arr: arrow_array::ListArray = match &*array.value {
            ArrayImpl::Int16(a) => build(array, a, Int16Builder::with_capacity(a.len()), |b, v| {
                b.append_option(v)
            }),
            ArrayImpl::Int32(a) => build(array, a, Int32Builder::with_capacity(a.len()), |b, v| {
                b.append_option(v)
            }),
            ArrayImpl::Int64(a) => build(array, a, Int64Builder::with_capacity(a.len()), |b, v| {
                b.append_option(v)
            }),

            ArrayImpl::Float32(a) => {
                build(array, a, Float32Builder::with_capacity(a.len()), |b, v| {
                    b.append_option(v.map(|f| f.0))
                })
            }
            ArrayImpl::Float64(a) => {
                build(array, a, Float64Builder::with_capacity(a.len()), |b, v| {
                    b.append_option(v.map(|f| f.0))
                })
            }
            ArrayImpl::Utf8(a) => build(
                array,
                a,
                StringBuilder::with_capacity(a.len(), a.data().len()),
                |b, v| b.append_option(v),
            ),
            ArrayImpl::Int256(a) => build(
                array,
                a,
                Decimal256Builder::with_capacity(a.len()).with_data_type(
                    arrow_schema::DataType::Decimal256(arrow_schema::DECIMAL256_MAX_PRECISION, 0),
                ),
                |b, v| b.append_option(v.map(Into::into)),
            ),
            ArrayImpl::Bool(a) => {
                build(array, a, BooleanBuilder::with_capacity(a.len()), |b, v| {
                    b.append_option(v)
                })
            }
            ArrayImpl::Decimal(a) => {
                let (precision, max_scale) = match inner_type {
                    arrow_schema::DataType::Decimal128(precision, scale) => (*precision, *scale),
                    _ => return Err(ArrayError::to_arrow("Invalid decimal type")),
                };
                build(
                    array,
                    a,
                    Decimal128Builder::with_capacity(a.len())
                        .with_data_type(DataType::Decimal128(precision, max_scale)),
                    |b, v| {
                        let v = v.and_then(|v| {
                            DeltaLakeConvert::decimal_to_i128(v, precision, max_scale)
                        });
                        b.append_option(v);
                    },
                )
            }
            ArrayImpl::Interval(a) => build(
                array,
                a,
                IntervalMonthDayNanoBuilder::with_capacity(a.len()),
                |b, v| b.append_option(v.map(|d| d.into_arrow())),
            ),
            ArrayImpl::Date(a) => build(array, a, Date32Builder::with_capacity(a.len()), |b, v| {
                b.append_option(v.map(|d| d.into_arrow()))
            }),
            ArrayImpl::Timestamp(a) => build(
                array,
                a,
                TimestampMicrosecondBuilder::with_capacity(a.len()),
                |b, v| b.append_option(v.map(|d| d.into_arrow())),
            ),
            ArrayImpl::Timestamptz(a) => build(
                array,
                a,
                TimestampMicrosecondBuilder::with_capacity(a.len()),
                |b, v| b.append_option(v.map(|d| d.into_arrow())),
            ),
            ArrayImpl::Time(a) => build(
                array,
                a,
                Time64MicrosecondBuilder::with_capacity(a.len()),
                |b, v| b.append_option(v.map(|d| d.into_arrow())),
            ),
            ArrayImpl::Jsonb(a) => build(
                array,
                a,
                LargeStringBuilder::with_capacity(a.len(), a.len() * 16),
                |b, v| b.append_option(v.map(|j| j.to_string())),
            ),
            ArrayImpl::Serial(_) => todo!("list of serial"),
            ArrayImpl::Struct(a) => {
                let values = Arc::new(arrow_array::StructArray::try_from(a)?);
                arrow_array::ListArray::new(
                    Arc::new(arrow_schema::Field::new(
                        "item",
                        a.data_type().try_into()?,
                        true,
                    )),
                    arrow_buffer::OffsetBuffer::new(arrow_buffer::ScalarBuffer::from(
                        array
                            .offsets()
                            .iter()
                            .map(|o| *o as i32)
                            .collect::<Vec<i32>>(),
                    )),
                    values,
                    Some(array.null_bitmap().into()),
                )
            }
            ArrayImpl::List(_) => todo!("list of list"),
            ArrayImpl::Bytea(a) => build(
                array,
                a,
                BinaryBuilder::with_capacity(a.len(), a.data().len()),
                |b, v| b.append_option(v),
            ),
        };
        Ok(Arc::new(arr))
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

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow_array::cast::AsArray;
    use arrow_array::ArrayRef;
    use arrow_schema::Field;
    use {arrow_array_deltalake as arrow_array, arrow_schema_deltalake as arrow_schema};

    use crate::array::{ArrayImpl, Decimal, DecimalArray, ListArray, ListValue};
    use crate::buffer::Bitmap;

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
                false,
            ))),
            false,
        )]);

        let record_batch =
            super::to_deltalake_record_batch_with_schema(Arc::new(schema), &chunk).unwrap();
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
