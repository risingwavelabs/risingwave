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

use arrow_array::{ArrayRef, StructArray};
use arrow_schema::DataType;
use itertools::Itertools;
use num_traits::abs;

use super::{ToArrowArrayWithTypeConvert, ToArrowTypeConvert};
use crate::array::{Array, ArrayError, DataChunk, DecimalArray};
use crate::util::iter_util::ZipEqFast;

struct IcebergArrowConvert;

impl ToArrowTypeConvert for IcebergArrowConvert {
    #[inline]
    fn decimal_type_to_arrow(&self) -> arrow_schema::DataType {
        arrow_schema::DataType::Decimal128(arrow_schema::DECIMAL128_MAX_PRECISION, 0)
    }
}

impl ToArrowArrayWithTypeConvert for IcebergArrowConvert {
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
                    crate::array::Decimal::PositiveInf => None,
                    crate::array::Decimal::NegativeInf => None,
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
/// The behavior is specified for iceberg:
/// For different struct type, try to use fields in schema to cast.
pub fn to_iceberg_record_batch_with_schema(
    schema: arrow_schema::SchemaRef,
    chunk: &DataChunk,
) -> Result<arrow_array::RecordBatch, ArrayError> {
    if !chunk.is_compacted() {
        let c = chunk.clone();
        return to_iceberg_record_batch_with_schema(schema, &c.compact());
    }
    let columns: Vec<_> = chunk
        .columns()
        .iter()
        .zip_eq_fast(schema.fields().iter())
        .map(|(column, field)| {
            let column: arrow_array::ArrayRef =
                IcebergArrowConvert {}.to_arrow_with_type(field.data_type(), column)?;
            if column.data_type() == field.data_type() {
                Ok(column)
            } else if let DataType::Struct(actual) = column.data_type()
                && let DataType::Struct(expect) = field.data_type()
            {
                // Special case for iceberg
                if actual.len() != expect.len() {
                    return Err(ArrayError::to_arrow(format!(
                        "Struct field count mismatch, expect {}, actual {}",
                        expect.len(),
                        actual.len()
                    )));
                }
                let column = column
                    .as_any()
                    .downcast_ref::<arrow_array::StructArray>()
                    .unwrap()
                    .clone();
                let (_, struct_columns, nullable) = column.into_parts();
                Ok(Arc::new(
                    StructArray::try_new(expect.clone(), struct_columns, nullable)
                        .map_err(ArrayError::from_arrow)?,
                ) as ArrayRef)
            } else {
                arrow_cast::cast(&column, field.data_type()).map_err(ArrayError::from_arrow)
            }
        })
        .try_collect::<_, _, ArrayError>()?;

    let opts = arrow_array::RecordBatchOptions::default().with_row_count(Some(chunk.capacity()));
    arrow_array::RecordBatch::try_new_with_options(schema, columns, &opts)
        .map_err(ArrayError::to_arrow)
}

pub fn iceberg_to_arrow_type(
    data_type: &crate::array::DataType,
) -> Result<arrow_schema::DataType, ArrayError> {
    IcebergArrowConvert {}.to_arrow_type(data_type)
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow_array::ArrayRef;

    use crate::array::arrow::arrow_iceberg::IcebergArrowConvert;
    use crate::array::arrow::ToArrowArrayWithTypeConvert;
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
        let ty = arrow_schema::DataType::Decimal128(38, 3);
        let arrow_array = IcebergArrowConvert.decimal_to_arrow(&ty, &array).unwrap();
        let expect_array = Arc::new(
            arrow_array::Decimal128Array::from(vec![
                None,
                None,
                None,
                None,
                Some(123400),
                Some(123456),
            ])
            .with_data_type(ty),
        ) as ArrayRef;
        assert_eq!(&arrow_array, &expect_array);
    }
}
