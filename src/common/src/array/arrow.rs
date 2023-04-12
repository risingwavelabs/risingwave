// Copyright 2023 RisingWave Labs
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

//! Converts between arrays and Apache Arrow arrays.

use std::fmt::Write;

use arrow_schema::{Field, DECIMAL256_MAX_PRECISION};
use chrono::{NaiveDateTime, NaiveTime};
use itertools::Itertools;

use super::column::Column;
use super::*;
use crate::types::num256::Int256;
use crate::types::struct_type::StructType;
use crate::util::iter_util::ZipEqFast;

// Implement bi-directional `From` between `DataChunk` and `arrow_array::RecordBatch`.

impl From<&DataChunk> for arrow_array::RecordBatch {
    fn from(chunk: &DataChunk) -> Self {
        arrow_array::RecordBatch::try_from_iter(
            chunk
                .columns()
                .iter()
                .map(|column| ("", column.array_ref().into())),
        )
        .unwrap()
    }
}

impl TryFrom<&arrow_array::RecordBatch> for DataChunk {
    type Error = ArrayError;

    fn try_from(batch: &arrow_array::RecordBatch) -> Result<Self, Self::Error> {
        let mut columns = Vec::with_capacity(batch.num_columns());
        for array in batch.columns() {
            let column = Column::new(Arc::new(array.try_into()?));
            columns.push(column);
        }
        Ok(DataChunk::new(columns, batch.num_rows()))
    }
}

/// Implement bi-directional `From` between `ArrayImpl` and `arrow_array::ArrayRef`.
macro_rules! converts_generic {
    ($({ $ArrowType:ty, $ArrowPattern:pat, $ArrayImplPattern:path }),*) => {
        // RisingWave array -> Arrow array
        impl From<&ArrayImpl> for arrow_array::ArrayRef {
            fn from(array: &ArrayImpl) -> Self {
                match array {
                    $($ArrayImplPattern(a) => Arc::new(<$ArrowType>::from(a)),)*
                    _ => todo!("unsupported array"),
                }
            }
        }
        // Arrow array -> RisingWave array
        impl TryFrom<&arrow_array::ArrayRef> for ArrayImpl {
            type Error = ArrayError;
            fn try_from(array: &arrow_array::ArrayRef) -> Result<Self, Self::Error> {
                use arrow_schema::DataType::*;
                use arrow_schema::IntervalUnit::*;
                use arrow_schema::TimeUnit::*;
                match array.data_type() {
                    $($ArrowPattern => Ok($ArrayImplPattern(
                        array
                            .as_any()
                            .downcast_ref::<$ArrowType>()
                            .unwrap()
                            .try_into()?,
                    )),)*
                    t => Err(ArrayError::FromArrow(format!("unsupported data type: {t:?}"))),
                }
            }
        }
    };
}
converts_generic! {
    { arrow_array::Int16Array, Int16, ArrayImpl::Int16 },
    { arrow_array::Int32Array, Int32, ArrayImpl::Int32 },
    { arrow_array::Int64Array, Int64, ArrayImpl::Int64 },
    { arrow_array::Float32Array, Float32, ArrayImpl::Float32 },
    { arrow_array::Float64Array, Float64, ArrayImpl::Float64 },
    { arrow_array::StringArray, Utf8, ArrayImpl::Utf8 },
    { arrow_array::BooleanArray, Boolean, ArrayImpl::Bool },
    { arrow_array::Decimal128Array, Decimal128(_, _), ArrayImpl::Decimal },
    { arrow_array::Decimal256Array, Decimal256(_, _), ArrayImpl::Int256 },
    { arrow_array::IntervalMonthDayNanoArray, Interval(MonthDayNano), ArrayImpl::Interval },
    { arrow_array::Date32Array, Date32, ArrayImpl::Date },
    { arrow_array::TimestampNanosecondArray, Timestamp(Nanosecond, _), ArrayImpl::Timestamp },
    { arrow_array::Time64NanosecondArray, Time64(Nanosecond), ArrayImpl::Time },
    { arrow_array::StructArray, Struct(_), ArrayImpl::Struct },
    { arrow_array::ListArray, List(_), ArrayImpl::List },
    { arrow_array::BinaryArray, Binary, ArrayImpl::Bytea },
    { arrow_array::LargeStringArray, LargeUtf8, ArrayImpl::Jsonb }    // we use LargeUtf8 to represent Jsonb in arrow
}

// Arrow Datatype -> Risingwave Datatype
impl From<&arrow_schema::DataType> for DataType {
    fn from(value: &arrow_schema::DataType) -> Self {
        use arrow_schema::DataType::*;
        match value {
            Boolean => Self::Boolean,
            Int16 => Self::Int16,
            Int32 => Self::Int32,
            Int64 => Self::Int64,
            Float32 => Self::Float32,
            Float64 => Self::Float64,
            Timestamp(_, _) => Self::Timestamp, // TODO: check time unit
            Date32 => Self::Date,
            Time64(_) => Self::Time,
            Interval(_) => Self::Interval, // TODO: check time unit
            Binary => Self::Bytea,
            Utf8 => Self::Varchar,
            LargeUtf8 => Self::Jsonb,
            Struct(field) => Self::Struct(Arc::new(struct_type::StructType {
                fields: field.iter().map(|f| f.data_type().into()).collect(),
                field_names: field.iter().map(|f| f.name().clone()).collect(),
            })),
            List(field) => Self::List {
                datatype: Box::new(field.data_type().into()),
            },
            Decimal128(_, _) => Self::Decimal,
            Decimal256(_, _) => Self::Int256,
            _ => todo!("Unsupported arrow data type: {value:?}"),
        }
    }
}

impl From<arrow_schema::DataType> for DataType {
    fn from(value: arrow_schema::DataType) -> Self {
        (&value).into()
    }
}

impl From<&DataType> for arrow_schema::DataType {
    fn from(value: &DataType) -> Self {
        match value {
            DataType::Boolean => Self::Boolean,
            DataType::Int16 => Self::Int16,
            DataType::Int32 => Self::Int32,
            DataType::Int64 => Self::Int64,
            DataType::Int256 => Self::Decimal256(DECIMAL256_MAX_PRECISION, 0),
            DataType::Float32 => Self::Float32,
            DataType::Float64 => Self::Float64,
            DataType::Date => Self::Date32,
            DataType::Timestamp => Self::Timestamp(arrow_schema::TimeUnit::Millisecond, None),
            DataType::Time => Self::Time64(arrow_schema::TimeUnit::Millisecond),
            DataType::Interval => Self::Interval(arrow_schema::IntervalUnit::DayTime),
            DataType::Varchar => Self::Utf8,
            DataType::Jsonb => Self::LargeUtf8,
            DataType::Bytea => Self::Binary,
            DataType::Decimal => Self::Decimal128(28, 0), // arrow precision can not be 0
            DataType::Struct(struct_type) => {
                Self::Struct(get_field_vector_from_struct_type(struct_type))
            }
            DataType::List { datatype } => {
                Self::List(Box::new(Field::new("item", datatype.as_ref().into(), true)))
            }
            _ => todo!("Unsupported arrow data type: {value:?}"),
        }
    }
}

impl From<DataType> for arrow_schema::DataType {
    fn from(value: DataType) -> Self {
        (&value).into()
    }
}

fn get_field_vector_from_struct_type(struct_type: &StructType) -> Vec<Field> {
    // Check for length equality between field_name vector and datatype vector.
    if struct_type.field_names.len() != struct_type.fields.len() {
        struct_type
            .fields
            .iter()
            .map(|f| Field::new("", f.into(), true))
            .collect()
    } else {
        struct_type
            .fields
            .iter()
            .zip_eq_fast(struct_type.field_names.clone())
            .map(|(f, f_name)| Field::new(f_name, f.into(), true))
            .collect()
    }
}

/// Implement bi-directional `From` between concrete array types.
macro_rules! converts {
    ($ArrayType:ty, $ArrowType:ty) => {
        impl From<&$ArrayType> for $ArrowType {
            fn from(array: &$ArrayType) -> Self {
                array.iter().collect()
            }
        }
        impl From<&$ArrowType> for $ArrayType {
            fn from(array: &$ArrowType) -> Self {
                array.iter().collect()
            }
        }
        impl From<&[$ArrowType]> for $ArrayType {
            fn from(arrays: &[$ArrowType]) -> Self {
                arrays.iter().flat_map(|a| a.iter()).collect()
            }
        }
    };
    // convert values using FromIntoArrow
    ($ArrayType:ty, $ArrowType:ty, @map) => {
        impl From<&$ArrayType> for $ArrowType {
            fn from(array: &$ArrayType) -> Self {
                array.iter().map(|o| o.map(|v| v.into_arrow())).collect()
            }
        }
        impl From<&$ArrowType> for $ArrayType {
            fn from(array: &$ArrowType) -> Self {
                array
                    .iter()
                    .map(|o| {
                        o.map(|v| {
                            <<$ArrayType as Array>::RefItem<'_> as FromIntoArrow>::from_arrow(v)
                        })
                    })
                    .collect()
            }
        }
        impl From<&[$ArrowType]> for $ArrayType {
            fn from(arrays: &[$ArrowType]) -> Self {
                arrays
                    .iter()
                    .flat_map(|a| a.iter())
                    .map(|o| {
                        o.map(|v| {
                            <<$ArrayType as Array>::RefItem<'_> as FromIntoArrow>::from_arrow(v)
                        })
                    })
                    .collect()
            }
        }
    };
}
converts!(BoolArray, arrow_array::BooleanArray);
converts!(I16Array, arrow_array::Int16Array);
converts!(I32Array, arrow_array::Int32Array);
converts!(I64Array, arrow_array::Int64Array);
converts!(F32Array, arrow_array::Float32Array, @map);
converts!(F64Array, arrow_array::Float64Array, @map);
converts!(BytesArray, arrow_array::BinaryArray);
converts!(Utf8Array, arrow_array::StringArray);
converts!(DateArray, arrow_array::Date32Array, @map);
converts!(TimeArray, arrow_array::Time64NanosecondArray, @map);
converts!(TimestampArray, arrow_array::TimestampNanosecondArray, @map);
converts!(IntervalArray, arrow_array::IntervalMonthDayNanoArray, @map);

/// Converts RisingWave value from and into Arrow value.
trait FromIntoArrow {
    /// The corresponding element type in the Arrow array.
    type ArrowType;
    fn from_arrow(value: Self::ArrowType) -> Self;
    fn into_arrow(self) -> Self::ArrowType;
}

impl FromIntoArrow for F32 {
    type ArrowType = f32;

    fn from_arrow(value: Self::ArrowType) -> Self {
        value.into()
    }

    fn into_arrow(self) -> Self::ArrowType {
        self.into()
    }
}

impl FromIntoArrow for F64 {
    type ArrowType = f64;

    fn from_arrow(value: Self::ArrowType) -> Self {
        value.into()
    }

    fn into_arrow(self) -> Self::ArrowType {
        self.into()
    }
}

impl FromIntoArrow for Date {
    type ArrowType = i32;

    fn from_arrow(value: Self::ArrowType) -> Self {
        Date(arrow_array::types::Date32Type::to_naive_date(value))
    }

    fn into_arrow(self) -> Self::ArrowType {
        arrow_array::types::Date32Type::from_naive_date(self.0)
    }
}

impl FromIntoArrow for Time {
    type ArrowType = i64;

    fn from_arrow(value: Self::ArrowType) -> Self {
        Time(
            NaiveTime::from_num_seconds_from_midnight_opt(
                (value / 1_000_000_000) as _,
                (value % 1_000_000_000) as _,
            )
            .unwrap(),
        )
    }

    fn into_arrow(self) -> Self::ArrowType {
        self.0
            .signed_duration_since(NaiveTime::default())
            .num_nanoseconds()
            .unwrap()
    }
}

impl FromIntoArrow for Timestamp {
    type ArrowType = i64;

    fn from_arrow(value: Self::ArrowType) -> Self {
        Timestamp(
            NaiveDateTime::from_timestamp_opt(
                (value / 1_000_000_000) as _,
                (value % 1_000_000_000) as _,
            )
            .unwrap(),
        )
    }

    fn into_arrow(self) -> Self::ArrowType {
        self.0
            .signed_duration_since(NaiveDateTime::default())
            .num_nanoseconds()
            .unwrap()
    }
}

impl FromIntoArrow for Interval {
    type ArrowType = i128;

    fn from_arrow(value: Self::ArrowType) -> Self {
        let (months, days, ns) = arrow_array::types::IntervalMonthDayNanoType::to_parts(value);
        Interval::from_month_day_usec(months, days, ns / 1000)
    }

    fn into_arrow(self) -> Self::ArrowType {
        arrow_array::types::IntervalMonthDayNanoType::make_value(
            self.months(),
            self.days(),
            // TODO: this may overflow and we need `try_into`
            self.usecs() * 1000,
        )
    }
}

// RisingWave Decimal type is self-contained, but Arrow is not.
// In Arrow DecimalArray, the scale is stored in data type as metadata, and the mantissa is stored
// as i128 in the array.
impl From<&DecimalArray> for arrow_array::Decimal128Array {
    fn from(array: &DecimalArray) -> Self {
        let max_scale = array
            .iter()
            .filter_map(|o| o.map(|v| v.scale()))
            .max()
            .unwrap_or(0) as u32;
        let mut builder = arrow_array::builder::Decimal128Builder::with_capacity(array.len())
            .with_data_type(arrow_schema::DataType::Decimal128(28, max_scale as i8));
        for value in array.iter() {
            builder.append_option(value.map(|d| decimal_to_i128(d, max_scale)));
        }
        builder.finish()
    }
}

fn decimal_to_i128(value: Decimal, scale: u32) -> i128 {
    match value {
        Decimal::Normalized(mut d) => {
            d.rescale(scale);
            d.mantissa()
        }
        Decimal::NaN => i128::MIN + 1,
        Decimal::PositiveInf => i128::MAX,
        Decimal::NegativeInf => i128::MIN,
    }
}

impl From<&arrow_array::Decimal128Array> for DecimalArray {
    fn from(array: &arrow_array::Decimal128Array) -> Self {
        assert!(array.scale() >= 0, "todo: support negative scale");
        let from_arrow = |value| {
            const NAN: i128 = i128::MIN + 1;
            match value {
                NAN => Decimal::NaN,
                i128::MAX => Decimal::PositiveInf,
                i128::MIN => Decimal::NegativeInf,
                _ => Decimal::Normalized(rust_decimal::Decimal::from_i128_with_scale(
                    value,
                    array.scale() as u32,
                )),
            }
        };
        array.iter().map(|o| o.map(from_arrow)).collect()
    }
}

impl From<&JsonbArray> for arrow_array::LargeStringArray {
    fn from(array: &JsonbArray) -> Self {
        let mut builder =
            arrow_array::builder::LargeStringBuilder::with_capacity(array.len(), array.len() * 16);
        for value in array.iter() {
            match value {
                Some(jsonb) => {
                    write!(&mut builder, "{}", jsonb).unwrap();
                    builder.append_value("");
                }
                None => builder.append_null(),
            }
        }
        builder.finish()
    }
}

impl TryFrom<&arrow_array::LargeStringArray> for JsonbArray {
    type Error = ArrayError;

    fn try_from(array: &arrow_array::LargeStringArray) -> Result<Self, Self::Error> {
        array
            .iter()
            .map(|o| {
                o.map(|s| {
                    s.parse()
                        .map_err(|_| ArrayError::FromArrow(format!("invalid json: {s}")))
                })
                .transpose()
            })
            .try_collect()
    }
}

impl From<&Int256Array> for arrow_array::Decimal256Array {
    fn from(array: &Int256Array) -> Self {
        array
            .iter()
            .map(|o| o.map(arrow_buffer::i256::from))
            .collect()
    }
}

impl From<&arrow_array::Decimal256Array> for Int256Array {
    fn from(array: &arrow_array::Decimal256Array) -> Self {
        let values = array.iter().map(|o| o.map(Int256::from)).collect_vec();

        values
            .iter()
            .map(|i| i.as_ref().map(|v| v.as_scalar_ref()))
            .collect()
    }
}

impl From<&ListArray> for arrow_array::ListArray {
    fn from(array: &ListArray) -> Self {
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
        match &*array.value {
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
                    arrow_schema::DataType::Decimal256(DECIMAL256_MAX_PRECISION, 0),
                ),
                |b, v| b.append_option(v.map(Into::into)),
            ),
            ArrayImpl::Bool(a) => {
                build(array, a, BooleanBuilder::with_capacity(a.len()), |b, v| {
                    b.append_option(v)
                })
            }
            ArrayImpl::Decimal(a) => {
                let max_scale = a
                    .iter()
                    .filter_map(|o| o.map(|v| v.scale()))
                    .max()
                    .unwrap_or(0) as u32;
                build(
                    array,
                    a,
                    Decimal128Builder::with_capacity(a.len())
                        .with_data_type(arrow_schema::DataType::Decimal128(28, max_scale as i8)),
                    |b, v| b.append_option(v.map(|d| decimal_to_i128(d, max_scale))),
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
                TimestampNanosecondBuilder::with_capacity(a.len()),
                |b, v| b.append_option(v.map(|d| d.into_arrow())),
            ),
            ArrayImpl::Time(a) => build(
                array,
                a,
                Time64NanosecondBuilder::with_capacity(a.len()),
                |b, v| b.append_option(v.map(|d| d.into_arrow())),
            ),
            ArrayImpl::Jsonb(a) => build(
                array,
                a,
                LargeStringBuilder::with_capacity(a.len(), a.len() * 16),
                |b, v| b.append_option(v.map(|j| j.to_string())),
            ),
            ArrayImpl::Serial(_) => todo!("list of serial"),
            ArrayImpl::Struct(_) => todo!("list of struct"),
            ArrayImpl::List(_) => todo!("list of list"),
            ArrayImpl::Bytea(a) => build(
                array,
                a,
                BinaryBuilder::with_capacity(a.len(), a.data().len()),
                |b, v| b.append_option(v),
            ),
        }
    }
}

impl TryFrom<&arrow_array::ListArray> for ListArray {
    type Error = ArrayError;

    fn try_from(array: &arrow_array::ListArray) -> Result<Self, Self::Error> {
        let iter: Vec<_> = array
            .iter()
            .map(|o| o.map(|a| ArrayImpl::try_from(&a)).transpose())
            .try_collect()?;
        Ok(ListArray::from_iter(iter, (&array.value_type()).into()))
    }
}

impl From<&StructArray> for arrow_array::StructArray {
    fn from(array: &StructArray) -> Self {
        let struct_data_vector: Vec<(arrow_schema::Field, arrow_array::ArrayRef)> =
            if array.children_names().len() != array.children_array_types().len() {
                array
                    .fields()
                    .zip_eq_fast(array.children_array_types())
                    .map(|(arr, datatype)| (Field::new("", datatype.into(), true), arr.into()))
                    .collect()
            } else {
                array
                    .fields()
                    .zip_eq_fast(array.children_array_types())
                    .zip_eq_fast(array.children_names())
                    .map(|((arr, datatype), field_name)| {
                        (Field::new(field_name, datatype.into(), true), arr.into())
                    })
                    .collect()
            };
        arrow_array::StructArray::from(struct_data_vector)
    }
}

impl TryFrom<&arrow_array::StructArray> for StructArray {
    type Error = ArrayError;

    fn try_from(array: &arrow_array::StructArray) -> Result<Self, Self::Error> {
        let mut null_bitmap = Vec::new();
        for i in 0..arrow_array::Array::len(&array) {
            null_bitmap.push(!arrow_array::Array::is_null(&array, i))
        }
        Ok(match arrow_array::Array::data_type(&array) {
            arrow_schema::DataType::Struct(fields) => StructArray::from_slices_with_field_names(
                &(null_bitmap),
                array
                    .columns()
                    .iter()
                    .map(ArrayImpl::try_from)
                    .try_collect()?,
                fields
                    .iter()
                    .map(|f| DataType::from(f.data_type()))
                    .collect(),
                array.column_names().into_iter().map(String::from).collect(),
            ),
            _ => panic!("nested field types cannot be determined."),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::interval::test_utils::IntervalTestExt;
    use crate::{array, empty_array};

    #[test]
    fn bool() {
        let array = BoolArray::from_iter([None, Some(false), Some(true)]);
        let arrow = arrow_array::BooleanArray::from(&array);
        assert_eq!(BoolArray::from(&arrow), array);
    }

    #[test]
    fn i16() {
        let array = I16Array::from_iter([None, Some(-7), Some(25)]);
        let arrow = arrow_array::Int16Array::from(&array);
        assert_eq!(I16Array::from(&arrow), array);
    }

    #[test]
    fn f32() {
        let array = F32Array::from_iter([None, Some(F32::from(-7.0)), Some(F32::from(25.0))]);
        let arrow = arrow_array::Float32Array::from(&array);
        assert_eq!(F32Array::from(&arrow), array);
    }

    #[test]
    fn date() {
        let array = DateArray::from_iter([
            None,
            Date::with_days(12345).ok(),
            Date::with_days(-12345).ok(),
        ]);
        let arrow = arrow_array::Date32Array::from(&array);
        assert_eq!(DateArray::from(&arrow), array);
    }

    #[test]
    fn time() {
        let array = TimeArray::from_iter([
            None,
            Time::with_secs_nano(12345, 123456789).ok(),
            Time::with_secs_nano(1, 0).ok(),
        ]);
        let arrow = arrow_array::Time64NanosecondArray::from(&array);
        assert_eq!(TimeArray::from(&arrow), array);
    }

    #[test]
    fn timestamp() {
        let array = TimestampArray::from_iter([
            None,
            Timestamp::with_secs_nsecs(12345, 123456789).ok(),
            Timestamp::with_secs_nsecs(1, 0).ok(),
        ]);
        let arrow = arrow_array::TimestampNanosecondArray::from(&array);
        assert_eq!(TimestampArray::from(&arrow), array);
    }

    #[test]
    fn interval() {
        let array = IntervalArray::from_iter([
            None,
            Some(Interval::from_millis(123456789)),
            Some(Interval::from_millis(-123456789)),
        ]);
        let arrow = arrow_array::IntervalMonthDayNanoArray::from(&array);
        assert_eq!(IntervalArray::from(&arrow), array);
    }

    #[test]
    fn string() {
        let array = Utf8Array::from_iter([None, Some("array"), Some("arrow")]);
        let arrow = arrow_array::StringArray::from(&array);
        assert_eq!(Utf8Array::from(&arrow), array);
    }

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
        let arrow = arrow_array::Decimal128Array::from(&array);
        assert_eq!(DecimalArray::from(&arrow), array);
    }

    #[test]
    fn jsonb() {
        let array = JsonbArray::from_iter([
            None,
            Some("null".parse().unwrap()),
            Some("false".parse().unwrap()),
            Some("1".parse().unwrap()),
            Some("[1, 2, 3]".parse().unwrap()),
            Some(r#"{ "a": 1, "b": null }"#.parse().unwrap()),
        ]);
        let arrow = arrow_array::LargeStringArray::from(&array);
        assert_eq!(JsonbArray::try_from(&arrow).unwrap(), array);
    }

    #[test]
    fn int256() {
        let values = vec![
            None,
            Some(Int256::from(1)),
            Some(Int256::from(i64::MAX)),
            Some(Int256::from(i64::MAX) * Int256::from(i64::MAX)),
            Some(Int256::from(i64::MAX) * Int256::from(i64::MAX) * Int256::from(i64::MAX)),
            Some(
                Int256::from(i64::MAX)
                    * Int256::from(i64::MAX)
                    * Int256::from(i64::MAX)
                    * Int256::from(i64::MAX),
            ),
            Some(Int256::min()),
            Some(Int256::max()),
        ];

        let array =
            Int256Array::from_iter(values.iter().map(|r| r.as_ref().map(|x| x.as_scalar_ref())));
        let arrow = arrow_array::Decimal256Array::from(&array);
        assert_eq!(Int256Array::from(&arrow), array);
    }

    #[test]
    fn struct_array() {
        use arrow_array::Array as _;

        // Empty array - risingwave to arrow conversion.
        let test_arr = StructArray::from_slices(&[true, false, true, false], vec![], vec![]);
        assert_eq!(arrow_array::StructArray::from(&test_arr).len(), 0);

        // Empty array - arrow to risingwave conversion.
        let test_arr_2 = arrow_array::StructArray::from(vec![]);
        assert_eq!(StructArray::try_from(&test_arr_2).unwrap().len(), 0);

        // Struct array with primitive types. arrow to risingwave conversion.
        let test_arrow_struct_array = arrow_array::StructArray::try_from(vec![
            (
                "a",
                Arc::new(arrow_array::BooleanArray::from(vec![
                    Some(false),
                    Some(false),
                    Some(true),
                    None,
                ])) as arrow_array::ArrayRef,
            ),
            (
                "b",
                Arc::new(arrow_array::Int32Array::from(vec![
                    Some(42),
                    Some(28),
                    Some(19),
                    None,
                ])) as arrow_array::ArrayRef,
            ),
        ])
        .unwrap();
        let actual_risingwave_struct_array =
            StructArray::try_from(&test_arrow_struct_array).unwrap();
        let expected_risingwave_struct_array = StructArray::from_slices_with_field_names(
            &[true, true, true, false],
            vec![
                array! { BoolArray, [Some(false), Some(false), Some(true), None]}.into(),
                array! { I32Array, [Some(42), Some(28), Some(19), None] }.into(),
            ],
            vec![DataType::Boolean, DataType::Int32],
            vec![String::from("a"), String::from("b")],
        );
        assert_eq!(
            expected_risingwave_struct_array,
            actual_risingwave_struct_array
        );
    }

    #[test]
    fn list() {
        let array = ListArray::from_iter(
            [
                Some(array! { I32Array, [None, Some(-7), Some(25)] }.into()),
                None,
                Some(array! { I32Array, [Some(0), Some(-127), Some(127), Some(50)] }.into()),
                Some(empty_array! { I32Array }.into()),
            ],
            DataType::Int32,
        );
        let arrow = arrow_array::ListArray::from(&array);
        assert_eq!(ListArray::try_from(&arrow).unwrap(), array);
    }
}
