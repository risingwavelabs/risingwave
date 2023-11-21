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

use chrono::{NaiveDateTime, NaiveTime};
use deltalake::arrow::array;
use deltalake::arrow::array::Array as ArrowArray;
use deltalake::arrow::compute::kernels::cast;
use deltalake::arrow::datatypes::{Field, Schema, SchemaRef, DECIMAL256_MAX_PRECISION};
use itertools::Itertools;

use super::*;
use crate::types::StructType;
use crate::util::iter_util::{ZipEqDebug, ZipEqFast};

/// Converts RisingWave array to Arrow array with the schema.
/// This function will try to convert the array if the type is not same with the schema.
pub fn to_record_batch_with_schema_arrow46(
    schema: SchemaRef,
    chunk: &DataChunk,
) -> Result<array::RecordBatch, ArrayError> {
    if !chunk.is_compacted() {
        let c = chunk.clone();
        return to_record_batch_with_schema_arrow46(schema, &c.compact());
    }
    let columns: Vec<_> = chunk
        .columns()
        .iter()
        .zip_eq_fast(schema.fields().iter())
        .map(|(column, field)| {
            let column: array::ArrayRef = column.as_ref().try_into()?;
            if column.data_type() == field.data_type() {
                Ok(column)
            } else {
                cast(&column, field.data_type())
                    .map_err(|err| ArrayError::FromArrow(err.to_string()))
            }
        })
        .try_collect::<_, _, ArrayError>()?;

    let opts = array::RecordBatchOptions::default().with_row_count(Some(chunk.capacity()));
    array::RecordBatch::try_new_with_options(schema, columns, &opts)
        .map_err(|err| ArrayError::ToArrow(err.to_string()))
}

// Implement bi-directional `From` between `DataChunk` and `array::RecordBatch`.
impl TryFrom<&DataChunk> for array::RecordBatch {
    type Error = ArrayError;

    fn try_from(chunk: &DataChunk) -> Result<Self, Self::Error> {
        if !chunk.is_compacted() {
            let c = chunk.clone();
            return Self::try_from(&c.compact());
        }
        let columns: Vec<_> = chunk
            .columns()
            .iter()
            .map(|column| column.as_ref().try_into())
            .try_collect::<_, _, Self::Error>()?;

        let fields: Vec<_> = columns
            .iter()
            .map(|array: &Arc<dyn ArrowArray>| {
                let nullable = array.null_count() > 0;
                let data_type = array.data_type().clone();
                Field::new("", data_type, nullable)
            })
            .collect();

        let schema = Arc::new(Schema::new(fields));
        let opts = array::RecordBatchOptions::default().with_row_count(Some(chunk.capacity()));
        array::RecordBatch::try_new_with_options(schema, columns, &opts)
            .map_err(|err| ArrayError::ToArrow(err.to_string()))
    }
}

impl TryFrom<&array::RecordBatch> for DataChunk {
    type Error = ArrayError;

    fn try_from(batch: &array::RecordBatch) -> Result<Self, Self::Error> {
        let mut columns = Vec::with_capacity(batch.num_columns());
        for array in batch.columns() {
            let column = Arc::new(array.try_into()?);
            columns.push(column);
        }
        Ok(DataChunk::new(columns, batch.num_rows()))
    }
}

/// Implement bi-directional `From` between `ArrayImpl` and `array::ArrayRef`.
macro_rules! converts_generic {
    ($({ $ArrowType:ty, $ArrowPattern:pat, $ArrayImplPattern:path }),*) => {
        // RisingWave array -> Arrow array
        impl TryFrom<&ArrayImpl> for array::ArrayRef {
            type Error = ArrayError;
            fn try_from(array: &ArrayImpl) -> Result<Self, Self::Error> {
                match array {
                    $($ArrayImplPattern(a) => Ok(Arc::new(<$ArrowType>::try_from(a)?)),)*
                    _ => todo!("unsupported array"),
                }
            }
        }
        // Arrow array -> RisingWave array
        impl TryFrom<&array::ArrayRef> for ArrayImpl {
            type Error = ArrayError;
            fn try_from(array: &array::ArrayRef) -> Result<Self, Self::Error> {
                use deltalake::arrow::datatypes::DataType::*;
                use deltalake::arrow::datatypes::IntervalUnit::*;
                use deltalake::arrow::datatypes::TimeUnit::*;
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
    { array::Int16Array, Int16, ArrayImpl::Int16 },
    { array::Int32Array, Int32, ArrayImpl::Int32 },
    { array::Int64Array, Int64, ArrayImpl::Int64 },
    { array::Float32Array, Float32, ArrayImpl::Float32 },
    { array::Float64Array, Float64, ArrayImpl::Float64 },
    { array::StringArray, Utf8, ArrayImpl::Utf8 },
    { array::BooleanArray, Boolean, ArrayImpl::Bool },
    { array::Decimal128Array, Decimal128(_, _), ArrayImpl::Decimal },
    // { array::Decimal256Array, Decimal256(_, _), ArrayImpl::Int256 },
    { array::Date32Array, Date32, ArrayImpl::Date },
    { array::TimestampMicrosecondArray, Timestamp(Microsecond, None), ArrayImpl::Timestamp },
    { array::TimestampMicrosecondArray, Timestamp(Microsecond, Some(_)), ArrayImpl::Timestamptz },
    { array::Time64MicrosecondArray, Time64(Microsecond), ArrayImpl::Time },
    { array::IntervalMonthDayNanoArray, Interval(MonthDayNano), ArrayImpl::Interval },
    { array::StructArray, Struct(_), ArrayImpl::Struct },
    { array::ListArray, List(_), ArrayImpl::List },
    { array::BinaryArray, Binary, ArrayImpl::Bytea },
    { array::LargeStringArray, LargeUtf8, ArrayImpl::Jsonb }    // we use LargeUtf8 to represent Jsonb in arrow
}

// Arrow Datatype -> Risingwave Datatype
impl From<&deltalake::arrow::datatypes::DataType> for DataType {
    fn from(value: &deltalake::arrow::datatypes::DataType) -> Self {
        use deltalake::arrow::datatypes::DataType::*;
        use deltalake::arrow::datatypes::IntervalUnit::*;
        use deltalake::arrow::datatypes::TimeUnit::*;
        match value {
            Boolean => Self::Boolean,
            Int16 => Self::Int16,
            Int32 => Self::Int32,
            Int64 => Self::Int64,
            Float32 => Self::Float32,
            Float64 => Self::Float64,
            Decimal128(_, _) => Self::Decimal,
            Decimal256(_, _) => Self::Int256,
            Date32 => Self::Date,
            Time64(Microsecond) => Self::Time,
            Timestamp(Microsecond, None) => Self::Timestamp,
            Timestamp(Microsecond, Some(_)) => Self::Timestamptz,
            Interval(MonthDayNano) => Self::Interval,
            Binary => Self::Bytea,
            Utf8 => Self::Varchar,
            LargeUtf8 => Self::Jsonb,
            Struct(fields) => Self::Struct(fields.into()),
            List(field) => Self::List(Box::new(field.data_type().into())),
            _ => todo!("Unsupported arrow data type: {value:?}"),
        }
    }
}

impl From<&deltalake::arrow::datatypes::Fields> for StructType {
    fn from(fields: &deltalake::arrow::datatypes::Fields) -> Self {
        Self::new(
            fields
                .iter()
                .map(|f| (f.name().clone(), f.data_type().into()))
                .collect(),
        )
    }
}

impl From<deltalake::arrow::datatypes::DataType> for DataType {
    fn from(value: deltalake::arrow::datatypes::DataType) -> Self {
        (&value).into()
    }
}

impl TryFrom<&DataType> for deltalake::arrow::datatypes::DataType {
    type Error = String;

    fn try_from(value: &DataType) -> Result<Self, Self::Error> {
        match value {
            DataType::Boolean => Ok(Self::Boolean),
            DataType::Int16 => Ok(Self::Int16),
            DataType::Int32 => Ok(Self::Int32),
            DataType::Int64 => Ok(Self::Int64),
            DataType::Int256 => Ok(Self::Decimal256(DECIMAL256_MAX_PRECISION, 0)),
            DataType::Float32 => Ok(Self::Float32),
            DataType::Float64 => Ok(Self::Float64),
            DataType::Date => Ok(Self::Date32),
            DataType::Timestamp => Ok(Self::Timestamp(
                deltalake::arrow::datatypes::TimeUnit::Microsecond,
                None,
            )),
            DataType::Timestamptz => Ok(Self::Timestamp(
                deltalake::arrow::datatypes::TimeUnit::Microsecond,
                Some("+00:00".into()),
            )),
            DataType::Time => Ok(Self::Time64(
                deltalake::arrow::datatypes::TimeUnit::Microsecond,
            )),
            DataType::Interval => Ok(Self::Interval(
                deltalake::arrow::datatypes::IntervalUnit::MonthDayNano,
            )),
            DataType::Varchar => Ok(Self::Utf8),
            DataType::Jsonb => Ok(Self::LargeUtf8),
            DataType::Bytea => Ok(Self::Binary),
            DataType::Decimal => Ok(Self::Decimal128(38, 0)), // arrow precision can not be 0
            DataType::Struct(struct_type) => Ok(Self::Struct(
                struct_type
                    .iter()
                    .map(|(name, ty)| Ok(Field::new(name, ty.try_into()?, true)))
                    .try_collect::<_, _, String>()?,
            )),
            DataType::List(datatype) => Ok(Self::List(Arc::new(Field::new(
                "item",
                datatype.as_ref().try_into()?,
                true,
            )))),
            DataType::Serial => Err("Serial type is not supported to convert to arrow".to_string()),
        }
    }
}

impl TryFrom<DataType> for deltalake::arrow::datatypes::DataType {
    type Error = String;

    fn try_from(value: DataType) -> Result<Self, Self::Error> {
        (&value).try_into()
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
converts!(BoolArray, array::BooleanArray);
converts!(I16Array, array::Int16Array);
converts!(I32Array, array::Int32Array);
converts!(I64Array, array::Int64Array);
converts!(F32Array, array::Float32Array, @map);
converts!(F64Array, array::Float64Array, @map);
converts!(BytesArray, array::BinaryArray);
converts!(Utf8Array, array::StringArray);
converts!(DateArray, array::Date32Array, @map);
converts!(TimeArray, array::Time64MicrosecondArray, @map);
converts!(TimestampArray, array::TimestampMicrosecondArray, @map);
converts!(TimestamptzArray, array::TimestampMicrosecondArray, @map);
converts!(IntervalArray, array::IntervalMonthDayNanoArray, @map);

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
        Date(array::types::Date32Type::to_naive_date(value))
    }

    fn into_arrow(self) -> Self::ArrowType {
        array::types::Date32Type::from_naive_date(self.0)
    }
}

impl FromIntoArrow for Time {
    type ArrowType = i64;

    fn from_arrow(value: Self::ArrowType) -> Self {
        Time(
            NaiveTime::from_num_seconds_from_midnight_opt(
                (value / 1_000_000) as _,
                (value % 1_000_000 * 1000) as _,
            )
            .unwrap(),
        )
    }

    fn into_arrow(self) -> Self::ArrowType {
        self.0
            .signed_duration_since(NaiveTime::default())
            .num_microseconds()
            .unwrap()
    }
}

impl FromIntoArrow for Timestamp {
    type ArrowType = i64;

    fn from_arrow(value: Self::ArrowType) -> Self {
        Timestamp(
            NaiveDateTime::from_timestamp_opt(
                (value / 1_000_000) as _,
                (value % 1_000_000 * 1000) as _,
            )
            .unwrap(),
        )
    }

    fn into_arrow(self) -> Self::ArrowType {
        self.0
            .signed_duration_since(NaiveDateTime::default())
            .num_microseconds()
            .unwrap()
    }
}

impl FromIntoArrow for Timestamptz {
    type ArrowType = i64;

    fn from_arrow(value: Self::ArrowType) -> Self {
        Timestamptz::from_micros(value)
    }

    fn into_arrow(self) -> Self::ArrowType {
        self.timestamp_micros()
    }
}

impl FromIntoArrow for Interval {
    type ArrowType = i128;

    fn from_arrow(value: Self::ArrowType) -> Self {
        // XXX: the arrow-rs decoding is incorrect
        // let (months, days, ns) = array::types::IntervalMonthDayNanoType::to_parts(value);
        let months = value as i32;
        let days = (value >> 32) as i32;
        let ns = (value >> 64) as i64;
        Interval::from_month_day_usec(months, days, ns / 1000)
    }

    fn into_arrow(self) -> Self::ArrowType {
        // XXX: the arrow-rs encoding is incorrect
        // array::types::IntervalMonthDayNanoType::make_value(
        //     self.months(),
        //     self.days(),
        //     // TODO: this may overflow and we need `try_into`
        //     self.usecs() * 1000,
        // )
        let m = self.months() as u128 & u32::MAX as u128;
        let d = (self.days() as u128 & u32::MAX as u128) << 32;
        let n = ((self.usecs() * 1000) as u128 & u64::MAX as u128) << 64;
        (m | d | n) as i128
    }
}

// RisingWave Decimal type is self-contained, but Arrow is not.
// In Arrow DecimalArray, the scale is stored in data type as metadata, and the mantissa is stored
// as i128 in the array.
impl From<&DecimalArray> for array::Decimal128Array {
    fn from(array: &DecimalArray) -> Self {
        let max_scale = array
            .iter()
            .filter_map(|o| o.map(|v| v.scale().unwrap_or(0)))
            .max()
            .unwrap_or(0) as u32;
        let mut builder =
            array::builder::Decimal128Builder::with_capacity(array.len()).with_data_type(
                deltalake::arrow::datatypes::DataType::Decimal128(38, max_scale as i8),
            );
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

impl From<&array::Decimal128Array> for DecimalArray {
    fn from(array: &array::Decimal128Array) -> Self {
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

impl From<&JsonbArray> for array::LargeStringArray {
    fn from(array: &JsonbArray) -> Self {
        let mut builder =
            array::builder::LargeStringBuilder::with_capacity(array.len(), array.len() * 16);
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

impl TryFrom<&array::LargeStringArray> for JsonbArray {
    type Error = ArrayError;

    fn try_from(array: &array::LargeStringArray) -> Result<Self, Self::Error> {
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

impl From<&ListArray> for array::ListArray {
    fn from(array: &ListArray) -> Self {
        use array::builder::*;
        fn build<A, B, F>(array: &ListArray, a: &A, builder: B, mut append: F) -> array::ListArray
        where
            A: Array,
            B: array::builder::ArrayBuilder,
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
            ArrayImpl::Int256(_) => panic!("cannot use int256"),
            ArrayImpl::Bool(a) => {
                build(array, a, BooleanBuilder::with_capacity(a.len()), |b, v| {
                    b.append_option(v)
                })
            }
            ArrayImpl::Decimal(a) => {
                let max_scale = a
                    .iter()
                    .filter_map(|o| o.map(|v| v.scale().unwrap_or(0)))
                    .max()
                    .unwrap_or(0) as u32;
                build(
                    array,
                    a,
                    Decimal128Builder::with_capacity(a.len()).with_data_type(
                        deltalake::arrow::datatypes::DataType::Decimal128(38, max_scale as i8),
                    ),
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

impl TryFrom<&array::ListArray> for ListArray {
    type Error = ArrayError;

    fn try_from(array: &array::ListArray) -> Result<Self, Self::Error> {
        let iter: Vec<_> = array
            .iter()
            .map(|o| o.map(|a| ArrayImpl::try_from(&a)).transpose())
            .try_collect()?;
        Ok(ListArray::from_iter(iter, (&array.value_type()).into()))
    }
}

impl TryFrom<&StructArray> for array::StructArray {
    type Error = ArrayError;

    fn try_from(array: &StructArray) -> Result<Self, Self::Error> {
        let struct_data_vector: Vec<(deltalake::arrow::datatypes::FieldRef, array::ArrayRef)> =
            array
                .fields()
                .zip_eq_debug(array.data_type().as_struct().iter())
                .map(|(arr, (name, ty))| {
                    Ok((
                        Field::new(name, ty.try_into().map_err(ArrayError::ToArrow)?, true).into(),
                        arr.as_ref().try_into()?,
                    ))
                })
                .try_collect::<_, _, ArrayError>()?;
        Ok(array::StructArray::from(struct_data_vector))
    }
}

impl TryFrom<&array::StructArray> for StructArray {
    type Error = ArrayError;

    fn try_from(array: &array::StructArray) -> Result<Self, Self::Error> {
        use array::Array;
        let deltalake::arrow::datatypes::DataType::Struct(fields) = array.data_type() else {
            panic!("nested field types cannot be determined.");
        };
        Ok(StructArray::new(
            fields.into(),
            array
                .columns()
                .iter()
                .map(|a| ArrayImpl::try_from(a).map(Arc::new))
                .try_collect()?,
            (0..array.len()).map(|i| !array.is_null(i)).collect(),
        ))
    }
}
