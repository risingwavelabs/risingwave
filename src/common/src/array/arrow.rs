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
use arrow_schema::Field;
use chrono::{NaiveDateTime, NaiveTime};

use super::column::Column;
use super::*;
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

impl From<&arrow_array::RecordBatch> for DataChunk {
    fn from(batch: &arrow_array::RecordBatch) -> Self {
        DataChunk::new(
            batch
                .columns()
                .iter()
                .map(|array| Column::new(Arc::new(array.into())))
                .collect(),
            batch.num_rows(),
        )
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
        impl From<&arrow_array::ArrayRef> for ArrayImpl {
            fn from(array: &arrow_array::ArrayRef) -> Self {
                use arrow_schema::DataType::*;
                use arrow_schema::IntervalUnit::*;
                use arrow_schema::TimeUnit::*;
                match array.data_type() {
                    $($ArrowPattern => $ArrayImplPattern(
                        array
                            .as_any()
                            .downcast_ref::<$ArrowType>()
                            .unwrap()
                            .into(),
                    ),)*
                    t => todo!("Unsupported arrow data type: {t:?}"),
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
    { arrow_array::IntervalMonthDayNanoArray, Interval(MonthDayNano), ArrayImpl::Interval },
    { arrow_array::Date32Array, Date32, ArrayImpl::NaiveDate },
    { arrow_array::TimestampNanosecondArray, Timestamp(Nanosecond, _), ArrayImpl::NaiveDateTime },
    { arrow_array::Time64NanosecondArray, Time64(Nanosecond), ArrayImpl::NaiveTime },
    // { arrow_array::StructArray, Struct(_), ArrayImpl::Struct }, // TODO: convert struct
    { arrow_array::ListArray, List(_), ArrayImpl::List },
    { arrow_array::BinaryArray, Binary, ArrayImpl::Bytea }
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
            Struct(field) => Self::Struct(Arc::new(struct_type::StructType {
                fields: field.iter().map(|f| f.data_type().into()).collect(),
                field_names: field.iter().map(|f| f.name().clone()).collect(),
            })),
            List(field) => Self::List {
                datatype: Box::new(field.data_type().into()),
            },
            Decimal128(_, _) => Self::Decimal,
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
            DataType::Float32 => Self::Float32,
            DataType::Float64 => Self::Float64,
            DataType::Date => Self::Date32,
            DataType::Timestamp => Self::Timestamp(arrow_schema::TimeUnit::Millisecond, None),
            DataType::Time => Self::Time64(arrow_schema::TimeUnit::Millisecond),
            DataType::Interval => Self::Interval(arrow_schema::IntervalUnit::DayTime),
            DataType::Varchar => Self::Utf8,
            DataType::Bytea => Self::Binary,
            DataType::Decimal => Self::Decimal128(0, 0),
            DataType::Struct(struct_type) => {
                Self::Struct(get_field_vector_from_struct_type(struct_type))
            }
            DataType::List { datatype } => {
                Self::List(Box::new(Field::new("", datatype.as_ref().into(), true)))
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
    };
}
converts!(BoolArray, arrow_array::BooleanArray);
converts!(I16Array, arrow_array::Int16Array);
converts!(I32Array, arrow_array::Int32Array);
converts!(I64Array, arrow_array::Int64Array);
converts!(F32Array, arrow_array::Float32Array, @map);
converts!(F64Array, arrow_array::Float64Array, @map);
converts!(DecimalArray, arrow_array::Decimal128Array, @map);
converts!(BytesArray, arrow_array::BinaryArray);
converts!(Utf8Array, arrow_array::StringArray);
converts!(NaiveDateArray, arrow_array::Date32Array, @map);
converts!(NaiveTimeArray, arrow_array::Time64NanosecondArray, @map);
converts!(NaiveDateTimeArray, arrow_array::TimestampNanosecondArray, @map);
converts!(IntervalArray, arrow_array::IntervalMonthDayNanoArray, @map);

/// Converts RisingWave value from and into Arrow value.
trait FromIntoArrow {
    /// The corresponding element type in the Arrow array.
    type ArrowType;
    fn from_arrow(value: Self::ArrowType) -> Self;
    fn into_arrow(self) -> Self::ArrowType;
}

impl FromIntoArrow for OrderedF32 {
    type ArrowType = f32;

    fn from_arrow(value: Self::ArrowType) -> Self {
        value.into()
    }

    fn into_arrow(self) -> Self::ArrowType {
        self.into()
    }
}

impl FromIntoArrow for OrderedF64 {
    type ArrowType = f64;

    fn from_arrow(value: Self::ArrowType) -> Self {
        value.into()
    }

    fn into_arrow(self) -> Self::ArrowType {
        self.into()
    }
}

impl FromIntoArrow for Decimal {
    type ArrowType = i128;

    fn from_arrow(value: Self::ArrowType) -> Self {
        const NAN: i128 = i128::MIN + 1;
        match value {
            NAN => Decimal::NaN,
            i128::MAX => Decimal::PositiveInf,
            i128::MIN => Decimal::NegativeInf,
            _ => Decimal::Normalized(rust_decimal::Decimal::deserialize(value.to_be_bytes())),
        }
    }

    fn into_arrow(self) -> Self::ArrowType {
        match self {
            Decimal::Normalized(d) => i128::from_be_bytes(d.serialize()),
            Decimal::NaN => i128::MIN + 1,
            Decimal::PositiveInf => i128::MAX,
            Decimal::NegativeInf => i128::MIN,
        }
    }
}

impl FromIntoArrow for NaiveDateWrapper {
    type ArrowType = i32;

    fn from_arrow(value: Self::ArrowType) -> Self {
        NaiveDateWrapper(arrow_array::types::Date32Type::to_naive_date(value))
    }

    fn into_arrow(self) -> Self::ArrowType {
        arrow_array::types::Date32Type::from_naive_date(self.0)
    }
}

impl FromIntoArrow for NaiveTimeWrapper {
    type ArrowType = i64;

    fn from_arrow(value: Self::ArrowType) -> Self {
        NaiveTimeWrapper(
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

impl FromIntoArrow for NaiveDateTimeWrapper {
    type ArrowType = i64;

    fn from_arrow(value: Self::ArrowType) -> Self {
        NaiveDateTimeWrapper(
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

impl FromIntoArrow for IntervalUnit {
    type ArrowType = i128;

    fn from_arrow(value: Self::ArrowType) -> Self {
        let (months, days, ns) = arrow_array::types::IntervalMonthDayNanoType::to_parts(value);
        IntervalUnit::new(months, days, ns / 1_000_000)
    }

    fn into_arrow(self) -> Self::ArrowType {
        arrow_array::types::IntervalMonthDayNanoType::make_value(
            self.get_months(),
            self.get_days(),
            self.get_ms() * 1_000_000,
        )
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
            ArrayImpl::Bool(a) => {
                build(array, a, BooleanBuilder::with_capacity(a.len()), |b, v| {
                    b.append_option(v)
                })
            }
            ArrayImpl::Decimal(a) => build(
                array,
                a,
                Decimal128Builder::with_capacity(a.len()),
                |b, v| b.append_option(v.map(|d| d.into_arrow())),
            ),
            ArrayImpl::Interval(a) => build(
                array,
                a,
                IntervalMonthDayNanoBuilder::with_capacity(a.len()),
                |b, v| b.append_option(v.map(|d| d.into_arrow())),
            ),
            ArrayImpl::NaiveDate(a) => {
                build(array, a, Date32Builder::with_capacity(a.len()), |b, v| {
                    b.append_option(v.map(|d| d.into_arrow()))
                })
            }
            ArrayImpl::NaiveDateTime(a) => build(
                array,
                a,
                TimestampNanosecondBuilder::with_capacity(a.len()),
                |b, v| b.append_option(v.map(|d| d.into_arrow())),
            ),
            ArrayImpl::NaiveTime(a) => build(
                array,
                a,
                Time64NanosecondBuilder::with_capacity(a.len()),
                |b, v| b.append_option(v.map(|d| d.into_arrow())),
            ),
            ArrayImpl::Jsonb(_) => todo!("list of jsonb"),
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

impl From<&arrow_array::ListArray> for ListArray {
    fn from(array: &arrow_array::ListArray) -> Self {
        let iter = array.iter().map(|o| o.map(|a| ArrayImpl::from(&a)));
        ListArray::from_iter(iter, (&array.value_type()).into())
    }
}

impl From<&StructArray> for arrow_array::StructArray {
    fn from(array: &StructArray) -> Self {
        let struct_data_vector: Vec<(arrow_schema::Field, arrow_array::ArrayRef)> =
            if array.children_names().len() != array.children_array_types().len() {
                array
                    .field_arrays()
                    .iter()
                    .zip_eq_fast(array.children_array_types())
                    .map(|(arr, datatype)| (Field::new("", datatype.into(), true), (*arr).into()))
                    .collect()
            } else {
                array
                    .field_arrays()
                    .iter()
                    .zip_eq_fast(array.children_array_types())
                    .zip_eq_fast(array.children_names())
                    .map(|((arr, datatype), field_name)| {
                        (Field::new(field_name, datatype.into(), true), (*arr).into())
                    })
                    .collect()
            };
        arrow_array::StructArray::from(struct_data_vector)
    }
}

impl From<&arrow_array::StructArray> for StructArray {
    fn from(array: &arrow_array::StructArray) -> Self {
        let mut null_bitmap = Vec::new();
        for i in 0..arrow_array::Array::len(&array) {
            null_bitmap.push(!arrow_array::Array::is_null(&array, i))
        }
        match arrow_array::Array::data_type(&array) {
            arrow_schema::DataType::Struct(fields) => StructArray::from_slices_with_field_names(
                &(null_bitmap),
                array.columns().iter().map(ArrayImpl::from).collect(),
                fields
                    .iter()
                    .map(|f| DataType::from(f.data_type()))
                    .collect(),
                array.column_names().into_iter().map(String::from).collect(),
            ),
            _ => panic!("nested field types cannot be determined."),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::interval::test_utils::IntervalUnitTestExt;
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
        let array = F32Array::from_iter([
            None,
            Some(OrderedF32::from(-7.0)),
            Some(OrderedF32::from(25.0)),
        ]);
        let arrow = arrow_array::Float32Array::from(&array);
        assert_eq!(F32Array::from(&arrow), array);
    }

    #[test]
    fn date() {
        let array = NaiveDateArray::from_iter([
            None,
            NaiveDateWrapper::with_days(12345).ok(),
            NaiveDateWrapper::with_days(-12345).ok(),
        ]);
        let arrow = arrow_array::Date32Array::from(&array);
        assert_eq!(NaiveDateArray::from(&arrow), array);
    }

    #[test]
    fn time() {
        let array = NaiveTimeArray::from_iter([
            None,
            NaiveTimeWrapper::with_secs_nano(12345, 123456789).ok(),
            NaiveTimeWrapper::with_secs_nano(1, 0).ok(),
        ]);
        let arrow = arrow_array::Time64NanosecondArray::from(&array);
        assert_eq!(NaiveTimeArray::from(&arrow), array);
    }

    #[test]
    fn timestamp() {
        let array = NaiveDateTimeArray::from_iter([
            None,
            NaiveDateTimeWrapper::with_secs_nsecs(12345, 123456789).ok(),
            NaiveDateTimeWrapper::with_secs_nsecs(1, 0).ok(),
        ]);
        let arrow = arrow_array::TimestampNanosecondArray::from(&array);
        assert_eq!(NaiveDateTimeArray::from(&arrow), array);
    }

    #[test]
    fn interval() {
        let array = IntervalArray::from_iter([
            None,
            Some(IntervalUnit::from_millis(123456789)),
            Some(IntervalUnit::from_millis(-123456789)),
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
            Some(Decimal::Normalized("123.456".parse().unwrap())),
        ]);
        let arrow = arrow_array::Decimal128Array::from(&array);
        assert_eq!(DecimalArray::from(&arrow), array);
    }
    #[test]
    fn struct_array() {
        use arrow_array::Array as _;

        // Empty array - risingwave to arrow conversion.
        let test_arr = StructArray::from_slices(&[true, false, true, false], vec![], vec![]);
        assert_eq!(arrow_array::StructArray::from(&test_arr).len(), 0);

        // Empty array - arrow to risingwave conversion.
        let test_arr_2 = arrow_array::StructArray::from(vec![]);
        assert_eq!(StructArray::from(&test_arr_2).len(), 0);

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
        let actual_risingwave_struct_array = StructArray::from(&test_arrow_struct_array);
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
        assert_eq!(ListArray::from(&arrow), array);
    }
}
