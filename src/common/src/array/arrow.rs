//! Converts between arrays and Apache Arrow arrays.

use chrono::{NaiveDateTime, NaiveTime};

use super::*;

/// Implement bi-directional `From` between 2 array types.
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
                (value / 1000_000_000) as _,
                (value % 1000_000_000) as _,
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
                (value / 1000_000_000) as _,
                (value % 1000_000_000) as _,
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
        IntervalUnit::new(months, days, ns / 1000_000)
    }

    fn into_arrow(self) -> Self::ArrowType {
        arrow_array::types::IntervalMonthDayNanoType::make_value(
            self.get_months(),
            self.get_days(),
            self.get_ms() * 1000_000,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
