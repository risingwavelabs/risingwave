use std::convert::TryFrom;
use std::sync::Arc;

use bytes::{Buf, BufMut};
use risingwave_pb::data::DataType as ProstDataType;
use serde::{Deserialize, Serialize};

use crate::error::{ErrorCode, Result, RwError};
mod native_type;

mod scalar_impl;
use std::fmt::Debug;

pub use native_type::*;
use risingwave_pb::data::data_type::TypeName;
pub use scalar_impl::*;

mod chrono_wrapper;
mod decimal;
pub mod interval;

mod ordered_float;
use chrono::{Datelike, Timelike};
pub use chrono_wrapper::{NaiveDateTimeWrapper, NaiveDateWrapper, NaiveTimeWrapper};
pub use decimal::Decimal;
pub use interval::*;
pub use ordered_float::IntoOrdered;
use paste::paste;

use crate::array::{ArrayBuilderImpl, PrimitiveArrayItemType, StructRef, StructValue};

pub type OrderedF32 = ordered_float::OrderedFloat<f32>;
pub type OrderedF64 = ordered_float::OrderedFloat<f64>;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum DataType {
    Boolean,
    Int16,
    Int32,
    Int64,
    Float32,
    Float64,
    Decimal,
    Date,
    Char,
    Varchar,
    Time,
    Timestamp,
    Timestampz,
    Interval,
    Struct { fields: Arc<[DataType]> },
}

const DECIMAL_DEFAULT_PRECISION: u32 = 20;
const DECIMAL_DEFAULT_SCALE: u32 = 6;

/// Number of bytes of one element in array of [`DataType`].
pub enum DataSize {
    /// For types with fixed size, e.g. int, float.
    Fixed(usize),
    /// For types with variable size, e.g. string.
    Variable,
}

impl From<&ProstDataType> for DataType {
    fn from(proto: &ProstDataType) -> DataType {
        match proto.get_type_name().expect("unsupported type") {
            TypeName::Int16 => DataType::Int16,
            TypeName::Int32 => DataType::Int32,
            TypeName::Int64 => DataType::Int64,
            TypeName::Float => DataType::Float32,
            TypeName::Double => DataType::Float64,
            TypeName::Boolean => DataType::Boolean,
            TypeName::Char => DataType::Char,
            TypeName::Varchar => DataType::Varchar,
            TypeName::Date => DataType::Date,
            TypeName::Time => DataType::Time,
            TypeName::Timestamp => DataType::Timestamp,
            TypeName::Timestampz => DataType::Timestampz,
            TypeName::Decimal => DataType::Decimal,
            TypeName::Interval => DataType::Interval,
            TypeName::Symbol => DataType::Varchar,
            TypeName::Struct => DataType::Struct {
                fields: Arc::new([]),
            },
        }
    }
}

impl DataType {
    pub fn create_array_builder(&self, capacity: usize) -> Result<ArrayBuilderImpl> {
        use crate::array::*;
        Ok(match self {
            DataType::Boolean => BoolArrayBuilder::new(capacity)?.into(),
            DataType::Int16 => PrimitiveArrayBuilder::<i16>::new(capacity)?.into(),
            DataType::Int32 => PrimitiveArrayBuilder::<i32>::new(capacity)?.into(),
            DataType::Int64 => PrimitiveArrayBuilder::<i64>::new(capacity)?.into(),
            DataType::Float32 => PrimitiveArrayBuilder::<OrderedF32>::new(capacity)?.into(),
            DataType::Float64 => PrimitiveArrayBuilder::<OrderedF64>::new(capacity)?.into(),
            DataType::Decimal => DecimalArrayBuilder::new(capacity)?.into(),
            DataType::Date => NaiveDateArrayBuilder::new(capacity)?.into(),
            DataType::Char | DataType::Varchar => Utf8ArrayBuilder::new(capacity)?.into(),
            DataType::Time => NaiveTimeArrayBuilder::new(capacity)?.into(),
            DataType::Timestamp => NaiveDateTimeArrayBuilder::new(capacity)?.into(),
            DataType::Timestampz => PrimitiveArrayBuilder::<i64>::new(capacity)?.into(),
            DataType::Interval => IntervalArrayBuilder::new(capacity)?.into(),
            DataType::Struct { .. } => {
                todo!()
            }
        })
    }

    fn prost_type_name(&self) -> TypeName {
        match self {
            DataType::Int16 => TypeName::Int16,
            DataType::Int32 => TypeName::Int32,
            DataType::Int64 => TypeName::Int64,
            DataType::Float32 => TypeName::Float,
            DataType::Float64 => TypeName::Double,
            DataType::Boolean => TypeName::Boolean,
            DataType::Char => TypeName::Char,
            DataType::Varchar => TypeName::Varchar,
            DataType::Date => TypeName::Date,
            DataType::Time => TypeName::Time,
            DataType::Timestamp => TypeName::Timestamp,
            DataType::Timestampz => TypeName::Timestampz,
            DataType::Decimal => TypeName::Decimal,
            DataType::Interval => TypeName::Interval,
            DataType::Struct { .. } => TypeName::Struct,
        }
    }

    pub fn to_protobuf(&self) -> ProstDataType {
        ProstDataType {
            type_name: self.prost_type_name() as i32,
            is_nullable: true,
            ..Default::default()
        }
    }

    pub fn data_size(&self) -> DataSize {
        use std::mem::size_of;
        match self {
            DataType::Boolean => DataSize::Variable,
            DataType::Int16 => DataSize::Fixed(size_of::<i16>()),
            DataType::Int32 => DataSize::Fixed(size_of::<i32>()),
            DataType::Int64 => DataSize::Fixed(size_of::<i64>()),
            DataType::Float32 => DataSize::Fixed(size_of::<OrderedF32>()),
            DataType::Float64 => DataSize::Fixed(size_of::<OrderedF64>()),
            DataType::Decimal => DataSize::Fixed(16),
            DataType::Char => DataSize::Variable,
            DataType::Varchar => DataSize::Variable,
            DataType::Date => DataSize::Fixed(size_of::<i32>()),
            DataType::Time => DataSize::Fixed(size_of::<i64>()),
            DataType::Timestamp => DataSize::Fixed(size_of::<i64>()),
            DataType::Timestampz => DataSize::Fixed(size_of::<i64>()),
            DataType::Interval => DataSize::Variable,
            DataType::Struct { .. } => DataSize::Variable,
        }
    }

    pub fn is_numeric(&self) -> bool {
        matches!(
            self,
            DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::Float32
                | DataType::Float64
                | DataType::Decimal
        )
    }

    pub fn is_string(&self) -> bool {
        matches!(self, DataType::Char | DataType::Varchar)
    }

    pub fn is_date_or_timestamp(&self) -> bool {
        matches!(self, DataType::Date | DataType::Timestamp)
    }

    /// Type index is used to find the least restrictive type that is of the larger index.
    pub fn type_index(&self) -> i32 {
        match self {
            DataType::Boolean => 1,
            DataType::Int16 => 2,
            DataType::Int32 => 3,
            DataType::Int64 => 4,
            DataType::Float32 => 5,
            DataType::Float64 => 6,
            DataType::Decimal => 7,
            DataType::Date => 8,
            DataType::Char => 9,
            DataType::Varchar => 10,
            DataType::Time => 11,
            DataType::Timestamp => 12,
            DataType::Timestampz => 13,
            DataType::Interval => 14,
            DataType::Struct { .. } => 15,
        }
    }
}

/// `Scalar` is a trait over all possible owned types in the evaluation
/// framework.
///
/// `Scalar` is reciprocal to `ScalarRef`. Use `as_scalar_ref` to get a
/// reference which has the same lifetime as `self`.
pub trait Scalar:
    std::fmt::Debug
    + Send
    + Sync
    + 'static
    + Clone
    + std::fmt::Debug
    + TryFrom<ScalarImpl, Error = RwError>
    + Into<ScalarImpl>
{
    /// Type for reference of `Scalar`
    type ScalarRefType<'a>: ScalarRef<'a, ScalarType = Self> + 'a
    where
        Self: 'a;

    /// Get a reference to current scalar.
    fn as_scalar_ref(&self) -> Self::ScalarRefType<'_>;

    fn to_scalar_value(self) -> ScalarImpl;
}

/// Convert an `Option<Scalar>` to corresponding `Option<ScalarRef>`.
pub fn option_as_scalar_ref<S: Scalar>(scalar: &Option<S>) -> Option<S::ScalarRefType<'_>> {
    scalar.as_ref().map(|x| x.as_scalar_ref())
}

/// Convert an `Option<ScalarRef>` to corresponding `Option<Scalar>`.
pub fn option_to_owned_scalar<S: Scalar>(scalar: &Option<S::ScalarRefType<'_>>) -> Option<S> {
    scalar.map(|x| x.to_owned_scalar())
}

/// `ScalarRef` is a trait over all possible references in the evaluation
/// framework.
///
/// `ScalarRef` is reciprocal to `Scalar`. Use `to_owned_scalar` to get an
/// owned scalar.
pub trait ScalarRef<'a>:
    Copy + std::fmt::Debug + 'a + TryFrom<ScalarRefImpl<'a>, Error = RwError> + Into<ScalarRefImpl<'a>>
{
    /// `ScalarType` is the owned type of current `ScalarRef`.
    type ScalarType: Scalar<ScalarRefType<'a> = Self>;

    /// Convert `ScalarRef` to an owned scalar.
    fn to_owned_scalar(&self) -> Self::ScalarType;
}

/// `for_all_scalar_variants` includes all variants of our scalar types. If you added a new scalar
/// type inside the project, be sure to add a variant here.
///
/// Every tuple has four elements, where
/// `{ enum variant name, function suffix name, scalar type, scalar ref type }`
#[macro_export]
macro_rules! for_all_scalar_variants {
    ($macro:tt $(, $x:tt)*) => {
        $macro! {
            [$($x),*],
            { Int16, int16, i16, i16 },
            { Int32, int32, i32, i32 },
            { Int64, int64, i64, i64 },
            { Float32, float32, OrderedF32, OrderedF32 },
            { Float64, float64, OrderedF64, OrderedF64 },
            { Utf8, utf8, String, &'scalar str },
            { Bool, bool, bool, bool },
            { Decimal, decimal, Decimal, Decimal  },
            { Interval, interval, IntervalUnit, IntervalUnit },
            { NaiveDate, naivedate, NaiveDateWrapper, NaiveDateWrapper },
            { NaiveDateTime, naivedatetime, NaiveDateTimeWrapper, NaiveDateTimeWrapper },
            { NaiveTime, naivetime, NaiveTimeWrapper, NaiveTimeWrapper },
            { Struct, struct, StructValue, StructRef<'scalar> }
        }
    };
}

/// Define `ScalarImpl` and `ScalarRefImpl` with macro.
macro_rules! scalar_impl_enum {
    ([], $( { $variant_name:ident, $suffix_name:ident, $scalar:ty, $scalar_ref:ty } ),*) => {
        /// `ScalarImpl` embeds all possible scalars in the evaluation framework.
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
        pub enum ScalarImpl {
            $( $variant_name($scalar) ),*
        }

        /// `ScalarRefImpl` embeds all possible scalar references in the evaluation
        /// framework.
        #[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
        pub enum ScalarRefImpl<'scalar> {
            $( $variant_name($scalar_ref) ),*
        }
    };
}

for_all_scalar_variants! { scalar_impl_enum }

pub type Datum = Option<ScalarImpl>;
pub type DatumRef<'a> = Option<ScalarRefImpl<'a>>;

// TODO: specify `NULL FIRST` or `NULL LAST`.
pub fn serialize_datum_ref_into(
    datum_ref: &DatumRef,
    serializer: &mut memcomparable::Serializer<impl BufMut>,
) -> memcomparable::Result<()> {
    if let Some(datum_ref) = datum_ref {
        1u8.serialize(&mut *serializer)?;
        datum_ref.serialize(serializer)?;
    } else {
        0u8.serialize(serializer)?;
    }
    Ok(())
}

pub fn serialize_datum_ref_not_null_into(
    datum_ref: &DatumRef,
    serializer: &mut memcomparable::Serializer<impl BufMut>,
) -> memcomparable::Result<()> {
    datum_ref
        .as_ref()
        .expect("datum cannot be null")
        .serialize(serializer)
}

// TODO(MrCroxx): turn Datum into a struct, and impl ser/de as its member functions.
// TODO: specify `NULL FIRST` or `NULL LAST`.
pub fn serialize_datum_into(
    datum: &Datum,
    serializer: &mut memcomparable::Serializer<impl BufMut>,
) -> memcomparable::Result<()> {
    if let Some(datum) = datum {
        1u8.serialize(&mut *serializer)?;
        datum.serialize(serializer)?;
    } else {
        0u8.serialize(serializer)?;
    }
    Ok(())
}

// TODO(MrCroxx): turn Datum into a struct, and impl ser/de as its member functions.
pub fn serialize_datum_not_null_into(
    datum: &Datum,
    serializer: &mut memcomparable::Serializer<impl BufMut>,
) -> memcomparable::Result<()> {
    datum
        .as_ref()
        .expect("datum cannot be null")
        .serialize(serializer)
}

// TODO(MrCroxx): turn Datum into a struct, and impl ser/de as its member functions.
pub fn deserialize_datum_from(
    ty: &DataType,
    deserializer: &mut memcomparable::Deserializer<impl Buf>,
) -> memcomparable::Result<Datum> {
    let null_tag = u8::deserialize(&mut *deserializer)?;
    match null_tag {
        0 => Ok(None),
        1 => Ok(Some(ScalarImpl::deserialize(ty.clone(), deserializer)?)),
        _ => Err(memcomparable::Error::InvalidTagEncoding(null_tag as _)),
    }
}

// TODO(MrCroxx): turn Datum into a struct, and impl ser/de as its member functions.
pub fn deserialize_datum_not_null_from(
    ty: DataType,
    deserializer: &mut memcomparable::Deserializer<impl Buf>,
) -> memcomparable::Result<Datum> {
    Ok(Some(ScalarImpl::deserialize(ty, deserializer)?))
}

/// This trait is to implement `to_owned_datum` for `Option<ScalarImpl>`
pub trait ToOwnedDatum {
    /// implement `to_owned_datum` for `DatumRef` to convert to `Datum`
    fn to_owned_datum(self) -> Datum;
}

impl ToOwnedDatum for DatumRef<'_> {
    fn to_owned_datum(self) -> Datum {
        self.map(ScalarRefImpl::into_scalar_impl)
    }
}

/// `for_all_native_types` includes all native variants of our scalar types.
#[macro_export]
macro_rules! for_all_native_types {
    ($macro:tt $(, $x:tt)*) => {
        $macro! {
            [$($x),*],
            { i16, Int16 },
            { i32, Int32 },
            { i64, Int64 },
            { $crate::types::OrderedF32, Float32 },
            { $crate::types::OrderedF64, Float64 }
        }
    };
}

/// `impl_convert` implements several conversions for `Scalar`.
/// * `Scalar <-> ScalarImpl` with `From` and `TryFrom` trait.
/// * `ScalarRef <-> ScalarRefImpl` with `From` and `TryFrom` trait.
/// * `&ScalarImpl -> &Scalar` with `impl.as_int16()`.
/// * `ScalarImpl -> Scalar` with `impl.into_int16()`.
macro_rules! impl_convert {
    ([], $( { $variant_name:ident, $suffix_name:ident, $scalar:ty, $scalar_ref:ty } ),*) => {
        $(
            impl From<$scalar> for ScalarImpl {
                fn from(val: $scalar) -> Self {
                    ScalarImpl::$variant_name(val)
                }
            }

            impl TryFrom<ScalarImpl> for $scalar {
                type Error = RwError;

                fn try_from(val: ScalarImpl) -> Result<Self> {
                    match val {
                        ScalarImpl::$variant_name(scalar) => Ok(scalar),
                        other_scalar => Err(ErrorCode::InternalError(
                            format!("cannot convert ScalarImpl::{} to concrete type", other_scalar.get_ident())
                        ).into())
                    }
                }
            }

            impl <'scalar> From<$scalar_ref> for ScalarRefImpl<'scalar> {
                fn from(val: $scalar_ref) -> Self {
                    ScalarRefImpl::$variant_name(val)
                }
            }

            impl <'scalar> TryFrom<ScalarRefImpl<'scalar>> for $scalar_ref {
                type Error = RwError;

                fn try_from(val: ScalarRefImpl<'scalar>) -> Result<Self> {
                    match val {
                        ScalarRefImpl::$variant_name(scalar_ref) => Ok(scalar_ref),
                        other_scalar => Err(ErrorCode::InternalError(
                            format!("cannot convert ScalarRefImpl::{} to concrete type", other_scalar.get_ident())
                        ).into())
                    }
                }
            }

        paste! {
            impl ScalarImpl {
            pub fn [<as_ $suffix_name>](&self) -> &$scalar {
                match self {
                Self::$variant_name(ref scalar) => scalar,
                other_scalar => panic!("cannot convert ScalarImpl::{} to concrete type", other_scalar.get_ident())
                }
            }

            pub fn [<into_ $suffix_name>](self) -> $scalar {
                match self {
                Self::$variant_name(scalar) => scalar,
                other_scalar =>  panic!("cannot convert ScalarImpl::{} to concrete type", other_scalar.get_ident())
                }
            }
            }

            impl <'scalar> ScalarRefImpl<'scalar> {
            // Note that this conversion consume self.
            pub fn [<into_ $suffix_name>](self) -> $scalar_ref {
                match self {
                Self::$variant_name(inner) => inner,
                other_scalar => panic!("cannot convert ScalarRefImpl::{} to concrete type", other_scalar.get_ident())
                }
            }
            }
        }
        )*
    };
}

for_all_scalar_variants! { impl_convert }

// Implement `From<raw float>` for `ScalarImpl` manually/
impl From<f32> for ScalarImpl {
    fn from(f: f32) -> Self {
        Self::Float32(f.into())
    }
}
impl From<f64> for ScalarImpl {
    fn from(f: f64) -> Self {
        Self::Float64(f.into())
    }
}

macro_rules! impl_scalar_impl_ref_conversion {
    ([], $( { $variant_name:ident, $suffix_name:ident, $scalar:ty, $scalar_ref:ty } ),*) => {
        impl ScalarImpl {
            /// Converts [`ScalarImpl`] to [`ScalarRefImpl`]
            pub fn as_scalar_ref_impl(&self) -> ScalarRefImpl<'_> {
                match self {
                    $(
                        Self::$variant_name(inner) => ScalarRefImpl::<'_>::$variant_name(inner.as_scalar_ref())
                    ), *
                }
            }
        }

        impl<'a> ScalarRefImpl<'a> {
            /// Converts [`ScalarRefImpl`] to [`ScalarImpl`]
            pub fn into_scalar_impl(self) -> ScalarImpl {
                match self {
                    $(
                        Self::$variant_name(inner) => ScalarImpl::$variant_name(inner.to_owned_scalar())
                    ), *
                }
            }
        }
    };
}

for_all_scalar_variants! { impl_scalar_impl_ref_conversion }

// FIXME: should implement Hash and Eq all by deriving
// TODO: may take type information into consideration later
#[allow(clippy::derive_hash_xor_eq)]
impl std::hash::Hash for ScalarImpl {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        macro_rules! impl_all_hash {
            ([$self:ident], $({ $variant_type:ty, $scalar_type:ident } ),*) => {
                match $self {
                    $( Self::$scalar_type(inner) => {
                        inner.hash_wrapper(state);
                    }, )*
                    Self::Bool(b) => b.hash(state),
                    Self::Utf8(s) => s.hash(state),
                    Self::Decimal(decimal) => decimal.hash(state),
                    Self::Interval(interval) => interval.hash(state),
                    Self::NaiveDate(naivedate) => naivedate.hash(state),
                    Self::NaiveDateTime(naivedatetime) => naivedatetime.hash(state),
                    Self::NaiveTime(naivetime) => naivetime.hash(state),
                    Self::Struct(v) => v.hash(state),
                }
            };
        }
        for_all_native_types! { impl_all_hash, self }
    }
}

impl ToString for ScalarImpl {
    fn to_string(&self) -> String {
        macro_rules! impl_to_string {
            ([], $( { $variant_name:ident, $suffix_name:ident, $scalar:ty, $scalar_ref:ty } ),*) => {
                match self {
                    $( Self::$variant_name(ref inner) => {
                        inner.to_string()
                    }, )*
                }
            }
        }

        for_all_scalar_variants! { impl_to_string }
    }
}

impl ToString for ScalarRefImpl<'_> {
    fn to_string(&self) -> String {
        macro_rules! impl_to_string {
            ([], $( { $variant_name:ident, $suffix_name:ident, $scalar:ty, $scalar_ref:ty } ),*) => {
                match self {
                    $( Self::$variant_name(inner) => {
                        inner.to_string()
                    }, )*
                }
            }
        }

        for_all_scalar_variants! { impl_to_string }
    }
}

impl ScalarRefImpl<'_> {
    /// Serialize the scalar.
    pub fn serialize(
        &self,
        ser: &mut memcomparable::Serializer<impl BufMut>,
    ) -> memcomparable::Result<()> {
        match self {
            &Self::Int16(v) => v.serialize(ser)?,
            &Self::Int32(v) => v.serialize(ser)?,
            &Self::Int64(v) => v.serialize(ser)?,
            &Self::Float32(v) => v.serialize(ser)?,
            &Self::Float64(v) => v.serialize(ser)?,
            &Self::Utf8(v) => v.serialize(ser)?,
            &Self::Bool(v) => v.serialize(ser)?,
            &Self::Decimal(v) => {
                let (mantissa, scale) = v.mantissa_scale_for_serialization();
                ser.serialize_decimal(mantissa, scale)?;
            }
            Self::Interval(v) => v.serialize(ser)?,
            &Self::NaiveDate(v) => ser.serialize_naivedate(v.0.num_days_from_ce())?,
            &Self::NaiveDateTime(v) => {
                ser.serialize_naivedatetime(v.0.timestamp(), v.0.timestamp_subsec_nanos())?
            }
            &Self::NaiveTime(v) => {
                ser.serialize_naivetime(v.0.num_seconds_from_midnight(), v.0.nanosecond())?
            }
            _ => {
                panic!("Type is unable to be serialized.")
            }
        };
        Ok(())
    }
}

impl ScalarImpl {
    /// Serialize the scalar.
    pub fn serialize(
        &self,
        ser: &mut memcomparable::Serializer<impl BufMut>,
    ) -> memcomparable::Result<()> {
        self.as_scalar_ref_impl().serialize(ser)
    }

    /// Deserialize the scalar.
    pub fn deserialize(
        ty: DataType,
        de: &mut memcomparable::Deserializer<impl Buf>,
    ) -> memcomparable::Result<Self> {
        use DataType as Ty;
        Ok(match ty {
            Ty::Int16 => Self::Int16(i16::deserialize(de)?),
            Ty::Int32 => Self::Int32(i32::deserialize(de)?),
            Ty::Int64 => Self::Int64(i64::deserialize(de)?),
            Ty::Float32 => Self::Float32(f32::deserialize(de)?.into()),
            Ty::Float64 => Self::Float64(f64::deserialize(de)?.into()),
            Ty::Char | Ty::Varchar => Self::Utf8(String::deserialize(de)?),
            Ty::Boolean => Self::Bool(bool::deserialize(de)?),
            Ty::Decimal => Self::Decimal({
                let (mantissa, scale) = de.deserialize_decimal()?;
                match scale {
                    29 => Decimal::NegativeINF,
                    30 => Decimal::PositiveINF,
                    31 => Decimal::NaN,
                    _ => Decimal::from_i128_with_scale(mantissa, scale as u32),
                }
            }),
            Ty::Interval => Self::Interval(IntervalUnit::deserialize(de)?),
            Ty::Time => Self::NaiveTime({
                let (secs, nano) = de.deserialize_naivetime()?;
                NaiveTimeWrapper::new_with_secs_nano(secs, nano)?
            }),
            Ty::Timestamp => Self::NaiveDateTime({
                let (secs, nsecs) = de.deserialize_naivedatetime()?;
                NaiveDateTimeWrapper::new_with_secs_nsecs(secs, nsecs)?
            }),
            Ty::Timestampz => Self::Int64(i64::deserialize(de)?),
            Ty::Date => Self::NaiveDate({
                let days = de.deserialize_naivedate()?;
                NaiveDateWrapper::new_with_days(days)?
            }),
            _ => {
                panic!("Type is unable to be deserialized.")
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Neg;

    use itertools::Itertools;
    use rand::thread_rng;

    use super::*;

    fn serialize_datum_not_null_into_vec(data: i64) -> Vec<u8> {
        let mut serializer = memcomparable::Serializer::new(vec![]);
        serialize_datum_not_null_into(&Some(ScalarImpl::Int64(data)), &mut serializer).unwrap();
        serializer.into_inner()
    }

    #[test]
    fn test_memcomparable() {
        let memcmp_minus_1 = serialize_datum_not_null_into_vec(-1);
        let memcmp_3874 = serialize_datum_not_null_into_vec(3874);
        let memcmp_45745 = serialize_datum_not_null_into_vec(45745);
        let memcmp_21234 = serialize_datum_not_null_into_vec(21234);
        assert!(memcmp_3874 < memcmp_45745);
        assert!(memcmp_3874 < memcmp_21234);
        assert!(memcmp_21234 < memcmp_45745);

        assert!(memcmp_minus_1 < memcmp_3874);
        assert!(memcmp_minus_1 < memcmp_21234);
        assert!(memcmp_minus_1 < memcmp_45745);
    }

    #[test]
    fn test_issue_2057_ordered_float_memcomparable() {
        use num_traits::*;
        use rand::seq::SliceRandom;

        fn serialize(f: OrderedF32) -> Vec<u8> {
            let mut serializer = memcomparable::Serializer::new(vec![]);
            serialize_datum_not_null_into(&Some(f.into()), &mut serializer).unwrap();
            serializer.into_inner()
        }

        fn deserialize(data: Vec<u8>) -> OrderedF32 {
            let mut deserializer = memcomparable::Deserializer::new(data.as_slice());
            let datum =
                deserialize_datum_not_null_from(DataType::Float32, &mut deserializer).unwrap();
            datum.unwrap().try_into().unwrap()
        }

        let floats = vec![
            // -inf
            OrderedF32::neg_infinity(),
            // -1
            OrderedF32::one().neg(),
            // 0, -0 should be treated the same
            OrderedF32::zero(),
            OrderedF32::neg_zero(),
            OrderedF32::zero(),
            // 1
            OrderedF32::one(),
            // inf
            OrderedF32::infinity(),
            // nan, -nan should be treated the same
            OrderedF32::nan(),
            OrderedF32::nan().neg(),
            OrderedF32::nan(),
        ];
        assert!(floats.is_sorted());

        let mut floats_clone = floats.clone();
        floats_clone.shuffle(&mut thread_rng());
        floats_clone.sort();
        assert_eq!(floats, floats_clone);

        let memcomparables = floats.clone().into_iter().map(serialize).collect_vec();
        assert!(memcomparables.is_sorted());

        let decoded_floats = memcomparables.into_iter().map(deserialize).collect_vec();
        assert!(decoded_floats.is_sorted());
        assert_eq!(floats, decoded_floats);
    }
}
