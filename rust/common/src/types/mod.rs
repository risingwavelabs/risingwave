use crate::error::{ErrorCode, Result, RwError};
use risingwave_pb::data::DataType as DataTypeProto;
use risingwave_pb::ToProst;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::convert::TryFrom;
use std::sync::Arc;

mod numeric_type;
pub use numeric_type::*;
mod primitive_data_type;
pub use primitive_data_type::*;
mod native_type;

mod scalar_impl;
pub use scalar_impl::*;

use crate::error::ErrorCode::InternalError;
pub use native_type::*;
use risingwave_pb::data::data_type::TypeName;
use std::fmt::Debug;

mod bool_type;
mod datetime_type;
mod decimal_type;
pub mod interval_type;
mod string_type;

pub use bool_type::*;
pub use datetime_type::*;
pub use decimal_type::*;
pub use interval_type::*;
pub use string_type::*;

use crate::array::{ArrayBuilderImpl, PrimitiveArrayItemType};
use paste::paste;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Copy)]
pub enum DataTypeKind {
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
}

/// Number of bytes of one element in array of [`DataType`].
pub enum DataSize {
    /// For types with fixed size, e.g. int, float.
    Fixed(usize),
    /// For types with variable size, e.g. string.
    Variable,
}

pub trait DataType: Debug + Sync + Send + 'static {
    fn data_type_kind(&self) -> DataTypeKind;
    fn is_nullable(&self) -> bool;
    fn create_array_builder(self: Arc<Self>, capacity: usize) -> Result<ArrayBuilderImpl>;
    fn to_protobuf(&self) -> Result<risingwave_proto::data::DataType>;
    fn to_prost(&self) -> Result<DataTypeProto>;
    fn as_any(&self) -> &dyn Any;
    fn data_size(&self) -> DataSize;
}

pub type DataTypeRef = Arc<dyn DataType>;

macro_rules! build_data_type {
  ($proto: expr, $($proto_type_name:path => $data_type:ty),*) => {
    match $proto.get_type_name() {
      $(
        $proto_type_name => {
          <$data_type>::try_from($proto).map(|d| Arc::new(d) as DataTypeRef)
        },
      )*
      _ => Err(InternalError(format!("Unsupported proto type: {:?}", $proto.get_type_name())).into())
    }
  }
}

pub fn build_from_prost(proto: &DataTypeProto) -> Result<DataTypeRef> {
    build_data_type! {
      proto,
      TypeName::Int16 => Int16Type,
      TypeName::Int32 => Int32Type,
      TypeName::Int64 => Int64Type,
      TypeName::Float => Float32Type,
      TypeName::Double => Float64Type,
      TypeName::Boolean => BoolType,
      TypeName::Char => StringType,
      TypeName::Varchar => StringType,
      TypeName::Date => DateType,
      TypeName::Time => TimeType,
      TypeName::Timestamp => TimestampType,
      TypeName::Timestampz => TimestampWithTimeZoneType,
      TypeName::Decimal => DecimalType,
      TypeName::Interval => IntervalType
    }
}

pub fn build_from_proto(proto: &risingwave_proto::data::DataType) -> Result<DataTypeRef> {
    build_from_prost(&proto.to_prost::<DataTypeProto>())
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Copy)]
pub enum ArithmeticOperatorKind {
    Plus,
    Subtract,
    Multiply,
    Divide,
    Mod,
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
  #[rustfmt::skip]
  // rustfmt will incorrectly remove GAT lifetime.
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
      { Float32, float32, f32, f32 },
      { Float64, float64, f64, f64 },
      { UTF8, utf8, String, &'scalar str },
      { Bool, bool, bool, bool },
      { Decimal, decimal, Decimal, Decimal  },
      { Interval, interval, IntervalUnit, IntervalUnit }
    }
  };
}

/// Define `ScalarImpl` and `ScalarRefImpl` with macro.
macro_rules! scalar_impl_enum {
  ([], $( { $variant_name:ident, $suffix_name:ident, $scalar:ty, $scalar_ref:ty } ),*) => {
    /// `ScalarImpl` embeds all possible scalars in the evaluation framework.
    #[derive(Debug, Clone, PartialEq, PartialOrd)]
    pub enum ScalarImpl {
      $( $variant_name($scalar) ),*
    }

    /// `ScalarRefImpl` embeds all possible scalar references in the evaluation
    /// framework.
    #[derive(Debug, Copy, Clone, PartialEq)]
    pub enum ScalarRefImpl<'scalar> {
      $( $variant_name($scalar_ref) ),*
    }
  };
}

for_all_scalar_variants! { scalar_impl_enum }

pub type Datum = Option<ScalarImpl>;
pub type DatumRef<'a> = Option<ScalarRefImpl<'a>>;

/// This trait is to implement `to_owned_datum` for `Option<ScalarImpl>`
pub trait ToOwnedDatum {
    /// implement `to_owned_datum` for `DatumRef` to covert to `Datum`
    fn to_owned_datum(self) -> Datum;
}

impl ToOwnedDatum for DatumRef<'_> {
    fn to_owned_datum(self) -> Datum {
        self.map(ScalarRefImpl::into_scalar_impl)
    }
}

// FIXME: `f32` is not `Eq`, and this is not safe. Consider using `ordered_float` in our project.
impl Eq for ScalarImpl {}

/// `for_all_native_types` includes all native variants of our scalar types.
#[macro_export]
macro_rules! for_all_native_types {
($macro:tt $(, $x:tt)*) => {
    $macro! {
      [$($x),*],
      { i16, Int16 },
      { i32, Int32 },
      { i64, Int64 },
      { f32, Float32 },
      { f64, Float64 }
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
      paste! {
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
                format!("cannot covert ScalarImpl::{} to concrete type", other_scalar.get_ident())
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
                format!("cannot covert ScalarRefImpl::{} to concrete type", other_scalar.get_ident())
              ).into())
            }
          }
        }

        impl ScalarImpl {
          pub fn [<as_ $suffix_name>](&self) -> &$scalar {
            match self {
              Self::$variant_name(ref scalar) => scalar,
              other_scalar => panic!("cannot covert ScalarImpl::{} to concrete type", other_scalar.get_ident())
            }
          }

          pub fn [<into_ $suffix_name>](self) -> $scalar {
            match self {
              Self::$variant_name(scalar) => scalar,
              other_scalar =>  panic!("cannot covert ScalarImpl::{} to concrete type", other_scalar.get_ident())
            }
          }
        }

        impl <'scalar> ScalarRefImpl<'scalar> {
          // Note that this conversion consume self.
          pub fn [<into_ $suffix_name>](self) -> $scalar_ref {
            match self {
              Self::$variant_name(inner) => inner,
              other_scalar => panic!("cannot covert ScalarRefImpl::{} to concrete type", other_scalar.get_ident())
            }
          }
        }
      }
    )*
  };
}

for_all_scalar_variants! { impl_convert }

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
          Self::UTF8(s) => s.hash(state),
          Self::Decimal(decimal) => decimal.hash(state),
          Self::Interval(interval) => interval.hash(state),
        }
      };
    }
        for_all_native_types! { impl_all_hash, self }
    }
}

impl ScalarImpl {
    /// Serialize the scalar.
    pub fn serialize(&self, ser: &mut memcomparable::Serializer) -> memcomparable::Result<()> {
        match self {
            &Self::Int16(v) => v.serialize(ser)?,
            &Self::Int32(v) => v.serialize(ser)?,
            &Self::Int64(v) => v.serialize(ser)?,
            &Self::Float32(v) => v.serialize(ser)?,
            &Self::Float64(v) => v.serialize(ser)?,
            Self::UTF8(v) => v.serialize(ser)?,
            &Self::Bool(v) => v.serialize(ser)?,
            &Self::Decimal(v) => ser.serialize_decimal(v.mantissa(), v.scale() as u8)?,
            Self::Interval(v) => v.serialize(ser)?,
        };
        Ok(())
    }

    /// Deserialize the scalar.
    pub fn deserialize(
        ty: DataTypeKind,
        de: &mut memcomparable::Deserializer<'_>,
    ) -> memcomparable::Result<Self> {
        use DataTypeKind as Ty;
        Ok(match ty {
            Ty::Int16 => Self::Int16(i16::deserialize(de)?),
            Ty::Int32 => Self::Int32(i32::deserialize(de)?),
            Ty::Int64 => Self::Int64(i64::deserialize(de)?),
            Ty::Float32 => Self::Float32(f32::deserialize(de)?),
            Ty::Float64 => Self::Float64(f64::deserialize(de)?),
            Ty::Char | Ty::Varchar => Self::UTF8(String::deserialize(de)?),
            Ty::Boolean => Self::Bool(bool::deserialize(de)?),
            Ty::Decimal => Self::Decimal({
                let (mantissa, scale) = de.deserialize_decimal()?;
                Decimal::from_i128_with_scale(mantissa, scale as u32)
            }),
            Ty::Interval | Ty::Date | Ty::Time | Ty::Timestamp | Ty::Timestampz => {
                Self::Interval(IntervalUnit::deserialize(de)?)
            }
        })
    }
}
