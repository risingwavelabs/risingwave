// Copyright 2023 RisingWave Labs
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

use std::convert::TryFrom;
use std::hash::Hash;
use std::sync::Arc;

use bytes::{Buf, BufMut, Bytes};
use num_traits::Float;
use parse_display::{Display, FromStr};
use risingwave_pb::data::DataType as ProstDataType;
use serde::{Deserialize, Serialize};

use crate::array::{ArrayError, ArrayResult, NULL_VAL_FOR_HASH};
use crate::error::BoxedError;

mod native_type;
mod ops;
mod scalar_impl;
mod successor;

use std::fmt::Debug;
use std::io::Cursor;
use std::str::FromStr;

pub use native_type::*;
use risingwave_pb::data::data_type::IntervalType::*;
use risingwave_pb::data::data_type::{IntervalType, TypeName};
pub use scalar_impl::*;
pub use successor::*;
pub mod chrono_wrapper;
pub mod decimal;
pub mod interval;
mod postgres_type;
pub mod struct_type;
pub mod to_binary;
pub mod to_text;

mod ordered_float;

use chrono::{Datelike, NaiveDate, NaiveDateTime, Timelike};
pub use chrono_wrapper::{
    NaiveDateTimeWrapper, NaiveDateWrapper, NaiveTimeWrapper, UNIX_EPOCH_DAYS,
};
pub use decimal::Decimal;
pub use interval::*;
use itertools::Itertools;
pub use ops::{CheckedAdd, IsNegative};
pub use ordered_float::IntoOrdered;
use paste::paste;
use postgres_types::{IsNull, ToSql, Type};
use strum_macros::EnumDiscriminants;

use self::struct_type::StructType;
use self::to_binary::ToBinary;
use self::to_text::ToText;
use crate::array::serial_array::Serial;
use crate::array::{
    read_interval_unit, ArrayBuilderImpl, JsonbRef, JsonbVal, ListRef, ListValue,
    PrimitiveArrayItemType, StructRef, StructValue,
};
use crate::error::Result as RwResult;

pub type OrderedF32 = ordered_float::OrderedFloat<f32>;
pub type OrderedF64 = ordered_float::OrderedFloat<f64>;

/// `EnumDiscriminants` will generate a `DataTypeName` enum with the same variants,
/// but without data fields.
#[derive(Debug, Display, Clone, PartialEq, Eq, Hash, EnumDiscriminants, FromStr)]
#[strum_discriminants(derive(strum_macros::EnumIter, Hash, Ord, PartialOrd))]
#[strum_discriminants(name(DataTypeName))]
#[strum_discriminants(vis(pub))]
pub enum DataType {
    #[display("boolean")]
    #[from_str(regex = "(?i)^bool$|^boolean$")]
    Boolean,
    #[display("smallint")]
    #[from_str(regex = "(?i)^smallint$|^int2$")]
    Int16,
    #[display("integer")]
    #[from_str(regex = "(?i)^integer$|^int$|^int4$")]
    Int32,
    #[display("bigint")]
    #[from_str(regex = "(?i)^bigint$|^int8$")]
    Int64,
    #[display("real")]
    #[from_str(regex = "(?i)^real$|^float4$")]
    Float32,
    #[display("double precision")]
    #[from_str(regex = "(?i)^double precision$|^float8$")]
    Float64,
    #[display("numeric")]
    #[from_str(regex = "(?i)^numeric$|^decimal$")]
    Decimal,
    #[display("date")]
    #[from_str(regex = "(?i)^date$")]
    Date,
    #[display("varchar")]
    #[from_str(regex = "(?i)^varchar$")]
    Varchar,
    #[display("time without time zone")]
    #[from_str(regex = "(?i)^time$|^time without time zone$")]
    Time,
    #[display("timestamp without time zone")]
    #[from_str(regex = "(?i)^timestamp$|^timestamp without time zone$")]
    Timestamp,
    #[display("timestamp with time zone")]
    #[from_str(regex = "(?i)^timestamptz$|^timestamp with time zone$")]
    Timestamptz,
    #[display("interval")]
    #[from_str(regex = "(?i)^interval$")]
    Interval,
    #[display("{0}")]
    #[from_str(ignore)]
    Struct(Arc<StructType>),
    #[display("{datatype}[]")]
    List { datatype: Box<DataType> },
    #[display("bytea")]
    #[from_str(regex = "(?i)^bytea$")]
    Bytea,
    #[display("jsonb")]
    #[from_str(regex = "(?i)^jsonb$")]
    Jsonb,
}

impl std::str::FromStr for Box<DataType> {
    type Err = BoxedError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Box::new(DataType::from_str(s)?))
    }
}

impl DataTypeName {
    pub fn is_scalar(&self) -> bool {
        match self {
            DataTypeName::Boolean
            | DataTypeName::Int16
            | DataTypeName::Int32
            | DataTypeName::Int64
            | DataTypeName::Decimal
            | DataTypeName::Float32
            | DataTypeName::Float64
            | DataTypeName::Varchar
            | DataTypeName::Date
            | DataTypeName::Timestamp
            | DataTypeName::Timestamptz
            | DataTypeName::Time
            | DataTypeName::Bytea
            | DataTypeName::Jsonb
            | DataTypeName::Interval => true,

            DataTypeName::Struct | DataTypeName::List => false,
        }
    }

    pub fn to_type(self) -> Option<DataType> {
        let t = match self {
            DataTypeName::Boolean => DataType::Boolean,
            DataTypeName::Int16 => DataType::Int16,
            DataTypeName::Int32 => DataType::Int32,
            DataTypeName::Int64 => DataType::Int64,
            DataTypeName::Decimal => DataType::Decimal,
            DataTypeName::Float32 => DataType::Float32,
            DataTypeName::Float64 => DataType::Float64,
            DataTypeName::Varchar => DataType::Varchar,
            DataTypeName::Bytea => DataType::Bytea,
            DataTypeName::Date => DataType::Date,
            DataTypeName::Timestamp => DataType::Timestamp,
            DataTypeName::Timestamptz => DataType::Timestamptz,
            DataTypeName::Time => DataType::Time,
            DataTypeName::Interval => DataType::Interval,
            DataTypeName::Jsonb => DataType::Jsonb,
            DataTypeName::Struct | DataTypeName::List => {
                return None;
            }
        };
        Some(t)
    }
}

impl From<DataTypeName> for DataType {
    fn from(type_name: DataTypeName) -> Self {
        type_name.to_type().unwrap_or_else(|| panic!("Functions returning struct or list can not be inferred. Please use `FunctionCall::new_unchecked`."))
    }
}

pub fn unnested_list_type(datatype: DataType) -> DataType {
    match datatype {
        DataType::List { datatype } => unnested_list_type(*datatype),
        _ => datatype,
    }
}

impl From<&ProstDataType> for DataType {
    fn from(proto: &ProstDataType) -> DataType {
        match proto.get_type_name().expect("missing type field") {
            TypeName::Int16 => DataType::Int16,
            TypeName::Int32 => DataType::Int32,
            TypeName::Int64 => DataType::Int64,
            TypeName::Float => DataType::Float32,
            TypeName::Double => DataType::Float64,
            TypeName::Boolean => DataType::Boolean,
            TypeName::Varchar => DataType::Varchar,
            TypeName::Date => DataType::Date,
            TypeName::Time => DataType::Time,
            TypeName::Timestamp => DataType::Timestamp,
            TypeName::Timestamptz => DataType::Timestamptz,
            TypeName::Decimal => DataType::Decimal,
            TypeName::Interval => DataType::Interval,
            TypeName::Bytea => DataType::Bytea,
            TypeName::Jsonb => DataType::Jsonb,
            TypeName::Struct => {
                let fields: Vec<DataType> = proto.field_type.iter().map(|f| f.into()).collect_vec();
                let field_names: Vec<String> = proto.field_names.iter().cloned().collect_vec();
                DataType::new_struct(fields, field_names)
            }
            TypeName::List => DataType::List {
                // The first (and only) item is the list element type.
                datatype: Box::new((&proto.field_type[0]).into()),
            },
            TypeName::TypeUnspecified => unreachable!(),
        }
    }
}

impl DataType {
    pub fn create_array_builder(&self, capacity: usize) -> ArrayBuilderImpl {
        use crate::array::*;
        match self {
            DataType::Boolean => BoolArrayBuilder::new(capacity).into(),
            DataType::Int16 => PrimitiveArrayBuilder::<i16>::new(capacity).into(),
            DataType::Int32 => PrimitiveArrayBuilder::<i32>::new(capacity).into(),
            DataType::Int64 => PrimitiveArrayBuilder::<i64>::new(capacity).into(),
            DataType::Float32 => PrimitiveArrayBuilder::<OrderedF32>::new(capacity).into(),
            DataType::Float64 => PrimitiveArrayBuilder::<OrderedF64>::new(capacity).into(),
            DataType::Decimal => DecimalArrayBuilder::new(capacity).into(),
            DataType::Date => NaiveDateArrayBuilder::new(capacity).into(),
            DataType::Varchar => Utf8ArrayBuilder::new(capacity).into(),
            DataType::Time => NaiveTimeArrayBuilder::new(capacity).into(),
            DataType::Timestamp => NaiveDateTimeArrayBuilder::new(capacity).into(),
            DataType::Timestamptz => PrimitiveArrayBuilder::<i64>::new(capacity).into(),
            DataType::Interval => IntervalArrayBuilder::new(capacity).into(),
            DataType::Jsonb => JsonbArrayBuilder::new(capacity).into(),
            DataType::Struct(t) => {
                StructArrayBuilder::with_meta(capacity, t.to_array_meta()).into()
            }
            DataType::List { datatype } => ListArrayBuilder::with_meta(
                capacity,
                ArrayMeta::List {
                    datatype: datatype.clone(),
                },
            )
            .into(),
            DataType::Bytea => BytesArrayBuilder::new(capacity).into(),
        }
    }

    pub fn prost_type_name(&self) -> TypeName {
        match self {
            DataType::Int16 => TypeName::Int16,
            DataType::Int32 => TypeName::Int32,
            DataType::Int64 => TypeName::Int64,
            DataType::Float32 => TypeName::Float,
            DataType::Float64 => TypeName::Double,
            DataType::Boolean => TypeName::Boolean,
            DataType::Varchar => TypeName::Varchar,
            DataType::Date => TypeName::Date,
            DataType::Time => TypeName::Time,
            DataType::Timestamp => TypeName::Timestamp,
            DataType::Timestamptz => TypeName::Timestamptz,
            DataType::Decimal => TypeName::Decimal,
            DataType::Interval => TypeName::Interval,
            DataType::Jsonb => TypeName::Jsonb,
            DataType::Struct { .. } => TypeName::Struct,
            DataType::List { .. } => TypeName::List,
            DataType::Bytea => TypeName::Bytea,
        }
    }

    pub fn to_protobuf(&self) -> ProstDataType {
        let mut pb = ProstDataType {
            type_name: self.prost_type_name() as i32,
            is_nullable: true,
            ..Default::default()
        };
        match self {
            DataType::Struct(t) => {
                pb.field_type = t.fields.iter().map(|f| f.to_protobuf()).collect_vec();
                pb.field_names = t.field_names.clone();
            }
            DataType::List { datatype } => {
                pb.field_type = vec![datatype.to_protobuf()];
            }
            _ => {}
        }
        pb
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

    pub fn is_scalar(&self) -> bool {
        DataTypeName::from(self).is_scalar()
    }

    pub fn is_int(&self) -> bool {
        matches!(self, DataType::Int16 | DataType::Int32 | DataType::Int64)
    }

    /// Returns the output type of window function on a given input type.
    pub fn window_of(input: &DataType) -> Option<DataType> {
        match input {
            DataType::Timestamptz => Some(DataType::Timestamptz),
            DataType::Timestamp | DataType::Date => Some(DataType::Timestamp),
            _ => None,
        }
    }

    pub fn new_struct(fields: Vec<DataType>, field_names: Vec<String>) -> Self {
        Self::Struct(
            StructType {
                fields,
                field_names,
            }
            .into(),
        )
    }

    /// WARNING: Currently this should only be used in `WatermarkFilterExecutor`. Please be careful
    /// if you want to use this.
    pub fn min(&self) -> ScalarImpl {
        match self {
            DataType::Int16 => ScalarImpl::Int16(i16::MIN),
            DataType::Int32 => ScalarImpl::Int32(i32::MIN),
            DataType::Int64 => ScalarImpl::Int64(i64::MIN),
            DataType::Float32 => ScalarImpl::Float32(OrderedF32::neg_infinity()),
            DataType::Float64 => ScalarImpl::Float64(OrderedF64::neg_infinity()),
            DataType::Boolean => ScalarImpl::Bool(false),
            DataType::Varchar => ScalarImpl::Utf8("".into()),
            DataType::Bytea => ScalarImpl::Bytea("".to_string().into_bytes().into()),
            DataType::Date => ScalarImpl::NaiveDate(NaiveDateWrapper(NaiveDate::MIN)),
            DataType::Time => ScalarImpl::NaiveTime(NaiveTimeWrapper::from_hms_uncheck(0, 0, 0)),
            DataType::Timestamp => {
                ScalarImpl::NaiveDateTime(NaiveDateTimeWrapper(NaiveDateTime::MIN))
            }
            // FIXME(yuhao): Add a timestamptz scalar.
            DataType::Timestamptz => ScalarImpl::Int64(i64::MIN),
            DataType::Decimal => ScalarImpl::Decimal(Decimal::NegativeInf),
            DataType::Interval => ScalarImpl::Interval(IntervalUnit::MIN),
            DataType::Jsonb => ScalarImpl::Jsonb(JsonbVal::dummy()), // NOT `min` #7981
            DataType::Struct(data_types) => ScalarImpl::Struct(StructValue::new(
                data_types
                    .fields
                    .iter()
                    .map(|data_type| Some(data_type.min()))
                    .collect_vec(),
            )),
            DataType::List { .. } => ScalarImpl::List(ListValue::new(vec![])),
        }
    }
}

impl From<DataType> for ProstDataType {
    fn from(data_type: DataType) -> Self {
        data_type.to_protobuf()
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
    + TryFrom<ScalarImpl, Error = ArrayError>
    + Into<ScalarImpl>
{
    /// Type for reference of `Scalar`
    type ScalarRefType<'a>: ScalarRef<'a, ScalarType = Self> + 'a
    where
        Self: 'a;

    /// Get a reference to current scalar.
    fn as_scalar_ref(&self) -> Self::ScalarRefType<'_>;

    fn to_scalar_value(self) -> ScalarImpl {
        self.into()
    }
}

/// Convert an `Option<Scalar>` to corresponding `Option<ScalarRef>`.
pub fn option_as_scalar_ref<S: Scalar>(scalar: &Option<S>) -> Option<S::ScalarRefType<'_>> {
    scalar.as_ref().map(|x| x.as_scalar_ref())
}

/// `ScalarRef` is a trait over all possible references in the evaluation
/// framework.
///
/// `ScalarRef` is reciprocal to `Scalar`. Use `to_owned_scalar` to get an
/// owned scalar.
pub trait ScalarRef<'a>:
    Copy
    + std::fmt::Debug
    + 'a
    + TryFrom<ScalarRefImpl<'a>, Error = ArrayError>
    + Into<ScalarRefImpl<'a>>
{
    /// `ScalarType` is the owned type of current `ScalarRef`.
    type ScalarType: Scalar<ScalarRefType<'a> = Self>;

    /// Convert `ScalarRef` to an owned scalar.
    fn to_owned_scalar(&self) -> Self::ScalarType;

    /// A wrapped hash function to get the hash value for this scaler.
    fn hash_scalar<H: std::hash::Hasher>(&self, state: &mut H);
}

/// `for_all_scalar_variants` includes all variants of our scalar types. If you added a new scalar
/// type inside the project, be sure to add a variant here.
///
/// It is used to simplify the boilerplate code of repeating all scalar types, while each type
/// has exactly the same code.
///
/// To use it, you need to provide a macro, whose input is `{ enum variant name, function suffix
/// name, scalar type, scalar ref type }` tuples. Refer to the following implementations as
/// examples.
#[macro_export]
macro_rules! for_all_scalar_variants {
    ($macro:ident) => {
        $macro! {
            { Int16, int16, i16, i16 },
            { Int32, int32, i32, i32 },
            { Int64, int64, i64, i64 },
            { Serial, serial, Serial, Serial },
            { Float32, float32, OrderedF32, OrderedF32 },
            { Float64, float64, OrderedF64, OrderedF64 },
            { Utf8, utf8, Box<str>, &'scalar str },
            { Bool, bool, bool, bool },
            { Decimal, decimal, Decimal, Decimal  },
            { Interval, interval, IntervalUnit, IntervalUnit },
            { NaiveDate, naivedate, NaiveDateWrapper, NaiveDateWrapper },
            { NaiveDateTime, naivedatetime, NaiveDateTimeWrapper, NaiveDateTimeWrapper },
            { NaiveTime, naivetime, NaiveTimeWrapper, NaiveTimeWrapper },
            { Jsonb, jsonb, JsonbVal, JsonbRef<'scalar> },
            { Struct, struct, StructValue, StructRef<'scalar> },
            { List, list, ListValue, ListRef<'scalar> },
            { Bytea, bytea, Box<[u8]>, &'scalar [u8] }
        }
    };
}

/// Define `ScalarImpl` and `ScalarRefImpl` with macro.
macro_rules! scalar_impl_enum {
    ($( { $variant_name:ident, $suffix_name:ident, $scalar:ty, $scalar_ref:ty } ),*) => {
        /// `ScalarImpl` embeds all possible scalars in the evaluation framework.
        #[derive(Debug, Clone, PartialEq, Eq)]
        pub enum ScalarImpl {
            $( $variant_name($scalar) ),*
        }

        /// `ScalarRefImpl` embeds all possible scalar references in the evaluation
        /// framework.
        #[derive(Debug, Copy, Clone, PartialEq, Eq)]
        pub enum ScalarRefImpl<'scalar> {
            $( $variant_name($scalar_ref) ),*
        }
    };
}

for_all_scalar_variants! { scalar_impl_enum }

/// Implement [`PartialOrd`] and [`Ord`] for [`ScalarImpl`] and [`ScalarRefImpl`].
///
/// Scalars of different types are not comparable. For this case, `partial_cmp` returns `None` and
/// `cmp` will panic.
macro_rules! scalar_impl_partial_ord {
    ($( { $variant_name:ident, $suffix_name:ident, $scalar:ty, $scalar_ref:ty } ),*) => {
        impl PartialOrd for ScalarImpl {
            fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
                match (self, other) {
                    $( (Self::$variant_name(lhs), Self::$variant_name(rhs)) => Some(lhs.cmp(rhs)), )*
                    _ => None,
                }
            }
        }
        impl Ord for ScalarImpl {
            fn cmp(&self, other: &Self) -> std::cmp::Ordering {
                self.partial_cmp(other).unwrap_or_else(|| panic!("cannot compare {self:?} with {other:?}"))
            }
        }

        impl PartialOrd for ScalarRefImpl<'_> {
            fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
                match (self, other) {
                    $( (Self::$variant_name(lhs), Self::$variant_name(rhs)) => Some(lhs.cmp(rhs)), )*
                    _ => None,
                }
            }
        }
        impl Ord for ScalarRefImpl<'_> {
            fn cmp(&self, other: &Self) -> std::cmp::Ordering {
                self.partial_cmp(other).unwrap_or_else(|| panic!("cannot compare {self:?} with {other:?}"))
            }
        }
    };
}

for_all_scalar_variants! { scalar_impl_partial_ord }

pub type Datum = Option<ScalarImpl>;
pub type DatumRef<'a> = Option<ScalarRefImpl<'a>>;

// TODO(MrCroxx): turn Datum into a struct, and impl ser/de as its member functions. (#477)
// TODO: specify `NULL FIRST` or `NULL LAST`.
pub fn memcmp_serialize_datum_into(
    datum: impl ToDatumRef,
    serializer: &mut memcomparable::Serializer<impl BufMut>,
) -> memcomparable::Result<()> {
    // By default, `null` is treated as largest in PostgreSQL.
    if let Some(datum) = datum.to_datum_ref() {
        0u8.serialize(&mut *serializer)?;
        datum.serialize(serializer)?;
    } else {
        1u8.serialize(serializer)?;
    }
    Ok(())
}

// TODO(MrCroxx): turn Datum into a struct, and impl ser/de as its member functions. (#477)
#[cfg_attr(not(test), expect(dead_code))]
fn memcmp_serialize_datum_not_null_into(
    datum: impl ToDatumRef,
    serializer: &mut memcomparable::Serializer<impl BufMut>,
) -> memcomparable::Result<()> {
    datum
        .to_datum_ref()
        .as_ref()
        .expect("datum cannot be null")
        .serialize(serializer)
}

// TODO(MrCroxx): turn Datum into a struct, and impl ser/de as its member functions. (#477)
pub fn memcmp_deserialize_datum_from(
    ty: &DataType,
    deserializer: &mut memcomparable::Deserializer<impl Buf>,
) -> memcomparable::Result<Datum> {
    let null_tag = u8::deserialize(&mut *deserializer)?;
    match null_tag {
        1 => Ok(None),
        0 => Ok(Some(ScalarImpl::deserialize(ty, deserializer)?)),
        _ => Err(memcomparable::Error::InvalidTagEncoding(null_tag as _)),
    }
}

// TODO(MrCroxx): turn Datum into a struct, and impl ser/de as its member functions. (#477)
#[cfg_attr(not(test), expect(dead_code))]
fn memcmp_deserialize_datum_not_null_from(
    ty: &DataType,
    deserializer: &mut memcomparable::Deserializer<impl Buf>,
) -> memcomparable::Result<Datum> {
    Ok(Some(ScalarImpl::deserialize(ty, deserializer)?))
}

/// This trait is to implement `to_owned_datum` for `Option<ScalarImpl>`
pub trait ToOwnedDatum {
    /// Convert the datum to an owned [`Datum`].
    fn to_owned_datum(self) -> Datum;
}

impl ToOwnedDatum for DatumRef<'_> {
    #[inline(always)]
    fn to_owned_datum(self) -> Datum {
        self.map(ScalarRefImpl::into_scalar_impl)
    }
}

pub trait ToDatumRef: PartialEq + Eq + std::fmt::Debug {
    /// Convert the datum to [`DatumRef`].
    fn to_datum_ref(&self) -> DatumRef<'_>;
}

impl ToDatumRef for Datum {
    #[inline(always)]
    fn to_datum_ref(&self) -> DatumRef<'_> {
        self.as_ref().map(|d| d.as_scalar_ref_impl())
    }
}
impl ToDatumRef for &Datum {
    #[inline(always)]
    fn to_datum_ref(&self) -> DatumRef<'_> {
        self.as_ref().map(|d| d.as_scalar_ref_impl())
    }
}
impl ToDatumRef for Option<&ScalarImpl> {
    #[inline(always)]
    fn to_datum_ref(&self) -> DatumRef<'_> {
        self.map(|d| d.as_scalar_ref_impl())
    }
}
impl ToDatumRef for DatumRef<'_> {
    #[inline(always)]
    fn to_datum_ref(&self) -> DatumRef<'_> {
        *self
    }
}

/// `for_all_native_types` includes all native variants of our scalar types.
///
/// Specifically, it doesn't support u8/u16/u32/u64.
#[macro_export]
macro_rules! for_all_native_types {
    ($macro:ident) => {
        $macro! {
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
    ($( { $variant_name:ident, $suffix_name:ident, $scalar:ty, $scalar_ref:ty } ),*) => {
        $(
            impl From<$scalar> for ScalarImpl {
                fn from(val: $scalar) -> Self {
                    ScalarImpl::$variant_name(val)
                }
            }

            impl TryFrom<ScalarImpl> for $scalar {
                type Error = ArrayError;

                fn try_from(val: ScalarImpl) -> ArrayResult<Self> {
                    match val {
                        ScalarImpl::$variant_name(scalar) => Ok(scalar),
                        other_scalar => bail!("cannot convert ScalarImpl::{} to concrete type", other_scalar.get_ident()),
                    }
                }
            }

            impl <'scalar> From<$scalar_ref> for ScalarRefImpl<'scalar> {
                fn from(val: $scalar_ref) -> Self {
                    ScalarRefImpl::$variant_name(val)
                }
            }

            impl <'scalar> TryFrom<ScalarRefImpl<'scalar>> for $scalar_ref {
                type Error = ArrayError;

                fn try_from(val: ScalarRefImpl<'scalar>) -> ArrayResult<Self> {
                    match val {
                        ScalarRefImpl::$variant_name(scalar_ref) => Ok(scalar_ref),
                        other_scalar => bail!("cannot convert ScalarRefImpl::{} to concrete type {}", other_scalar.get_ident(), stringify!($variant_name)),
                    }
                }
            }

            paste! {
                impl ScalarImpl {
                    pub fn [<as_ $suffix_name>](&self) -> &$scalar {
                        match self {
                            Self::$variant_name(ref scalar) => scalar,
                            other_scalar => panic!("cannot convert ScalarImpl::{} to concrete type {}", other_scalar.get_ident(), stringify!($variant_name))
                        }
                    }

                    pub fn [<into_ $suffix_name>](self) -> $scalar {
                        match self {
                            Self::$variant_name(scalar) => scalar,
                            other_scalar =>  panic!("cannot convert ScalarImpl::{} to concrete type {}", other_scalar.get_ident(), stringify!($variant_name))
                        }
                    }
                }

                impl <'scalar> ScalarRefImpl<'scalar> {
                    // Note that this conversion consume self.
                    pub fn [<into_ $suffix_name>](self) -> $scalar_ref {
                        match self {
                            Self::$variant_name(inner) => inner,
                            other_scalar => panic!("cannot convert ScalarRefImpl::{} to concrete type {}", other_scalar.get_ident(), stringify!($variant_name))
                        }
                    }
                }
            }
        )*
    };
}

for_all_scalar_variants! { impl_convert }

// Implement `From<raw float>` for `ScalarImpl::Float` as a sugar.
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

// Implement `From<string like>` for `ScalarImpl::Utf8` as a sugar.
impl From<String> for ScalarImpl {
    fn from(s: String) -> Self {
        Self::Utf8(s.into_boxed_str())
    }
}
impl From<&str> for ScalarImpl {
    fn from(s: &str) -> Self {
        Self::Utf8(s.into())
    }
}
impl From<&String> for ScalarImpl {
    fn from(s: &String) -> Self {
        Self::Utf8(s.as_str().into())
    }
}

macro_rules! impl_scalar_impl_ref_conversion {
    ($( { $variant_name:ident, $suffix_name:ident, $scalar:ty, $scalar_ref:ty } ),*) => {
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

/// Implement [`Hash`] for [`ScalarImpl`] and [`ScalarRefImpl`] with `hash_scalar`.
///
/// Should behave the same as [`crate::array::Array::hash_at`].
macro_rules! scalar_impl_hash {
    ($( { $variant_name:ident, $suffix_name:ident, $scalar:ty, $scalar_ref:ty } ),*) => {
        impl Hash for ScalarRefImpl<'_> {
            fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
                match self {
                    $( Self::$variant_name(inner) => inner.hash_scalar(state), )*
                }
            }
        }

        impl Hash for ScalarImpl {
            fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
                match self {
                    $( Self::$variant_name(inner) => inner.as_scalar_ref().hash_scalar(state), )*
                }
            }
        }
    };
}

for_all_scalar_variants! { scalar_impl_hash }

/// Feeds the raw scalar reference of `datum` to the given `state`, which should behave the same
/// as [`crate::array::Array::hash_at`], where NULL value will be carefully handled.
///
/// **FIXME**: the result of this function might be different from [`std::hash::Hash`] due to the
/// type alias of `DatumRef = Option<_>`, we should manually implement [`std::hash::Hash`] for
/// [`DatumRef`] in the future when it becomes a newtype. (#477)
#[inline(always)]
pub fn hash_datum(datum: impl ToDatumRef, state: &mut impl std::hash::Hasher) {
    match datum.to_datum_ref() {
        Some(scalar_ref) => scalar_ref.hash(state),
        None => NULL_VAL_FOR_HASH.hash(state),
    }
}

impl ScalarRefImpl<'_> {
    /// Encode the scalar to postgresql binary format.
    /// The encoder implements encoding using <https://docs.rs/postgres-types/0.2.3/postgres_types/trait.ToSql.html>
    pub fn binary_format(&self, data_type: &DataType) -> RwResult<Bytes> {
        self.to_binary_with_type(data_type).transpose().unwrap()
    }

    pub fn text_format(&self, data_type: &DataType) -> String {
        self.to_text_with_type(data_type)
    }

    /// Serialize the scalar.
    pub fn serialize(
        &self,
        ser: &mut memcomparable::Serializer<impl BufMut>,
    ) -> memcomparable::Result<()> {
        match self {
            Self::Int16(v) => v.serialize(ser)?,
            Self::Int32(v) => v.serialize(ser)?,
            Self::Int64(v) => v.serialize(ser)?,
            Self::Serial(v) => v.serialize(ser)?,
            Self::Float32(v) => v.serialize(ser)?,
            Self::Float64(v) => v.serialize(ser)?,
            Self::Utf8(v) => v.serialize(ser)?,
            Self::Bytea(v) => v.serialize(ser)?,
            Self::Bool(v) => v.serialize(ser)?,
            Self::Decimal(v) => ser.serialize_decimal((*v).into())?,
            Self::Interval(v) => v.serialize(ser)?,
            Self::NaiveDate(v) => v.0.num_days_from_ce().serialize(ser)?,
            Self::NaiveDateTime(v) => {
                v.0.timestamp().serialize(&mut *ser)?;
                v.0.timestamp_subsec_nanos().serialize(ser)?;
            }
            Self::NaiveTime(v) => {
                v.0.num_seconds_from_midnight().serialize(&mut *ser)?;
                v.0.nanosecond().serialize(ser)?;
            }
            Self::Jsonb(v) => v.memcmp_serialize(ser)?,
            Self::Struct(v) => v.memcmp_serialize(ser)?,
            Self::List(v) => v.memcmp_serialize(ser)?,
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
        ty: &DataType,
        de: &mut memcomparable::Deserializer<impl Buf>,
    ) -> memcomparable::Result<Self> {
        use DataType as Ty;
        Ok(match ty {
            Ty::Int16 => Self::Int16(i16::deserialize(de)?),
            Ty::Int32 => Self::Int32(i32::deserialize(de)?),
            Ty::Int64 => Self::Int64(i64::deserialize(de)?),
            Ty::Float32 => Self::Float32(f32::deserialize(de)?.into()),
            Ty::Float64 => Self::Float64(f64::deserialize(de)?.into()),
            Ty::Varchar => Self::Utf8(Box::<str>::deserialize(de)?),
            Ty::Bytea => Self::Bytea(Box::<[u8]>::deserialize(de)?),
            Ty::Boolean => Self::Bool(bool::deserialize(de)?),
            Ty::Decimal => Self::Decimal(de.deserialize_decimal()?.into()),
            Ty::Interval => Self::Interval(IntervalUnit::deserialize(de)?),
            Ty::Time => Self::NaiveTime({
                let secs = u32::deserialize(&mut *de)?;
                let nano = u32::deserialize(de)?;
                NaiveTimeWrapper::with_secs_nano(secs, nano)
                    .map_err(|e| memcomparable::Error::Message(format!("{e}")))?
            }),
            Ty::Timestamp => Self::NaiveDateTime({
                let secs = i64::deserialize(&mut *de)?;
                let nsecs = u32::deserialize(de)?;
                NaiveDateTimeWrapper::with_secs_nsecs(secs, nsecs)
                    .map_err(|e| memcomparable::Error::Message(format!("{e}")))?
            }),
            Ty::Timestamptz => Self::Int64(i64::deserialize(de)?),
            Ty::Date => Self::NaiveDate({
                let days = i32::deserialize(de)?;
                NaiveDateWrapper::with_days(days)
                    .map_err(|e| memcomparable::Error::Message(format!("{e}")))?
            }),
            Ty::Jsonb => Self::Jsonb(JsonbVal::memcmp_deserialize(de)?),
            Ty::Struct(t) => StructValue::memcmp_deserialize(&t.fields, de)?.to_scalar_value(),
            Ty::List { datatype } => ListValue::memcmp_deserialize(datatype, de)?.to_scalar_value(),
        })
    }

    /// Deserialize the `data_size` of `input_data_type` in `storage_encoding`. This function will
    /// consume the offset of deserializer then return the length (without memcopy, only length
    /// calculation). The difference between `encoding_data_size` and `ScalarImpl::data_size` is
    /// that `ScalarImpl::data_size` calculates the `memory_length` of type instead of
    /// `storage_encoding`
    pub fn encoding_data_size(
        data_type: &DataType,
        deserializer: &mut memcomparable::Deserializer<impl Buf>,
    ) -> memcomparable::Result<usize> {
        let base_position = deserializer.position();
        let null_tag = u8::deserialize(&mut *deserializer)?;
        match null_tag {
            1 => {}
            0 => {
                use std::mem::size_of;
                let len = match data_type {
                    DataType::Int16 => size_of::<i16>(),
                    DataType::Int32 => size_of::<i32>(),
                    DataType::Int64 => size_of::<i64>(),
                    DataType::Float32 => size_of::<OrderedF32>(),
                    DataType::Float64 => size_of::<OrderedF64>(),
                    DataType::Date => size_of::<NaiveDateWrapper>(),
                    DataType::Time => size_of::<NaiveTimeWrapper>(),
                    DataType::Timestamp => size_of::<NaiveDateTimeWrapper>(),
                    DataType::Timestamptz => size_of::<i64>(),
                    DataType::Boolean => size_of::<u8>(),
                    // IntervalUnit is serialized as (i32, i32, i64)
                    DataType::Interval => size_of::<(i32, i32, i64)>(),
                    DataType::Decimal => {
                        deserializer.deserialize_decimal()?;
                        0 // the len is not used since decimal is not a fixed length type
                    }
                    // these two types is var-length and should only be determine at runtime.
                    // TODO: need some test for this case (e.g. e2e test)
                    DataType::List { .. } => deserializer.skip_bytes()?,
                    DataType::Struct(t) => t
                        .fields
                        .iter()
                        .map(|field| Self::encoding_data_size(field, deserializer))
                        .try_fold(0, |a, b| b.map(|b| a + b))?,
                    DataType::Jsonb => deserializer.skip_bytes()?,
                    DataType::Varchar => deserializer.skip_bytes()?,
                    DataType::Bytea => deserializer.skip_bytes()?,
                };

                // consume offset of fixed_type
                if deserializer.position() == base_position + 1 {
                    // fixed type
                    deserializer.advance(len);
                }
            }
            _ => return Err(memcomparable::Error::InvalidTagEncoding(null_tag as _)),
        }

        Ok(deserializer.position() - base_position)
    }

    pub fn as_integral(&self) -> i64 {
        match self {
            Self::Int16(v) => *v as i64,
            Self::Int32(v) => *v as i64,
            Self::Int64(v) => *v,
            _ => panic!(
                "Can't convert ScalarImpl::{} to a integral",
                self.get_ident()
            ),
        }
    }
}

pub fn literal_type_match(data_type: &DataType, literal: Option<&ScalarImpl>) -> bool {
    match literal {
        Some(datum) => {
            matches!(
                (data_type, datum),
                (DataType::Boolean, ScalarImpl::Bool(_))
                    | (DataType::Int16, ScalarImpl::Int16(_))
                    | (DataType::Int32, ScalarImpl::Int32(_))
                    | (DataType::Int64, ScalarImpl::Int64(_))
                    | (DataType::Float32, ScalarImpl::Float32(_))
                    | (DataType::Float64, ScalarImpl::Float64(_))
                    | (DataType::Varchar, ScalarImpl::Utf8(_))
                    | (DataType::Bytea, ScalarImpl::Bytea(_))
                    | (DataType::Date, ScalarImpl::NaiveDate(_))
                    | (DataType::Time, ScalarImpl::NaiveTime(_))
                    | (DataType::Timestamp, ScalarImpl::NaiveDateTime(_))
                    | (DataType::Timestamptz, ScalarImpl::Int64(_))
                    | (DataType::Decimal, ScalarImpl::Decimal(_))
                    | (DataType::Interval, ScalarImpl::Interval(_))
                    | (DataType::Jsonb, ScalarImpl::Jsonb(_))
                    | (DataType::Struct { .. }, ScalarImpl::Struct(_))
                    | (DataType::List { .. }, ScalarImpl::List(_))
            )
        }
        None => true,
    }
}

#[cfg(test)]
mod tests {
    use std::hash::{BuildHasher, Hasher};
    use std::ops::Neg;

    use itertools::Itertools;
    use rand::thread_rng;
    use strum::IntoEnumIterator;

    use super::*;
    use crate::util::hash_util::Crc32FastBuilder;

    fn serialize_datum_not_null_into_vec(data: i64) -> Vec<u8> {
        let mut serializer = memcomparable::Serializer::new(vec![]);
        memcmp_serialize_datum_not_null_into(&Some(ScalarImpl::Int64(data)), &mut serializer)
            .unwrap();
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
    fn test_issue_legacy_2057_ordered_float_memcomparable() {
        use num_traits::*;
        use rand::seq::SliceRandom;

        fn serialize(f: OrderedF32) -> Vec<u8> {
            let mut serializer = memcomparable::Serializer::new(vec![]);
            memcmp_serialize_datum_not_null_into(&Some(f.into()), &mut serializer).unwrap();
            serializer.into_inner()
        }

        fn deserialize(data: Vec<u8>) -> OrderedF32 {
            let mut deserializer = memcomparable::Deserializer::new(data.as_slice());
            let datum =
                memcmp_deserialize_datum_not_null_from(&DataType::Float32, &mut deserializer)
                    .unwrap();
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

    #[test]
    fn test_size() {
        use static_assertions::const_assert_eq;

        use crate::array::*;

        macro_rules! assert_item_size_eq {
            ($array:ty, $size:literal) => {
                const_assert_eq!(std::mem::size_of::<<$array as Array>::OwnedItem>(), $size);
            };
        }

        assert_item_size_eq!(StructArray, 16); // Box<[Datum]>
        assert_item_size_eq!(ListArray, 16); // Box<[Datum]>
        assert_item_size_eq!(Utf8Array, 16); // Box<str>
        assert_item_size_eq!(IntervalArray, 16);
        assert_item_size_eq!(NaiveDateTimeArray, 12);

        // TODO: try to reduce the memory usage of `Decimal`, `ScalarImpl` and `Datum`.
        assert_item_size_eq!(DecimalArray, 20);

        const_assert_eq!(std::mem::size_of::<ScalarImpl>(), 24);
        const_assert_eq!(std::mem::size_of::<Datum>(), 24);
    }

    #[test]
    fn test_data_type_display() {
        let d: DataType = DataType::new_struct(
            vec![DataType::Int32, DataType::Varchar],
            vec!["i".to_string(), "j".to_string()],
        );
        assert_eq!(format!("{}", d), "struct<i integer,j varchar>".to_string());
    }

    #[test]
    fn test_hash_implementation() {
        fn test(datum: Datum, data_type: DataType) {
            assert!(literal_type_match(&data_type, datum.as_ref()));

            let mut builder = data_type.create_array_builder(6);
            for _ in 0..3 {
                builder.append_null();
                builder.append_datum(&datum);
            }
            let array = builder.finish();

            let hash_from_array = {
                let mut state = Crc32FastBuilder.build_hasher();
                array.hash_at(3, &mut state);
                state.finish()
            };

            let hash_from_datum = {
                let mut state = Crc32FastBuilder.build_hasher();
                hash_datum(&datum, &mut state);
                state.finish()
            };

            let hash_from_datum_ref = {
                let mut state = Crc32FastBuilder.build_hasher();
                hash_datum(datum.to_datum_ref(), &mut state);
                state.finish()
            };

            assert_eq!(hash_from_array, hash_from_datum);
            assert_eq!(hash_from_datum, hash_from_datum_ref);
        }

        for name in DataTypeName::iter() {
            let (scalar, data_type) = match name {
                DataTypeName::Boolean => (ScalarImpl::Bool(true), DataType::Boolean),
                DataTypeName::Int16 => (ScalarImpl::Int16(233), DataType::Int16),
                DataTypeName::Int32 => (ScalarImpl::Int32(233333), DataType::Int32),
                DataTypeName::Int64 => (ScalarImpl::Int64(233333333333), DataType::Int64),
                DataTypeName::Float32 => (ScalarImpl::Float32(23.33.into()), DataType::Float32),
                DataTypeName::Float64 => (
                    ScalarImpl::Float64(23.333333333333.into()),
                    DataType::Float64,
                ),
                DataTypeName::Decimal => (
                    ScalarImpl::Decimal("233.33".parse().unwrap()),
                    DataType::Decimal,
                ),
                DataTypeName::Date => (
                    ScalarImpl::NaiveDate(NaiveDateWrapper::from_ymd_uncheck(2333, 3, 3)),
                    DataType::Date,
                ),
                DataTypeName::Varchar => (ScalarImpl::Utf8("233".into()), DataType::Varchar),
                DataTypeName::Bytea => (
                    ScalarImpl::Bytea("\\x233".as_bytes().into()),
                    DataType::Bytea,
                ),
                DataTypeName::Time => (
                    ScalarImpl::NaiveTime(NaiveTimeWrapper::from_hms_uncheck(2, 3, 3)),
                    DataType::Time,
                ),
                DataTypeName::Timestamp => (
                    ScalarImpl::NaiveDateTime(NaiveDateTimeWrapper::from_timestamp_uncheck(
                        23333333, 2333,
                    )),
                    DataType::Timestamp,
                ),
                DataTypeName::Timestamptz => (ScalarImpl::Int64(233333333), DataType::Timestamptz),
                DataTypeName::Interval => (
                    ScalarImpl::Interval(IntervalUnit::new(2, 3, 3333)),
                    DataType::Interval,
                ),
                DataTypeName::Jsonb => (ScalarImpl::Jsonb(JsonbVal::dummy()), DataType::Jsonb),
                DataTypeName::Struct => (
                    ScalarImpl::Struct(StructValue::new(vec![
                        ScalarImpl::Int64(233).into(),
                        ScalarImpl::Float64(23.33.into()).into(),
                    ])),
                    DataType::Struct(
                        StructType::new(vec![
                            (DataType::Int64, "a".to_string()),
                            (DataType::Float64, "b".to_string()),
                        ])
                        .into(),
                    ),
                ),
                DataTypeName::List => (
                    ScalarImpl::List(ListValue::new(vec![
                        ScalarImpl::Int64(233).into(),
                        ScalarImpl::Int64(2333).into(),
                    ])),
                    DataType::List {
                        datatype: Box::new(DataType::Int64),
                    },
                ),
            };

            test(Some(scalar), data_type.clone());
            test(None, data_type);
        }
    }

    #[test]
    fn test_data_type_from_str() {
        assert_eq!(DataType::from_str("bool").unwrap(), DataType::Boolean);
        assert_eq!(DataType::from_str("boolean").unwrap(), DataType::Boolean);
        assert_eq!(DataType::from_str("BOOL").unwrap(), DataType::Boolean);
        assert_eq!(DataType::from_str("BOOLEAN").unwrap(), DataType::Boolean);

        assert_eq!(DataType::from_str("int2").unwrap(), DataType::Int16);
        assert_eq!(DataType::from_str("smallint").unwrap(), DataType::Int16);
        assert_eq!(DataType::from_str("INT2").unwrap(), DataType::Int16);
        assert_eq!(DataType::from_str("SMALLINT").unwrap(), DataType::Int16);

        assert_eq!(DataType::from_str("int4").unwrap(), DataType::Int32);
        assert_eq!(DataType::from_str("integer").unwrap(), DataType::Int32);
        assert_eq!(DataType::from_str("int4").unwrap(), DataType::Int32);
        assert_eq!(DataType::from_str("INT4").unwrap(), DataType::Int32);
        assert_eq!(DataType::from_str("INTEGER").unwrap(), DataType::Int32);
        assert_eq!(DataType::from_str("INT").unwrap(), DataType::Int32);

        assert_eq!(DataType::from_str("int8").unwrap(), DataType::Int64);
        assert_eq!(DataType::from_str("bigint").unwrap(), DataType::Int64);
        assert_eq!(DataType::from_str("INT8").unwrap(), DataType::Int64);
        assert_eq!(DataType::from_str("BIGINT").unwrap(), DataType::Int64);

        assert_eq!(DataType::from_str("float4").unwrap(), DataType::Float32);
        assert_eq!(DataType::from_str("real").unwrap(), DataType::Float32);
        assert_eq!(DataType::from_str("FLOAT4").unwrap(), DataType::Float32);
        assert_eq!(DataType::from_str("REAL").unwrap(), DataType::Float32);

        assert_eq!(DataType::from_str("float8").unwrap(), DataType::Float64);
        assert_eq!(
            DataType::from_str("double precision").unwrap(),
            DataType::Float64
        );
        assert_eq!(DataType::from_str("FLOAT8").unwrap(), DataType::Float64);
        assert_eq!(
            DataType::from_str("DOUBLE PRECISION").unwrap(),
            DataType::Float64
        );

        assert_eq!(DataType::from_str("decimal").unwrap(), DataType::Decimal);
        assert_eq!(DataType::from_str("DECIMAL").unwrap(), DataType::Decimal);
        assert_eq!(DataType::from_str("numeric").unwrap(), DataType::Decimal);
        assert_eq!(DataType::from_str("NUMERIC").unwrap(), DataType::Decimal);

        assert_eq!(DataType::from_str("date").unwrap(), DataType::Date);
        assert_eq!(DataType::from_str("DATE").unwrap(), DataType::Date);

        assert_eq!(DataType::from_str("varchar").unwrap(), DataType::Varchar);
        assert_eq!(DataType::from_str("VARCHAR").unwrap(), DataType::Varchar);

        assert_eq!(DataType::from_str("time").unwrap(), DataType::Time);
        assert_eq!(
            DataType::from_str("time without time zone").unwrap(),
            DataType::Time
        );
        assert_eq!(DataType::from_str("TIME").unwrap(), DataType::Time);
        assert_eq!(
            DataType::from_str("TIME WITHOUT TIME ZONE").unwrap(),
            DataType::Time
        );

        assert_eq!(
            DataType::from_str("timestamp").unwrap(),
            DataType::Timestamp
        );
        assert_eq!(
            DataType::from_str("timestamp without time zone").unwrap(),
            DataType::Timestamp
        );
        assert_eq!(
            DataType::from_str("TIMESTAMP").unwrap(),
            DataType::Timestamp
        );
        assert_eq!(
            DataType::from_str("TIMESTAMP WITHOUT TIME ZONE").unwrap(),
            DataType::Timestamp
        );

        assert_eq!(
            DataType::from_str("timestamptz").unwrap(),
            DataType::Timestamptz
        );
        assert_eq!(
            DataType::from_str("timestamp with time zone").unwrap(),
            DataType::Timestamptz
        );
        assert_eq!(
            DataType::from_str("TIMESTAMPTZ").unwrap(),
            DataType::Timestamptz
        );
        assert_eq!(
            DataType::from_str("TIMESTAMP WITH TIME ZONE").unwrap(),
            DataType::Timestamptz
        );

        assert_eq!(DataType::from_str("interval").unwrap(), DataType::Interval);
        assert_eq!(DataType::from_str("INTERVAL").unwrap(), DataType::Interval);

        assert_eq!(
            DataType::from_str("int2[]").unwrap(),
            DataType::List {
                datatype: Box::new(DataType::Int16)
            }
        );
        assert_eq!(
            DataType::from_str("int[]").unwrap(),
            DataType::List {
                datatype: Box::new(DataType::Int32)
            }
        );
        assert_eq!(
            DataType::from_str("int8[]").unwrap(),
            DataType::List {
                datatype: Box::new(DataType::Int64)
            }
        );
        assert_eq!(
            DataType::from_str("float4[]").unwrap(),
            DataType::List {
                datatype: Box::new(DataType::Float32)
            }
        );
        assert_eq!(
            DataType::from_str("float8[]").unwrap(),
            DataType::List {
                datatype: Box::new(DataType::Float64)
            }
        );
        assert_eq!(
            DataType::from_str("decimal[]").unwrap(),
            DataType::List {
                datatype: Box::new(DataType::Decimal)
            }
        );
        assert_eq!(
            DataType::from_str("varchar[]").unwrap(),
            DataType::List {
                datatype: Box::new(DataType::Varchar)
            }
        );
        assert_eq!(
            DataType::from_str("date[]").unwrap(),
            DataType::List {
                datatype: Box::new(DataType::Date)
            }
        );
        assert_eq!(
            DataType::from_str("time[]").unwrap(),
            DataType::List {
                datatype: Box::new(DataType::Time)
            }
        );
        assert_eq!(
            DataType::from_str("timestamp[]").unwrap(),
            DataType::List {
                datatype: Box::new(DataType::Timestamp)
            }
        );
        assert_eq!(
            DataType::from_str("timestamptz[]").unwrap(),
            DataType::List {
                datatype: Box::new(DataType::Timestamptz)
            }
        );
        assert_eq!(
            DataType::from_str("interval[]").unwrap(),
            DataType::List {
                datatype: Box::new(DataType::Interval)
            }
        );
    }
}
