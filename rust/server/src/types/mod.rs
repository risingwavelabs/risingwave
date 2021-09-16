use crate::error::{Result, RwError};
use risingwave_proto::data::DataType as DataTypeProto;
use rust_decimal::Decimal;
use std::any::Any;
use std::sync::Arc;

mod numeric_type;
pub use numeric_type::*;
mod primitive_data_type;
pub use primitive_data_type::*;
mod native_type;

use crate::error::ErrorCode::InternalError;
pub use native_type::*;
use risingwave_proto::data::DataType_TypeName::BOOLEAN;
use risingwave_proto::data::DataType_TypeName::CHAR;
use risingwave_proto::data::DataType_TypeName::DATE;
use risingwave_proto::data::DataType_TypeName::DECIMAL;
use risingwave_proto::data::DataType_TypeName::DOUBLE;
use risingwave_proto::data::DataType_TypeName::FLOAT;
use risingwave_proto::data::DataType_TypeName::INT16;
use risingwave_proto::data::DataType_TypeName::INT32;
use risingwave_proto::data::DataType_TypeName::INT64;
use risingwave_proto::data::DataType_TypeName::TIME;
use risingwave_proto::data::DataType_TypeName::TIMESTAMP;
use risingwave_proto::data::DataType_TypeName::TIMESTAMPZ;
use risingwave_proto::data::DataType_TypeName::VARCHAR;
use std::convert::TryFrom;
use std::fmt::Debug;

mod bool_type;
mod datetime_type;
mod decimal_type;
pub mod interval_type;
mod string_type;

pub use bool_type::*;
pub use datetime_type::*;
pub use decimal_type::*;
pub use string_type::*;

use crate::array2::{ArrayBuilderImpl, PrimitiveArrayItemType};
use risingwave_proto::expr::ExprNode_ExprNodeType;
use risingwave_proto::expr::ExprNode_ExprNodeType::ADD;
use risingwave_proto::expr::ExprNode_ExprNodeType::DIVIDE;
use risingwave_proto::expr::ExprNode_ExprNodeType::MODULUS;
use risingwave_proto::expr::ExprNode_ExprNodeType::MULTIPLY;
use risingwave_proto::expr::ExprNode_ExprNodeType::SUBTRACT;

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

pub trait DataType: Debug + Sync + Send + 'static {
    fn data_type_kind(&self) -> DataTypeKind;
    fn is_nullable(&self) -> bool;
    fn create_array_builder(self: Arc<Self>, capacity: usize) -> Result<ArrayBuilderImpl>;
    fn to_protobuf(&self) -> Result<DataTypeProto>;
    fn as_any(&self) -> &dyn Any;
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

pub fn build_from_proto(proto: &DataTypeProto) -> Result<DataTypeRef> {
    build_data_type! {
      proto,
      INT16 => Int16Type,
      INT32 => Int32Type,
      INT64 => Int64Type,
      FLOAT => Float32Type,
      DOUBLE => Float64Type,
      BOOLEAN => BoolType,
      CHAR => StringType,
      VARCHAR => StringType,
      DATE => DateType,
      TIME => TimeType,
      TIMESTAMP => TimestampType,
      TIMESTAMPZ => TimestampWithTimeZoneType,
      DECIMAL => DecimalType
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Copy)]
pub enum ArithmeticOperatorKind {
    Plus,
    Subtract,
    Multiply,
    Divide,
    Mod,
}

pub fn is_arithmetic_operator(expr_type: &ExprNode_ExprNodeType) -> bool {
    matches!(expr_type, ADD | SUBTRACT | MULTIPLY | DIVIDE | MODULUS)
}

impl TryFrom<&ExprNode_ExprNodeType> for ArithmeticOperatorKind {
    type Error = RwError;
    fn try_from(value: &ExprNode_ExprNodeType) -> Result<ArithmeticOperatorKind> {
        match value {
            ADD => Ok(ArithmeticOperatorKind::Plus),
            SUBTRACT => Ok(ArithmeticOperatorKind::Subtract),
            MULTIPLY => Ok(ArithmeticOperatorKind::Multiply),
            DIVIDE => Ok(ArithmeticOperatorKind::Divide),
            MODULUS => Ok(ArithmeticOperatorKind::Mod),
            _ => Err(InternalError("Not arithmetic operator.".to_string()).into()),
        }
    }
}

/// `Scalar` is a trait over all possible owned types in the evaluation
/// framework.
///
/// `Scalar` is reciprocal to `ScalarRef`. Use `as_scalar_ref` to get a
/// reference which has the same lifetime as `self`.
pub trait Scalar: Send + Sync + 'static + Clone + std::fmt::Debug {
    /// Type for reference of `Scalar`
    type ScalarRefType<'a>: ScalarRef<'a, ScalarType = Self> + 'a
    where
        Self: 'a;

    /// Get a reference to current scalar.
    fn as_scalar_ref(&self) -> Self::ScalarRefType<'_>;
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
pub trait ScalarRef<'a>: Copy + std::fmt::Debug + 'a {
    /// `ScalarType` is the owned type of current `ScalarRef`.
  #[rustfmt::skip]
  // rustfmt will incorrectly remove GAT lifetime.
  type ScalarType: Scalar<ScalarRefType<'a> = Self>;

    /// Convert `ScalarRef` to an owned scalar.
    fn to_owned_scalar(&self) -> Self::ScalarType;
}

/// `ScalarPartialOrd` allows comparison between `Scalar` and `ScalarRef`.
///
/// TODO: see if it is possible to implement this trait directly on `ScalarRef`.
pub trait ScalarPartialOrd: Scalar {
    fn scalar_cmp(&self, other: Self::ScalarRefType<'_>) -> Option<std::cmp::Ordering>;
}

/// Implement `Scalar` for `PrimitiveArrayItemType`.
/// For PrimitiveArrayItemType, clone is trivial, so `T` is both `Scalar` and `ScalarRef`.
impl<T: PrimitiveArrayItemType> Scalar for T {
    type ScalarRefType<'a> = T;

    fn as_scalar_ref(&self) -> T {
        *self
    }
}

/// Implement `ScalarRef` for `PrimitiveArrayItemType`.
/// For PrimitiveArrayItemType, clone is trivial, so `T` is both `Scalar` and `ScalarRef`.
impl<'a, T: PrimitiveArrayItemType> ScalarRef<'a> for T {
    type ScalarType = T;

    fn to_owned_scalar(&self) -> T {
        *self
    }
}

/// Implement `Scalar` for `String`.
/// `String` could be converted to `&str`.
impl Scalar for String {
    type ScalarRefType<'a> = &'a str;

    fn as_scalar_ref(&self) -> &str {
        self.as_str()
    }
}

/// Implement `ScalarRef` for `String`.
/// `String` could be converted to `&str`.
impl<'a> ScalarRef<'a> for &'a str {
    type ScalarType = String;

    fn to_owned_scalar(&self) -> String {
        self.to_string()
    }
}

impl ScalarPartialOrd for String {
    fn scalar_cmp(&self, other: &str) -> Option<std::cmp::Ordering> {
        self.as_str().partial_cmp(other)
    }
}

impl<T: PrimitiveArrayItemType> ScalarPartialOrd for T {
    fn scalar_cmp(&self, other: Self) -> Option<std::cmp::Ordering> {
        self.partial_cmp(&other)
    }
}

impl ScalarPartialOrd for bool {
    fn scalar_cmp(&self, other: Self) -> Option<std::cmp::Ordering> {
        self.partial_cmp(&other)
    }
}

/// Implement `Scalar` for `bool`.
impl Scalar for bool {
    type ScalarRefType<'a> = bool;

    fn as_scalar_ref(&self) -> bool {
        *self
    }
}

/// Implement `Scalar` and `ScalarRef` for `String`.
/// `String` could be converted to `&str`.
impl<'a> ScalarRef<'a> for bool {
    type ScalarType = bool;

    fn to_owned_scalar(&self) -> bool {
        *self
    }
}

/// Implement `Scalar` for `Decimal`.
impl Scalar for Decimal {
    type ScalarRefType<'a> = Decimal;

    fn as_scalar_ref(&self) -> Decimal {
        *self
    }
}

/// Implement `Scalar` for `Decimal`.
impl<'a> ScalarRef<'a> for Decimal {
    type ScalarType = Decimal;

    fn to_owned_scalar(&self) -> Decimal {
        *self
    }
}

/// Implement `Scalar` for `IntervalUnit`.
impl Scalar for IntervalUnit {
    type ScalarRefType<'a> = IntervalUnit;

    fn as_scalar_ref(&self) -> IntervalUnit {
        *self
    }
}

/// Implement `Scalar` for `IntervalUnit`.
impl<'a> ScalarRef<'a> for IntervalUnit {
    type ScalarType = IntervalUnit;

    fn to_owned_scalar(&self) -> IntervalUnit {
        *self
    }
}

/// `ScalarImpl` embeds all possible scalars in the evaluation framework.
pub enum ScalarImpl {
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Float32(f32),
    Float64(f64),
    UTF8(String),
    Bool(bool),
    Decimal(Decimal),
    Interval(IntervalUnit),
}

/// `ScalarRefImpl` embeds all possible scalar references in the evaluation
/// framework.
pub enum ScalarRefImpl<'a> {
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Float32(f32),
    Float64(f64),
    UTF8(&'a str),
    Bool(bool),
    Interval(IntervalUnit),
}

/// Every interval can be represented by a `IntervalUnit`.
/// Note that the difference between Interval and Instant.
/// For example, `5 yrs 1 month 25 days 23:22:57` is a interval (Can be interpreted by Interval Unit
/// with month = 61, days = 25, seconds = (57 + 23 * 3600 + 22 * 60) * 1000),
/// `1970-01-01 04:05:06` is a Instant or Timestamp
/// One month may contain 28/31 days. One day may contain 23/25 hours.
/// This internals is learned from PG:
/// https://www.postgresql.org/docs/9.1/datatype-datetime.html#:~:text=field%20is%20negative.-,Internally,-interval%20values%20are
#[derive(Debug, Clone, Copy)]
pub struct IntervalUnit {
    months: i32,
    days: i32,
    ms: i64,
}

impl IntervalUnit {
    pub fn new(months: i32, days: i32, ms: i64) -> Self {
        IntervalUnit { months, days, ms }
    }
    pub fn get_days(&self) -> i32 {
        self.days
    }

    pub fn get_months(&self) -> i32 {
        self.months
    }

    pub fn get_years(&self) -> i32 {
        self.months / 12
    }

    pub fn get_ms(&self) -> i64 {
        self.ms
    }

    pub fn from_ymd(year: i32, month: i32, days: i32) -> Self {
        let months = year * 12 + month;
        let days = days;
        let ms = 0;
        IntervalUnit { months, days, ms }
    }
}
