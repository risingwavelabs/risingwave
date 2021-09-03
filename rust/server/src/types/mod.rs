use crate::error::{Result, RwError};
use risingwave_proto::data::DataType as DataTypeProto;
use std::any::Any;
use std::sync::Arc;

mod numeric_type;
pub use numeric_type::*;
mod primitive;
pub use primitive::*;
mod native;
use crate::array::BoxedArrayBuilder;
use crate::error::ErrorCode::InternalError;
pub use native::*;
use risingwave_proto::data::DataType_TypeName::BOOLEAN;
use risingwave_proto::data::DataType_TypeName::CHAR;
use risingwave_proto::data::DataType_TypeName::DATE;
use risingwave_proto::data::DataType_TypeName::DECIMAL;
use risingwave_proto::data::DataType_TypeName::DOUBLE;
use risingwave_proto::data::DataType_TypeName::FLOAT;
use risingwave_proto::data::DataType_TypeName::INT16;
use risingwave_proto::data::DataType_TypeName::INT32;
use risingwave_proto::data::DataType_TypeName::INT64;
use risingwave_proto::data::DataType_TypeName::VARCHAR;
use std::convert::TryFrom;
use std::fmt::Debug;

mod bool_type;
mod decimal_type;
pub mod interval_type;
mod string_type;
mod timestamp_type;

pub use bool_type::*;
pub use decimal_type::*;
pub use string_type::*;

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
    Interval,
    Timestamp,
}

pub trait DataType: Debug + Sync + Send + 'static {
    fn data_type_kind(&self) -> DataTypeKind;
    fn is_nullable(&self) -> bool;
    fn create_array_builder(self: Arc<Self>, capacity: usize) -> Result<BoxedArrayBuilder>;
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
      DECIMAL => DecimalType,
      DATE => DateType,
      CHAR => StringType,
      VARCHAR => StringType,
      BOOLEAN => BoolType
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
