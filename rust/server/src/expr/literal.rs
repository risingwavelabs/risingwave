use crate::array::{ArrayRef, DataChunk};
use crate::error::ErrorCode::{InternalError, ProtobufError};
use crate::error::{Result, RwError};

use crate::expr::Expression;
use crate::types::{build_from_proto, DataType, DataTypeRef};
use std::convert::TryFrom;
use std::convert::TryInto;

use protobuf::Message;

use risingwave_proto::data::DataType_TypeName;
use risingwave_proto::expr::{ConstantValue, ExprNode, ExprNode_ExprNodeType};

#[derive(Clone, Debug)]
pub(crate) enum Datum {
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Float32(f32),
    Float64(f64),
}

pub(super) struct LiteralExpression {
    return_type: DataTypeRef,
    literal: Datum,
}

impl Expression for LiteralExpression {
    fn return_type(&self) -> &dyn DataType {
        &*self.return_type
    }

    fn return_type_ref(&self) -> DataTypeRef {
        self.return_type.clone()
    }

    fn eval(&mut self, _input: &DataChunk) -> Result<ArrayRef> {
        let mut array_builder =
            DataType::create_array_builder(self.return_type.clone(), _input.cardinality())?;
        for _ in 0.._input.cardinality() {
            array_builder.append(&self.literal)?;
        }
        array_builder.finish()
    }
}

impl<'a> TryFrom<&'a ExprNode> for LiteralExpression {
    type Error = RwError;

    fn try_from(proto: &'a ExprNode) -> Result<Self> {
        ensure!(proto.expr_type == ExprNode_ExprNodeType::CONSTANT_VALUE);
        let data_type = build_from_proto(proto.get_return_type())?;

        let proto_value =
            ConstantValue::parse_from_bytes(proto.get_body().get_value()).map_err(ProtobufError)?;

        // TODO: We need to unify these
        let value = match proto.get_return_type().get_type_name() {
            DataType_TypeName::INT16 => Datum::Int16(i16::from_be_bytes(
                proto_value.get_body().try_into().map_err(|e| {
                    InternalError(format!("Failed to deserialize i16, reaseon: {:?}", e))
                })?,
            )),
            DataType_TypeName::INT32 => Datum::Int32(i32::from_be_bytes(
                proto_value.get_body().try_into().map_err(|e| {
                    InternalError(format!("Failed to deserialize i32, reaseon: {:?}", e))
                })?,
            )),
            DataType_TypeName::INT64 => Datum::Int64(i64::from_be_bytes(
                proto_value.get_body().try_into().map_err(|e| {
                    InternalError(format!("Failed to deserialize i64, reaseon: {:?}", e))
                })?,
            )),
            _ => {
                return Err(InternalError(format!(
                    "Unrecognized type name: {:?}",
                    proto.get_return_type().get_type_name()
                ))
                .into())
            }
        };

        Ok(Self {
            return_type: data_type,
            literal: value,
        })
    }
}
