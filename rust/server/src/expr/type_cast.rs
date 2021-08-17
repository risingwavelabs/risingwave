use std::convert::TryFrom;

use protobuf::Message;

use risingwave_proto::expr::{ExprNode, ExprNode_ExprNodeType, FunctionCall};

use crate::array::{ArrayRef, DataChunk};
use crate::error::ErrorCode::{InternalError, ProtobufError};
use crate::error::{Result, RwError};
use crate::expr::build_from_proto as expr_build_from_proto;
use crate::expr::BoxedExpression;
use crate::expr::Expression;
use crate::types::{build_from_proto as type_build_from_proto, DataType, DataTypeRef};

pub(super) struct TypeCastExpression {
    return_type: DataTypeRef,
    child: BoxedExpression,
}

impl Expression for TypeCastExpression {
    fn return_type(&self) -> &dyn DataType {
        &*self.return_type
    }

    fn return_type_ref(&self) -> DataTypeRef {
        self.return_type.clone()
    }

    fn eval(&mut self, _input: &DataChunk) -> Result<ArrayRef> {
        // let child_data_type = self.child.return_type_ref();
        // let child_expr = self.child.eval(input)?;
        // // Build a array from the child expression.
        // let mut array_builder = DataType::create_array_builder(child_data_type, 1)?;
        // array_builder.append_expr_output(child_expr)?;
        // let _array = array_builder.finish()?;
        // match (
        //   self.return_type.data_type_kind(),
        //   self.child.return_type_ref().data_type_kind(),
        // ) {
        //   (DataTypeKind::Date, _) => {
        //     unimplemented!()
        //   }
        //
        //   _ => unimplemented!(),
        // };
        // Finish the casting and correctly return the result.
        todo!()
    }
}

impl<'a> TryFrom<&'a ExprNode> for TypeCastExpression {
    type Error = RwError;

    fn try_from(proto: &'a ExprNode) -> Result<Self> {
        ensure!(proto.get_expr_type() == ExprNode_ExprNodeType::CAST);
        let data_type = type_build_from_proto(proto.get_return_type())?;
        let function_call_node =
            FunctionCall::parse_from_bytes(proto.get_body().get_value()).map_err(ProtobufError)?;

        ensure!(
            function_call_node.get_children().len() == 1,
            "Type cast expression can only have exactly one child"
        );

        match function_call_node.get_children().get(0) {
            Some(child_expr_node) => {
                let child_expr = expr_build_from_proto(child_expr_node)?;
                Ok(Self {
                    return_type: data_type,
                    child: child_expr,
                })
            }

            None => Err(InternalError(
                "Type cast expression can only have exactly one child".to_string(),
            )
            .into()),
        }
    }
}
