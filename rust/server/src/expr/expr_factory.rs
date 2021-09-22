use crate::error::ErrorCode::{InternalError, ProtobufError};
use crate::error::Result;
use crate::expr::build_from_proto as expr_build_from_proto;
use crate::expr::unary_expr::new_unary_expr;
use crate::expr::BoxedExpression;
use crate::types::build_from_proto as type_build_from_proto;
use protobuf::Message;
use risingwave_proto::expr::{ExprNode, FunctionCall};

pub fn new_expr(proto: &ExprNode) -> Result<BoxedExpression> {
    // FIXME: Note that now it only contains some other expressions so it may not be general enough.
    let data_type = type_build_from_proto(proto.get_return_type())?;
    let function_call_node =
        FunctionCall::parse_from_bytes(proto.get_body().get_value()).map_err(ProtobufError)?;
    match function_call_node.get_children().get(0) {
        Some(child_expr_node) => {
            let child_expr = expr_build_from_proto(child_expr_node)?;
            Ok(new_unary_expr(proto.get_expr_type(), data_type, child_expr))
        }

        None => Err(InternalError(
            "Type cast expression can only have exactly one child".to_string(),
        )
        .into()),
    }
}
