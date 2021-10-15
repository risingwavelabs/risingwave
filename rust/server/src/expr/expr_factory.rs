use crate::error::ErrorCode::{InternalError, ProtobufError};
use crate::error::Result;
use crate::expr::binary_expr::new_binary_expr;
use crate::expr::binary_expr::new_like_default;
use crate::expr::binary_expr_bytes::new_substr_start;
use crate::expr::build_from_proto as expr_build_from_proto;
use crate::expr::tenary_expr_bytes::new_substr_start_end;
use crate::expr::unary_expr::new_length_default;
use crate::expr::unary_expr::new_trim_expr;
use crate::expr::unary_expr::new_unary_expr;
use crate::expr::BoxedExpression;
use crate::types::build_from_proto as type_build_from_proto;
use protobuf::Message;
use risingwave_proto::expr::{ExprNode, FunctionCall};

pub fn build_unary_expr(proto: &ExprNode) -> Result<BoxedExpression> {
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

pub fn build_binary_expr(proto: &ExprNode) -> Result<BoxedExpression> {
    // FIXME: Note that now it only contains some other expressions so it may not be general enough.
    let data_type = type_build_from_proto(proto.get_return_type())?;
    let function_call_node =
        FunctionCall::parse_from_bytes(proto.get_body().get_value()).map_err(ProtobufError)?;
    let children = function_call_node.get_children();
    ensure!(children.len() == 2);
    let left_expr = expr_build_from_proto(&children[0])?;
    let right_expr = expr_build_from_proto(&children[1])?;
    Ok(new_binary_expr(
        proto.get_expr_type(),
        data_type,
        left_expr,
        right_expr,
    ))
}

pub fn build_substr_expr(proto: &ExprNode) -> Result<BoxedExpression> {
    // FIXME: Note that now it only contains some other expressions so it may not be general enough.
    let data_type = type_build_from_proto(proto.get_return_type())?;
    let function_call_node =
        FunctionCall::parse_from_bytes(proto.get_body().get_value()).map_err(ProtobufError)?;
    let children = function_call_node.get_children();
    let child = expr_build_from_proto(&children[0])?;
    ensure!(children.len() == 2 || children.len() == 3);
    if children.len() == 2 {
        let off = expr_build_from_proto(&children[1])?;
        Ok(new_substr_start(child, off, data_type))
    } else if children.len() == 3 {
        let off = expr_build_from_proto(&children[1])?;
        let len = expr_build_from_proto(&children[2])?;
        Ok(new_substr_start_end(child, off, len, data_type))
    } else {
        unreachable!()
    }
}

pub fn build_trim_expr(proto: &ExprNode) -> Result<BoxedExpression> {
    // FIXME: Note that now it only contains some other expressions so it may not be general enough.
    let data_type = type_build_from_proto(proto.get_return_type())?;
    let function_call_node =
        FunctionCall::parse_from_bytes(proto.get_body().get_value()).map_err(ProtobufError)?;
    let children = function_call_node.get_children();
    let child = expr_build_from_proto(&children[0])?;
    // TODO: add delimiter trim expr
    ensure!(children.len() == 1);
    Ok(new_trim_expr(child, data_type))
}

pub fn build_length_expr(proto: &ExprNode) -> Result<BoxedExpression> {
    // FIXME: Note that now it only contains some other expressions so it may not be general enough.
    let data_type = type_build_from_proto(proto.get_return_type())?;
    let function_call_node =
        FunctionCall::parse_from_bytes(proto.get_body().get_value()).map_err(ProtobufError)?;
    let children = function_call_node.get_children();
    let child = expr_build_from_proto(&children[0])?;
    // TODO: add encoding length expr
    ensure!(children.len() == 1);
    Ok(new_length_default(child, data_type))
}

pub fn build_like_expr(proto: &ExprNode) -> Result<BoxedExpression> {
    let data_type = type_build_from_proto(proto.get_return_type())?;
    let function_call_node =
        FunctionCall::parse_from_bytes(proto.get_body().get_value()).map_err(ProtobufError)?;
    let children = function_call_node.get_children();
    ensure!(children.len() == 2);
    let expr_ia1 = expr_build_from_proto(&children[0])?;
    let expr_ia2 = expr_build_from_proto(&children[1])?;
    Ok(new_like_default(expr_ia1, expr_ia2, data_type))
}
