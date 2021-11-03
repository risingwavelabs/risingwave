use crate::array::RwError;
use crate::error::ErrorCode::ProtobufError;
use crate::error::{ErrorCode, Result};
use crate::expr::binary_expr::new_binary_expr;
use crate::expr::binary_expr::new_like_default;
use crate::expr::binary_expr::new_position_expr;
use crate::expr::binary_expr_bytes::new_substr_start;
use crate::expr::build_from_prost as expr_build_from_prost;
use crate::expr::build_from_proto as expr_build_from_proto;
use crate::expr::ternary_expr_bytes::new_replace_expr;
use crate::expr::ternary_expr_bytes::new_substr_start_end;
use crate::expr::unary_expr::new_length_default;
use crate::expr::unary_expr::new_ltrim_expr;
use crate::expr::unary_expr::new_rtrim_expr;
use crate::expr::unary_expr::new_trim_expr;
use crate::expr::unary_expr::new_unary_expr;
use crate::expr::BoxedExpression;
use crate::types::build_from_prost as type_build_from_prost;
use crate::types::build_from_proto as type_build_from_proto;
use protobuf::Message;
use risingwave_pb::expr::expr_node::RexNode;
use risingwave_pb::expr::ExprNode as ProstExprNode;
use risingwave_proto::expr::{ExprNode, FunctionCall};

pub fn build_unary_expr_prost(prost: &ProstExprNode) -> Result<BoxedExpression> {
    let data_type = type_build_from_prost(prost.get_return_type())?;
    match prost.get_rex_node() {
        RexNode::FuncCall(func_call) => {
            let children = func_call.get_children();
            ensure!(children.len() == 1);
            let child_expr = expr_build_from_prost(&children[0])?;
            Ok(new_unary_expr(prost.get_expr_type(), data_type, child_expr))
        }
        _ => Err(RwError::from(ErrorCode::NotImplementedError(
            "unary expr expects a function call".to_string(),
        ))),
    }
}

pub fn build_binary_expr_prost(prost: &ProstExprNode) -> Result<BoxedExpression> {
    let data_type = type_build_from_prost(prost.get_return_type())?;
    match prost.get_rex_node() {
        RexNode::FuncCall(func_call) => {
            let children = func_call.get_children();
            ensure!(children.len() == 2);
            let left_expr = expr_build_from_prost(&children[0])?;
            let right_expr = expr_build_from_prost(&children[1])?;
            Ok(new_binary_expr(
                prost.get_expr_type(),
                data_type,
                left_expr,
                right_expr,
            ))
        }
        _ => Err(RwError::from(ErrorCode::NotImplementedError(
            "binary expr expects a function call".to_string(),
        ))),
    }
}

pub fn build_substr_expr(proto: &ExprNode) -> Result<BoxedExpression> {
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

pub fn build_position_expr(proto: &ExprNode) -> Result<BoxedExpression> {
    let data_type = type_build_from_proto(proto.get_return_type())?;
    let function_call_node =
        FunctionCall::parse_from_bytes(proto.get_body().get_value()).map_err(ProtobufError)?;
    let children = function_call_node.get_children();
    ensure!(children.len() == 2);
    let str = expr_build_from_proto(&children[0])?;
    let sub_str = expr_build_from_proto(&children[1])?;
    Ok(new_position_expr(str, sub_str, data_type))
}

pub fn build_trim_expr(proto: &ExprNode) -> Result<BoxedExpression> {
    let data_type = type_build_from_proto(proto.get_return_type())?;
    let function_call_node =
        FunctionCall::parse_from_bytes(proto.get_body().get_value()).map_err(ProtobufError)?;
    let children = function_call_node.get_children();
    // TODO: add expr with the delimiter parameter
    ensure!(children.len() == 1);
    let child = expr_build_from_proto(&children[0])?;
    Ok(new_trim_expr(child, data_type))
}

pub fn build_ltrim_expr(proto: &ExprNode) -> Result<BoxedExpression> {
    let data_type = type_build_from_proto(proto.get_return_type())?;
    let function_call_node =
        FunctionCall::parse_from_bytes(proto.get_body().get_value()).map_err(ProtobufError)?;
    let children = function_call_node.get_children();
    // TODO: add expr with the delimiter parameter
    ensure!(children.len() == 1);
    let child = expr_build_from_proto(&children[0])?;
    Ok(new_ltrim_expr(child, data_type))
}

pub fn build_rtrim_expr(proto: &ExprNode) -> Result<BoxedExpression> {
    let data_type = type_build_from_proto(proto.get_return_type())?;
    let function_call_node =
        FunctionCall::parse_from_bytes(proto.get_body().get_value()).map_err(ProtobufError)?;
    let children = function_call_node.get_children();
    // TODO: add expr with the delimiter parameter
    ensure!(children.len() == 1);
    let child = expr_build_from_proto(&children[0])?;
    Ok(new_rtrim_expr(child, data_type))
}

pub fn build_replace_expr(proto: &ExprNode) -> Result<BoxedExpression> {
    let data_type = type_build_from_proto(proto.get_return_type())?;
    let function_call_node =
        FunctionCall::parse_from_bytes(proto.get_body().get_value()).map_err(ProtobufError)?;
    let children = function_call_node.get_children();
    ensure!(children.len() == 3);
    let s = expr_build_from_proto(&children[0])?;
    let from_str = expr_build_from_proto(&children[1])?;
    let to_str = expr_build_from_proto(&children[2])?;
    Ok(new_replace_expr(s, from_str, to_str, data_type))
}

pub fn build_length_expr(proto: &ExprNode) -> Result<BoxedExpression> {
    let data_type = type_build_from_proto(proto.get_return_type())?;
    let function_call_node =
        FunctionCall::parse_from_bytes(proto.get_body().get_value()).map_err(ProtobufError)?;
    let children = function_call_node.get_children();
    // TODO: add encoding length expr
    ensure!(children.len() == 1);
    let child = expr_build_from_proto(&children[0])?;
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
