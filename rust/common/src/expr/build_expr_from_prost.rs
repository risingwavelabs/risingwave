use crate::array::RwError;
use crate::error::{ErrorCode, Result};
use crate::expr::build_from_prost as expr_build_from_prost;
use crate::expr::expr_binary_bytes::new_substr_start;
use crate::expr::expr_binary_nonnull::new_binary_expr;
use crate::expr::expr_binary_nonnull::new_like_default;
use crate::expr::expr_binary_nonnull::new_position_expr;
use crate::expr::expr_binary_nullable::new_nullable_binary_expr;
use crate::expr::expr_ternary_bytes::new_replace_expr;
use crate::expr::expr_ternary_bytes::new_substr_start_end;
use crate::expr::expr_unary_nonnull::new_length_default;
use crate::expr::expr_unary_nonnull::new_ltrim_expr;
use crate::expr::expr_unary_nonnull::new_rtrim_expr;
use crate::expr::expr_unary_nonnull::new_trim_expr;
use crate::expr::expr_unary_nonnull::new_unary_expr;
use crate::expr::BoxedExpression;
use crate::types::{build_from_prost as type_build_from_prost, DataTypeRef};
use risingwave_pb::expr::expr_node::RexNode;
use risingwave_pb::expr::ExprNode as ProstExprNode;

fn get_return_type_and_children(
    prost: &ProstExprNode,
) -> Result<(Vec<ProstExprNode>, DataTypeRef)> {
    let ret_type = type_build_from_prost(prost.get_return_type())?;
    if let RexNode::FuncCall(func_call) = prost.get_rex_node() {
        Ok((func_call.get_children().to_vec(), ret_type))
    } else {
        Err(RwError::from(ErrorCode::NotImplementedError(
            "expects a function call".to_string(),
        )))
    }
}

pub fn build_unary_expr_prost(prost: &ProstExprNode) -> Result<BoxedExpression> {
    let (children, ret_type) = get_return_type_and_children(prost)?;
    ensure!(children.len() == 1);
    let child_expr = expr_build_from_prost(&children[0])?;
    Ok(new_unary_expr(prost.get_expr_type(), ret_type, child_expr))
}

pub fn build_binary_expr_prost(prost: &ProstExprNode) -> Result<BoxedExpression> {
    let (children, ret_type) = get_return_type_and_children(prost)?;
    ensure!(children.len() == 2);
    let left_expr = expr_build_from_prost(&children[0])?;
    let right_expr = expr_build_from_prost(&children[1])?;
    Ok(new_binary_expr(
        prost.get_expr_type(),
        ret_type,
        left_expr,
        right_expr,
    ))
}

pub fn build_nullable_binary_expr_prost(prost: &ProstExprNode) -> Result<BoxedExpression> {
    let (children, ret_type) = get_return_type_and_children(prost)?;
    ensure!(children.len() == 2);
    let left_expr = expr_build_from_prost(&children[0])?;
    let right_expr = expr_build_from_prost(&children[1])?;
    Ok(new_nullable_binary_expr(
        prost.get_expr_type(),
        ret_type,
        left_expr,
        right_expr,
    ))
}

pub fn build_substr_expr(prost: &ProstExprNode) -> Result<BoxedExpression> {
    let (children, ret_type) = get_return_type_and_children(prost)?;
    let child = expr_build_from_prost(&children[0])?;
    ensure!(children.len() == 2 || children.len() == 3);
    if children.len() == 2 {
        let off = expr_build_from_prost(&children[1])?;
        Ok(new_substr_start(child, off, ret_type))
    } else if children.len() == 3 {
        let off = expr_build_from_prost(&children[1])?;
        let len = expr_build_from_prost(&children[2])?;
        Ok(new_substr_start_end(child, off, len, ret_type))
    } else {
        unreachable!()
    }
}

pub fn build_position_expr(prost: &ProstExprNode) -> Result<BoxedExpression> {
    let (children, ret_type) = get_return_type_and_children(prost)?;
    ensure!(children.len() == 2);
    let str = expr_build_from_prost(&children[0])?;
    let sub_str = expr_build_from_prost(&children[1])?;
    Ok(new_position_expr(str, sub_str, ret_type))
}

pub fn build_trim_expr(prost: &ProstExprNode) -> Result<BoxedExpression> {
    let (children, ret_type) = get_return_type_and_children(prost)?;
    // TODO: add expr with the delimiter parameter
    ensure!(children.len() == 1);
    let child = expr_build_from_prost(&children[0])?;
    Ok(new_trim_expr(child, ret_type))
}

pub fn build_ltrim_expr(prost: &ProstExprNode) -> Result<BoxedExpression> {
    let (children, ret_type) = get_return_type_and_children(prost)?;
    // TODO: add expr with the delimiter parameter
    ensure!(children.len() == 1);
    let child = expr_build_from_prost(&children[0])?;
    Ok(new_ltrim_expr(child, ret_type))
}

pub fn build_rtrim_expr(prost: &ProstExprNode) -> Result<BoxedExpression> {
    let (children, ret_type) = get_return_type_and_children(prost)?;
    // TODO: add expr with the delimiter parameter
    ensure!(children.len() == 1);
    let child = expr_build_from_prost(&children[0])?;
    Ok(new_rtrim_expr(child, ret_type))
}

pub fn build_replace_expr(prost: &ProstExprNode) -> Result<BoxedExpression> {
    let (children, ret_type) = get_return_type_and_children(prost)?;
    ensure!(children.len() == 3);
    let s = expr_build_from_prost(&children[0])?;
    let from_str = expr_build_from_prost(&children[1])?;
    let to_str = expr_build_from_prost(&children[2])?;
    Ok(new_replace_expr(s, from_str, to_str, ret_type))
}

pub fn build_length_expr(prost: &ProstExprNode) -> Result<BoxedExpression> {
    let (children, ret_type) = get_return_type_and_children(prost)?;
    // TODO: add encoding length expr
    ensure!(children.len() == 1);
    let child = expr_build_from_prost(&children[0])?;
    Ok(new_length_default(child, ret_type))
}

pub fn build_like_expr(prost: &ProstExprNode) -> Result<BoxedExpression> {
    let (children, ret_type) = get_return_type_and_children(prost)?;
    ensure!(children.len() == 2);
    let expr_ia1 = expr_build_from_prost(&children[0])?;
    let expr_ia2 = expr_build_from_prost(&children[1])?;
    Ok(new_like_default(expr_ia1, expr_ia2, ret_type))
}
