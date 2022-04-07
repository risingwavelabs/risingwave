// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use risingwave_common::array::DataChunk;
use risingwave_common::ensure;
use risingwave_common::error::{ErrorCode, Result, RwError};
use risingwave_common::types::{DataType, ToOwnedDatum};
use risingwave_pb::expr::expr_node::RexNode;
use risingwave_pb::expr::{expr_node, ExprNode};

use crate::expr::expr_binary_bytes::new_substr_start;
use crate::expr::expr_binary_nonnull::{new_binary_expr, new_like_default};
use crate::expr::expr_binary_nullable::new_nullable_binary_expr;
use crate::expr::expr_case::{CaseExpression, WhenClause};
use crate::expr::expr_in::InExpression;
use crate::expr::expr_ternary_bytes::{new_replace_expr, new_substr_start_end, new_translate_expr};
use crate::expr::expr_unary::{
    new_length_default, new_ltrim_expr, new_rtrim_expr, new_trim_expr, new_unary_expr,
};
use crate::expr::{build_from_prost as expr_build_from_prost, BoxedExpression};

fn get_return_type_and_children(prost: &ExprNode) -> Result<(Vec<ExprNode>, DataType)> {
    let ret_type = DataType::from(prost.get_return_type()?);
    if let RexNode::FuncCall(func_call) = prost.get_rex_node()? {
        Ok((func_call.get_children().to_vec(), ret_type))
    } else {
        Err(RwError::from(ErrorCode::InternalError(
            "expects a function call".to_string(),
        )))
    }
}

pub fn build_unary_expr_prost(prost: &ExprNode) -> Result<BoxedExpression> {
    let (children, ret_type) = get_return_type_and_children(prost)?;
    ensure!(children.len() == 1);
    let child_expr = expr_build_from_prost(&children[0])?;
    new_unary_expr(prost.get_expr_type()?, ret_type, child_expr)
}

pub fn build_binary_expr_prost(prost: &ExprNode) -> Result<BoxedExpression> {
    let (children, ret_type) = get_return_type_and_children(prost)?;
    ensure!(children.len() == 2);
    let left_expr = expr_build_from_prost(&children[0])?;
    let right_expr = expr_build_from_prost(&children[1])?;
    Ok(new_binary_expr(
        prost.get_expr_type()?,
        ret_type,
        left_expr,
        right_expr,
    ))
}

pub fn build_nullable_binary_expr_prost(prost: &ExprNode) -> Result<BoxedExpression> {
    let (children, ret_type) = get_return_type_and_children(prost)?;
    ensure!(children.len() == 2);
    let left_expr = expr_build_from_prost(&children[0])?;
    let right_expr = expr_build_from_prost(&children[1])?;
    Ok(new_nullable_binary_expr(
        prost.get_expr_type()?,
        ret_type,
        left_expr,
        right_expr,
    ))
}

pub fn build_substr_expr(prost: &ExprNode) -> Result<BoxedExpression> {
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

pub fn build_trim_expr(prost: &ExprNode) -> Result<BoxedExpression> {
    let (children, ret_type) = get_return_type_and_children(prost)?;
    // TODO: add expr with the delimiter parameter
    ensure!(children.len() == 1);
    let child = expr_build_from_prost(&children[0])?;
    Ok(new_trim_expr(child, ret_type))
}

pub fn build_ltrim_expr(prost: &ExprNode) -> Result<BoxedExpression> {
    let (children, ret_type) = get_return_type_and_children(prost)?;
    // TODO: add expr with the delimiter parameter
    ensure!(children.len() == 1);
    let child = expr_build_from_prost(&children[0])?;
    Ok(new_ltrim_expr(child, ret_type))
}

pub fn build_rtrim_expr(prost: &ExprNode) -> Result<BoxedExpression> {
    let (children, ret_type) = get_return_type_and_children(prost)?;
    // TODO: add expr with the delimiter parameter
    ensure!(children.len() == 1);
    let child = expr_build_from_prost(&children[0])?;
    Ok(new_rtrim_expr(child, ret_type))
}

pub fn build_replace_expr(prost: &ExprNode) -> Result<BoxedExpression> {
    let (children, ret_type) = get_return_type_and_children(prost)?;
    ensure!(children.len() == 3);
    let s = expr_build_from_prost(&children[0])?;
    let from_str = expr_build_from_prost(&children[1])?;
    let to_str = expr_build_from_prost(&children[2])?;
    Ok(new_replace_expr(s, from_str, to_str, ret_type))
}

pub fn build_length_expr(prost: &ExprNode) -> Result<BoxedExpression> {
    let (children, ret_type) = get_return_type_and_children(prost)?;
    // TODO: add encoding length expr
    ensure!(children.len() == 1);
    let child = expr_build_from_prost(&children[0])?;
    Ok(new_length_default(child, ret_type))
}

pub fn build_like_expr(prost: &ExprNode) -> Result<BoxedExpression> {
    let (children, ret_type) = get_return_type_and_children(prost)?;
    ensure!(children.len() == 2);
    let expr_ia1 = expr_build_from_prost(&children[0])?;
    let expr_ia2 = expr_build_from_prost(&children[1])?;
    Ok(new_like_default(expr_ia1, expr_ia2, ret_type))
}

pub fn build_in_expr(prost: &ExprNode) -> Result<BoxedExpression> {
    let (children, ret_type) = get_return_type_and_children(prost)?;
    ensure!(ret_type == DataType::Boolean);
    ensure!(children[0].get_expr_type()? == expr_node::Type::InputRef);
    let input_ref_expr = expr_build_from_prost(&children[0])?;
    for child in &children[1..] {
        ensure!(child.get_expr_type()? == expr_node::Type::ConstantValue);
    }
    let mut data = Vec::new();
    // Used for literal expression below to generate datum
    let data_chunk = DataChunk::new_dummy(1);
    for child in &children[1..] {
        let literal_expr = expr_build_from_prost(child)?;
        let array = literal_expr.eval(&data_chunk)?;
        let datum = array.value_at(0).to_owned_datum();
        data.push(datum);
    }
    Ok(Box::new(InExpression::new(
        input_ref_expr,
        data.into_iter(),
        ret_type,
    )))
}

pub fn build_case_expr(prost: &ExprNode) -> Result<BoxedExpression> {
    let (children, ret_type) = get_return_type_and_children(prost)?;
    // children: (when, then)+, (else_clause)?
    let len = children.len();
    let else_clause = if len % 2 == 1 {
        let else_clause = expr_build_from_prost(&children[len - 1])?;
        if else_clause.return_type() != ret_type {
            return Err(RwError::from(ErrorCode::ProtocolError(
                "the return type of else and case not match".to_string(),
            )));
        }
        Some(else_clause)
    } else {
        None
    };
    let mut when_clauses = vec![];
    for i in 0..len / 2 {
        let when_index = i * 2;
        let then_index = i * 2 + 1;
        let when_expr = expr_build_from_prost(&children[when_index])?;
        let then_expr = expr_build_from_prost(&children[then_index])?;
        if when_expr.return_type() != DataType::Boolean {
            return Err(RwError::from(ErrorCode::ProtocolError(
                "the return type of when clause and condition not match".to_string(),
            )));
        }
        if then_expr.return_type() != ret_type {
            return Err(RwError::from(ErrorCode::ProtocolError(
                "the return type of then clause and case not match".to_string(),
            )));
        }
        let when_clause = WhenClause::new(when_expr, then_expr);
        when_clauses.push(when_clause);
    }
    Ok(Box::new(CaseExpression::new(
        ret_type,
        when_clauses,
        else_clause,
    )))
}

pub fn build_translate_expr(prost: &ExprNode) -> Result<BoxedExpression> {
    let (children, ret_type) = get_return_type_and_children(prost)?;
    ensure!(children.len() == 3);
    let s = expr_build_from_prost(&children[0])?;
    let match_str = expr_build_from_prost(&children[1])?;
    let replace_str = expr_build_from_prost(&children[2])?;
    Ok(new_translate_expr(s, match_str, replace_str, ret_type))
}

#[cfg(test)]
mod tests {
    use std::vec;

    use risingwave_pb::data::data_type::TypeName;
    use risingwave_pb::data::DataType as ProstDataType;
    use risingwave_pb::expr::expr_node::{RexNode, Type};
    use risingwave_pb::expr::{ConstantValue, ExprNode, FunctionCall, InputRefExpr};

    use super::*;

    #[test]
    fn test_build_in_expr() {
        let input_ref = InputRefExpr { column_idx: 0 };
        let input_ref_expr_node = ExprNode {
            expr_type: Type::InputRef as i32,
            return_type: Some(ProstDataType {
                type_name: TypeName::Char as i32,
                ..Default::default()
            }),
            rex_node: Some(RexNode::InputRef(input_ref)),
        };
        let constant_values = vec![
            ExprNode {
                expr_type: Type::ConstantValue as i32,
                return_type: Some(ProstDataType {
                    type_name: TypeName::Char as i32,
                    ..Default::default()
                }),
                rex_node: Some(RexNode::Constant(ConstantValue {
                    body: "ABC".as_bytes().to_vec(),
                })),
            },
            ExprNode {
                expr_type: Type::ConstantValue as i32,
                return_type: Some(ProstDataType {
                    type_name: TypeName::Char as i32,
                    ..Default::default()
                }),
                rex_node: Some(RexNode::Constant(ConstantValue {
                    body: "def".as_bytes().to_vec(),
                })),
            },
        ];
        let mut in_children = vec![input_ref_expr_node];
        in_children.extend(constant_values.into_iter());
        let call = FunctionCall {
            children: in_children,
        };
        let p = ExprNode {
            expr_type: Type::In as i32,
            return_type: Some(ProstDataType {
                type_name: TypeName::Boolean as i32,
                ..Default::default()
            }),
            rex_node: Some(RexNode::FuncCall(call)),
        };
        assert!(build_in_expr(&p).is_ok());
    }

    #[test]
    fn test_build_case_expr() {
        let call = FunctionCall {
            children: vec![
                ExprNode {
                    expr_type: Type::ConstantValue as i32,
                    return_type: Some(ProstDataType {
                        type_name: TypeName::Boolean as i32,
                        ..Default::default()
                    }),
                    rex_node: None,
                },
                ExprNode {
                    expr_type: Type::ConstantValue as i32,
                    return_type: Some(ProstDataType {
                        type_name: TypeName::Int32 as i32,
                        ..Default::default()
                    }),
                    rex_node: None,
                },
            ],
        };
        let p = ExprNode {
            expr_type: Type::Case as i32,
            return_type: Some(ProstDataType {
                type_name: TypeName::Int32 as i32,
                ..Default::default()
            }),
            rex_node: Some(RexNode::FuncCall(call)),
        };
        assert!(build_case_expr(&p).is_ok());
    }

    #[test]
    fn test_build_extract_expr() {
        let left = ExprNode {
            expr_type: Type::ConstantValue as i32,
            return_type: Some(ProstDataType {
                type_name: TypeName::Symbol as i32,
                precision: 11,
                ..Default::default()
            }),
            rex_node: Some(RexNode::Constant(ConstantValue {
                body: "DAY".as_bytes().to_vec(),
            })),
        };
        let right_date = ExprNode {
            expr_type: Type::ConstantValue as i32,
            return_type: Some(ProstDataType {
                type_name: TypeName::Date as i32,
                ..Default::default()
            }),
            rex_node: None,
        };
        let right_time = ExprNode {
            expr_type: Type::ConstantValue as i32,
            return_type: Some(ProstDataType {
                type_name: TypeName::Timestamp as i32,
                ..Default::default()
            }),
            rex_node: None,
        };

        let expr = ExprNode {
            expr_type: Type::Extract as i32,
            return_type: Some(ProstDataType {
                type_name: TypeName::Int64 as i32,
                ..Default::default()
            }),
            rex_node: Some(RexNode::FuncCall(FunctionCall {
                children: vec![left.clone(), right_date],
            })),
        };
        assert!(build_binary_expr_prost(&expr).is_ok());
        let expr = ExprNode {
            expr_type: Type::Extract as i32,
            return_type: Some(ProstDataType {
                type_name: TypeName::Int64 as i32,
                ..Default::default()
            }),
            rex_node: Some(RexNode::FuncCall(FunctionCall {
                children: vec![left, right_time],
            })),
        };
        assert!(build_binary_expr_prost(&expr).is_ok());
    }
}
