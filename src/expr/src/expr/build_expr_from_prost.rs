// Copyright 2023 Singularity Data
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

use risingwave_common::try_match_expand;
use risingwave_common::types::DataType;
use risingwave_common::util::value_encoding::deserialize_datum;
use risingwave_pb::expr::expr_node::{RexNode, Type};
use risingwave_pb::expr::{ExprNode, FunctionCall, InputRefExpr};

use super::expr_some_all::SomeAllExpression;
use crate::expr::expr_binary_bytes::{
    new_ltrim_characters, new_repeat, new_rtrim_characters, new_substr_start, new_to_char,
    new_trim_characters,
};
use crate::expr::expr_binary_nonnull::{
    new_binary_expr, new_date_trunc_expr, new_like_default, new_to_timestamp,
};
use crate::expr::expr_binary_nullable::new_nullable_binary_expr;
use crate::expr::expr_quaternary_bytes::new_overlay_for_exp;
use crate::expr::expr_ternary_bytes::{
    new_overlay_exp, new_replace_expr, new_split_part_expr, new_substr_start_end,
    new_translate_expr,
};
use crate::expr::expr_to_char_const_tmpl::{ExprToCharConstTmpl, ExprToCharConstTmplContext};
use crate::expr::expr_to_timestamp_const_tmpl::{
    ExprToTimestampConstTmpl, ExprToTimestampConstTmplContext,
};
use crate::expr::expr_unary::{
    new_length_default, new_ltrim_expr, new_rtrim_expr, new_trim_expr, new_unary_expr,
};
use crate::expr::{
    build_from_prost as expr_build_from_prost, BoxedExpression, Expression, LiteralExpression,
};
use crate::vector_op::to_char::compile_pattern_to_chrono;
use crate::{bail, ensure, Result};

fn get_children_and_return_type(prost: &ExprNode) -> Result<(Vec<ExprNode>, DataType)> {
    let ret_type = DataType::from(prost.get_return_type().unwrap());
    if let RexNode::FuncCall(func_call) = prost.get_rex_node().unwrap() {
        Ok((func_call.get_children().to_vec(), ret_type))
    } else {
        bail!("Expected RexNode::FuncCall");
    }
}

pub fn build_unary_expr_prost(prost: &ExprNode) -> Result<BoxedExpression> {
    let (children, ret_type) = get_children_and_return_type(prost)?;
    let [child]: [_; 1] = children.try_into().unwrap();
    let child_expr = expr_build_from_prost(&child)?;
    new_unary_expr(prost.get_expr_type().unwrap(), ret_type, child_expr)
}

pub fn build_binary_expr_prost(prost: &ExprNode) -> Result<BoxedExpression> {
    let (children, ret_type) = get_children_and_return_type(prost)?;
    let [left_child, right_child]: [_; 2] = children.try_into().unwrap();
    let left_expr = expr_build_from_prost(&left_child)?;
    let right_expr = expr_build_from_prost(&right_child)?;
    new_binary_expr(
        prost.get_expr_type().unwrap(),
        ret_type,
        left_expr,
        right_expr,
    )
}

pub fn build_nullable_binary_expr_prost(prost: &ExprNode) -> Result<BoxedExpression> {
    let (children, ret_type) = get_children_and_return_type(prost)?;
    let [left_child, right_child]: [_; 2] = children.try_into().unwrap();
    let left_expr = expr_build_from_prost(&left_child)?;
    let right_expr = expr_build_from_prost(&right_child)?;
    new_nullable_binary_expr(
        prost.get_expr_type().unwrap(),
        ret_type,
        left_expr,
        right_expr,
    )
}

pub fn build_overlay_expr(prost: &ExprNode) -> Result<BoxedExpression> {
    let (children, ret_type) = get_children_and_return_type(prost)?;
    ensure!(children.len() == 3 || children.len() == 4);

    let s = expr_build_from_prost(&children[0])?;
    let new_sub_str = expr_build_from_prost(&children[1])?;
    let start = expr_build_from_prost(&children[2])?;

    if children.len() == 3 {
        Ok(new_overlay_exp(s, new_sub_str, start, ret_type))
    } else if children.len() == 4 {
        let count = expr_build_from_prost(&children[3])?;
        Ok(new_overlay_for_exp(s, new_sub_str, start, count, ret_type))
    } else {
        unreachable!()
    }
}

pub fn build_repeat_expr(prost: &ExprNode) -> Result<BoxedExpression> {
    let (children, ret_type) = get_children_and_return_type(prost)?;
    let [left_child, right_child]: [_; 2] = children.try_into().unwrap();
    let left_expr = expr_build_from_prost(&left_child)?;
    let right_expr = expr_build_from_prost(&right_child)?;
    Ok(new_repeat(left_expr, right_expr, ret_type))
}

pub fn build_substr_expr(prost: &ExprNode) -> Result<BoxedExpression> {
    let (children, ret_type) = get_children_and_return_type(prost)?;
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
    let (children, ret_type) = get_children_and_return_type(prost)?;
    ensure!(!children.is_empty() && children.len() <= 2);
    let original = expr_build_from_prost(&children[0])?;
    match children.len() {
        1 => Ok(new_trim_expr(original, ret_type)),
        2 => {
            let characters = expr_build_from_prost(&children[1])?;
            Ok(new_trim_characters(original, characters, ret_type))
        }
        _ => unreachable!(),
    }
}

pub fn build_ltrim_expr(prost: &ExprNode) -> Result<BoxedExpression> {
    let (children, ret_type) = get_children_and_return_type(prost)?;
    ensure!(!children.is_empty() && children.len() <= 2);
    let original = expr_build_from_prost(&children[0])?;
    match children.len() {
        1 => Ok(new_ltrim_expr(original, ret_type)),
        2 => {
            let characters = expr_build_from_prost(&children[1])?;
            Ok(new_ltrim_characters(original, characters, ret_type))
        }
        _ => unreachable!(),
    }
}

pub fn build_rtrim_expr(prost: &ExprNode) -> Result<BoxedExpression> {
    let (children, ret_type) = get_children_and_return_type(prost)?;
    ensure!(!children.is_empty() && children.len() <= 2);
    let original = expr_build_from_prost(&children[0])?;
    match children.len() {
        1 => Ok(new_rtrim_expr(original, ret_type)),
        2 => {
            let characters = expr_build_from_prost(&children[1])?;
            Ok(new_rtrim_characters(original, characters, ret_type))
        }
        _ => unreachable!(),
    }
}

pub fn build_replace_expr(prost: &ExprNode) -> Result<BoxedExpression> {
    let (children, ret_type) = get_children_and_return_type(prost)?;
    ensure!(children.len() == 3);
    let s = expr_build_from_prost(&children[0])?;
    let from_str = expr_build_from_prost(&children[1])?;
    let to_str = expr_build_from_prost(&children[2])?;
    Ok(new_replace_expr(s, from_str, to_str, ret_type))
}

pub fn build_date_trunc_expr(prost: &ExprNode) -> Result<BoxedExpression> {
    let (children, ret_type) = get_children_and_return_type(prost)?;
    ensure!(children.len() == 2 || children.len() == 3);
    let field = expr_build_from_prost(&children[0])?;
    let source = expr_build_from_prost(&children[1])?;
    let time_zone = if let Some(child) = children.get(2) {
        Some((expr_build_from_prost(child)?, expr_build_from_prost(child)?))
    } else {
        None
    };
    Ok(new_date_trunc_expr(ret_type, field, source, time_zone))
}

pub fn build_length_expr(prost: &ExprNode) -> Result<BoxedExpression> {
    let (children, ret_type) = get_children_and_return_type(prost)?;
    // TODO: add encoding length expr
    let [child]: [_; 1] = children.try_into().unwrap();
    let child = expr_build_from_prost(&child)?;
    Ok(new_length_default(child, ret_type))
}

pub fn build_like_expr(prost: &ExprNode) -> Result<BoxedExpression> {
    let (children, ret_type) = get_children_and_return_type(prost)?;
    ensure!(children.len() == 2);
    let expr_ia1 = expr_build_from_prost(&children[0])?;
    let expr_ia2 = expr_build_from_prost(&children[1])?;
    Ok(new_like_default(expr_ia1, expr_ia2, ret_type))
}

pub fn build_translate_expr(prost: &ExprNode) -> Result<BoxedExpression> {
    let (children, ret_type) = get_children_and_return_type(prost)?;
    ensure!(children.len() == 3);
    let s = expr_build_from_prost(&children[0])?;
    let match_str = expr_build_from_prost(&children[1])?;
    let replace_str = expr_build_from_prost(&children[2])?;
    Ok(new_translate_expr(s, match_str, replace_str, ret_type))
}

pub fn build_split_part_expr(prost: &ExprNode) -> Result<BoxedExpression> {
    let (children, ret_type) = get_children_and_return_type(prost)?;
    ensure!(children.len() == 3);
    let string_expr = expr_build_from_prost(&children[0])?;
    let delimiter_expr = expr_build_from_prost(&children[1])?;
    let nth_expr = expr_build_from_prost(&children[2])?;
    Ok(new_split_part_expr(
        string_expr,
        delimiter_expr,
        nth_expr,
        ret_type,
    ))
}

pub fn build_to_char_expr(prost: &ExprNode) -> Result<BoxedExpression> {
    let (children, ret_type) = get_children_and_return_type(prost)?;
    ensure!(children.len() == 2);
    let data_expr = expr_build_from_prost(&children[0])?;
    let tmpl_node = &children[1];
    if let RexNode::Constant(tmpl_value) = tmpl_node.get_rex_node().unwrap()
        && let Ok(Some(tmpl)) = deserialize_datum(tmpl_value.get_body().as_slice(), &DataType::from(tmpl_node.get_return_type().unwrap()))
    {
        let tmpl = tmpl.as_utf8();
        let pattern = compile_pattern_to_chrono(tmpl);

        Ok(ExprToCharConstTmpl {
            ctx: ExprToCharConstTmplContext {
                chrono_pattern: pattern,
            },
            child: data_expr,
        }.boxed())
    } else {
        let tmpl_expr = expr_build_from_prost(&children[1])?;
        Ok(new_to_char(data_expr, tmpl_expr, ret_type))
    }
}

pub fn build_now_expr(prost: &ExprNode) -> Result<BoxedExpression> {
    let rex_node = try_match_expand!(prost.get_rex_node(), Ok)?;
    let RexNode::FuncCall(func_call_node) = rex_node else {
        bail!("Expected RexNode::FuncCall in Now");
    };
    let Some(bind_timestamp) = func_call_node.children.first() else {
        bail!("Expected epoch timestamp bound into Now");
    };
    LiteralExpression::try_from(bind_timestamp).map(Expression::boxed)
}

pub fn build_to_timestamp_expr(prost: &ExprNode) -> Result<BoxedExpression> {
    let (children, ret_type) = get_children_and_return_type(prost)?;
    ensure!(children.len() == 2);
    let data_expr = expr_build_from_prost(&children[0])?;
    let tmpl_node = &children[1];
    if let RexNode::Constant(tmpl_value) = tmpl_node.get_rex_node().unwrap()
        && let Ok(Some(tmpl)) = deserialize_datum(tmpl_value.get_body().as_slice(), &DataType::from(tmpl_node.get_return_type().unwrap()))
    {
        let tmpl = tmpl.as_utf8();
        let pattern = compile_pattern_to_chrono(tmpl);

        Ok(ExprToTimestampConstTmpl {
            ctx: ExprToTimestampConstTmplContext {
                chrono_pattern: pattern,
            },
            child: data_expr,
        }.boxed())
    } else {
        let tmpl_expr = expr_build_from_prost(&children[1])?;
        Ok(new_to_timestamp(data_expr, tmpl_expr, ret_type))
    }
}

pub fn build_some_all_expr_prost(prost: &ExprNode) -> Result<BoxedExpression> {
    let outer_expr_type = prost.get_expr_type().unwrap();
    let (outer_children, outer_return_type) = get_children_and_return_type(prost)?;
    ensure!(matches!(outer_return_type, DataType::Boolean));

    let mut inner_expr_type = outer_children[0].get_expr_type().unwrap();
    let (mut inner_children, mut inner_return_type) =
        get_children_and_return_type(&outer_children[0])?;
    let mut stack = vec![];
    while inner_children.len() != 2 {
        stack.push((inner_expr_type, inner_return_type));
        inner_expr_type = inner_children[0].get_expr_type().unwrap();
        (inner_children, inner_return_type) = get_children_and_return_type(&inner_children[0])?;
    }

    let [left_child, right_child]: [_; 2] = inner_children.try_into().unwrap();
    let left_expr = expr_build_from_prost(&left_child)?;
    let right_expr = expr_build_from_prost(&right_child)?;

    let DataType::List { datatype: right_expr_return_type } = right_expr.return_type() else {
        bail!("Expect Array Type");
    };

    let eval_func = {
        let left_expr_input_ref = ExprNode {
            expr_type: Type::InputRef as i32,
            return_type: Some(left_expr.return_type().to_protobuf()),
            rex_node: Some(RexNode::InputRef(InputRefExpr { column_idx: 0 })),
        };
        let right_expr_input_ref = ExprNode {
            expr_type: Type::InputRef as i32,
            return_type: Some(right_expr_return_type.to_protobuf()),
            rex_node: Some(RexNode::InputRef(InputRefExpr { column_idx: 1 })),
        };
        let mut root_expr_node = ExprNode {
            expr_type: inner_expr_type as i32,
            return_type: Some(inner_return_type.to_protobuf()),
            rex_node: Some(RexNode::FuncCall(FunctionCall {
                children: vec![left_expr_input_ref, right_expr_input_ref],
            })),
        };
        while let Some((expr_type, return_type)) = stack.pop() {
            root_expr_node = ExprNode {
                expr_type: expr_type as i32,
                return_type: Some(return_type.to_protobuf()),
                rex_node: Some(RexNode::FuncCall(FunctionCall {
                    children: vec![root_expr_node],
                })),
            }
        }
        expr_build_from_prost(&root_expr_node)?
    };

    Ok(Box::new(SomeAllExpression::new(
        left_expr,
        right_expr,
        outer_expr_type,
        eval_func,
    )))
}

#[cfg(test)]
mod tests {
    use std::vec;

    use risingwave_common::array::{ArrayImpl, DataChunk, Utf8Array};
    use risingwave_common::types::Scalar;
    use risingwave_common::util::value_encoding::serialize_datum;
    use risingwave_pb::data::data_type::TypeName;
    use risingwave_pb::data::{DataType as ProstDataType, Datum as ProstDatum};
    use risingwave_pb::expr::expr_node::{RexNode, Type};
    use risingwave_pb::expr::{ExprNode, FunctionCall};

    use super::*;

    #[test]
    fn test_array_access_expr() {
        let values = FunctionCall {
            children: vec![
                ExprNode {
                    expr_type: Type::ConstantValue as i32,
                    return_type: Some(ProstDataType {
                        type_name: TypeName::Varchar as i32,
                        ..Default::default()
                    }),
                    rex_node: Some(RexNode::Constant(ProstDatum {
                        body: serialize_datum(Some("foo".into()).as_ref()),
                    })),
                },
                ExprNode {
                    expr_type: Type::ConstantValue as i32,
                    return_type: Some(ProstDataType {
                        type_name: TypeName::Varchar as i32,
                        ..Default::default()
                    }),
                    rex_node: Some(RexNode::Constant(ProstDatum {
                        body: serialize_datum(Some("bar".into()).as_ref()),
                    })),
                },
            ],
        };
        let array_index = FunctionCall {
            children: vec![
                ExprNode {
                    expr_type: Type::Array as i32,
                    return_type: Some(ProstDataType {
                        type_name: TypeName::List as i32,
                        field_type: vec![ProstDataType {
                            type_name: TypeName::Varchar as i32,
                            ..Default::default()
                        }],
                        ..Default::default()
                    }),
                    rex_node: Some(RexNode::FuncCall(values)),
                },
                ExprNode {
                    expr_type: Type::ConstantValue as i32,
                    return_type: Some(ProstDataType {
                        type_name: TypeName::Int32 as i32,
                        ..Default::default()
                    }),
                    rex_node: Some(RexNode::Constant(ProstDatum {
                        body: serialize_datum(Some(1_i32.to_scalar_value()).as_ref()),
                    })),
                },
            ],
        };
        let access = ExprNode {
            expr_type: Type::ArrayAccess as i32,
            return_type: Some(ProstDataType {
                type_name: TypeName::Varchar as i32,
                ..Default::default()
            }),
            rex_node: Some(RexNode::FuncCall(array_index)),
        };
        let expr = build_nullable_binary_expr_prost(&access);
        assert!(expr.is_ok());

        let res = expr.unwrap().eval(&DataChunk::new_dummy(1)).unwrap();
        assert_eq!(*res, ArrayImpl::Utf8(Utf8Array::from_iter(["foo"])));
    }

    #[test]
    fn test_build_extract_expr() {
        let left = ExprNode {
            expr_type: Type::ConstantValue as i32,
            return_type: Some(ProstDataType {
                type_name: TypeName::Varchar as i32,
                precision: 11,
                ..Default::default()
            }),
            rex_node: Some(RexNode::Constant(ProstDatum {
                body: serialize_datum(Some("DAY".into()).as_ref()),
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
