// Copyright 2023 RisingWave Labs
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
use risingwave_pb::expr::{ExprNode, FunctionCall};

use super::expr_array_concat::ArrayConcatExpression;
use super::expr_case::CaseExpression;
use super::expr_coalesce::CoalesceExpression;
use super::expr_concat_ws::ConcatWsExpression;
use super::expr_field::FieldExpression;
use super::expr_in::InExpression;
use super::expr_nested_construct::NestedConstructExpression;
use super::expr_regexp::RegexpMatchExpression;
use super::expr_some_all::SomeAllExpression;
use super::expr_to_char_const_tmpl::{ExprToCharConstTmpl, ExprToCharConstTmplContext};
use super::expr_to_timestamp_const_tmpl::{
    ExprToTimestampConstTmpl, ExprToTimestampConstTmplContext,
};
use super::expr_udf::UdfExpression;
use super::expr_vnode::VnodeExpression;
use crate::expr::{
    build_from_prost as expr_build_from_prost, BoxedExpression, Expression, InputRefExpression,
    LiteralExpression,
};
use crate::vector_op::to_char::compile_pattern_to_chrono;
use crate::{bail, ensure, ExprError, Result};

pub fn build_from_prost(prost: &ExprNode) -> Result<BoxedExpression> {
    use risingwave_pb::expr::expr_node::Type::*;

    match prost.get_expr_type().unwrap() {
        // Dedicated types
        All | Some => SomeAllExpression::try_from(prost).map(Expression::boxed),
        In => InExpression::try_from(prost).map(Expression::boxed),
        Case => CaseExpression::try_from(prost).map(Expression::boxed),
        Coalesce => CoalesceExpression::try_from(prost).map(Expression::boxed),
        ConcatWs => ConcatWsExpression::try_from(prost).map(Expression::boxed),
        ConstantValue => LiteralExpression::try_from(prost).map(Expression::boxed),
        InputRef => InputRefExpression::try_from(prost).map(Expression::boxed),
        Field => FieldExpression::try_from(prost).map(Expression::boxed),
        Array => NestedConstructExpression::try_from(prost).map(Expression::boxed),
        Row => NestedConstructExpression::try_from(prost).map(Expression::boxed),
        RegexpMatch => RegexpMatchExpression::try_from(prost).map(Expression::boxed),
        ArrayCat | ArrayAppend | ArrayPrepend => {
            // Now we implement these three functions as a single expression for the
            // sake of simplicity. If performance matters at some time, we can split
            // the implementation to improve performance.
            ArrayConcatExpression::try_from(prost).map(Expression::boxed)
        }
        Vnode => VnodeExpression::try_from(prost).map(Expression::boxed),
        Now => build_now_expr(prost),
        Udf => UdfExpression::try_from(prost).map(Expression::boxed),
        _ => Err(ExprError::UnsupportedFunction(format!(
            "{:?}",
            prost.get_expr_type()
        ))),
    }
}

fn get_children_and_return_type(prost: &ExprNode) -> Result<(Vec<ExprNode>, DataType)> {
    let ret_type = DataType::from(prost.get_return_type().unwrap());
    if let RexNode::FuncCall(func_call) = prost.get_rex_node().unwrap() {
        Ok((func_call.get_children().to_vec(), ret_type))
    } else {
        bail!("Expected RexNode::FuncCall");
    }
}

fn build_to_char_expr(prost: &ExprNode) -> Result<BoxedExpression> {
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
