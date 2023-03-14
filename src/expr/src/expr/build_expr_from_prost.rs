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

use itertools::Itertools;
use risingwave_common::try_match_expand;
use risingwave_common::types::DataType;
use risingwave_common::util::value_encoding::deserialize_datum;
use risingwave_expr_macro::build_function;
use risingwave_pb::expr::expr_node::RexNode;
use risingwave_pb::expr::ExprNode;

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
use crate::expr::expr_jsonb_access::build_jsonb_expr;
use crate::expr::template::{BinaryBytesExpression, BinaryExpression};
use crate::expr::{BoxedExpression, Expression, InputRefExpression, LiteralExpression};
use crate::sig::func::FUNC_SIG_MAP;
use crate::{bail, ensure, ExprError, Result};

pub fn build_from_prost(prost: &ExprNode) -> Result<BoxedExpression> {
    use risingwave_pb::expr::expr_node::Type as E;

    if let Some(RexNode::FuncCall(call)) = &prost.rex_node {
        let args = call
            .children
            .iter()
            .map(|c| DataType::from(c.get_return_type().unwrap()).into())
            .collect_vec();
        if let Some(desc) = FUNC_SIG_MAP.get(prost.expr_type(), &args) {
            return (desc.build_from_prost)(prost);
        }
    }

    match prost.expr_type() {
        // Dedicated types
        E::All | E::Some => SomeAllExpression::try_from(prost).map(Expression::boxed),
        E::In => InExpression::try_from(prost).map(Expression::boxed),
        E::Case => CaseExpression::try_from(prost).map(Expression::boxed),
        E::Coalesce => CoalesceExpression::try_from(prost).map(Expression::boxed),
        E::ConcatWs => ConcatWsExpression::try_from(prost).map(Expression::boxed),
        E::ConstantValue => LiteralExpression::try_from(prost).map(Expression::boxed),
        E::InputRef => InputRefExpression::try_from(prost).map(Expression::boxed),
        E::Field => FieldExpression::try_from(prost).map(Expression::boxed),
        E::Array => NestedConstructExpression::try_from(prost).map(Expression::boxed),
        E::Row => NestedConstructExpression::try_from(prost).map(Expression::boxed),
        E::RegexpMatch => RegexpMatchExpression::try_from(prost).map(Expression::boxed),
        E::ArrayCat | E::ArrayAppend | E::ArrayPrepend => {
            // Now we implement these three functions as a single expression for the
            // sake of simplicity. If performance matters at some time, we can split
            // the implementation to improve performance.
            ArrayConcatExpression::try_from(prost).map(Expression::boxed)
        }
        E::JsonbAccessInner | E::JsonbAccessStr => build_jsonb_expr(prost),
        E::Vnode => VnodeExpression::try_from(prost).map(Expression::boxed),
        E::Now => build_now_expr(prost),
        E::Udf => UdfExpression::try_from(prost).map(Expression::boxed),
        _ => Err(ExprError::UnsupportedFunction(format!(
            "{:?}",
            prost.get_expr_type()
        ))),
    }
}

pub(super) fn get_children_and_return_type(prost: &ExprNode) -> Result<(&[ExprNode], DataType)> {
    let ret_type = DataType::from(prost.get_return_type().unwrap());
    if let RexNode::FuncCall(func_call) = prost.get_rex_node().unwrap() {
        Ok((func_call.get_children(), ret_type))
    } else {
        bail!("Expected RexNode::FuncCall");
    }
}

#[build_function("to_char(timestamp, varchar) -> varchar")]
fn build_to_char_expr(prost: &ExprNode) -> Result<BoxedExpression> {
    use risingwave_common::array::*;

    use crate::vector_op::to_char::{compile_pattern_to_chrono, to_char_timestamp};

    let (children, ret_type) = get_children_and_return_type(prost)?;
    ensure!(children.len() == 2);
    let data_expr = build_from_prost(&children[0])?;
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
        let tmpl_expr = build_from_prost(&children[1])?;
        Ok(BinaryBytesExpression::<NaiveDateTimeArray, Utf8Array, _>::new(
            data_expr,
            tmpl_expr,
            ret_type,
            |a, b, w| Ok(to_char_timestamp(a, b, w)),
        )
        .boxed())
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

#[build_function("to_timestamp(varchar, varchar) -> timestamp")]
pub fn build_to_timestamp_expr(prost: &ExprNode) -> Result<BoxedExpression> {
    use risingwave_common::array::*;

    use crate::vector_op::to_char::compile_pattern_to_chrono;
    use crate::vector_op::to_timestamp::to_timestamp;

    let (children, ret_type) = get_children_and_return_type(prost)?;
    ensure!(children.len() == 2);
    let data_expr = build_from_prost(&children[0])?;
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
        let tmpl_expr = build_from_prost(&children[1])?;
        Ok(BinaryExpression::<Utf8Array, Utf8Array, NaiveDateTimeArray, _>::new(
            data_expr,
            tmpl_expr,
            ret_type,
            to_timestamp,
        )
        .boxed())
    }
}
