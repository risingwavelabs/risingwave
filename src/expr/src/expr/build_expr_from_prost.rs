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
use risingwave_common::types::DataType;
use risingwave_pb::expr::expr_node::{PbType, RexNode};
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
use super::expr_udf::UdfExpression;
use super::expr_vnode::VnodeExpression;
use crate::expr::expr_array_length::ArrayLengthExpression;
use crate::expr::{BoxedExpression, Expression, InputRefExpression, LiteralExpression};
use crate::sig::func::FUNC_SIG_MAP;
use crate::{bail, ExprError, Result};

/// Build an expression from protobuf.
pub fn build_from_prost(prost: &ExprNode) -> Result<BoxedExpression> {
    use PbType as E;

    if let Some(RexNode::FuncCall(call)) = &prost.rex_node {
        let args = call
            .children
            .iter()
            .map(|c| DataType::from(c.get_return_type().unwrap()).into())
            .collect_vec();
        let return_type = DataType::from(prost.get_return_type().unwrap());

        if let Some(desc) = FUNC_SIG_MAP.get(prost.expr_type(), &args, (&return_type).into()) {
            let RexNode::FuncCall(func_call) = prost.get_rex_node().unwrap() else {
                bail!("Expected RexNode::FuncCall");
            };

            let children = func_call
                .get_children()
                .iter()
                .map(build_from_prost)
                .try_collect()?;
            return (desc.build)(return_type, children);
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
        E::ArrayLength => ArrayLengthExpression::try_from(prost).map(Expression::boxed),
        E::Vnode => VnodeExpression::try_from(prost).map(Expression::boxed),
        E::Udf => UdfExpression::try_from(prost).map(Expression::boxed),
        _ => Err(ExprError::UnsupportedFunction(format!(
            "{:?}",
            prost.get_expr_type()
        ))),
    }
}

/// Build an expression.
pub fn build(
    func: PbType,
    ret_type: DataType,
    children: Vec<BoxedExpression>,
) -> Result<BoxedExpression> {
    let args = children
        .iter()
        .map(|c| c.return_type().into())
        .collect_vec();
    let desc = FUNC_SIG_MAP
        .get(func, &args, (&ret_type).into())
        .ok_or_else(|| {
            ExprError::UnsupportedFunction(format!(
                "{:?}({}) -> {:?}",
                func,
                args.iter().map(|t| format!("{:?}", t)).join(", "),
                ret_type
            ))
        })?;
    (desc.build)(ret_type, children)
}

pub(super) fn get_children_and_return_type(prost: &ExprNode) -> Result<(&[ExprNode], DataType)> {
    let ret_type = DataType::from(prost.get_return_type().unwrap());
    if let RexNode::FuncCall(func_call) = prost.get_rex_node().unwrap() {
        Ok((func_call.get_children(), ret_type))
    } else {
        bail!("Expected RexNode::FuncCall");
    }
}
