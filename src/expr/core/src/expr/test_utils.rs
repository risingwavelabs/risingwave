// Copyright 2025 RisingWave Labs
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

//! Helper functions to construct prost [`ExprNode`] for test.

use std::num::NonZeroUsize;

use num_traits::CheckedSub;
use risingwave_common::types::{DataType, Interval, ScalarImpl};
use risingwave_common::util::value_encoding::DatumToProtoExt;
use risingwave_pb::data::PbDataType;
use risingwave_pb::data::data_type::TypeName;
use risingwave_pb::expr::expr_node::Type::Field;
use risingwave_pb::expr::expr_node::{self, RexNode, Type};
use risingwave_pb::expr::{ExprNode, FunctionCall};

use super::{BoxedExpression, Result, build_from_prost};
use crate::ExprError;

pub fn make_func_call(kind: Type, ret: TypeName, children: Vec<ExprNode>) -> ExprNode {
    ExprNode {
        function_type: kind as i32,
        return_type: Some(PbDataType {
            type_name: ret as i32,
            ..Default::default()
        }),
        rex_node: Some(RexNode::FuncCall(FunctionCall { children })),
    }
}

pub fn make_input_ref(idx: usize, ret: TypeName) -> ExprNode {
    ExprNode {
        function_type: Type::Unspecified as i32,
        return_type: Some(PbDataType {
            type_name: ret as i32,
            ..Default::default()
        }),
        rex_node: Some(RexNode::InputRef(idx as _)),
    }
}

pub fn make_i32_literal(data: i32) -> ExprNode {
    ExprNode {
        function_type: Type::Unspecified as i32,
        return_type: Some(PbDataType {
            type_name: TypeName::Int32 as i32,
            ..Default::default()
        }),
        rex_node: Some(RexNode::Constant(
            Some(ScalarImpl::Int32(data)).to_protobuf(),
        )),
    }
}

fn make_interval_literal(data: Interval) -> ExprNode {
    ExprNode {
        function_type: Type::Unspecified as i32,
        return_type: Some(PbDataType {
            type_name: TypeName::Interval as i32,
            ..Default::default()
        }),
        rex_node: Some(RexNode::Constant(
            Some(ScalarImpl::Interval(data)).to_protobuf(),
        )),
    }
}

pub fn make_field_function(children: Vec<ExprNode>, ret: TypeName) -> ExprNode {
    ExprNode {
        function_type: Field as i32,
        return_type: Some(PbDataType {
            type_name: ret as i32,
            ..Default::default()
        }),
        rex_node: Some(RexNode::FuncCall(FunctionCall { children })),
    }
}

pub fn make_hop_window_expression(
    time_col_data_type: DataType,
    time_col_idx: usize,
    window_size: Interval,
    window_slide: Interval,
    window_offset: Interval,
) -> Result<(Vec<BoxedExpression>, Vec<BoxedExpression>)> {
    let units = window_size
        .exact_div(&window_slide)
        .and_then(|x| NonZeroUsize::new(usize::try_from(x).ok()?))
        .ok_or_else(|| ExprError::InvalidParam {
            name: "window",
            reason: format!(
                "window_size {} cannot be divided by window_slide {}",
                window_size, window_slide
            )
            .into(),
        })?
        .get();

    let output_type = DataType::window_of(&time_col_data_type)
        .unwrap()
        .to_protobuf()
        .type_name();

    let time_col_ref = make_input_ref(time_col_idx, time_col_data_type.to_protobuf().type_name());

    // The first window_start of hop window should be:
    // tumble_start(`time_col` - (`window_size` - `window_slide`), `window_slide`, `window_offset`).
    // Let's pre calculate (`window_size` - `window_slide`).
    let window_size_sub_slide = window_size
        .checked_sub(&window_slide)
        .ok_or_else(|| ExprError::InvalidParam {
            name: "window",
            reason: format!(
                "window_size {} cannot be subtracted by window_slide {}",
                window_size, window_slide
            )
            .into(),
        })
        .unwrap();

    let hop_window_start = make_func_call(
        expr_node::Type::TumbleStart,
        output_type,
        vec![
            make_func_call(
                expr_node::Type::Subtract,
                output_type,
                vec![time_col_ref, make_interval_literal(window_size_sub_slide)],
            ),
            make_interval_literal(window_slide),
            make_interval_literal(window_offset),
        ],
    );

    let mut window_start_exprs = Vec::with_capacity(units);
    let mut window_end_exprs = Vec::with_capacity(units);
    for i in 0..units {
        let window_start_offset =
            window_slide
                .checked_mul_int(i)
                .ok_or_else(|| ExprError::InvalidParam {
                    name: "window",
                    reason: format!(
                        "window_slide {} cannot be multiplied by {}",
                        window_slide, i
                    )
                    .into(),
                })?;
        let window_end_offset =
            window_slide
                .checked_mul_int(i + units)
                .ok_or_else(|| ExprError::InvalidParam {
                    name: "window",
                    reason: format!(
                        "window_slide {} cannot be multiplied by {}",
                        window_slide, i
                    )
                    .into(),
                })?;
        let window_start_expr = make_func_call(
            expr_node::Type::Add,
            output_type,
            vec![
                hop_window_start.clone(),
                make_interval_literal(window_start_offset),
            ],
        );
        window_start_exprs.push(build_from_prost(&window_start_expr).unwrap());
        let window_end_expr = make_func_call(
            expr_node::Type::Add,
            output_type,
            vec![
                hop_window_start.clone(),
                make_interval_literal(window_end_offset),
            ],
        );
        window_end_exprs.push(build_from_prost(&window_end_expr).unwrap());
    }
    Ok((window_start_exprs, window_end_exprs))
}
