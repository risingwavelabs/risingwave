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

//! For expression that only accept two nullable arguments as input.

use risingwave_common::array::BoolArray;
use risingwave_common::types::DataType;
use risingwave_pb::expr::expr_node::Type;

use super::BoxedExpression;
use crate::expr::template::BinaryNullableExpression;
use crate::vector_op::conjunction::{and, or};

pub fn new_nullable_binary_expr(
    expr_type: Type,
    ret: DataType,
    l: BoxedExpression,
    r: BoxedExpression,
) -> BoxedExpression {
    match expr_type {
        Type::And => Box::new(
            BinaryNullableExpression::<BoolArray, BoolArray, BoolArray, _>::new(l, r, ret, and),
        ),
        Type::Or => Box::new(
            BinaryNullableExpression::<BoolArray, BoolArray, BoolArray, _>::new(l, r, ret, or),
        ),
        tp => {
            unimplemented!(
                "The expression {:?} using vectorized expression framework is not supported yet!",
                tp
            )
        }
    }
}
