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

use itertools::Itertools;
use risingwave_pb::data::data_type::TypeName;
use risingwave_pb::data::DataType;
use risingwave_pb::expr::expr_node::Type::InputRef;
use risingwave_pb::expr::expr_node::{RexNode, Type};
use risingwave_pb::expr::{ExprNode, FunctionCall, InputRefExpr};

pub fn make_expression(kind: Type, rets: &[TypeName], indices: &[i32]) -> ExprNode {
    let mut exprs = Vec::new();
    for (idx, ret) in indices.iter().zip_eq(rets.iter()) {
        exprs.push(make_input_ref(*idx, *ret));
    }
    let function_call = FunctionCall { children: exprs };
    let return_type = DataType {
        type_name: TypeName::Timestamp as i32,
        ..Default::default()
    };
    ExprNode {
        expr_type: kind as i32,
        return_type: Some(return_type),
        rex_node: Some(RexNode::FuncCall(function_call)),
    }
}

pub fn make_input_ref(idx: i32, ret: TypeName) -> ExprNode {
    ExprNode {
        expr_type: InputRef as i32,
        return_type: Some(DataType {
            type_name: ret as i32,
            ..Default::default()
        }),
        rex_node: Some(RexNode::InputRef(InputRefExpr { column_idx: idx })),
    }
}
