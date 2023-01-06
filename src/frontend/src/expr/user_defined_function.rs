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

use itertools::Itertools;
use risingwave_common::catalog::Schema;
use risingwave_common::error::{ErrorCode, Result};
use risingwave_common::types::DataType;
use risingwave_pb::catalog::Function;

use super::{cast_ok, infer_type, CastContext, Expr, ExprImpl, Literal};
use crate::expr::{ExprDisplay, ExprType};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct UserDefinedFunction {
    pub name: String,
    pub return_type: DataType,
    pub args: Vec<ExprImpl>,
}

impl UserDefinedFunction {
    pub fn new(name: &str, args: Vec<ExprImpl>, return_type: DataType) -> Self {
        Self {
            name: name.into(),
            return_type,
            args,
        }
    }
}

impl Expr for UserDefinedFunction {
    fn return_type(&self) -> DataType {
        self.return_type.clone()
    }

    fn to_expr_proto(&self) -> risingwave_pb::expr::ExprNode {
        use risingwave_pb::expr::expr_node::*;
        use risingwave_pb::expr::*;
        ExprNode {
            expr_type: Type::Udf.into(),
            return_type: Some(self.return_type().to_protobuf()),
            rex_node: Some(RexNode::FuncCall(FunctionCall {
                children: self.args.iter().map(Expr::to_expr_proto).collect(),
            })),
        }
    }
}
