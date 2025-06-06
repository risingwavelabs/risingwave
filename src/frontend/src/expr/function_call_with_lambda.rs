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

use risingwave_common::types::DataType;

use super::{ExprImpl, FunctionCall};
use crate::expr::{Expr, ExprType};

/// Similar to [`FunctionCall`], with an extra lambda function argument.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct FunctionCallWithLambda {
    base: FunctionCall,
    lambda_arg: ExprImpl,
}

impl std::fmt::Debug for FunctionCallWithLambda {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if f.alternate() {
            f.debug_struct("FunctionCallWithLambda")
                .field("func_type", &self.base.func_type)
                .field("return_type", &self.base.return_type)
                .field("inputs", &self.base.inputs)
                .field("lambda_arg", &self.lambda_arg)
                .finish()
        } else {
            let func_name = format!("{:?}", self.base.func_type);
            let mut builder = f.debug_tuple(&func_name);
            for input in &self.base.inputs {
                builder.field(input);
            }
            builder.field(&self.lambda_arg);
            builder.finish()
        }
    }
}

impl FunctionCallWithLambda {
    pub fn new_unchecked(
        func_type: ExprType,
        inputs: Vec<ExprImpl>,
        lambda_arg: ExprImpl,
        return_type: DataType,
    ) -> Self {
        assert!([ExprType::ArrayTransform, ExprType::MapFilter].contains(&func_type));
        Self {
            base: FunctionCall::new_unchecked(func_type, inputs, return_type),
            lambda_arg,
        }
    }

    pub fn inputs(&self) -> &[ExprImpl] {
        self.base.inputs()
    }

    pub fn func_type(&self) -> ExprType {
        self.base.func_type()
    }

    pub fn return_type(&self) -> DataType {
        self.base.return_type()
    }

    pub fn base(&self) -> &FunctionCall {
        &self.base
    }

    pub fn base_mut(&mut self) -> &mut FunctionCall {
        &mut self.base
    }

    pub fn inputs_with_lambda_arg(&self) -> impl Iterator<Item = &'_ ExprImpl> {
        self.inputs().iter().chain([&self.lambda_arg])
    }

    pub fn to_full_function_call(&self) -> FunctionCall {
        let full_inputs = self.inputs_with_lambda_arg().cloned();
        FunctionCall::new_unchecked(self.func_type(), full_inputs.collect(), self.return_type())
    }

    pub fn into_parts(self) -> (ExprType, Vec<ExprImpl>, ExprImpl, DataType) {
        let Self { base, lambda_arg } = self;
        let (func_type, inputs, return_type) = base.decompose();
        (func_type, inputs, lambda_arg, return_type)
    }
}

impl Expr for FunctionCallWithLambda {
    fn return_type(&self) -> DataType {
        self.base.return_type()
    }

    fn to_expr_proto(&self) -> risingwave_pb::expr::ExprNode {
        use risingwave_pb::expr::expr_node::*;
        use risingwave_pb::expr::*;
        ExprNode {
            function_type: self.func_type().into(),
            return_type: Some(self.return_type().to_protobuf()),
            rex_node: Some(RexNode::FuncCall(FunctionCall {
                children: self
                    .inputs_with_lambda_arg()
                    .map(Expr::to_expr_proto)
                    .collect(),
            })),
        }
    }
}
