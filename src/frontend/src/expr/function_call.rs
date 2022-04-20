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

use risingwave_common::error::{ErrorCode, Result, RwError};
use risingwave_common::types::{DataType, ScalarImpl};

use super::{cast_ok, infer_type, CastContext, Expr, ExprImpl, Literal};
use crate::expr::ExprType;

#[derive(Clone, Eq, PartialEq, Hash)]
pub struct FunctionCall {
    func_type: ExprType,
    return_type: DataType,
    inputs: Vec<ExprImpl>,
}

fn debug_binary_op(
    f: &mut std::fmt::Formatter<'_>,
    op: &str,
    inputs: &[ExprImpl],
) -> std::fmt::Result {
    use std::fmt::Debug;

    assert_eq!(inputs.len(), 2);

    write!(f, "(")?;
    inputs[0].fmt(f)?;
    write!(f, " {} ", op)?;
    inputs[1].fmt(f)?;
    write!(f, ")")?;

    Ok(())
}

impl std::fmt::Debug for FunctionCall {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if f.alternate() {
            f.debug_struct("FunctionCall")
                .field("func_type", &self.func_type)
                .field("return_type", &self.return_type)
                .field("inputs", &self.inputs)
                .finish()
        } else {
            match &self.func_type {
                ExprType::Cast => {
                    assert_eq!(self.inputs.len(), 1);
                    self.inputs[0].fmt(f)?;
                    return write!(f, "::{:?}", self.return_type);
                }
                ExprType::Add => debug_binary_op(f, "+", &self.inputs),
                ExprType::Subtract => debug_binary_op(f, "-", &self.inputs),
                ExprType::Multiply => debug_binary_op(f, "*", &self.inputs),
                ExprType::Divide => debug_binary_op(f, "/", &self.inputs),
                ExprType::Modulus => debug_binary_op(f, "%", &self.inputs),
                ExprType::Equal => debug_binary_op(f, "=", &self.inputs),
                ExprType::NotEqual => debug_binary_op(f, "<>", &self.inputs),
                ExprType::LessThan => debug_binary_op(f, "<", &self.inputs),
                ExprType::LessThanOrEqual => debug_binary_op(f, "<=", &self.inputs),
                ExprType::GreaterThan => debug_binary_op(f, ">", &self.inputs),
                ExprType::GreaterThanOrEqual => debug_binary_op(f, ">=", &self.inputs),
                ExprType::And => debug_binary_op(f, "AND", &self.inputs),
                ExprType::Or => debug_binary_op(f, "OR", &self.inputs),
                _ => {
                    let func_name = format!("{:?}", self.func_type);
                    let mut builder = f.debug_tuple(&func_name);
                    self.inputs.iter().for_each(|child| {
                        builder.field(child);
                    });
                    builder.finish()
                }
            }
        }
    }
}

impl FunctionCall {
    /// Returns error if the function call is not valid.
    pub fn new_or_else<F>(func_type: ExprType, inputs: Vec<ExprImpl>, err_f: F) -> Result<Self>
    where
        F: FnOnce(&Vec<ExprImpl>) -> RwError,
    {
        infer_type(
            func_type,
            inputs.iter().map(|expr| expr.return_type()).collect(),
        )
        .ok_or_else(|| err_f(&inputs))
        .map(|return_type| Self::new_with_return_type(func_type, inputs, return_type))
    }

    pub fn new(func_type: ExprType, inputs: Vec<ExprImpl>) -> Option<Self> {
        let return_type = infer_type(
            func_type,
            inputs.iter().map(|expr| expr.return_type()).collect(),
        )?; // should be derived from inputs
        Some(Self::new_with_return_type(func_type, inputs, return_type))
    }

    /// Create a cast expr over `child` to `target` type in `allows` context.
    pub fn new_cast(child: ExprImpl, target: DataType, allows: CastContext) -> Result<ExprImpl> {
        let source = child.return_type();
        if child.is_null() {
            Ok(Literal::new(None, target).into())
        } else if source == target {
            Ok(child)
        } else if cast_ok(&source, &target, &allows) {
            Ok(Self {
                func_type: ExprType::Cast,
                return_type: target,
                inputs: vec![child],
            }
            .into())
        } else {
            Err(ErrorCode::BindError(format!(
                "cannot cast type {:?} to {:?} in {:?} context",
                source, target, allows
            ))
            .into())
        }
    }

    /// used for expressions like cast
    pub fn new_with_return_type(
        func_type: ExprType,
        inputs: Vec<ExprImpl>,
        return_type: DataType,
    ) -> Self {
        FunctionCall {
            func_type,
            return_type,
            inputs,
        }
    }

    pub fn decompose(self) -> (ExprType, Vec<ExprImpl>, DataType) {
        (self.func_type, self.inputs, self.return_type)
    }

    pub fn decompose_as_binary(self) -> (ExprType, ExprImpl, ExprImpl) {
        assert_eq!(self.inputs.len(), 2);
        let mut iter = self.inputs.into_iter();
        let left = iter.next().unwrap();
        let right = iter.next().unwrap();
        (self.func_type, left, right)
    }

    pub fn decompose_as_unary(self) -> (ExprType, ExprImpl) {
        assert_eq!(self.inputs.len(), 1);
        let mut iter = self.inputs.into_iter();
        let input = iter.next().unwrap();
        (self.func_type, input)
    }

    pub fn get_expr_type(&self) -> ExprType {
        self.func_type
    }

    /// Get a reference to the function call's inputs.
    pub fn inputs(&self) -> &[ExprImpl] {
        self.inputs.as_ref()
    }
}
impl Expr for FunctionCall {
    fn return_type(&self) -> DataType {
        self.return_type.clone()
    }

    fn to_protobuf(&self) -> risingwave_pb::expr::ExprNode {
        use risingwave_pb::expr::expr_node::*;
        use risingwave_pb::expr::*;
        ExprNode {
            expr_type: self.get_expr_type().into(),
            return_type: Some(self.return_type().to_protobuf()),
            rex_node: Some(RexNode::FuncCall(FunctionCall {
                children: self.inputs().iter().map(Expr::to_protobuf).collect(),
            })),
        }
    }

    /// If `func_type` is `Field` which means this is a nested column,
    /// so concat the index of each inputs.
    /// The first of inputs must be `InputRef` and following must be `Literal` which contains i32
    /// value.
    fn get_indexs(&self) -> Option<Vec<usize>> {
        if ExprType::Field == self.func_type {
            if let ExprImpl::InputRef(input) = &self.inputs[0] {
                let mut indexs = vec![input.index()];
                for expr in &self.inputs[1..] {
                    if let ExprImpl::Literal(literal) = expr {
                        match literal.get_data() {
                            Some(ScalarImpl::Int32(i)) => {
                                indexs.push(*i as usize);
                            }
                            _ => {
                                unreachable!()
                            }
                        }
                    } else {
                        unreachable!()
                    }
                }
                return Some(indexs);
            }
            unreachable!()
        } else {
            match self.inputs.get(0) {
                Some(expr) => expr.get_indexs(),
                None => None,
            }
        }
    }
}
