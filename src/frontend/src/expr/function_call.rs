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

use num_integer::Integer as _;
use risingwave_common::error::{ErrorCode, Result};
use risingwave_common::types::DataType;

use super::{align_types, cast_ok, infer_type, CastContext, Expr, ExprImpl, Literal};
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
                ExprType::BitwiseShiftLeft => debug_binary_op(f, "<<", &self.inputs),
                ExprType::BitwiseShiftRight => debug_binary_op(f, ">>", &self.inputs),
                ExprType::BitwiseAnd => debug_binary_op(f, "&", &self.inputs),
                ExprType::BitwiseOr => debug_binary_op(f, "|", &self.inputs),
                ExprType::BitwiseXor => debug_binary_op(f, "#", &self.inputs),
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
    /// Create a `FunctionCall` expr with the return type inferred from `func_type` and types of
    /// `inputs`.
    // The functions listed here are all variadic.  Type signatures of functions that take a fixed
    // number of arguments are checked
    // [elsewhere](crate::expr::type_inference::build_type_derive_map).
    pub fn new(func_type: ExprType, mut inputs: Vec<ExprImpl>) -> Result<Self> {
        let return_type = match func_type {
            ExprType::Case => {
                let len = inputs.len();
                align_types(inputs.iter_mut().enumerate().filter_map(|(i, e)| {
                    // `Case` organize `inputs` as (cond, res) pairs with a possible `else` res at
                    // the end. So we align exprs at odd indices as well as the last one when length
                    // is odd.
                    match i.is_odd() || len.is_odd() && i == len - 1 {
                        true => Some(e),
                        false => None,
                    }
                }))
            }
            ExprType::In => {
                align_types(inputs.iter_mut())?;
                Ok(DataType::Boolean)
            }
            ExprType::Coalesce => {
                if inputs.is_empty() {
                    return Err(ErrorCode::BindError(format!(
                        "Function `Coalesce` takes at least {} arguments ({} given)",
                        1, 0
                    ))
                    .into());
                }
                align_types(inputs.iter_mut())
            }
            ExprType::Concat => {
                if inputs.is_empty() {
                    return Err(ErrorCode::BindError(format!(
                        "Function `Concat` takes at least {} arguments ({} given)",
                        1, 0
                    ))
                    .into());
                }

                inputs = inputs
                    .into_iter()
                    .map(|input| input.cast_explicit(DataType::Varchar))
                    .collect::<Result<Vec<_>>>()?;
                Ok(DataType::Varchar)
            }
            ExprType::ConcatWs => {
                let expected = 2;
                let actual = inputs.len();
                if actual < expected {
                    return Err(ErrorCode::BindError(format!(
                        "Function `ConcatWs` takes at least {} arguments ({} given)",
                        expected, actual
                    ))
                    .into());
                }

                inputs = inputs
                    .into_iter()
                    .enumerate()
                    .map(|(i, input)| match i {
                        // 0-th arg must be string
                        0 => input.cast_implicit(DataType::Varchar),
                        // subsequent can be any type
                        _ => input.cast_explicit(DataType::Varchar),
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok(DataType::Varchar)
            }

            _ => infer_type(
                func_type,
                inputs.iter().map(|expr| expr.return_type()).collect(),
            ),
        }?;
        Ok(Self {
            func_type,
            return_type,
            inputs,
        })
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

    /// Construct a `FunctionCall` expr directly with the provided `return_type`, bypassing type
    /// inference. Use with caution.
    pub fn new_unchecked(
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

    fn to_expr_proto(&self) -> risingwave_pb::expr::ExprNode {
        use risingwave_pb::expr::expr_node::*;
        use risingwave_pb::expr::*;
        ExprNode {
            expr_type: self.get_expr_type().into(),
            return_type: Some(self.return_type().to_protobuf()),
            rex_node: Some(RexNode::FuncCall(FunctionCall {
                children: self.inputs().iter().map(Expr::to_expr_proto).collect(),
            })),
        }
    }
}
