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
use num_integer::Integer as _;
use risingwave_common::catalog::Schema;
use risingwave_common::error::{ErrorCode, Result};
use risingwave_common::types::DataType;

use super::{align_types, cast_ok, infer_type, CastContext, Expr, ExprImpl, Literal};
use crate::expr::{ExprDisplay, ExprType};

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
                    write!(f, "::{:?}", self.return_type)
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

macro_rules! ensure_arity {
    ($func:literal, $lower:literal <= | $inputs:ident | <= $upper:literal) => {
        if !($lower <= $inputs.len() && $inputs.len() <= $upper) {
            return Err(ErrorCode::BindError(format!(
                "Function `{}` takes {} to {} arguments ({} given)",
                $func,
                $lower,
                $upper,
                $inputs.len(),
            ))
            .into());
        }
    };
    ($func:literal, $lower:literal <= | $inputs:ident |) => {
        if !($lower <= $inputs.len()) {
            return Err(ErrorCode::BindError(format!(
                "Function `{}` takes at least {} arguments ({} given)",
                $func,
                $lower,
                $inputs.len(),
            ))
            .into());
        }
    };
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
                ensure_arity!("coalesce", 1 <= | inputs |);
                align_types(inputs.iter_mut())
            }
            ExprType::ConcatWs => {
                ensure_arity!("concat_ws", 2 <= | inputs |);
                inputs = inputs
                    .into_iter()
                    .enumerate()
                    .map(|(i, input)| match i {
                        // 0-th arg must be string
                        0 => input.cast_implicit(DataType::Varchar),
                        // subsequent can be any type, using the output format
                        _ => input.cast_output(),
                    })
                    .try_collect()?;
                Ok(DataType::Varchar)
            }
            ExprType::ConcatOp => {
                inputs = inputs
                    .into_iter()
                    .map(|input| input.cast_explicit(DataType::Varchar))
                    .try_collect()?;
                Ok(DataType::Varchar)
            }
            ExprType::RegexpMatch => {
                ensure_arity!("regexp_match", 2 <= | inputs | <= 3);
                if inputs.len() == 3 {
                    return Err(ErrorCode::NotImplemented(
                        "flag in regexp_match".to_string(),
                        4545.into(),
                    )
                    .into());
                }
                Ok(DataType::List {
                    datatype: Box::new(DataType::Varchar),
                })
            }
            ExprType::Vnode => {
                ensure_arity!("vnode", 1 <= | inputs |);
                Ok(DataType::Int16)
            }
            _ => {
                // TODO(xiangjin): move variadic functions above as part of `infer_type`, as its
                // interface has been enhanced to support mutating (casting) inputs as well.
                infer_type(func_type, &mut inputs)
            }
        }?;
        Ok(Self {
            func_type,
            return_type,
            inputs,
        })
    }

    /// Create a cast expr over `child` to `target` type in `allows` context.
    pub fn new_cast(child: ExprImpl, target: DataType, allows: CastContext) -> Result<ExprImpl> {
        if is_row_function(&child) {
            return Self::cast_nested(child, target, allows);
        }
        let source = child.return_type();
        if child.is_null() {
            Ok(Literal::new(None, target).into())
        } else if source == target {
            Ok(child)
        // Casting from unknown is allowed in all context. And PostgreSQL actually does the parsing
        // in frontend.
        } else if child.is_unknown() || cast_ok(&source, &target, allows) {
            Ok(Self {
                func_type: ExprType::Cast,
                return_type: target,
                inputs: vec![child],
            }
            .into())
        } else {
            Err(ErrorCode::BindError(format!(
                "cannot cast type \"{}\" to \"{}\" in {:?} context",
                source, target, allows
            ))
            .into())
        }
    }

    /// Cast a `ROW` expression to the target type. We intentionally disallow casting arbitrary
    /// expressions, like `ROW(1)::STRUCT<i INTEGER>` to `STRUCT<VARCHAR>`, although an integer
    /// is castible to VARCHAR. It's to simply the casting rules.
    fn cast_nested(expr: ExprImpl, target_type: DataType, allows: CastContext) -> Result<ExprImpl> {
        let func = *expr.into_function_call().unwrap();
        let (fields, field_names) = if let DataType::Struct(t) = &target_type {
            (t.fields.clone(), t.field_names.clone())
        } else {
            return Err(ErrorCode::BindError(format!(
                "column is of type '{}' but expression is of type record",
                target_type
            ))
            .into());
        };
        let (func_type, inputs, _) = func.decompose();
        let msg = match fields.len().cmp(&inputs.len()) {
            std::cmp::Ordering::Equal => {
                let inputs = inputs
                    .into_iter()
                    .zip_eq(fields.to_vec())
                    .map(|(e, t)| Self::new_cast(e, t, allows))
                    .collect::<Result<Vec<_>>>()?;
                let return_type = DataType::new_struct(
                    inputs.iter().map(|i| i.return_type()).collect_vec(),
                    field_names,
                );
                return Ok(FunctionCall::new_unchecked(func_type, inputs, return_type).into());
            }
            std::cmp::Ordering::Less => "Input has too few columns.",
            std::cmp::Ordering::Greater => "Input has too many columns.",
        };
        Err(ErrorCode::BindError(format!("cannot cast record to {} ({})", target_type, msg)).into())
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

    pub fn inputs_mut(&mut self) -> &mut [ExprImpl] {
        self.inputs.as_mut()
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

pub struct FunctionCallDisplay<'a> {
    pub function_call: &'a FunctionCall,
    pub input_schema: &'a Schema,
}

impl std::fmt::Debug for FunctionCallDisplay<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let that = self.function_call;
        match &that.func_type {
            ExprType::Cast => {
                assert_eq!(that.inputs.len(), 1);
                ExprDisplay {
                    expr: &that.inputs[0],
                    input_schema: self.input_schema,
                }
                .fmt(f)?;
                write!(f, "::{:?}", that.return_type)
            }
            ExprType::Add => explain_verbose_binary_op(f, "+", &that.inputs, self.input_schema),
            ExprType::Subtract => {
                explain_verbose_binary_op(f, "-", &that.inputs, self.input_schema)
            }
            ExprType::Multiply => {
                explain_verbose_binary_op(f, "*", &that.inputs, self.input_schema)
            }
            ExprType::Divide => explain_verbose_binary_op(f, "/", &that.inputs, self.input_schema),
            ExprType::Modulus => explain_verbose_binary_op(f, "%", &that.inputs, self.input_schema),
            ExprType::Equal => explain_verbose_binary_op(f, "=", &that.inputs, self.input_schema),
            ExprType::NotEqual => {
                explain_verbose_binary_op(f, "<>", &that.inputs, self.input_schema)
            }
            ExprType::LessThan => {
                explain_verbose_binary_op(f, "<", &that.inputs, self.input_schema)
            }
            ExprType::LessThanOrEqual => {
                explain_verbose_binary_op(f, "<=", &that.inputs, self.input_schema)
            }
            ExprType::GreaterThan => {
                explain_verbose_binary_op(f, ">", &that.inputs, self.input_schema)
            }
            ExprType::GreaterThanOrEqual => {
                explain_verbose_binary_op(f, ">=", &that.inputs, self.input_schema)
            }
            ExprType::And => explain_verbose_binary_op(f, "AND", &that.inputs, self.input_schema),
            ExprType::Or => explain_verbose_binary_op(f, "OR", &that.inputs, self.input_schema),
            ExprType::BitwiseShiftLeft => {
                explain_verbose_binary_op(f, "<<", &that.inputs, self.input_schema)
            }
            ExprType::BitwiseShiftRight => {
                explain_verbose_binary_op(f, ">>", &that.inputs, self.input_schema)
            }
            ExprType::BitwiseAnd => {
                explain_verbose_binary_op(f, "&", &that.inputs, self.input_schema)
            }
            ExprType::BitwiseOr => {
                explain_verbose_binary_op(f, "|", &that.inputs, self.input_schema)
            }
            ExprType::BitwiseXor => {
                explain_verbose_binary_op(f, "#", &that.inputs, self.input_schema)
            }
            _ => {
                let func_name = format!("{:?}", that.func_type);
                let mut builder = f.debug_tuple(&func_name);
                that.inputs.iter().for_each(|child| {
                    builder.field(&ExprDisplay {
                        expr: child,
                        input_schema: self.input_schema,
                    });
                });
                builder.finish()
            }
        }
    }
}

fn explain_verbose_binary_op(
    f: &mut std::fmt::Formatter<'_>,
    op: &str,
    inputs: &[ExprImpl],
    input_schema: &Schema,
) -> std::fmt::Result {
    use std::fmt::Debug;

    assert_eq!(inputs.len(), 2);

    write!(f, "(")?;
    ExprDisplay {
        expr: &inputs[0],
        input_schema,
    }
    .fmt(f)?;
    write!(f, " {} ", op)?;
    ExprDisplay {
        expr: &inputs[1],
        input_schema,
    }
    .fmt(f)?;
    write!(f, ")")?;

    Ok(())
}

fn is_row_function(expr: &ExprImpl) -> bool {
    if let ExprImpl::FunctionCall(func) = expr {
        if func.get_expr_type() == ExprType::Row {
            return true;
        }
    }
    false
}
