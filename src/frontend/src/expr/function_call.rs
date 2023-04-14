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
use risingwave_common::catalog::Schema;
use risingwave_common::error::{ErrorCode, Result as RwResult, RwError};
use risingwave_common::types::DataType;
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_expr::vector_op::cast::literal_parsing;
use thiserror::Error;

use super::{cast_ok, infer_some_all, infer_type, CastContext, Expr, ExprImpl, Literal};
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

impl FunctionCall {
    /// Create a `FunctionCall` expr with the return type inferred from `func_type` and types of
    /// `inputs`.
    // The functions listed here are all variadic.  Type signatures of functions that take a fixed
    // number of arguments are checked
    // [elsewhere](crate::expr::type_inference::build_type_derive_map).
    pub fn new(func_type: ExprType, mut inputs: Vec<ExprImpl>) -> RwResult<Self> {
        let return_type = infer_type(func_type, &mut inputs)?;
        Ok(Self {
            func_type,
            return_type,
            inputs,
        })
    }

    /// Create a cast expr over `child` to `target` type in `allows` context.
    pub fn new_cast(
        mut child: ExprImpl,
        target: DataType,
        allows: CastContext,
    ) -> Result<ExprImpl, CastError> {
        if let ExprImpl::Parameter(expr) = &mut child && !expr.has_infer() {
            expr.cast_infer_type(target);
            return Ok(child);
        }
        if is_row_function(&child) {
            // Row function will have empty fields in Datatype::Struct at this point. Therefore,
            // we will need to take some special care to generate the cast types. For normal struct
            // types, they will be handled in `cast_ok`.
            return Self::cast_row_expr(child, target, allows);
        }
        if child.is_unknown() {
            // `is_unknown` makes sure `as_literal` and `as_utf8` will never panic.
            let literal = child.as_literal().unwrap();
            let datum = literal
                .get_data()
                .as_ref()
                .map(|scalar| {
                    let s = scalar.as_utf8();
                    literal_parsing(&target, s)
                })
                .transpose();
            if let Ok(datum) = datum {
                return Ok(Literal::new(datum, target).into());
            }
            // else when eager parsing fails, just proceed as normal.
            // Some callers are not ready to handle `'a'::int` error here.
        }
        let source = child.return_type();
        if source == target {
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
            Err(CastError(format!(
                "cannot cast type \"{}\" to \"{}\" in {:?} context",
                source, target, allows
            )))
        }
    }

    /// Cast a `ROW` expression to the target type. We intentionally disallow casting arbitrary
    /// expressions, like `ROW(1)::STRUCT<i INTEGER>` to `STRUCT<VARCHAR>`, although an integer
    /// is castible to VARCHAR. It's to simply the casting rules.
    fn cast_row_expr(
        expr: ExprImpl,
        target_type: DataType,
        allows: CastContext,
    ) -> Result<ExprImpl, CastError> {
        let func = *expr.into_function_call().unwrap();
        let (fields, field_names) = if let DataType::Struct(t) = &target_type {
            (t.fields.clone(), t.field_names.clone())
        } else {
            return Err(CastError(format!(
                "cannot cast type \"{}\" to \"{}\" in {:?} context",
                func.return_type(),
                target_type,
                allows
            )));
        };
        let (func_type, inputs, _) = func.decompose();
        match fields.len().cmp(&inputs.len()) {
            std::cmp::Ordering::Equal => {
                let inputs = inputs
                    .into_iter()
                    .zip_eq_fast(fields.to_vec())
                    .map(|(e, t)| Self::new_cast(e, t, allows))
                    .collect::<Result<Vec<_>, CastError>>()?;
                let return_type = DataType::new_struct(
                    inputs.iter().map(|i| i.return_type()).collect_vec(),
                    field_names,
                );
                Ok(FunctionCall::new_unchecked(func_type, inputs, return_type).into())
            }
            std::cmp::Ordering::Less => Err(CastError("Input has too few columns.".to_string())),
            std::cmp::Ordering::Greater => {
                Err(CastError("Input has too many columns.".to_string()))
            }
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

    pub fn new_binary_op_func(
        mut func_types: Vec<ExprType>,
        mut inputs: Vec<ExprImpl>,
    ) -> RwResult<ExprImpl> {
        let expr_type = func_types.remove(0);
        match expr_type {
            ExprType::Some | ExprType::All => {
                let return_type = infer_some_all(func_types, &mut inputs)?;

                if return_type != DataType::Boolean {
                    return Err(ErrorCode::BindError(format!(
                        "op ANY/ALL (array) requires operator to yield boolean, but got {:?}",
                        return_type
                    ))
                    .into());
                }

                Ok(FunctionCall::new_unchecked(expr_type, inputs, return_type).into())
            }
            ExprType::Not | ExprType::IsNotNull | ExprType::IsNull => Ok(FunctionCall::new(
                expr_type,
                vec![Self::new_binary_op_func(func_types, inputs)?],
            )?
            .into()),
            _ => Ok(FunctionCall::new(expr_type, inputs)?.into()),
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

    /// Refer to [`ExprType`] for details.
    pub fn is_pure(&self) -> bool {
        0 < self.func_type as i32 && self.func_type as i32 <= 600
    }

    /// Get a reference to the function call's inputs.
    pub fn inputs(&self) -> &[ExprImpl] {
        self.inputs.as_ref()
    }

    pub fn inputs_mut(&mut self) -> &mut [ExprImpl] {
        self.inputs.as_mut()
    }

    pub(super) fn from_expr_proto(
        function_call: &risingwave_pb::expr::FunctionCall,
        expr_type: ExprType,
        ret_type: DataType,
    ) -> RwResult<Self> {
        let inputs: Vec<_> = function_call
            .get_children()
            .iter()
            .map(ExprImpl::from_expr_proto)
            .try_collect()?;
        Ok(Self {
            func_type: expr_type,
            return_type: ret_type,
            inputs,
        })
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
            ExprType::Now | ExprType::Proctime => {
                write!(f, "{:?}", that.func_type)
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

pub fn is_row_function(expr: &ExprImpl) -> bool {
    if let ExprImpl::FunctionCall(func) = expr {
        if func.get_expr_type() == ExprType::Row {
            return true;
        }
    }
    false
}

#[derive(Debug, Error)]
#[error("{0}")]
pub struct CastError(String);

impl From<CastError> for ErrorCode {
    fn from(value: CastError) -> Self {
        ErrorCode::BindError(value.to_string())
    }
}

impl From<CastError> for RwError {
    fn from(value: CastError) -> Self {
        ErrorCode::from(value).into()
    }
}
