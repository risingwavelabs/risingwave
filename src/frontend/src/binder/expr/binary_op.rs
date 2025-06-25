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

use risingwave_common::bail_not_implemented;
use risingwave_common::types::{DataType, JsonbVal};
use risingwave_sqlparser::ast::{BinaryOperator, Expr};

use crate::binder::Binder;
use crate::error::{ErrorCode, Result};
use crate::expr::{Expr as _, ExprImpl, ExprType, FunctionCall};

impl Binder {
    pub(super) fn bind_binary_op(
        &mut self,
        left: Expr,
        op: BinaryOperator,
        mut right: Expr,
    ) -> Result<ExprImpl> {
        let bound_left = self.bind_expr_inner(left)?;

        let mut func_types = vec![];

        right = match right {
            Expr::SomeOp(expr) => {
                func_types.push(ExprType::Some);
                *expr
            }
            Expr::AllOp(expr) => {
                func_types.push(ExprType::All);
                *expr
            }
            right => right,
        };

        let bound_right = self.bind_expr_inner(right)?;

        if let BinaryOperator::Custom(name) = &op
            && matches!(name.as_str(), "@?" | "@@")
        {
            // jsonb @? jsonpath => jsonb_path_exists(jsonb, jsonpath, '{}', silent => true)
            // jsonb @@ jsonpath => jsonb_path_match(jsonb, jsonpath, '{}', silent => true)
            return Ok(FunctionCall::new_unchecked(
                match name.as_str() {
                    "@?" => ExprType::JsonbPathExists,
                    "@@" => ExprType::JsonbPathMatch,
                    _ => unreachable!(),
                },
                vec![
                    bound_left,
                    bound_right,
                    ExprImpl::literal_jsonb(JsonbVal::empty_object()), // vars
                    ExprImpl::literal_bool(true),                      // silent
                ],
                DataType::Boolean,
            )
            .into());
        }

        func_types.extend(Self::resolve_binary_operator(
            op,
            &bound_left,
            &bound_right,
        )?);

        FunctionCall::new_binary_op_func(func_types, vec![bound_left, bound_right])
    }

    fn resolve_binary_operator(
        op: BinaryOperator,
        bound_left: &ExprImpl,
        bound_right: &ExprImpl,
    ) -> Result<Vec<ExprType>> {
        let mut func_types = vec![];
        let final_type = match op {
            BinaryOperator::Plus => ExprType::Add,
            BinaryOperator::Minus => ExprType::Subtract,
            BinaryOperator::Multiply => ExprType::Multiply,
            BinaryOperator::Divide => ExprType::Divide,
            BinaryOperator::Modulo => ExprType::Modulus,
            BinaryOperator::NotEq => ExprType::NotEqual,
            BinaryOperator::Eq => ExprType::Equal,
            BinaryOperator::Lt => ExprType::LessThan,
            BinaryOperator::LtEq => ExprType::LessThanOrEqual,
            BinaryOperator::Gt => ExprType::GreaterThan,
            BinaryOperator::GtEq => ExprType::GreaterThanOrEqual,
            BinaryOperator::And => ExprType::And,
            BinaryOperator::Or => ExprType::Or,
            BinaryOperator::BitwiseOr => ExprType::BitwiseOr,
            BinaryOperator::BitwiseAnd => ExprType::BitwiseAnd,
            BinaryOperator::BitwiseXor => ExprType::Pow,
            BinaryOperator::PGBitwiseXor => ExprType::BitwiseXor,
            BinaryOperator::Arrow => ExprType::JsonbAccess,
            BinaryOperator::Custom(name) if name == "@>" => {
                let left_type = (!bound_left.is_untyped()).then(|| bound_left.return_type());
                let right_type = (!bound_right.is_untyped()).then(|| bound_right.return_type());
                match (left_type, right_type) {
                    (Some(DataType::List { .. }), Some(DataType::List { .. }))
                    | (Some(DataType::List { .. }), None)
                    | (None, Some(DataType::List { .. })) => ExprType::ArrayContains,
                    (Some(DataType::Jsonb), Some(DataType::Jsonb))
                    | (Some(DataType::Jsonb), None)
                    | (None, Some(DataType::Jsonb)) => ExprType::JsonbContains,
                    (left, right) => {
                        return Err(ErrorCode::BindError(format!(
                            "operator does not exist: {} @> {}",
                            left.map_or_else(|| String::from("unknown"), |x| x.to_string()),
                            right.map_or_else(|| String::from("unknown"), |x| x.to_string()),
                        ))
                        .into());
                    }
                }
            }
            BinaryOperator::Custom(name) if name == "<@" => {
                let left_type = (!bound_left.is_untyped()).then(|| bound_left.return_type());
                let right_type = (!bound_right.is_untyped()).then(|| bound_right.return_type());
                match (left_type, right_type) {
                    (Some(DataType::List { .. }), Some(DataType::List { .. }))
                    | (Some(DataType::List { .. }), None)
                    | (None, Some(DataType::List { .. })) => ExprType::ArrayContained,
                    (Some(DataType::Jsonb), Some(DataType::Jsonb))
                    | (Some(DataType::Jsonb), None)
                    | (None, Some(DataType::Jsonb)) => ExprType::JsonbContained,
                    (left, right) => {
                        return Err(ErrorCode::BindError(format!(
                            "operator does not exist: {} <@ {}",
                            left.map_or_else(|| String::from("unknown"), |x| x.to_string()),
                            right.map_or_else(|| String::from("unknown"), |x| x.to_string()),
                        ))
                        .into());
                    }
                }
            }
            BinaryOperator::Custom(name) if name == "||" => {
                let left_type = (!bound_left.is_untyped()).then(|| bound_left.return_type());
                let right_type = (!bound_right.is_untyped()).then(|| bound_right.return_type());
                match (left_type, right_type) {
                    // array concatenation
                    (Some(DataType::List { .. }), Some(DataType::List { .. }))
                    | (Some(DataType::List { .. }), None)
                    | (None, Some(DataType::List { .. })) => ExprType::ArrayCat,
                    (Some(DataType::List { .. }), Some(_)) => ExprType::ArrayAppend,
                    (Some(_), Some(DataType::List { .. })) => ExprType::ArrayPrepend,

                    // string concatenation
                    (Some(DataType::Varchar), _) | (_, Some(DataType::Varchar)) => {
                        ExprType::ConcatOp
                    }

                    (Some(DataType::Jsonb), Some(DataType::Jsonb))
                    | (Some(DataType::Jsonb), None)
                    | (None, Some(DataType::Jsonb)) => ExprType::JsonbConcat,

                    // bytea (and varbit, tsvector, tsquery)
                    (Some(DataType::Bytea), Some(DataType::Bytea))
                    | (Some(DataType::Bytea), None)
                    | (None, Some(DataType::Bytea)) => ExprType::ByteaConcatOp,

                    // string concatenation
                    (None, _) | (_, None) => ExprType::ConcatOp,

                    // invalid
                    (Some(left_type), Some(right_type)) => {
                        return Err(ErrorCode::BindError(format!(
                            "operator does not exist: {} || {}",
                            left_type, right_type
                        ))
                        .into());
                    }
                }
            }
            BinaryOperator::PGRegexMatch => ExprType::RegexpEq,
            BinaryOperator::Custom(name) => match name.as_str() {
                "<<" => ExprType::BitwiseShiftLeft,
                ">>" => ExprType::BitwiseShiftRight,
                "^@" => ExprType::StartsWith,
                "~~" => ExprType::Like,
                "~~*" => ExprType::ILike,
                "!~" => {
                    func_types.push(ExprType::Not);
                    ExprType::RegexpEq
                }
                "!~~" => {
                    func_types.push(ExprType::Not);
                    ExprType::Like
                }
                "!~~*" => {
                    func_types.push(ExprType::Not);
                    ExprType::ILike
                }
                "->>" => ExprType::JsonbAccessStr,
                "#-" => ExprType::JsonbDeletePath,
                "#>" => ExprType::JsonbExtractPathVariadic,
                "#>>" => ExprType::JsonbExtractPathTextVariadic,
                "?" => ExprType::JsonbExists,
                "?|" => ExprType::JsonbExistsAny,
                "?&" => ExprType::JsonbExistsAll,
                _ => bail_not_implemented!(issue = 112, "binary op: {:?}", name),
            },
            _ => bail_not_implemented!(issue = 112, "binary op: {:?}", op),
        };
        func_types.push(final_type);
        Ok(func_types)
    }
}
