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
//
use risingwave_common::error::{ErrorCode, Result};
use risingwave_sqlparser::ast::{BinaryOperator, Expr};

use crate::binder::Binder;
use crate::expr::{Expr as _, ExprType, FunctionCall};

impl Binder {
    pub(super) fn bind_binary_op(
        &mut self,
        left: Expr,
        op: BinaryOperator,
        right: Expr,
    ) -> Result<FunctionCall> {
        let bound_left = self.bind_expr(left)?;
        let bound_right = self.bind_expr(right)?;
        let func_type = match op {
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
            BinaryOperator::Like => ExprType::Like,
            _ => return Err(ErrorCode::NotImplementedError(format!("{:?}", op)).into()),
        };
        let desc = format!(
            "{:?} {:?} {:?}",
            bound_left.return_type(),
            op,
            bound_right.return_type(),
        );
        FunctionCall::new(func_type, vec![bound_left, bound_right])
            .ok_or_else(|| ErrorCode::NotImplementedError(desc).into())
    }
}
