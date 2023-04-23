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
use risingwave_common::error::{ErrorCode, RwError};
use risingwave_common::types::DataType;
use risingwave_expr::function::window::{Frame, WindowFuncKind};

use super::{AggCall, Expr, ExprImpl, OrderBy, RwResult};

/// A window function performs a calculation across a set of table rows that are somehow related to
/// the current row, according to the window spec `OVER (PARTITION BY .. ORDER BY ..)`.
/// One output row is calculated for each row in the input table.
///
/// Window functions are permitted only in the `SELECT` list and the `ORDER BY` clause of the query.
/// They are forbidden elsewhere, such as in `GROUP BY`, `HAVING` and `WHERE` clauses.
#[derive(Clone, Eq, PartialEq, Hash)]
pub struct WindowFunction {
    pub kind: WindowFuncKind,
    pub args: Vec<ExprImpl>,
    pub return_type: DataType,
    pub partition_by: Vec<ExprImpl>,
    pub order_by: OrderBy,
    pub frame: Option<Frame>,
}

impl WindowFunction {
    /// Create a `WindowFunction` expr with the return type inferred from `func_type` and types of
    /// `inputs`.
    pub fn new(
        kind: WindowFuncKind,
        partition_by: Vec<ExprImpl>,
        order_by: OrderBy,
        args: Vec<ExprImpl>,
        frame: Option<Frame>,
    ) -> RwResult<Self> {
        let return_type = Self::infer_return_type(kind, &args)?;
        Ok(Self {
            kind,
            args,
            return_type,
            partition_by,
            order_by,
            frame,
        })
    }

    fn infer_return_type(kind: WindowFuncKind, args: &[ExprImpl]) -> RwResult<DataType> {
        use WindowFuncKind::*;
        match (kind, args) {
            (RowNumber, []) => Ok(DataType::Int64),
            (Rank, []) => Ok(DataType::Int64),
            (DenseRank, []) => Ok(DataType::Int64),

            (Lag | Lead, [value]) => Ok(value.return_type()),
            (Lag | Lead, [value, offset]) => {
                if !offset.return_type().is_int() {
                    return Err(ErrorCode::InvalidInputSyntax(format!(
                        "the `offset` of `{kind}` function should be integer"
                    ))
                    .into());
                }
                if offset.as_literal().is_none() {
                    return Err(ErrorCode::NotImplemented(
                        format!("non-const `offset` of `{kind}` function is not supported yet"),
                        None.into(),
                    )
                    .into());
                }
                Ok(value.return_type())
            }
            (Lag | Lead, [_value, _offset, _default]) => {
                Err(RwError::from(ErrorCode::NotImplemented(
                    format!(
                        "`{kind}` window function with `default` argument is not supported yet"
                    ),
                    None.into(),
                )))
            }

            (Aggregate(agg_kind), args) => {
                let arg_types = args.iter().map(ExprImpl::return_type).collect::<Vec<_>>();
                AggCall::infer_return_type(agg_kind, &arg_types)
            }

            _ => {
                let args = args
                    .iter()
                    .map(|e| format!("{}", e.return_type()))
                    .join(", ");
                Err(RwError::from(ErrorCode::InvalidInputSyntax(format!(
                    "Invalid window function: {kind}({args})"
                ))))
            }
        }
    }
}

impl std::fmt::Debug for WindowFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if f.alternate() {
            let mut builder = f.debug_struct("WindowFunction");
            builder
                .field("kind", &self.kind)
                .field("return_type", &self.return_type)
                .field("args", &self.args)
                .field("partition_by", &self.partition_by)
                .field("order_by", &format_args!("{}", self.order_by));
            if let Some(frame) = &self.frame {
                builder.field("frame", &format_args!("{}", frame));
            } else {
                builder.field("frame", &"None".to_string());
            }
            builder.finish()
        } else {
            write!(f, "{}() OVER(", self.kind)?;

            let mut delim = "";
            if !self.partition_by.is_empty() {
                delim = " ";
                write!(
                    f,
                    "PARTITION BY {:?}",
                    self.partition_by.iter().format(", ")
                )?;
            }
            if !self.order_by.sort_exprs.is_empty() {
                write!(f, "{delim}{}", self.order_by)?;
            }
            if let Some(frame) = &self.frame {
                write!(f, "{delim}{}", frame)?;
            }
            f.write_str(")")?;

            Ok(())
        }
    }
}

impl Expr for WindowFunction {
    fn return_type(&self) -> DataType {
        self.return_type.clone()
    }

    fn to_expr_proto(&self) -> risingwave_pb::expr::ExprNode {
        unreachable!("Window function should not be converted to ExprNode")
    }
}
