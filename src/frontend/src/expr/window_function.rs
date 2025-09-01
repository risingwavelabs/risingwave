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

use itertools::Itertools;
use risingwave_common::bail_not_implemented;
use risingwave_common::types::DataType;
use risingwave_expr::aggregate::AggType;
use risingwave_expr::window_function::{Frame, WindowFuncKind};

use super::{Expr, ExprImpl, OrderBy, RwResult};
use crate::error::{ErrorCode, RwError};
use crate::expr::infer_type;

/// A window function performs a calculation across a set of table rows that are somehow related to
/// the current row, according to the window spec `OVER (PARTITION BY .. ORDER BY ..)`.
/// One output row is calculated for each row in the input table.
///
/// Window functions are permitted only in the `SELECT` list and the `ORDER BY` clause of the query.
/// They are forbidden elsewhere, such as in `GROUP BY`, `HAVING` and `WHERE` clauses.
#[derive(Clone, Eq, PartialEq, Hash)]
pub struct WindowFunction {
    pub kind: WindowFuncKind,
    pub return_type: DataType,
    pub args: Vec<ExprImpl>,
    pub ignore_nulls: bool,
    pub partition_by: Vec<ExprImpl>,
    pub order_by: OrderBy,
    pub frame: Option<Frame>,
}

impl WindowFunction {
    /// Create a `WindowFunction` expr with the return type inferred from `func_type` and types of
    /// `inputs`.
    pub fn new(
        kind: WindowFuncKind,
        mut args: Vec<ExprImpl>,
        ignore_nulls: bool,
        partition_by: Vec<ExprImpl>,
        order_by: OrderBy,
        frame: Option<Frame>,
    ) -> RwResult<Self> {
        let return_type = Self::infer_return_type(&kind, &mut args)?;
        Ok(Self {
            kind,
            return_type,
            args,
            ignore_nulls,
            partition_by,
            order_by,
            frame,
        })
    }

    fn infer_return_type(kind: &WindowFuncKind, args: &mut [ExprImpl]) -> RwResult<DataType> {
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
                if !offset.is_const() {
                    bail_not_implemented!(
                        "non-const `offset` of `{kind}` function is not supported yet"
                    );
                }
                Ok(value.return_type())
            }
            (Lag | Lead, [_value, _offset, _default]) => {
                bail_not_implemented!(
                    "`{kind}` window function with `default` argument is not supported yet"
                );
            }

            (Aggregate(agg_type), args) => Ok(match agg_type {
                AggType::Builtin(kind) => infer_type((*kind).into(), args)?,
                AggType::UserDefined(udf) => udf.return_type.as_ref().unwrap().into(),
                AggType::WrapScalar(expr) => expr.return_type.as_ref().unwrap().into(),
            }),

            (_, args) => {
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
                .field("ignore_nulls", &self.ignore_nulls)
                .field("partition_by", &self.partition_by)
                .field("order_by", &format_args!("{}", self.order_by));
            if let Some(frame) = &self.frame {
                builder.field("frame", &format_args!("{}", frame));
            } else {
                builder.field("frame", &"None".to_owned());
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

    fn try_to_expr_proto(&self) -> Result<risingwave_pb::expr::ExprNode, String> {
        Err("Window function should not be converted to ExprNode".to_owned())
    }
}
