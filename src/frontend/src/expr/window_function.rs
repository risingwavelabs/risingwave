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

use std::str::FromStr;

use risingwave_common::error::ErrorCode;
use risingwave_common::types::DataType;

use super::{Expr, ExprImpl, Result};
use crate::optimizer::property::Direction;

/// A window function performs a calculation across a set of table rows that are somehow related to
/// the current row, according to the window spec `OVER (PARTITION BY .. ORDER BY ..)`.
/// One output row is calculated for each row in the input table.
///
/// Window functions are permitted only in the `SELECT` list and the `ORDER BY` clause of the query.
/// They are forbidden elsewhere, such as in `GROUP BY`, `HAVING` and `WHERE` clauses.
#[derive(Clone, Eq, PartialEq, Hash)]
pub struct WindowFunction {
    pub args: Vec<ExprImpl>,
    pub return_type: DataType,
    pub function_type: WindowFunctionType,
    pub partition_by: Vec<ExprImpl>,
    pub order_by: Vec<(ExprImpl, Direction)>,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum WindowFunctionType {
    RowNumber,
    Rank,
    DenseRank,
}

impl WindowFunctionType {
    pub fn name(&self) -> &str {
        match self {
            WindowFunctionType::RowNumber => "row_number",
            WindowFunctionType::Rank => "rank",
            WindowFunctionType::DenseRank => "dense_rank",
        }
    }
}

impl FromStr for WindowFunctionType {
    type Err = ErrorCode;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        if s.eq_ignore_ascii_case("row_number") {
            Ok(WindowFunctionType::RowNumber)
        } else if s.eq_ignore_ascii_case("rank") {
            Ok(WindowFunctionType::Rank)
        } else if s.eq_ignore_ascii_case("dense_rank") {
            Ok(WindowFunctionType::DenseRank)
        } else {
            Err(ErrorCode::NotImplemented(
                format!("unknown table function kind: {s}"),
                None.into(),
            ))
        }
    }
}

impl WindowFunction {
    /// Create a `WindowFunction` expr with the return type inferred from `func_type` and types of
    /// `inputs`.
    pub fn new(
        function_type: WindowFunctionType,
        partition_by: Vec<ExprImpl>,
        order_by: Vec<(ExprImpl, Direction)>,
        args: Vec<ExprImpl>,
    ) -> Result<Self> {
        if !args.is_empty() {
            return Err(ErrorCode::BindError(format!(
                "the length of args of {} function should be 0",
                function_type.name()
            ))
            .into());
        }

        Ok(Self {
            args,
            return_type: DataType::Int64,
            function_type,
            partition_by,
            order_by,
        })
    }
}

impl std::fmt::Debug for WindowFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if f.alternate() {
            f.debug_struct("WindowFunction")
                .field("function_type", &self.function_type)
                .field("return_type", &self.return_type)
                .field("args", &self.args)
                .field("partition_by", &self.partition_by)
                .field("order_by", &self.order_by)
                .finish()
        } else {
            let func_name = format!("{:?}", self.function_type);
            let mut builder = f.debug_tuple(&func_name);
            self.args.iter().for_each(|child| {
                builder.field(child);
            });
            builder.finish()?;

            f.write_str("OVER(")?;

            let mut delim = "";
            if !self.partition_by.is_empty() {
                delim = " ";
                let mut builder = f.debug_tuple("PARTITION BY");
                self.partition_by.iter().for_each(|child| {
                    builder.field(child);
                });
                builder.finish()?;
            }
            if !self.order_by.is_empty() {
                f.write_str(delim)?;
                let mut builder = f.debug_tuple("ORDER BY");
                self.order_by.iter().for_each(|child| {
                    builder.field(child);
                });
                builder.finish()?;
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
