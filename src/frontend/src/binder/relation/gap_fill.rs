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
use risingwave_common::catalog::Field;
use risingwave_common::gap_fill::FillStrategy;
use risingwave_common::types::DataType;
use risingwave_sqlparser::ast::{Expr as AstExpr, FunctionArg, FunctionArgExpr, TableAlias};

use super::{Binder, Relation};
use crate::binder::BoundFillStrategy;
use crate::error::{ErrorCode, Result as RwResult};
use crate::expr::{Expr, ExprImpl, InputRef};

#[derive(Debug, Clone)]
pub struct BoundGapFill {
    pub input: Relation,
    pub time_col: InputRef,
    pub interval: ExprImpl,
    pub fill_strategies: Vec<BoundFillStrategy>,
}

impl Binder {
    pub(super) fn bind_gap_fill(
        &mut self,
        alias: Option<&TableAlias>,
        args: &[FunctionArg],
    ) -> RwResult<BoundGapFill> {
        if args.len() < 3 {
            return Err(ErrorCode::BindError(
                "GAP_FILL requires at least 3 arguments: input, time_col, interval".to_owned(),
            )
            .into());
        }

        let mut args_iter = args.iter();

        self.push_context();

        let (input, table_name) = self.bind_relation_by_function_arg(
            args_iter.next(),
            "The 1st arg of GAP_FILL should be a table name",
        )?;

        let time_col = self.bind_column_by_function_args(
            args_iter.next(),
            "The 2nd arg of GAP_FILL should be a column name",
        )?;

        if !matches!(
            time_col.data_type,
            DataType::Timestamp | DataType::Timestamptz
        ) {
            return Err(ErrorCode::BindError(
                "The 2nd arg of GAP_FILL should be a column of type timestamp or timestamptz"
                    .to_owned(),
            )
            .into());
        }

        let interval_arg = args_iter.next().unwrap();
        let interval_exprs = self.bind_function_arg(interval_arg)?;
        let interval = interval_exprs.into_iter().exactly_one().map_err(|_| {
            ErrorCode::BindError("The 3rd arg of GAP_FILL should be a single expression".to_owned())
        })?;
        if interval.return_type() != DataType::Interval {
            return Err(ErrorCode::BindError(
                "The 3rd arg of GAP_FILL should be an interval".to_owned(),
            )
            .into());
        }

        // Validate that the interval is not zero (only works for constant intervals)
        if let ExprImpl::Literal(literal) = &interval
            && let Some(risingwave_common::types::ScalarImpl::Interval(interval_value)) =
                literal.get_data()
            && interval_value.months() == 0
            && interval_value.days() == 0
            && interval_value.usecs() == 0
        {
            return Err(
                ErrorCode::BindError("The gap fill interval cannot be zero".to_owned()).into(),
            );
        }

        let mut fill_strategies = vec![];
        for arg in args_iter {
            let (strategy, target_col) =
                if let FunctionArg::Unnamed(FunctionArgExpr::Expr(AstExpr::Function(func))) = arg {
                    let name = func.name.0[0].real_value().to_ascii_lowercase();

                    let strategy = match name.as_str() {
                        "interpolate" => FillStrategy::Interpolate,
                        "locf" => FillStrategy::Locf,
                        "keepnull" => FillStrategy::Null,
                        _ => {
                            return Err(ErrorCode::BindError(format!(
                                "Unsupported fill strategy: {}",
                                name
                            ))
                            .into());
                        }
                    };

                    if func.arg_list.args.len() != 1 {
                        return Err(ErrorCode::BindError(format!(
                            "Fill strategy function {} expects exactly one argument",
                            name
                        ))
                        .into());
                    }

                    let arg_exprs = self.bind_function_arg(&func.arg_list.args[0])?;
                    let arg_expr = arg_exprs.into_iter().exactly_one().map_err(|_| {
                        ErrorCode::BindError(
                            "Fill strategy argument should be a single expression".to_owned(),
                        )
                    })?;

                    if let ExprImpl::InputRef(input_ref) = arg_expr {
                        // Check datatype for interpolate
                        if matches!(strategy, FillStrategy::Interpolate) {
                            let data_type = &input_ref.data_type;
                            if !data_type.is_numeric() || matches!(data_type, DataType::Serial) {
                                return Err(ErrorCode::BindError(format!(
                                    "INTERPOLATE only supports numeric types, got {}",
                                    data_type
                                ))
                                .into());
                            }
                        }
                        (strategy, *input_ref)
                    } else {
                        return Err(ErrorCode::BindError(
                            "Fill strategy argument must be a column reference".to_owned(),
                        )
                        .into());
                    }
                } else {
                    return Err(ErrorCode::BindError(
                        "Fill strategy must be a function call like LOCF(col)".to_owned(),
                    )
                    .into());
                };
            fill_strategies.push(BoundFillStrategy {
                strategy,
                target_col,
            });
        }

        let base_columns = std::mem::take(&mut self.context.columns);
        self.pop_context()?;

        let columns = base_columns
            .into_iter()
            .map(|c| (c.is_hidden, c.field))
            .collect::<Vec<(bool, Field)>>();

        let (_, table_name) = Self::resolve_schema_qualified_name(&self.db_name, &table_name)?;
        self.bind_table_to_context(columns, table_name, alias)?;

        Ok(BoundGapFill {
            input,
            time_col: *time_col,
            interval,
            fill_strategies,
        })
    }
}
