// Copyright 2024 RisingWave Labs
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

use std::collections::HashMap;
use std::iter::once;
use std::str::FromStr;
use std::sync::{Arc, LazyLock};

use bk_tree::{metrics, BKTree};
use itertools::Itertools;
use risingwave_common::array::ListValue;
use risingwave_common::catalog::{INFORMATION_SCHEMA_SCHEMA_NAME, PG_CATALOG_SCHEMA_NAME};
use risingwave_common::session_config::USER_NAME_WILD_CARD;
use risingwave_common::types::{data_types, DataType, ScalarImpl, Timestamptz};
use risingwave_common::{bail_not_implemented, current_cluster_version, must_match, no_function};
use risingwave_expr::aggregate::{agg_kinds, AggKind};
use risingwave_expr::window_function::{
    Frame, FrameBound, FrameBounds, FrameExclusion, RangeFrameBounds, RangeFrameOffset,
    RowsFrameBounds, SessionFrameBounds, SessionFrameGap, WindowFuncKind,
};
use risingwave_sqlparser::ast::{
    self, Function, FunctionArg, FunctionArgExpr, Ident, WindowFrameBound, WindowFrameBounds,
    WindowFrameExclusion, WindowFrameUnits, WindowSpec,
};
use risingwave_sqlparser::parser::ParserError;
use thiserror_ext::AsReport;

use crate::binder::bind_context::Clause;
use crate::binder::{Binder, UdfContext};
use crate::catalog::function_catalog::FunctionCatalog;
use crate::error::{ErrorCode, Result, RwError};
use crate::expr::{
    AggCall, CastContext, Expr, ExprImpl, ExprType, FunctionCall, FunctionCallWithLambda, Literal,
    Now, OrderBy, TableFunction, TableFunctionType, UserDefinedFunction, WindowFunction,
};
use crate::utils::Condition;

// Defines system functions that without args, ref: https://www.postgresql.org/docs/current/functions-info.html
pub const SYS_FUNCTION_WITHOUT_ARGS: &[&str] = &[
    "session_user",
    "user",
    "current_user",
    "current_role",
    "current_schema",
    "current_timestamp",
];

/// The global max calling depth for the global counter in `udf_context`
/// To reduce the chance that the current running rw thread
/// be killed by os, the current allowance depth of calling
/// stack is set to `16`.
const SQL_UDF_MAX_CALLING_DEPTH: u32 = 16;

impl Binder {
    pub(in crate::binder) fn bind_function(&mut self, f: Function) -> Result<ExprImpl> {
        let function_name = match f.name.0.as_slice() {
            [name] => name.real_value(),
            [schema, name] => {
                let schema_name = schema.real_value();
                if schema_name == PG_CATALOG_SCHEMA_NAME {
                    // pg_catalog is always effectively part of the search path, so we can always bind the function.
                    // Ref: https://www.postgresql.org/docs/current/ddl-schemas.html#DDL-SCHEMAS-CATALOG
                    name.real_value()
                } else if schema_name == INFORMATION_SCHEMA_SCHEMA_NAME {
                    // definition of information_schema: https://github.com/postgres/postgres/blob/e0b2eed047df9045664da6f724cb42c10f8b12f0/src/backend/catalog/information_schema.sql
                    //
                    // FIXME: handle schema correctly, so that the functions are hidden if the schema is not in the search path.
                    let function_name = name.real_value();
                    if function_name != "_pg_expandarray" {
                        bail_not_implemented!(
                            issue = 12422,
                            "Unsupported function name under schema: {}",
                            schema_name
                        );
                    }
                    function_name
                } else {
                    bail_not_implemented!(
                        issue = 12422,
                        "Unsupported function name under schema: {}",
                        schema_name
                    );
                }
            }
            _ => bail_not_implemented!(issue = 112, "qualified function {}", f.name),
        };

        // FIXME: This is a hack to support [Bytebase queries](https://github.com/TennyZhuang/bytebase/blob/4a26f7c62b80e86e58ad2f77063138dc2f420623/backend/plugin/db/pg/sync.go#L549).
        // Bytebase widely used the pattern like `obj_description(format('%s.%s',
        // quote_ident(idx.schemaname), quote_ident(idx.indexname))::regclass) AS comment` to
        // retrieve object comment, however we don't support casting a non-literal expression to
        // regclass. We just hack the `obj_description` and `col_description` here, to disable it to
        // bind its arguments.
        if function_name == "obj_description" || function_name == "col_description" {
            return Ok(ExprImpl::literal_varchar("".to_string()));
        }
        if function_name == "array_transform" {
            // For type inference, we need to bind the array type first.
            return self.bind_array_transform(f);
        }

        let mut inputs: Vec<_> = f
            .args
            .iter()
            .map(|arg| self.bind_function_arg(arg.clone()))
            .flatten_ok()
            .try_collect()?;

        // user defined function
        // TODO: resolve schema name https://github.com/risingwavelabs/risingwave/issues/12422
        if let Ok(schema) = self.first_valid_schema()
            && let Some(func) = schema.get_function_by_name_inputs(&function_name, &mut inputs)
        {
            use crate::catalog::function_catalog::FunctionKind::*;

            if func.language == "sql" {
                if func.body.is_none() {
                    return Err(ErrorCode::InvalidInputSyntax(
                        "`body` must exist for sql udf".to_string(),
                    )
                    .into());
                }

                // This represents the current user defined function is `language sql`
                let parse_result = risingwave_sqlparser::parser::Parser::parse_sql(
                    func.body.as_ref().unwrap().as_str(),
                );
                if let Err(ParserError::ParserError(err)) | Err(ParserError::TokenizerError(err)) =
                    parse_result
                {
                    // Here we just return the original parse error message
                    return Err(ErrorCode::InvalidInputSyntax(err).into());
                }

                debug_assert!(parse_result.is_ok());

                // We can safely unwrap here
                let ast = parse_result.unwrap();

                // Stash the current `udf_context`
                // Note that the `udf_context` may be empty,
                // if the current binding is the root (top-most) sql udf.
                // In this case the empty context will be stashed
                // and restored later, no need to maintain other flags.
                let stashed_udf_context = self.udf_context.get_context();

                // The actual inline logic for sql udf
                // Note that we will always create new udf context for each sql udf
                let Ok(context) = UdfContext::create_udf_context(&f.args, &Arc::clone(func)) else {
                    return Err(ErrorCode::InvalidInputSyntax(
                        "failed to create the `udf_context`, please recheck your function definition and syntax".to_string()
                    )
                    .into());
                };

                let mut udf_context = HashMap::new();
                for (c, e) in context {
                    // Note that we need to bind the args before actual delve in the function body
                    // This will update the context in the subsequent inner calling function
                    // e.g.,
                    // - create function print(INT) returns int language sql as 'select $1';
                    // - create function print_add_one(INT) returns int language sql as 'select print($1 + 1)';
                    // - select print_add_one(1); # The result should be 2 instead of 1.
                    // Without the pre-binding here, the ($1 + 1) will not be correctly populated,
                    // causing the result to always be 1.
                    let Ok(e) = self.bind_expr(e) else {
                        return Err(ErrorCode::BindError(
                            "failed to bind the argument, please recheck the syntax".to_string(),
                        )
                        .into());
                    };
                    udf_context.insert(c, e);
                }
                self.udf_context.update_context(udf_context);

                // Check for potential recursive calling
                if self.udf_context.global_count() >= SQL_UDF_MAX_CALLING_DEPTH {
                    return Err(ErrorCode::BindError(format!(
                        "function {} calling stack depth limit exceeded",
                        &function_name
                    ))
                    .into());
                } else {
                    // Update the status for the global counter
                    self.udf_context.incr_global_count();
                }

                if let Ok(expr) = UdfContext::extract_udf_expression(ast) {
                    let bind_result = self.bind_expr(expr);

                    // We should properly decrement global count after a successful binding
                    // Since the subsequent probe operation in `bind_column` or
                    // `bind_parameter` relies on global counting
                    self.udf_context.decr_global_count();

                    // Restore context information for subsequent binding
                    self.udf_context.update_context(stashed_udf_context);

                    return bind_result;
                } else {
                    return Err(ErrorCode::InvalidInputSyntax(
                        "failed to parse the input query and extract the udf expression,
                        please recheck the syntax"
                            .to_string(),
                    )
                    .into());
                }
            } else {
                match &func.kind {
                    Scalar { .. } => {
                        return Ok(UserDefinedFunction::new(func.clone(), inputs).into())
                    }
                    Table { .. } => {
                        self.ensure_table_function_allowed()?;
                        return Ok(TableFunction::new_user_defined(func.clone(), inputs).into());
                    }
                    Aggregate => {
                        return self.bind_agg(f, AggKind::UserDefined, Some(func.clone()));
                    }
                }
            }
        }

        // agg calls
        if f.over.is_none()
            && let Ok(kind) = function_name.parse()
        {
            return self.bind_agg(f, kind, None);
        }

        if f.distinct || !f.order_by.is_empty() || f.filter.is_some() {
            return Err(ErrorCode::InvalidInputSyntax(format!(
                    "DISTINCT, ORDER BY or FILTER is only allowed in aggregation functions, but `{}` is not an aggregation function", function_name
                )
                )
                .into());
        }

        // window function
        let window_func_kind = WindowFuncKind::from_str(function_name.as_str());
        if let Ok(kind) = window_func_kind {
            if let Some(window_spec) = f.over {
                return self.bind_window_function(kind, inputs, window_spec);
            }
            return Err(ErrorCode::InvalidInputSyntax(format!(
                "Window function `{}` must have OVER clause",
                function_name
            ))
            .into());
        } else if f.over.is_some() {
            bail_not_implemented!(
                issue = 8961,
                "Unrecognized window function: {}",
                function_name
            );
        }

        // table function
        if let Ok(function_type) = TableFunctionType::from_str(function_name.as_str()) {
            self.ensure_table_function_allowed()?;
            return Ok(TableFunction::new(function_type, inputs)?.into());
        }

        self.bind_builtin_scalar_function(function_name.as_str(), inputs, f.variadic)
    }

    fn bind_array_transform(&mut self, f: Function) -> Result<ExprImpl> {
        let [array, lambda] = <[FunctionArg; 2]>::try_from(f.args).map_err(|args| -> RwError {
            ErrorCode::BindError(format!(
                "`array_transform` expect two inputs `array` and `lambda`, but {} were given",
                args.len()
            ))
            .into()
        })?;

        let bound_array = self.bind_function_arg(array)?;
        let [bound_array] = <[ExprImpl; 1]>::try_from(bound_array).map_err(|bound_array| -> RwError {
            ErrorCode::BindError(format!("The `array` argument for `array_transform` should be bound to one argument, but {} were got", bound_array.len()))
                .into()
        })?;

        let inner_ty = match bound_array.return_type() {
            DataType::List(ty) => *ty,
            real_type => {
                return Err(ErrorCode::BindError(format!(
                "The `array` argument for `array_transform` should be an array, but {} were got",
                real_type
            ))
                .into())
            }
        };

        let ast::FunctionArgExpr::Expr(ast::Expr::LambdaFunction {
            args: lambda_args,
            body: lambda_body,
        }) = lambda.get_expr()
        else {
            return Err(ErrorCode::BindError(
                "The `lambda` argument for `array_transform` should be a lambda function"
                    .to_string(),
            )
            .into());
        };

        let [lambda_arg] = <[Ident; 1]>::try_from(lambda_args).map_err(|args| -> RwError {
            ErrorCode::BindError(format!(
                "The `lambda` argument for `array_transform` should be a lambda function with one argument, but {} were given",
                args.len()
            ))
            .into()
        })?;

        let bound_lambda = self.bind_unary_lambda_function(inner_ty, lambda_arg, *lambda_body)?;

        let lambda_ret_type = bound_lambda.return_type();
        let transform_ret_type = DataType::List(Box::new(lambda_ret_type));

        Ok(ExprImpl::FunctionCallWithLambda(Box::new(
            FunctionCallWithLambda::new_unchecked(
                ExprType::ArrayTransform,
                vec![bound_array],
                bound_lambda,
                transform_ret_type,
            ),
        )))
    }

    fn bind_unary_lambda_function(
        &mut self,
        input_ty: DataType,
        arg: Ident,
        body: ast::Expr,
    ) -> Result<ExprImpl> {
        let lambda_args = HashMap::from([(arg.real_value(), (0usize, input_ty))]);
        let orig_lambda_args = self.context.lambda_args.replace(lambda_args);
        let body = self.bind_expr_inner(body)?;
        self.context.lambda_args = orig_lambda_args;

        Ok(body)
    }

    pub(super) fn bind_agg(
        &mut self,
        f: Function,
        kind: AggKind,
        user_defined: Option<Arc<FunctionCatalog>>,
    ) -> Result<ExprImpl> {
        self.ensure_aggregate_allowed()?;

        let distinct = f.distinct;
        let filter_expr = f.filter.clone();

        let (direct_args, args, order_by) = if matches!(kind, agg_kinds::ordered_set!()) {
            self.bind_ordered_set_agg(f, kind)?
        } else {
            self.bind_normal_agg(f, kind)?
        };

        let filter = match filter_expr {
            Some(filter) => {
                let mut clause = Some(Clause::Filter);
                std::mem::swap(&mut self.context.clause, &mut clause);
                let expr = self
                    .bind_expr_inner(*filter)
                    .and_then(|expr| expr.enforce_bool_clause("FILTER"))?;
                self.context.clause = clause;
                if expr.has_subquery() {
                    bail_not_implemented!("subquery in filter clause");
                }
                if expr.has_agg_call() {
                    bail_not_implemented!("aggregation function in filter clause");
                }
                if expr.has_table_function() {
                    bail_not_implemented!("table function in filter clause");
                }
                Condition::with_expr(expr)
            }
            None => Condition::true_cond(),
        };

        if let Some(user_defined) = user_defined {
            Ok(AggCall::new_user_defined(
                args,
                distinct,
                order_by,
                filter,
                direct_args,
                user_defined,
            )?
            .into())
        } else {
            Ok(ExprImpl::AggCall(Box::new(AggCall::new(
                kind,
                args,
                distinct,
                order_by,
                filter,
                direct_args,
            )?)))
        }
    }

    fn bind_ordered_set_agg(
        &mut self,
        f: Function,
        kind: AggKind,
    ) -> Result<(Vec<Literal>, Vec<ExprImpl>, OrderBy)> {
        // Syntax:
        // aggregate_name ( [ expression [ , ... ] ] ) WITHIN GROUP ( order_by_clause ) [ FILTER
        // ( WHERE filter_clause ) ]

        assert!(matches!(kind, agg_kinds::ordered_set!()));

        if !f.order_by.is_empty() {
            return Err(ErrorCode::InvalidInputSyntax(format!(
                "ORDER BY is not allowed for ordered-set aggregation `{}`",
                kind
            ))
            .into());
        }
        if f.distinct {
            return Err(ErrorCode::InvalidInputSyntax(format!(
                "DISTINCT is not allowed for ordered-set aggregation `{}`",
                kind
            ))
            .into());
        }

        let within_group = *f.within_group.ok_or_else(|| {
            ErrorCode::InvalidInputSyntax(format!(
                "WITHIN GROUP is expected for ordered-set aggregation `{}`",
                kind
            ))
        })?;

        let mut direct_args: Vec<_> = f
            .args
            .into_iter()
            .map(|arg| self.bind_function_arg(arg))
            .flatten_ok()
            .try_collect()?;
        let mut args =
            self.bind_function_expr_arg(FunctionArgExpr::Expr(within_group.expr.clone()))?;
        let order_by = OrderBy::new(vec![self.bind_order_by_expr(within_group)?]);

        // check signature and do implicit cast
        match (kind, direct_args.as_mut_slice(), args.as_mut_slice()) {
            (AggKind::PercentileCont | AggKind::PercentileDisc, [fraction], [arg]) => {
                if fraction.cast_implicit_mut(DataType::Float64).is_err() {
                    return Err(ErrorCode::InvalidInputSyntax(format!(
                        "direct arg in `{}` must be castable to float64",
                        kind
                    ))
                    .into());
                }

                let Some(Ok(fraction_datum)) = fraction.try_fold_const() else {
                    bail_not_implemented!(
                        issue = 14079,
                        "variable as direct argument of ordered-set aggregate",
                    );
                };

                if let Some(ref fraction_value) = fraction_datum
                    && !(0.0..=1.0).contains(&fraction_value.as_float64().0)
                {
                    return Err(ErrorCode::InvalidInputSyntax(format!(
                        "direct arg in `{}` must between 0.0 and 1.0",
                        kind
                    ))
                    .into());
                }
                // note that the fraction can be NULL
                *fraction = Literal::new(fraction_datum, DataType::Float64).into();

                if kind == AggKind::PercentileCont {
                    arg.cast_implicit_mut(DataType::Float64).map_err(|_| {
                        ErrorCode::InvalidInputSyntax(format!(
                            "arg in `{}` must be castable to float64",
                            kind
                        ))
                    })?;
                }
            }
            (AggKind::Mode, [], [_arg]) => {}
            _ => {
                return Err(ErrorCode::InvalidInputSyntax(format!(
                    "invalid direct args or within group argument for `{}` aggregation",
                    kind
                ))
                .into())
            }
        }

        Ok((
            direct_args
                .into_iter()
                .map(|arg| *arg.into_literal().unwrap())
                .collect(),
            args,
            order_by,
        ))
    }

    fn bind_normal_agg(
        &mut self,
        f: Function,
        kind: AggKind,
    ) -> Result<(Vec<Literal>, Vec<ExprImpl>, OrderBy)> {
        // Syntax:
        // aggregate_name (expression [ , ... ] [ order_by_clause ] ) [ FILTER ( WHERE
        //   filter_clause ) ]
        // aggregate_name (ALL expression [ , ... ] [ order_by_clause ] ) [ FILTER ( WHERE
        //   filter_clause ) ]
        // aggregate_name (DISTINCT expression [ , ... ] [ order_by_clause ] ) [ FILTER ( WHERE
        //   filter_clause ) ]
        // aggregate_name ( * ) [ FILTER ( WHERE filter_clause ) ]

        assert!(!matches!(kind, agg_kinds::ordered_set!()));

        if f.within_group.is_some() {
            return Err(ErrorCode::InvalidInputSyntax(format!(
                "WITHIN GROUP is not allowed for non-ordered-set aggregation `{}`",
                kind
            ))
            .into());
        }

        let args: Vec<_> = f
            .args
            .iter()
            .map(|arg| self.bind_function_arg(arg.clone()))
            .flatten_ok()
            .try_collect()?;
        let order_by = OrderBy::new(
            f.order_by
                .into_iter()
                .map(|e| self.bind_order_by_expr(e))
                .try_collect()?,
        );

        if f.distinct {
            if kind == AggKind::ApproxCountDistinct {
                return Err(ErrorCode::InvalidInputSyntax(format!(
                    "DISTINCT is not allowed for approximate aggregation `{}`",
                    kind
                ))
                .into());
            }

            if args.is_empty() {
                return Err(ErrorCode::InvalidInputSyntax(format!(
                    "DISTINCT is not allowed for aggregate function `{}` without args",
                    kind
                ))
                .into());
            }

            // restrict arguments[1..] to be constant because we don't support multiple distinct key
            // indices for now
            if args.iter().skip(1).any(|arg| arg.as_literal().is_none()) {
                bail_not_implemented!("non-constant arguments other than the first one for DISTINCT aggregation is not supported now");
            }

            // restrict ORDER BY to align with PG, which says:
            // > If DISTINCT is specified in addition to an order_by_clause, then all the ORDER BY
            // > expressions must match regular arguments of the aggregate; that is, you cannot sort
            // > on an expression that is not included in the DISTINCT list.
            if !order_by.sort_exprs.iter().all(|e| args.contains(&e.expr)) {
                return Err(ErrorCode::InvalidInputSyntax(format!(
                    "ORDER BY expressions must match regular arguments of the aggregate for `{}` when DISTINCT is provided",
                    kind
                ))
                .into());
            }
        }

        Ok((vec![], args, order_by))
    }

    /// Bind window function calls according to PostgreSQL syntax.
    /// See <https://www.postgresql.org/docs/current/sql-expressions.html#SYNTAX-WINDOW-FUNCTIONS> for syntax detail.
    pub(super) fn bind_window_function(
        &mut self,
        kind: WindowFuncKind,
        inputs: Vec<ExprImpl>,
        WindowSpec {
            partition_by,
            order_by,
            window_frame,
        }: WindowSpec,
    ) -> Result<ExprImpl> {
        self.ensure_window_function_allowed()?;
        let partition_by = partition_by
            .into_iter()
            .map(|arg| self.bind_expr_inner(arg))
            .try_collect()?;
        let order_by = OrderBy::new(
            order_by
                .into_iter()
                .map(|order_by_expr| self.bind_order_by_expr(order_by_expr))
                .collect::<Result<_>>()?,
        );
        let frame = if let Some(frame) = window_frame {
            let exclusion = if let Some(exclusion) = frame.exclusion {
                match exclusion {
                    WindowFrameExclusion::CurrentRow => FrameExclusion::CurrentRow,
                    WindowFrameExclusion::Group | WindowFrameExclusion::Ties => {
                        bail_not_implemented!(
                            issue = 9124,
                            "window frame exclusion `{}` is not supported yet",
                            exclusion
                        );
                    }
                    WindowFrameExclusion::NoOthers => FrameExclusion::NoOthers,
                }
            } else {
                FrameExclusion::NoOthers
            };
            let bounds = match frame.units {
                WindowFrameUnits::Rows => {
                    let (start, end) = must_match!(frame.bounds, WindowFrameBounds::Bounds { start, end } => (start, end));
                    let (start, end) = self.bind_window_frame_usize_bounds(start, end)?;
                    FrameBounds::Rows(RowsFrameBounds { start, end })
                }
                unit @ (WindowFrameUnits::Range | WindowFrameUnits::Session) => {
                    let order_by_expr = order_by
                        .sort_exprs
                        .iter()
                        // for `RANGE | SESSION` frame, there should be exactly one `ORDER BY` column
                        .exactly_one()
                        .map_err(|_| {
                            ErrorCode::InvalidInputSyntax(format!(
                                "there should be exactly one ordering column for `{}` frame",
                                unit
                            ))
                        })?;
                    let order_data_type = order_by_expr.expr.return_type();
                    let order_type = order_by_expr.order_type;

                    let offset_data_type = match &order_data_type {
                        // for numeric ordering columns, `offset`/`gap` should be the same type
                        // NOTE: actually in PG it can be a larger type, but we don't support this here
                        t @ data_types::range_frame_numeric!() => t.clone(),
                        // for datetime ordering columns, `offset`/`gap` should be interval
                        t @ data_types::range_frame_datetime!() => {
                            if matches!(t, DataType::Date | DataType::Time) {
                                bail_not_implemented!(
                                    "`{}` frame with offset of type `{}` is not implemented yet, please manually cast the `ORDER BY` column to `timestamp`",
                                    unit,
                                    t
                                );
                            }
                            DataType::Interval
                        }
                        // other types are not supported
                        t => {
                            return Err(ErrorCode::NotSupported(
                                format!(
                                    "`{}` frame with offset of type `{}` is not supported",
                                    unit, t
                                ),
                                "Please re-consider the `ORDER BY` column".to_string(),
                            )
                            .into())
                        }
                    };

                    if unit == WindowFrameUnits::Range {
                        let (start, end) = must_match!(frame.bounds, WindowFrameBounds::Bounds { start, end } => (start, end));
                        let (start, end) = self.bind_window_frame_scalar_impl_bounds(
                            start,
                            end,
                            &offset_data_type,
                        )?;
                        FrameBounds::Range(RangeFrameBounds {
                            order_data_type,
                            order_type,
                            offset_data_type,
                            start: start.map(RangeFrameOffset::new),
                            end: end.map(RangeFrameOffset::new),
                        })
                    } else {
                        let gap = must_match!(frame.bounds, WindowFrameBounds::Gap(gap) => gap);
                        let gap_value =
                            self.bind_window_frame_bound_offset(*gap, offset_data_type.clone())?;
                        FrameBounds::Session(SessionFrameBounds {
                            order_data_type,
                            order_type,
                            gap_data_type: offset_data_type,
                            gap: SessionFrameGap::new(gap_value),
                        })
                    }
                }
                WindowFrameUnits::Groups => {
                    bail_not_implemented!(
                        issue = 9124,
                        "window frame in `GROUPS` mode is not supported yet",
                    );
                }
            };

            // Validate the frame bounds, may return `ExprError` to user if the bounds given are not valid.
            bounds.validate()?;

            Some(Frame { bounds, exclusion })
        } else {
            None
        };
        Ok(WindowFunction::new(kind, partition_by, order_by, inputs, frame)?.into())
    }

    fn bind_window_frame_usize_bounds(
        &mut self,
        start: WindowFrameBound,
        end: Option<WindowFrameBound>,
    ) -> Result<(FrameBound<usize>, FrameBound<usize>)> {
        let mut convert_offset = |offset: Box<ast::Expr>| -> Result<usize> {
            let offset = self
                .bind_window_frame_bound_offset(*offset, DataType::Int64)?
                .into_int64();
            if offset < 0 {
                return Err(ErrorCode::InvalidInputSyntax(
                    "offset in window frame bounds must be non-negative".to_string(),
                )
                .into());
            }
            Ok(offset as usize)
        };
        let mut convert_bound = |bound| -> Result<FrameBound<usize>> {
            Ok(match bound {
                WindowFrameBound::CurrentRow => FrameBound::CurrentRow,
                WindowFrameBound::Preceding(None) => FrameBound::UnboundedPreceding,
                WindowFrameBound::Preceding(Some(offset)) => {
                    FrameBound::Preceding(convert_offset(offset)?)
                }
                WindowFrameBound::Following(None) => FrameBound::UnboundedFollowing,
                WindowFrameBound::Following(Some(offset)) => {
                    FrameBound::Following(convert_offset(offset)?)
                }
            })
        };
        let start = convert_bound(start)?;
        let end = if let Some(end_bound) = end {
            convert_bound(end_bound)?
        } else {
            FrameBound::CurrentRow
        };
        Ok((start, end))
    }

    fn bind_window_frame_scalar_impl_bounds(
        &mut self,
        start: WindowFrameBound,
        end: Option<WindowFrameBound>,
        offset_data_type: &DataType,
    ) -> Result<(FrameBound<ScalarImpl>, FrameBound<ScalarImpl>)> {
        let mut convert_bound = |bound| -> Result<FrameBound<_>> {
            Ok(match bound {
                WindowFrameBound::CurrentRow => FrameBound::CurrentRow,
                WindowFrameBound::Preceding(None) => FrameBound::UnboundedPreceding,
                WindowFrameBound::Preceding(Some(offset)) => FrameBound::Preceding(
                    self.bind_window_frame_bound_offset(*offset, offset_data_type.clone())?,
                ),
                WindowFrameBound::Following(None) => FrameBound::UnboundedFollowing,
                WindowFrameBound::Following(Some(offset)) => FrameBound::Following(
                    self.bind_window_frame_bound_offset(*offset, offset_data_type.clone())?,
                ),
            })
        };
        let start = convert_bound(start)?;
        let end = if let Some(end_bound) = end {
            convert_bound(end_bound)?
        } else {
            FrameBound::CurrentRow
        };
        Ok((start, end))
    }

    fn bind_window_frame_bound_offset(
        &mut self,
        offset: ast::Expr,
        cast_to: DataType,
    ) -> Result<ScalarImpl> {
        let mut offset = self.bind_expr(offset)?;
        if !offset.is_const() {
            return Err(ErrorCode::InvalidInputSyntax(
                "offset/gap in window frame bounds must be constant".to_string(),
            )
            .into());
        }
        if offset.cast_implicit_mut(cast_to.clone()).is_err() {
            return Err(ErrorCode::InvalidInputSyntax(format!(
                "offset/gap in window frame bounds must be castable to {}",
                cast_to
            ))
            .into());
        }
        let offset = offset.fold_const()?;
        let Some(offset) = offset else {
            return Err(ErrorCode::InvalidInputSyntax(
                "offset/gap in window frame bounds must not be NULL".to_string(),
            )
            .into());
        };
        Ok(offset)
    }

    fn bind_builtin_scalar_function(
        &mut self,
        function_name: &str,
        inputs: Vec<ExprImpl>,
        variadic: bool,
    ) -> Result<ExprImpl> {
        type Inputs = Vec<ExprImpl>;

        type Handle = Box<dyn Fn(&mut Binder, Inputs) -> Result<ExprImpl> + Sync + Send>;

        fn rewrite(r#type: ExprType, rewriter: fn(Inputs) -> Result<Inputs>) -> Handle {
            Box::new(move |_binder, mut inputs| {
                inputs = (rewriter)(inputs)?;
                Ok(FunctionCall::new(r#type, inputs)?.into())
            })
        }

        fn raw_call(r#type: ExprType) -> Handle {
            rewrite(r#type, Ok)
        }

        fn guard_by_len(expected_len: usize, handle: Handle) -> Handle {
            Box::new(move |binder, inputs| {
                if inputs.len() == expected_len {
                    handle(binder, inputs)
                } else {
                    Err(ErrorCode::ExprError("unexpected arguments number".into()).into())
                }
            })
        }

        fn raw<F: Fn(&mut Binder, Inputs) -> Result<ExprImpl> + Sync + Send + 'static>(
            f: F,
        ) -> Handle {
            Box::new(f)
        }

        fn dispatch_by_len(mapping: Vec<(usize, Handle)>) -> Handle {
            Box::new(move |binder, inputs| {
                for (len, handle) in &mapping {
                    if inputs.len() == *len {
                        return handle(binder, inputs);
                    }
                }
                Err(ErrorCode::ExprError("unexpected arguments number".into()).into())
            })
        }

        fn raw_literal(literal: ExprImpl) -> Handle {
            Box::new(move |_binder, _inputs| Ok(literal.clone()))
        }

        fn now() -> Handle {
            guard_by_len(
                0,
                raw(move |binder, _inputs| {
                    binder.ensure_now_function_allowed()?;
                    // NOTE: this will be further transformed during optimization. See the
                    // documentation of `Now`.
                    Ok(Now.into())
                }),
            )
        }

        fn pi() -> Handle {
            raw_literal(ExprImpl::literal_f64(std::f64::consts::PI))
        }

        fn proctime() -> Handle {
            Box::new(move |binder, inputs| {
                binder.ensure_proctime_function_allowed()?;
                raw_call(ExprType::Proctime)(binder, inputs)
            })
        }

        // `SESSION_USER` is the user name of the user that is connected to the database.
        fn session_user() -> Handle {
            guard_by_len(
                0,
                raw(|binder, _inputs| {
                    Ok(ExprImpl::literal_varchar(
                        binder.auth_context.user_name.clone(),
                    ))
                }),
            )
        }

        // `CURRENT_USER` is the user name of the user that is executing the command,
        // `CURRENT_ROLE`, `USER` are synonyms for `CURRENT_USER`. Since we don't support
        // `SET ROLE xxx` for now, they will all returns session user name.
        fn current_user() -> Handle {
            guard_by_len(
                0,
                raw(|binder, _inputs| {
                    Ok(ExprImpl::literal_varchar(
                        binder.auth_context.user_name.clone(),
                    ))
                }),
            )
        }

        static HANDLES: LazyLock<HashMap<&'static str, Handle>> = LazyLock::new(|| {
            [
                (
                    "booleq",
                    rewrite(ExprType::Equal, Binder::rewrite_two_bool_inputs),
                ),
                (
                    "boolne",
                    rewrite(ExprType::NotEqual, Binder::rewrite_two_bool_inputs),
                ),
                ("coalesce", rewrite(ExprType::Coalesce, |inputs| {
                    if inputs.iter().any(ExprImpl::has_table_function) {
                        return Err(ErrorCode::BindError("table functions are not allowed in COALESCE".into()).into());
                    }
                    Ok(inputs)
                })),
                (
                    "nullif",
                    rewrite(ExprType::Case, Binder::rewrite_nullif_to_case_when),
                ),
                (
                    "round",
                    dispatch_by_len(vec![
                        (2, raw_call(ExprType::RoundDigit)),
                        (1, raw_call(ExprType::Round)),
                    ]),
                ),
                ("pow", raw_call(ExprType::Pow)),
                // "power" is the function name used in PG.
                ("power", raw_call(ExprType::Pow)),
                ("ceil", raw_call(ExprType::Ceil)),
                ("ceiling", raw_call(ExprType::Ceil)),
                ("floor", raw_call(ExprType::Floor)),
                ("trunc", raw_call(ExprType::Trunc)),
                ("abs", raw_call(ExprType::Abs)),
                ("exp", raw_call(ExprType::Exp)),
                ("ln", raw_call(ExprType::Ln)),
                ("log", raw_call(ExprType::Log10)),
                ("log10", raw_call(ExprType::Log10)),
                ("mod", raw_call(ExprType::Modulus)),
                ("sin", raw_call(ExprType::Sin)),
                ("cos", raw_call(ExprType::Cos)),
                ("tan", raw_call(ExprType::Tan)),
                ("cot", raw_call(ExprType::Cot)),
                ("asin", raw_call(ExprType::Asin)),
                ("acos", raw_call(ExprType::Acos)),
                ("atan", raw_call(ExprType::Atan)),
                ("atan2", raw_call(ExprType::Atan2)),
                ("sind", raw_call(ExprType::Sind)),
                ("cosd", raw_call(ExprType::Cosd)),
                ("cotd", raw_call(ExprType::Cotd)),
                ("tand", raw_call(ExprType::Tand)),
                ("sinh", raw_call(ExprType::Sinh)),
                ("cosh", raw_call(ExprType::Cosh)),
                ("tanh", raw_call(ExprType::Tanh)),
                ("coth", raw_call(ExprType::Coth)),
                ("asinh", raw_call(ExprType::Asinh)),
                ("acosh", raw_call(ExprType::Acosh)),
                ("atanh", raw_call(ExprType::Atanh)),
                ("asind", raw_call(ExprType::Asind)),
                ("degrees", raw_call(ExprType::Degrees)),
                ("radians", raw_call(ExprType::Radians)),
                ("sqrt", raw_call(ExprType::Sqrt)),
                ("cbrt", raw_call(ExprType::Cbrt)),
                ("sign", raw_call(ExprType::Sign)),
                ("scale", raw_call(ExprType::Scale)),
                ("min_scale", raw_call(ExprType::MinScale)),
                ("trim_scale", raw_call(ExprType::TrimScale)),

                (
                    "to_timestamp",
                    dispatch_by_len(vec![
                        (1, raw_call(ExprType::SecToTimestamptz)),
                        (2, raw_call(ExprType::CharToTimestamptz)),
                    ]),
                ),
                ("date_trunc", raw_call(ExprType::DateTrunc)),
                ("date_part", raw_call(ExprType::DatePart)),
                ("make_date", raw_call(ExprType::MakeDate)),
                ("make_time", raw_call(ExprType::MakeTime)),
                ("make_timestamp", raw_call(ExprType::MakeTimestamp)),
                ("to_date", raw_call(ExprType::CharToDate)),
                ("make_timestamptz", raw_call(ExprType::MakeTimestamptz)),
                // string
                ("substr", raw_call(ExprType::Substr)),
                ("length", raw_call(ExprType::Length)),
                ("upper", raw_call(ExprType::Upper)),
                ("lower", raw_call(ExprType::Lower)),
                ("trim", raw_call(ExprType::Trim)),
                ("replace", raw_call(ExprType::Replace)),
                ("overlay", raw_call(ExprType::Overlay)),
                ("btrim", raw_call(ExprType::Trim)),
                ("ltrim", raw_call(ExprType::Ltrim)),
                ("rtrim", raw_call(ExprType::Rtrim)),
                ("md5", raw_call(ExprType::Md5)),
                ("to_char", raw_call(ExprType::ToChar)),
                (
                    "concat",
                    rewrite(ExprType::ConcatWs, Binder::rewrite_concat_to_concat_ws),
                ),
                ("concat_ws", raw_call(ExprType::ConcatWs)),
                ("format", raw_call(ExprType::Format)),
                ("translate", raw_call(ExprType::Translate)),
                ("split_part", raw_call(ExprType::SplitPart)),
                ("char_length", raw_call(ExprType::CharLength)),
                ("character_length", raw_call(ExprType::CharLength)),
                ("repeat", raw_call(ExprType::Repeat)),
                ("ascii", raw_call(ExprType::Ascii)),
                ("octet_length", raw_call(ExprType::OctetLength)),
                ("bit_length", raw_call(ExprType::BitLength)),
                ("regexp_match", raw_call(ExprType::RegexpMatch)),
                ("regexp_replace", raw_call(ExprType::RegexpReplace)),
                ("regexp_count", raw_call(ExprType::RegexpCount)),
                ("regexp_split_to_array", raw_call(ExprType::RegexpSplitToArray)),
                ("chr", raw_call(ExprType::Chr)),
                ("starts_with", raw_call(ExprType::StartsWith)),
                ("initcap", raw_call(ExprType::Initcap)),
                ("lpad", raw_call(ExprType::Lpad)),
                ("rpad", raw_call(ExprType::Rpad)),
                ("reverse", raw_call(ExprType::Reverse)),
                ("strpos", raw_call(ExprType::Position)),
                ("to_ascii", raw_call(ExprType::ToAscii)),
                ("to_hex", raw_call(ExprType::ToHex)),
                ("quote_ident", raw_call(ExprType::QuoteIdent)),
                ("quote_literal", guard_by_len(1, raw(|_binder, mut inputs| {
                    if inputs[0].return_type() != DataType::Varchar {
                        // Support `quote_literal(any)` by converting it to `quote_literal(any::text)`
                        // Ref. https://github.com/postgres/postgres/blob/REL_16_1/src/include/catalog/pg_proc.dat#L4641
                        FunctionCall::cast_mut(&mut inputs[0], DataType::Varchar, CastContext::Explicit)?;
                    }
                    Ok(FunctionCall::new_unchecked(ExprType::QuoteLiteral, inputs, DataType::Varchar).into())
                }))),
                ("quote_nullable", guard_by_len(1, raw(|_binder, mut inputs| {
                    if inputs[0].return_type() != DataType::Varchar {
                        // Support `quote_nullable(any)` by converting it to `quote_nullable(any::text)`
                        // Ref. https://github.com/postgres/postgres/blob/REL_16_1/src/include/catalog/pg_proc.dat#L4650
                        FunctionCall::cast_mut(&mut inputs[0], DataType::Varchar, CastContext::Explicit)?;
                    }
                    Ok(FunctionCall::new_unchecked(ExprType::QuoteNullable, inputs, DataType::Varchar).into())
                }))),
                ("string_to_array", raw_call(ExprType::StringToArray)),
                ("encode", raw_call(ExprType::Encode)),
                ("decode", raw_call(ExprType::Decode)),
                ("convert_from", raw_call(ExprType::ConvertFrom)),
                ("convert_to", raw_call(ExprType::ConvertTo)),
                ("sha1", raw_call(ExprType::Sha1)),
                ("sha224", raw_call(ExprType::Sha224)),
                ("sha256", raw_call(ExprType::Sha256)),
                ("sha384", raw_call(ExprType::Sha384)),
                ("sha512", raw_call(ExprType::Sha512)),
                ("encrypt", raw_call(ExprType::Encrypt)),
                ("decrypt", raw_call(ExprType::Decrypt)),
                ("left", raw_call(ExprType::Left)),
                ("right", raw_call(ExprType::Right)),
                ("inet_aton", raw_call(ExprType::InetAton)),
                ("inet_ntoa", raw_call(ExprType::InetNtoa)),
                ("int8send", raw_call(ExprType::PgwireSend)),
                ("int8recv", guard_by_len(1, raw(|_binder, mut inputs| {
                    // Similar to `cast` from string, return type is set explicitly rather than inferred.
                    let hint = if !inputs[0].is_untyped() && inputs[0].return_type() == DataType::Varchar {
                        " Consider `decode` or cast."
                    } else {
                        ""
                    };
                    inputs[0].cast_implicit_mut(DataType::Bytea).map_err(|e| {
                        ErrorCode::BindError(format!("{} in `recv`.{hint}", e.as_report()))
                    })?;
                    Ok(FunctionCall::new_unchecked(ExprType::PgwireRecv, inputs, DataType::Int64).into())
                }))),
                // array
                ("array_cat", raw_call(ExprType::ArrayCat)),
                ("array_append", raw_call(ExprType::ArrayAppend)),
                ("array_join", raw_call(ExprType::ArrayToString)),
                ("array_prepend", raw_call(ExprType::ArrayPrepend)),
                ("array_to_string", raw_call(ExprType::ArrayToString)),
                ("array_distinct", raw_call(ExprType::ArrayDistinct)),
                ("array_min", raw_call(ExprType::ArrayMin)),
                ("array_sort", raw_call(ExprType::ArraySort)),
                ("array_length", raw_call(ExprType::ArrayLength)),
                ("cardinality", raw_call(ExprType::Cardinality)),
                ("array_remove", raw_call(ExprType::ArrayRemove)),
                ("array_replace", raw_call(ExprType::ArrayReplace)),
                ("array_max", raw_call(ExprType::ArrayMax)),
                ("array_sum", raw_call(ExprType::ArraySum)),
                ("array_position", raw_call(ExprType::ArrayPosition)),
                ("array_positions", raw_call(ExprType::ArrayPositions)),
                ("array_contains", raw_call(ExprType::ArrayContains)),
                ("arraycontains", raw_call(ExprType::ArrayContains)),
                ("array_contained", raw_call(ExprType::ArrayContained)),
                ("arraycontained", raw_call(ExprType::ArrayContained)),
                ("trim_array", raw_call(ExprType::TrimArray)),
                (
                    "array_ndims",
                    guard_by_len(1, raw(|_binder, inputs| {
                        inputs[0].ensure_array_type()?;

                        let n = inputs[0].return_type().array_ndims()
                                .try_into().map_err(|_| ErrorCode::BindError("array_ndims integer overflow".into()))?;
                        Ok(ExprImpl::literal_int(n))
                    })),
                ),
                (
                    "array_lower",
                    guard_by_len(2, raw(|binder, inputs| {
                        let (arg0, arg1) = inputs.into_iter().next_tuple().unwrap();
                        // rewrite into `CASE WHEN 0 < arg1 AND arg1 <= array_ndims(arg0) THEN 1 END`
                        let ndims_expr = binder.bind_builtin_scalar_function("array_ndims", vec![arg0], false)?;
                        let arg1 = arg1.cast_implicit(DataType::Int32)?;

                        FunctionCall::new(
                            ExprType::Case,
                            vec![
                                FunctionCall::new(
                                    ExprType::And,
                                    vec![
                                        FunctionCall::new(ExprType::LessThan, vec![ExprImpl::literal_int(0), arg1.clone()])?.into(),
                                        FunctionCall::new(ExprType::LessThanOrEqual, vec![arg1, ndims_expr])?.into(),
                                    ],
                                )?.into(),
                                ExprImpl::literal_int(1),
                            ],
                        ).map(Into::into)
                    })),
                ),
                ("array_upper", raw_call(ExprType::ArrayLength)), // `lower == 1` implies `upper == length`
                ("array_dims", raw_call(ExprType::ArrayDims)),
                // int256
                ("hex_to_int256", raw_call(ExprType::HexToInt256)),
                // jsonb
                ("jsonb_object_field", raw_call(ExprType::JsonbAccess)),
                ("jsonb_array_element", raw_call(ExprType::JsonbAccess)),
                ("jsonb_object_field_text", raw_call(ExprType::JsonbAccessStr)),
                ("jsonb_array_element_text", raw_call(ExprType::JsonbAccessStr)),
                ("jsonb_extract_path", raw_call(ExprType::JsonbExtractPath)),
                ("jsonb_extract_path_text", raw_call(ExprType::JsonbExtractPathText)),
                ("jsonb_typeof", raw_call(ExprType::JsonbTypeof)),
                ("jsonb_array_length", raw_call(ExprType::JsonbArrayLength)),
                ("jsonb_concat", raw_call(ExprType::JsonbConcat)),
                ("jsonb_object", raw_call(ExprType::JsonbObject)),
                ("jsonb_pretty", raw_call(ExprType::JsonbPretty)),
                ("jsonb_contains", raw_call(ExprType::JsonbContains)),
                ("jsonb_contained", raw_call(ExprType::JsonbContained)),
                ("jsonb_exists", raw_call(ExprType::JsonbExists)),
                ("jsonb_exists_any", raw_call(ExprType::JsonbExistsAny)),
                ("jsonb_exists_all", raw_call(ExprType::JsonbExistsAll)),
                ("jsonb_delete", raw_call(ExprType::Subtract)),
                ("jsonb_delete_path", raw_call(ExprType::JsonbDeletePath)),
                ("jsonb_strip_nulls", raw_call(ExprType::JsonbStripNulls)),
                ("to_jsonb", raw_call(ExprType::ToJsonb)),
                ("jsonb_build_array", raw_call(ExprType::JsonbBuildArray)),
                ("jsonb_build_object", raw_call(ExprType::JsonbBuildObject)),
                ("jsonb_populate_record", raw_call(ExprType::JsonbPopulateRecord)),
                ("jsonb_path_match", raw_call(ExprType::JsonbPathMatch)),
                ("jsonb_path_exists", raw_call(ExprType::JsonbPathExists)),
                ("jsonb_path_query_array", raw_call(ExprType::JsonbPathQueryArray)),
                ("jsonb_path_query_first", raw_call(ExprType::JsonbPathQueryFirst)),
                ("jsonb_set", raw_call(ExprType::JsonbSet)),
                // Functions that return a constant value
                ("pi", pi()),
                // greatest and least
                ("greatest", raw_call(ExprType::Greatest)),
                ("least", raw_call(ExprType::Least)),
                // System information operations.
                (
                    "pg_typeof",
                    guard_by_len(1, raw(|_binder, inputs| {
                        let input = &inputs[0];
                        let v = match input.is_untyped() {
                            true => "unknown".into(),
                            false => input.return_type().to_string(),
                        };
                        Ok(ExprImpl::literal_varchar(v))
                    })),
                ),
                ("current_database", guard_by_len(0, raw(|binder, _inputs| {
                    Ok(ExprImpl::literal_varchar(binder.db_name.clone()))
                }))),
                ("current_schema", guard_by_len(0, raw(|binder, _inputs| {
                    return Ok(binder
                        .first_valid_schema()
                        .map(|schema| ExprImpl::literal_varchar(schema.name()))
                        .unwrap_or_else(|_| ExprImpl::literal_null(DataType::Varchar)));
                }))),
                ("current_schemas", raw(|binder, mut inputs| {
                    let no_match_err = ErrorCode::ExprError(
                            "No function matches the given name and argument types. You might need to add explicit type casts.".into()
                        );
                    if inputs.len() != 1 {
                        return Err(no_match_err.into());
                    }
                    let input = inputs
                        .pop()
                        .unwrap()
                        .enforce_bool_clause("current_schemas")
                        .map_err(|_| no_match_err)?;

                    let ExprImpl::Literal(literal) = &input else {
                        bail_not_implemented!("Only boolean literals are supported in `current_schemas`.");
                    };

                    let Some(bool) = literal.get_data().as_ref().map(|bool| bool.clone().into_bool()) else {
                        return Ok(ExprImpl::literal_null(DataType::List(Box::new(DataType::Varchar))));
                    };

                    let paths = if bool {
                        binder.search_path.path()
                    } else {
                        binder.search_path.real_path()
                    };

                    let mut schema_names = vec![];
                    for path in paths {
                        let mut schema_name = path;
                        if schema_name == USER_NAME_WILD_CARD {
                            schema_name = &binder.auth_context.user_name;
                        }

                        if binder
                            .catalog
                            .get_schema_by_name(&binder.db_name, schema_name)
                            .is_ok()
                        {
                            schema_names.push(schema_name.as_str());
                        }
                    }

                    Ok(ExprImpl::literal_list(
                        ListValue::from_iter(schema_names),
                        DataType::Varchar,
                    ))
                })),
                ("session_user", session_user()),
                ("current_role", current_user()),
                ("current_user", current_user()),
                ("user", current_user()),
                ("pg_get_userbyid", raw_call(ExprType::PgGetUserbyid)),
                ("pg_get_indexdef", raw_call(ExprType::PgGetIndexdef)),
                ("pg_get_viewdef", raw_call(ExprType::PgGetViewdef)),
                ("pg_index_column_has_property", raw_call(ExprType::PgIndexColumnHasProperty)),
                ("pg_relation_size", raw(|_binder, mut inputs|{
                    if inputs.is_empty() {
                        return Err(ErrorCode::ExprError(
                            "function pg_relation_size() does not exist".into(),
                        )
                        .into());
                    }
                    inputs[0].cast_to_regclass_mut()?;
                    Ok(FunctionCall::new(ExprType::PgRelationSize, inputs)?.into())
                })),
                ("pg_get_serial_sequence", raw_literal(ExprImpl::literal_null(DataType::Varchar))),
                ("pg_table_size", guard_by_len(1, raw(|_binder, mut inputs|{
                    inputs[0].cast_to_regclass_mut()?;
                    Ok(FunctionCall::new(ExprType::PgRelationSize, inputs)?.into())
                }))),
                ("pg_indexes_size", guard_by_len(1, raw(|_binder, mut inputs|{
                    inputs[0].cast_to_regclass_mut()?;
                    Ok(FunctionCall::new(ExprType::PgIndexesSize, inputs)?.into())
                }))),
                ("pg_get_expr", raw(|_binder, inputs|{
                    if inputs.len() == 2 || inputs.len() == 3 {
                        // TODO: implement pg_get_expr rather than just return empty as an workaround.
                        Ok(ExprImpl::literal_varchar("".into()))
                    } else {
                        Err(ErrorCode::ExprError(
                            "Too many/few arguments for pg_catalog.pg_get_expr()".into(),
                        )
                        .into())
                    }
                })),
                ("current_setting", guard_by_len(1, raw(|binder, inputs| {
                    let input = &inputs[0];
                    let input = if let ExprImpl::Literal(literal) = input &&
                        let Some(ScalarImpl::Utf8(input)) = literal.get_data()
                    {
                        input
                    } else {
                        return Err(ErrorCode::ExprError(
                            "Only literal is supported in `setting_name`.".into(),
                        )
                        .into());
                    };
                    let session_config = binder.session_config.read();
                    Ok(ExprImpl::literal_varchar(session_config.get(input.as_ref())?))
                }))),
                ("set_config", guard_by_len(3, raw(|binder, inputs| {
                    let setting_name = if let ExprImpl::Literal(literal) = &inputs[0] && let Some(ScalarImpl::Utf8(input)) = literal.get_data() {
                        input
                    } else {
                        return Err(ErrorCode::ExprError(
                            "Only string literal is supported in `setting_name`.".into(),
                        )
                        .into());
                    };

                    let new_value = if let ExprImpl::Literal(literal) = &inputs[1] && let Some(ScalarImpl::Utf8(input)) = literal.get_data() {
                        input
                    } else {
                        return Err(ErrorCode::ExprError(
                            "Only string literal is supported in `setting_name`.".into(),
                        )
                        .into());
                    };

                    let is_local = if let ExprImpl::Literal(literal) = &inputs[2] && let Some(ScalarImpl::Bool(input)) = literal.get_data() {
                        input
                    } else {
                        return Err(ErrorCode::ExprError(
                            "Only bool literal is supported in `is_local`.".into(),
                        )
                        .into());
                    };

                    if *is_local {
                        return Err(ErrorCode::ExprError(
                            "`is_local = true` is not supported now.".into(),
                        )
                        .into());
                    }

                    let mut session_config = binder.session_config.write();

                    // TODO: report session config changes if necessary.
                    session_config.set(setting_name, new_value.to_string(), &mut())?;

                    Ok(ExprImpl::literal_varchar(new_value.to_string()))
                }))),
                ("format_type", raw_call(ExprType::FormatType)),
                ("pg_table_is_visible", raw_literal(ExprImpl::literal_bool(true))),
                ("pg_type_is_visible", raw_literal(ExprImpl::literal_bool(true))),
                ("pg_get_constraintdef", raw_literal(ExprImpl::literal_null(DataType::Varchar))),
                ("pg_get_partkeydef", raw_literal(ExprImpl::literal_null(DataType::Varchar))),
                ("pg_encoding_to_char", raw_literal(ExprImpl::literal_varchar("UTF8".into()))),
                ("has_database_privilege", raw_literal(ExprImpl::literal_bool(true))),
                ("has_table_privilege", raw(|binder, mut inputs|{
                    if inputs.len() == 2 {
                        inputs.insert(0, ExprImpl::literal_varchar(binder.auth_context.user_name.clone()));
                    }
                    if inputs.len() == 3 {
                        if inputs[1].return_type() == DataType::Varchar {
                            inputs[1].cast_to_regclass_mut()?;
                        }
                        Ok(FunctionCall::new(ExprType::HasTablePrivilege, inputs)?.into())
                    } else {
                        Err(ErrorCode::ExprError(
                            "Too many/few arguments for pg_catalog.has_table_privilege()".into(),
                        )
                        .into())
                    }
                })),
                ("has_any_column_privilege", raw(|binder, mut inputs|{
                    if inputs.len() == 2 {
                        inputs.insert(0, ExprImpl::literal_varchar(binder.auth_context.user_name.clone()));
                    }
                    if inputs.len() == 3 {
                        if inputs[1].return_type() == DataType::Varchar {
                            inputs[1].cast_to_regclass_mut()?;
                        }
                        Ok(FunctionCall::new(ExprType::HasAnyColumnPrivilege, inputs)?.into())
                    } else {
                        Err(ErrorCode::ExprError(
                            "Too many/few arguments for pg_catalog.has_any_column_privilege()".into(),
                        )
                        .into())
                    }
                })),
                ("has_schema_privilege", raw(|binder, mut inputs|{
                    if inputs.len() == 2 {
                        inputs.insert(0, ExprImpl::literal_varchar(binder.auth_context.user_name.clone()));
                    }
                    if inputs.len() == 3 {
                        Ok(FunctionCall::new(ExprType::HasSchemaPrivilege, inputs)?.into())
                    } else {
                        Err(ErrorCode::ExprError(
                            "Too many/few arguments for pg_catalog.has_schema_privilege()".into(),
                        )
                        .into())
                    }
                })),
                ("pg_stat_get_numscans", raw_literal(ExprImpl::literal_bigint(0))),
                ("pg_backend_pid", raw(|binder, _inputs| {
                    // FIXME: the session id is not global unique in multi-frontend env.
                    Ok(ExprImpl::literal_int(binder.session_id.0))
                })),
                ("pg_cancel_backend", guard_by_len(1, raw(|_binder, _inputs| {
                        // TODO: implement real cancel rather than just return false as an workaround.
                        Ok(ExprImpl::literal_bool(false))
                }))),
                ("pg_terminate_backend", guard_by_len(1, raw(|_binder, _inputs|{
                        // TODO: implement real terminate rather than just return false as an
                        // workaround.
                        Ok(ExprImpl::literal_bool(false))
                }))),
                ("pg_tablespace_location", guard_by_len(1, raw_literal(ExprImpl::literal_null(DataType::Varchar)))),
                ("pg_postmaster_start_time", guard_by_len(0, raw(|_binder, _inputs|{
                    let server_start_time = risingwave_variables::get_server_start_time();
                    let datum = server_start_time.map(Timestamptz::from).map(ScalarImpl::from);
                    let literal = Literal::new(datum, DataType::Timestamptz);
                    Ok(literal.into())
                }))),
                // TODO: really implement them.
                // https://www.postgresql.org/docs/9.5/functions-info.html#FUNCTIONS-INFO-COMMENT-TABLE
                // WARN: Hacked in [`Binder::bind_function`]!!!
                ("col_description", raw_call(ExprType::ColDescription)),
                ("obj_description", raw_literal(ExprImpl::literal_varchar("".to_string()))),
                ("shobj_description", raw_literal(ExprImpl::literal_varchar("".to_string()))),
                ("pg_is_in_recovery", raw_literal(ExprImpl::literal_bool(false))),
                // internal
                ("rw_vnode", raw_call(ExprType::Vnode)),
                ("rw_test_paid_tier", raw_call(ExprType::TestPaidTier)), // for testing purposes
                // TODO: choose which pg version we should return.
                ("version", raw_literal(ExprImpl::literal_varchar(current_cluster_version()))),
                // non-deterministic
                ("now", now()),
                ("current_timestamp", now()),
                ("proctime", proctime()),
                ("pg_sleep", raw_call(ExprType::PgSleep)),
                ("pg_sleep_for", raw_call(ExprType::PgSleepFor)),
                // TODO: implement pg_sleep_until
                // ("pg_sleep_until", raw_call(ExprType::PgSleepUntil)),

                // cast functions
                // only functions required by the existing PostgreSQL tool are implemented
                ("date", guard_by_len(1, raw(|_binder, inputs| {
                    inputs[0].clone().cast_explicit(DataType::Date).map_err(Into::into)
                }))),
            ]
            .into_iter()
            .collect()
        });

        static FUNCTIONS_BKTREE: LazyLock<BKTree<&str>> = LazyLock::new(|| {
            let mut tree = BKTree::new(metrics::Levenshtein);

            // TODO: Also hint other functinos, e.g., Agg or UDF.
            for k in HANDLES.keys() {
                tree.add(*k);
            }

            tree
        });

        if variadic {
            let func = match function_name {
                "format" => ExprType::FormatVariadic,
                "concat" => ExprType::ConcatVariadic,
                "concat_ws" => ExprType::ConcatWsVariadic,
                "jsonb_build_array" => ExprType::JsonbBuildArrayVariadic,
                "jsonb_build_object" => ExprType::JsonbBuildObjectVariadic,
                "jsonb_extract_path" => ExprType::JsonbExtractPathVariadic,
                "jsonb_extract_path_text" => ExprType::JsonbExtractPathTextVariadic,
                _ => {
                    return Err(ErrorCode::BindError(format!(
                        "VARIADIC argument is not allowed in function \"{}\"",
                        function_name
                    ))
                    .into())
                }
            };
            return Ok(FunctionCall::new(func, inputs)?.into());
        }

        match HANDLES.get(function_name) {
            Some(handle) => handle(self, inputs),
            None => {
                let allowed_distance = if function_name.len() > 3 { 2 } else { 1 };

                let candidates = FUNCTIONS_BKTREE
                    .find(function_name, allowed_distance)
                    .map(|(_idx, c)| c)
                    .join(" or ");

                Err(no_function!(
                    candidates = (!candidates.is_empty()).then_some(candidates),
                    "{}({})",
                    function_name,
                    inputs.iter().map(|e| e.return_type()).join(", ")
                )
                .into())
            }
        }
    }

    fn rewrite_concat_to_concat_ws(inputs: Vec<ExprImpl>) -> Result<Vec<ExprImpl>> {
        if inputs.is_empty() {
            Err(ErrorCode::BindError(
                "Function `concat` takes at least 1 arguments (0 given)".to_string(),
            )
            .into())
        } else {
            let inputs = once(ExprImpl::literal_varchar("".to_string()))
                .chain(inputs)
                .collect();
            Ok(inputs)
        }
    }

    /// Make sure inputs only have 2 value and rewrite the arguments.
    /// Nullif(expr1,expr2) -> Case(Equal(expr1 = expr2),null,expr1).
    fn rewrite_nullif_to_case_when(inputs: Vec<ExprImpl>) -> Result<Vec<ExprImpl>> {
        if inputs.len() != 2 {
            Err(
                ErrorCode::BindError("Function `nullif` must contain 2 arguments".to_string())
                    .into(),
            )
        } else {
            let inputs = vec![
                FunctionCall::new(ExprType::Equal, inputs.clone())?.into(),
                Literal::new(None, inputs[0].return_type()).into(),
                inputs[0].clone(),
            ];
            Ok(inputs)
        }
    }

    fn rewrite_two_bool_inputs(mut inputs: Vec<ExprImpl>) -> Result<Vec<ExprImpl>> {
        if inputs.len() != 2 {
            return Err(
                ErrorCode::BindError("function must contain only 2 arguments".to_string()).into(),
            );
        }
        let left = inputs.pop().unwrap();
        let right = inputs.pop().unwrap();
        Ok(vec![
            left.cast_implicit(DataType::Boolean)?,
            right.cast_implicit(DataType::Boolean)?,
        ])
    }

    fn ensure_window_function_allowed(&self) -> Result<()> {
        if let Some(clause) = self.context.clause {
            match clause {
                Clause::Where
                | Clause::Values
                | Clause::GroupBy
                | Clause::Having
                | Clause::Filter
                | Clause::GeneratedColumn
                | Clause::From
                | Clause::Insert
                | Clause::JoinOn => {
                    return Err(ErrorCode::InvalidInputSyntax(format!(
                        "window functions are not allowed in {}",
                        clause
                    ))
                    .into());
                }
            }
        }
        Ok(())
    }

    fn ensure_now_function_allowed(&self) -> Result<()> {
        if self.is_for_stream()
            && !matches!(
                self.context.clause,
                Some(Clause::Where)
                    | Some(Clause::Having)
                    | Some(Clause::JoinOn)
                    | Some(Clause::From)
            )
        {
            return Err(ErrorCode::InvalidInputSyntax(format!(
                "For streaming queries, `NOW()` function is only allowed in `WHERE`, `HAVING`, `ON` and `FROM`. Found in clause: {:?}. \
                Please please refer to https://www.risingwave.dev/docs/current/sql-pattern-temporal-filters/ for more information",
                self.context.clause
            ))
            .into());
        }
        if matches!(self.context.clause, Some(Clause::GeneratedColumn)) {
            return Err(ErrorCode::InvalidInputSyntax(
                "Cannot use `NOW()` function in generated columns. Do you want `PROCTIME()`?"
                    .to_string(),
            )
            .into());
        }
        Ok(())
    }

    fn ensure_proctime_function_allowed(&self) -> Result<()> {
        if !self.is_for_ddl() {
            return Err(ErrorCode::InvalidInputSyntax(
                "Function `PROCTIME()` is only allowed in CREATE TABLE/SOURCE. Is `NOW()` what you want?".to_string(),
            )
            .into());
        }
        Ok(())
    }

    fn ensure_aggregate_allowed(&self) -> Result<()> {
        if let Some(clause) = self.context.clause {
            match clause {
                Clause::Where
                | Clause::Values
                | Clause::From
                | Clause::GeneratedColumn
                | Clause::Insert
                | Clause::JoinOn => {
                    return Err(ErrorCode::InvalidInputSyntax(format!(
                        "aggregate functions are not allowed in {}",
                        clause
                    ))
                    .into())
                }
                Clause::Having | Clause::Filter | Clause::GroupBy => {}
            }
        }
        Ok(())
    }

    fn ensure_table_function_allowed(&self) -> Result<()> {
        if let Some(clause) = self.context.clause {
            match clause {
                Clause::JoinOn
                | Clause::Where
                | Clause::Having
                | Clause::Filter
                | Clause::Values
                | Clause::Insert
                | Clause::GeneratedColumn => {
                    return Err(ErrorCode::InvalidInputSyntax(format!(
                        "table functions are not allowed in {}",
                        clause
                    ))
                    .into());
                }
                Clause::GroupBy | Clause::From => {}
            }
        }
        Ok(())
    }

    pub(in crate::binder) fn bind_function_expr_arg(
        &mut self,
        arg_expr: FunctionArgExpr,
    ) -> Result<Vec<ExprImpl>> {
        match arg_expr {
            FunctionArgExpr::Expr(expr) => Ok(vec![self.bind_expr_inner(expr)?]),
            FunctionArgExpr::QualifiedWildcard(_, _)
            | FunctionArgExpr::ExprQualifiedWildcard(_, _) => Err(ErrorCode::InvalidInputSyntax(
                format!("unexpected wildcard {}", arg_expr),
            )
            .into()),
            FunctionArgExpr::Wildcard(None) => Ok(vec![]),
            FunctionArgExpr::Wildcard(Some(_)) => unreachable!(),
        }
    }

    pub(in crate::binder) fn bind_function_arg(
        &mut self,
        arg: FunctionArg,
    ) -> Result<Vec<ExprImpl>> {
        match arg {
            FunctionArg::Unnamed(expr) => self.bind_function_expr_arg(expr),
            FunctionArg::Named { .. } => todo!(),
        }
    }
}
