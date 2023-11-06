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

use std::collections::HashMap;
use std::iter::once;
use std::str::FromStr;
use std::sync::LazyLock;

use bk_tree::{metrics, BKTree};
use itertools::Itertools;
use risingwave_common::array::ListValue;
use risingwave_common::catalog::{INFORMATION_SCHEMA_SCHEMA_NAME, PG_CATALOG_SCHEMA_NAME};
use risingwave_common::error::{ErrorCode, Result, RwError};
use risingwave_common::session_config::USER_NAME_WILD_CARD;
use risingwave_common::types::{DataType, ScalarImpl, Timestamptz};
use risingwave_common::{GIT_SHA, RW_VERSION};
use risingwave_expr::aggregate::{agg_kinds, AggKind};
use risingwave_expr::window_function::{
    Frame, FrameBound, FrameBounds, FrameExclusion, WindowFuncKind,
};
use risingwave_sqlparser::ast::{
    self, Function, FunctionArg, FunctionArgExpr, Ident, WindowFrameBound, WindowFrameExclusion,
    WindowFrameUnits, WindowSpec,
};

use crate::binder::bind_context::Clause;
use crate::binder::{Binder, BoundQuery, BoundSetExpr};
use crate::expr::{
    AggCall, Expr, ExprImpl, ExprType, FunctionCall, FunctionCallWithLambda, Literal, Now, OrderBy,
    Subquery, SubqueryKind, TableFunction, TableFunctionType, UserDefinedFunction, WindowFunction,
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
                        return Err(ErrorCode::NotImplemented(
                            format!("Unsupported function name under schema: {}", schema_name),
                            12422.into(),
                        )
                        .into());
                    }
                    function_name
                } else {
                    return Err(ErrorCode::NotImplemented(
                        format!("Unsupported function name under schema: {}", schema_name),
                        12422.into(),
                    )
                    .into());
                }
            }
            _ => {
                return Err(ErrorCode::NotImplemented(
                    format!("qualified function: {}", f.name),
                    112.into(),
                )
                .into());
            }
        };

        // agg calls
        if f.over.is_none()
            && let Ok(kind) = function_name.parse()
        {
            return self.bind_agg(f, kind);
        }

        if f.distinct || !f.order_by.is_empty() || f.filter.is_some() {
            return Err(ErrorCode::InvalidInputSyntax(format!(
                    "DISTINCT, ORDER BY or FILTER is only allowed in aggregation functions, but `{}` is not an aggregation function", function_name
                )
                )
                .into());
        }

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

        let inputs = f
            .args
            .into_iter()
            .map(|arg| self.bind_function_arg(arg))
            .flatten_ok()
            .try_collect()?;

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
            return Err(ErrorCode::NotImplemented(
                format!("Unrecognized window function: {}", function_name),
                8961.into(),
            )
            .into());
        }

        // table function
        if let Ok(function_type) = TableFunctionType::from_str(function_name.as_str()) {
            self.ensure_table_function_allowed()?;
            return Ok(TableFunction::new(function_type, inputs)?.into());
        }

        // user defined function
        // TODO: resolve schema name https://github.com/risingwavelabs/risingwave/issues/12422
        if let Ok(schema) = self.first_valid_schema()
            && let Some(func) = schema.get_function_by_name_args(
                &function_name,
                &inputs.iter().map(|arg| arg.return_type()).collect_vec(),
            )
        {
            use crate::catalog::function_catalog::FunctionKind::*;
            match &func.kind {
                Scalar { .. } => return Ok(UserDefinedFunction::new(func.clone(), inputs).into()),
                Table { .. } => {
                    self.ensure_table_function_allowed()?;
                    return Ok(TableFunction::new_user_defined(func.clone(), inputs).into());
                }
                Aggregate => todo!("support UDAF"),
            }
        }

        self.bind_builtin_scalar_function(function_name.as_str(), inputs)
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

    pub(super) fn bind_agg(&mut self, f: Function, kind: AggKind) -> Result<ExprImpl> {
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
                    return Err(ErrorCode::NotImplemented(
                        "subquery in filter clause".to_string(),
                        None.into(),
                    )
                    .into());
                }
                if expr.has_agg_call() {
                    return Err(ErrorCode::NotImplemented(
                        "aggregation function in filter clause".to_string(),
                        None.into(),
                    )
                    .into());
                }
                if expr.has_table_function() {
                    return Err(ErrorCode::NotImplemented(
                        "table function in filter clause".to_string(),
                        None.into(),
                    )
                    .into());
                }
                Condition::with_expr(expr)
            }
            None => Condition::true_cond(),
        };

        Ok(ExprImpl::AggCall(Box::new(AggCall::new(
            kind,
            args,
            distinct,
            order_by,
            filter,
            direct_args,
        )?)))
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

        let mut direct_args = {
            let args: Vec<_> = f
                .args
                .into_iter()
                .map(|arg| self.bind_function_arg(arg))
                .flatten_ok()
                .try_collect()?;
            if args.iter().any(|arg| arg.as_literal().is_none()) {
                return Err(ErrorCode::NotImplemented(
                    "non-constant direct arguments for ordered-set aggregation is not supported now".to_string(),
                    None.into()
                )
                .into());
            }
            args
        };
        let mut args =
            self.bind_function_expr_arg(FunctionArgExpr::Expr(within_group.expr.clone()))?;
        let order_by = OrderBy::new(vec![self.bind_order_by_expr(within_group)?]);

        // check signature and do implicit cast
        match (kind, direct_args.as_mut_slice(), args.as_mut_slice()) {
            (AggKind::PercentileCont | AggKind::PercentileDisc, [fraction], [arg]) => {
                if fraction.cast_implicit_mut(DataType::Float64).is_ok()
                    && let Ok(casted) = fraction.fold_const()
                {
                    if let Some(ref casted) = casted
                        && !(0.0..=1.0).contains(&casted.as_float64().0)
                    {
                        return Err(ErrorCode::InvalidInputSyntax(format!(
                            "direct arg in `{}` must between 0.0 and 1.0",
                            kind
                        ))
                        .into());
                    }
                    *fraction = Literal::new(casted, DataType::Float64).into();
                } else {
                    return Err(ErrorCode::InvalidInputSyntax(format!(
                        "direct arg in `{}` must be castable to float64",
                        kind
                    ))
                    .into());
                }

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
                return Err(ErrorCode::NotImplemented(
                    "non-constant arguments other than the first one for DISTINCT aggregation is not supported now"
                        .to_string(),
                    None.into(),
                )
                .into());
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
                        return Err(ErrorCode::NotImplemented(
                            format!(
                                "window frame exclusion `{}` is not supported yet",
                                exclusion
                            ),
                            9124.into(),
                        )
                        .into());
                    }
                    WindowFrameExclusion::NoOthers => FrameExclusion::NoOthers,
                }
            } else {
                FrameExclusion::NoOthers
            };
            let bounds = match frame.units {
                WindowFrameUnits::Rows => {
                    let convert_bound = |bound| match bound {
                        WindowFrameBound::CurrentRow => FrameBound::CurrentRow,
                        WindowFrameBound::Preceding(None) => FrameBound::UnboundedPreceding,
                        WindowFrameBound::Preceding(Some(offset)) => {
                            FrameBound::Preceding(offset as usize)
                        }
                        WindowFrameBound::Following(None) => FrameBound::UnboundedFollowing,
                        WindowFrameBound::Following(Some(offset)) => {
                            FrameBound::Following(offset as usize)
                        }
                    };
                    let start = convert_bound(frame.start_bound);
                    let end = if let Some(end_bound) = frame.end_bound {
                        convert_bound(end_bound)
                    } else {
                        FrameBound::CurrentRow
                    };
                    FrameBounds::Rows(start, end)
                }
                WindowFrameUnits::Range | WindowFrameUnits::Groups => {
                    return Err(ErrorCode::NotImplemented(
                        format!(
                            "window frame in `{}` mode is not supported yet",
                            frame.units
                        ),
                        9124.into(),
                    )
                    .into());
                }
            };
            if !bounds.is_valid() {
                return Err(ErrorCode::InvalidInputSyntax(format!(
                    "window frame bounds `{bounds}` is not valid",
                ))
                .into());
            }
            Some(Frame { bounds, exclusion })
        } else {
            None
        };
        Ok(WindowFunction::new(kind, partition_by, order_by, inputs, frame)?.into())
    }

    fn bind_builtin_scalar_function(
        &mut self,
        function_name: &str,
        inputs: Vec<ExprImpl>,
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
                        (1, raw_call(ExprType::ToTimestamp)),
                        (2, raw_call(ExprType::ToTimestamp1)),
                    ]),
                ),
                ("date_trunc", raw_call(ExprType::DateTrunc)),
                ("date_part", raw_call(ExprType::DatePart)),
                ("to_date", raw_call(ExprType::CharToDate)),
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
                ("string_to_array", raw_call(ExprType::StringToArray)),
                ("encode", raw_call(ExprType::Encode)),
                ("decode", raw_call(ExprType::Decode)),
                ("sha1", raw_call(ExprType::Sha1)),
                ("sha224", raw_call(ExprType::Sha224)),
                ("sha256", raw_call(ExprType::Sha256)),
                ("sha384", raw_call(ExprType::Sha384)),
                ("sha512", raw_call(ExprType::Sha512)),
                ("left", raw_call(ExprType::Left)),
                ("right", raw_call(ExprType::Right)),
                ("int8send", raw_call(ExprType::PgwireSend)),
                ("int8recv", guard_by_len(1, raw(|_binder, mut inputs| {
                    // Similar to `cast` from string, return type is set explicitly rather than inferred.
                    let hint = if !inputs[0].is_untyped() && inputs[0].return_type() == DataType::Varchar {
                        " Consider `decode` or cast."
                    } else {
                        ""
                    };
                    inputs[0].cast_implicit_mut(DataType::Bytea).map_err(|e| {
                        ErrorCode::BindError(format!("{e} in `recv`.{hint}"))
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
                        let ndims_expr = binder.bind_builtin_scalar_function("array_ndims", vec![arg0])?;
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
                ("jsonb_extract_path", raw(|_binder, mut inputs| {
                    // rewrite: jsonb_extract_path(jsonb, s1, s2...)
                    // to:      jsonb_extract_path(jsonb, array[s1, s2...])
                    if inputs.len() < 2 {
                        return Err(ErrorCode::ExprError("unexpected arguments number".into()).into());
                    }
                    inputs[0].cast_implicit_mut(DataType::Jsonb)?;
                    let mut variadic_inputs = inputs.split_off(1);
                    for input in &mut variadic_inputs {
                        input.cast_implicit_mut(DataType::Varchar)?;
                    }
                    let array = FunctionCall::new_unchecked(ExprType::Array, variadic_inputs, DataType::List(Box::new(DataType::Varchar)));
                    inputs.push(array.into());
                    Ok(FunctionCall::new_unchecked(ExprType::JsonbExtractPath, inputs, DataType::Jsonb).into())
                })),
                ("jsonb_extract_path_text", raw(|_binder, mut inputs| {
                    // rewrite: jsonb_extract_path_text(jsonb, s1, s2...)
                    // to:      jsonb_extract_path_text(jsonb, array[s1, s2...])
                    if inputs.len() < 2 {
                        return Err(ErrorCode::ExprError("unexpected arguments number".into()).into());
                    }
                    inputs[0].cast_implicit_mut(DataType::Jsonb)?;
                    let mut variadic_inputs = inputs.split_off(1);
                    for input in &mut variadic_inputs {
                        input.cast_implicit_mut(DataType::Varchar)?;
                    }
                    let array = FunctionCall::new_unchecked(ExprType::Array, variadic_inputs, DataType::List(Box::new(DataType::Varchar)));
                    inputs.push(array.into());
                    Ok(FunctionCall::new_unchecked(ExprType::JsonbExtractPathText, inputs, DataType::Varchar).into())
                })),
                ("jsonb_typeof", raw_call(ExprType::JsonbTypeof)),
                ("jsonb_array_length", raw_call(ExprType::JsonbArrayLength)),
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
                        return Err(ErrorCode::NotImplemented(
                            "Only boolean literals are supported in `current_schemas`.".to_string(), None.into()
                        )
                        .into());
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
                            schema_names.push(Some(schema_name.into()));
                        }
                    }

                    Ok(ExprImpl::literal_list(
                        ListValue::new(schema_names),
                        DataType::Varchar,
                    ))
                })),
                ("session_user", session_user()),
                ("current_role", current_user()),
                ("current_user", current_user()),
                ("user", current_user()),
                ("pg_get_userbyid", guard_by_len(1, raw(|binder, inputs|{
                        let input = &inputs[0];
                        let bound_query = binder.bind_get_user_by_id_select(input)?;
                        Ok(ExprImpl::Subquery(Box::new(Subquery::new(
                            BoundQuery {
                                body: BoundSetExpr::Select(Box::new(bound_query)),
                                order: vec![],
                                limit: None,
                                offset: None,
                                with_ties: false,
                                extra_order_exprs: vec![],
                            },
                            SubqueryKind::Scalar,
                        ))))
                    }
                ))),
                ("pg_relation_size", dispatch_by_len(vec![
                    (1, raw(|binder, inputs|{
                        let table_name = &inputs[0];
                        let bound_query = binder.bind_get_table_size_select("pg_relation_size", table_name)?;
                        Ok(ExprImpl::Subquery(Box::new(Subquery::new(
                            BoundQuery {
                                body: BoundSetExpr::Select(Box::new(bound_query)),
                                order: vec![],
                                limit: None,
                                offset: None,
                                with_ties: false,
                                extra_order_exprs: vec![],
                            },
                            SubqueryKind::Scalar,
                        ))))
                    })),
                    (2, raw(|binder, inputs|{
                        let table_name = &inputs[0];
                        match inputs[1].as_literal() {
                            Some(literal) if literal.return_type() == DataType::Varchar => {
                                match literal
                                     .get_data()
                                     .as_ref()
                                     .expect("ExprImpl value is a Literal but cannot get ref to data")
                                     .as_utf8()
                                     .as_ref() {
                                        "main" => {
                                            let bound_query = binder.bind_get_table_size_select("pg_relation_size", table_name)?;
                                            Ok(ExprImpl::Subquery(Box::new(Subquery::new(
                                                BoundQuery {
                                                    body: BoundSetExpr::Select(Box::new(bound_query)),
                                                    order: vec![],
                                                    limit: None,
                                                    offset: None,
                                                    with_ties: false,
                                                    extra_order_exprs: vec![],
                                                },
                                                SubqueryKind::Scalar,
                                            ))))
                                        },
                                        // These options are invalid in RW so we return 0 value as the result
                                        "fsm"|"vm"|"init" => {
                                            Ok(ExprImpl::literal_int(0))
                                        },
                                        _ => Err(ErrorCode::InvalidInputSyntax(
                                            "invalid fork name. Valid fork names are \"main\", \"fsm\", \"vm\", and \"init\"".into()).into())
                                    }
                            },
                            _ => Err(ErrorCode::ExprError(
                                "The 2nd argument of `pg_relation_size` must be string literal.".into(),
                            )
                            .into())
                        }
                    })),
                    ]
                )),
                ("pg_table_size", guard_by_len(1, raw(|binder, inputs|{
                        let input = &inputs[0];
                        let bound_query = binder.bind_get_table_size_select("pg_table_size", input)?;
                        Ok(ExprImpl::Subquery(Box::new(Subquery::new(
                            BoundQuery {
                                body: BoundSetExpr::Select(Box::new(bound_query)),
                                order: vec![],
                                limit: None,
                                offset: None,
                                with_ties: false,
                                extra_order_exprs: vec![],
                            },
                            SubqueryKind::Scalar,
                        ))))
                    }
                ))),
                ("pg_indexes_size", guard_by_len(1, raw(|binder, inputs|{
                        let input = &inputs[0];
                        let bound_query = binder.bind_get_indexes_size_select(input)?;
                        Ok(ExprImpl::Subquery(Box::new(Subquery::new(
                            BoundQuery {
                                body: BoundSetExpr::Select(Box::new(bound_query)),
                                order: vec![],
                                limit: None,
                                offset: None,
                                with_ties: false,
                                extra_order_exprs: vec![],
                            },
                            SubqueryKind::Scalar,
                        ))))
                    }
                ))),
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
                    session_config.set(setting_name, vec![new_value.to_string()], ())?;

                    Ok(ExprImpl::literal_varchar(new_value.to_string()))
                }))),
                ("format_type", raw_call(ExprType::FormatType)),
                ("pg_table_is_visible", raw_literal(ExprImpl::literal_bool(true))),
                ("pg_type_is_visible", raw_literal(ExprImpl::literal_bool(true))),
                ("pg_get_constraintdef", raw_literal(ExprImpl::literal_null(DataType::Varchar))),
                ("pg_encoding_to_char", raw_literal(ExprImpl::literal_varchar("UTF8".into()))),
                ("has_database_privilege", raw_literal(ExprImpl::literal_bool(true))),
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
                // TODO: choose which pg version we should return.
                ("version", raw_literal(ExprImpl::literal_varchar(format!(
                    "PostgreSQL 9.5-RisingWave-{} ({})",
                    RW_VERSION,
                    GIT_SHA
                )))),
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

            // TODO: Also hint other functinos, e,g, Agg or UDF.
            for k in HANDLES.keys() {
                tree.add(*k);
            }

            tree
        });

        match HANDLES.get(function_name) {
            Some(handle) => handle(self, inputs),
            None => Err({
                let allowed_distance = if function_name.len() > 3 { 2 } else { 1 };

                let candidates = FUNCTIONS_BKTREE
                    .find(function_name, allowed_distance)
                    .map(|(_idx, c)| c);

                let mut candidates = candidates.peekable();

                let err_msg = if candidates.peek().is_none() {
                    format!("unsupported function: \"{}\"", function_name)
                } else {
                    format!(
                        "unsupported function \"{}\", do you mean \"{}\"?",
                        function_name,
                        candidates.join(" or ")
                    )
                };

                ErrorCode::NotImplemented(err_msg, 112.into()).into()
            }),
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
                Some(Clause::Where) | Some(Clause::Having) | Some(Clause::JoinOn)
            )
        {
            return Err(ErrorCode::InvalidInputSyntax(format!(
                "For streaming queries, `NOW()` function is only allowed in `WHERE`, `HAVING` and `ON`. Found in clause: {:?}. Please please refer to https://www.risingwave.dev/docs/current/sql-pattern-temporal-filters/ for more information",
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
            FunctionArgExpr::QualifiedWildcard(_, _) => todo!(),
            FunctionArgExpr::ExprQualifiedWildcard(_, _) => todo!(),
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
