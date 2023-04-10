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
use risingwave_common::catalog::PG_CATALOG_SCHEMA_NAME;
use risingwave_common::error::{ErrorCode, Result};
use risingwave_common::session_config::USER_NAME_WILD_CARD;
use risingwave_common::types::{DataType, ScalarImpl};
use risingwave_common::{GIT_SHA, RW_VERSION};
use risingwave_expr::expr::AggKind;
use risingwave_sqlparser::ast::{Function, FunctionArg, FunctionArgExpr, WindowSpec};

use crate::binder::bind_context::Clause;
use crate::binder::{Binder, BoundQuery, BoundSetExpr};
use crate::expr::{
    AggCall, Expr, ExprImpl, ExprType, FunctionCall, Literal, OrderBy, Subquery, SubqueryKind,
    TableFunction, TableFunctionType, UserDefinedFunction, WindowFunction, WindowFunctionType,
};
use crate::utils::Condition;

impl Binder {
    pub(super) fn bind_function(&mut self, f: Function) -> Result<ExprImpl> {
        let function_name = match f.name.0.as_slice() {
            [name] => name.real_value(),
            [schema, name] => {
                let schema_name = schema.real_value();
                if schema_name == PG_CATALOG_SCHEMA_NAME {
                    name.real_value()
                } else {
                    return Err(ErrorCode::BindError(format!(
                        "Unsupported function name under schema: {}",
                        schema_name
                    ))
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
        if let Ok(kind) = function_name.parse() {
            if f.over.is_some() {
                return Err(ErrorCode::NotImplemented(
                    format!("aggregate function as over window function: {}", kind),
                    4978.into(),
                )
                .into());
            }
            return self.bind_agg(f, kind);
        }

        if f.distinct || !f.order_by.is_empty() || f.filter.is_some() {
            return Err(ErrorCode::InvalidInputSyntax(format!(
                    "DISTINCT, ORDER BY or FILTER is only allowed in aggregation functions, but `{}` is not an aggregation function", function_name
                )
                )
                .into());
        }

        let inputs = f
            .args
            .into_iter()
            .map(|arg| self.bind_function_arg(arg))
            .flatten_ok()
            .try_collect()?;

        // window function
        if let Some(window_spec) = f.over {
            return self.bind_window_function(window_spec, function_name, inputs);
        }

        // table function
        let table_function_type = TableFunctionType::from_str(function_name.as_str());
        if let Ok(function_type) = table_function_type {
            self.ensure_table_function_allowed()?;
            return Ok(TableFunction::new(function_type, inputs)?.into());
        }

        // user defined function
        // TODO: resolve schema name
        if let Some(func) = self
            .catalog
            .first_valid_schema(
                &self.db_name,
                &self.search_path,
                &self.auth_context.user_name,
            )?
            .get_function_by_name_args(
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

    pub(super) fn bind_agg(&mut self, mut f: Function, kind: AggKind) -> Result<ExprImpl> {
        self.ensure_aggregate_allowed()?;
        let inputs: Vec<ExprImpl> = f
            .args
            .into_iter()
            .map(|arg| self.bind_function_arg(arg))
            .flatten_ok()
            .try_collect()?;
        if f.distinct {
            match &kind {
                AggKind::Count if inputs.is_empty() => {
                    // count(distinct *) is disallowed because of unclear semantics.
                    return Err(ErrorCode::InvalidInputSyntax(
                        "count(distinct *) is disallowed".to_string(),
                    )
                    .into());
                }
                AggKind::ApproxCountDistinct => {
                    // approx_count_distinct(distinct ..) is disallowed because this defeats
                    // its purpose of trading accuracy for
                    // speed.
                    return Err(ErrorCode::InvalidInputSyntax(
                        "approx_count_distinct(distinct) is disallowed.\n\
                        Remove distinct for speed or just use count(distinct) for accuracy"
                            .into(),
                    )
                    .into());
                }
                AggKind::Max | AggKind::Min => {
                    // distinct max or min returns the same result as non-distinct max or min.
                    f.distinct = false;
                }
                _ => (),
            };
        }

        let filter = match f.filter {
            Some(filter) => {
                let mut clause = Some(Clause::Filter);
                std::mem::swap(&mut self.context.clause, &mut clause);
                let expr = self
                    .bind_expr(*filter)
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

        if f.distinct && !f.order_by.is_empty() {
            // <https://www.postgresql.org/docs/current/sql-expressions.html#SYNTAX-AGGREGATES:~:text=the%20DISTINCT%20list.-,Note,-The%20ability%20to>
            return Err(ErrorCode::InvalidInputSyntax(
                "DISTINCT and ORDER BY are not supported to appear at the same time now"
                    .to_string(),
            )
            .into());
        }
        let order_by = OrderBy::new(
            f.order_by
                .into_iter()
                .map(|e| self.bind_order_by_expr(e))
                .try_collect()?,
        );
        Ok(ExprImpl::AggCall(Box::new(AggCall::new(
            kind, inputs, f.distinct, order_by, filter,
        )?)))
    }

    pub(super) fn bind_window_function(
        &mut self,
        WindowSpec {
            partition_by,
            order_by,
            window_frame,
        }: WindowSpec,
        function_name: String,
        inputs: Vec<ExprImpl>,
    ) -> Result<ExprImpl> {
        self.ensure_window_function_allowed()?;
        if let Some(window_frame) = window_frame {
            return Err(ErrorCode::NotImplemented(
                format!("window frame: {}", window_frame),
                None.into(),
            )
            .into());
        }
        let window_function_type = WindowFunctionType::from_str(&function_name)?;
        let partition_by = partition_by
            .into_iter()
            .map(|arg| self.bind_expr(arg))
            .try_collect()?;

        let order_by = OrderBy::new(
            order_by
                .into_iter()
                .map(|order_by_expr| self.bind_order_by_expr(order_by_expr))
                .collect::<Result<_>>()?,
        );
        Ok(WindowFunction::new(window_function_type, partition_by, order_by, inputs)?.into())
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
            Box::new(move |binder, mut inputs| {
                binder.ensure_now_function_allowed()?;
                if !binder.in_streaming {
                    inputs.push(ExprImpl::from(Literal::new(
                        Some(ScalarImpl::Int64((binder.bind_timestamp_ms * 1000) as i64)),
                        DataType::Timestamptz,
                    )));
                }
                raw_call(ExprType::Now)(binder, inputs)
            })
        }
        fn pi() -> Handle {
            raw_literal(ExprImpl::literal_f64(std::f64::consts::PI))
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
                ("coalesce", raw_call(ExprType::Coalesce)),
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
                ("abs", raw_call(ExprType::Abs)),
                ("exp", raw_call(ExprType::Exp)),
                ("mod", raw_call(ExprType::Modulus)),
                ("sin", raw_call(ExprType::Sin)),
                ("cos", raw_call(ExprType::Cos)), 
                ("tan", raw_call(ExprType::Tan)), 
                ("cot", raw_call(ExprType::Cot)), 
                ("asin", raw_call(ExprType::Asin)), 
                ("acos", raw_call(ExprType::Acos)), 
                ("atan", raw_call(ExprType::Atan)), 
                ("atan2", raw_call(ExprType::Atan2)),
                ("degrees", raw_call(ExprType::Degrees)),
                ("radians", raw_call(ExprType::Radians)),

                (
                    "to_timestamp",
                    dispatch_by_len(vec![
                        (1, raw_call(ExprType::ToTimestamp)),
                        (2, raw_call(ExprType::ToTimestamp1)),
                    ]),
                ),
                ("date_trunc", raw_call(ExprType::DateTrunc)),
                ("date_part", raw_call(ExprType::DatePart)),
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
                ("translate", raw_call(ExprType::Translate)),
                ("split_part", raw_call(ExprType::SplitPart)),
                ("char_length", raw_call(ExprType::CharLength)),
                ("character_length", raw_call(ExprType::CharLength)),
                ("repeat", raw_call(ExprType::Repeat)),
                ("ascii", raw_call(ExprType::Ascii)),
                ("octet_length", raw_call(ExprType::OctetLength)),
                ("bit_length", raw_call(ExprType::BitLength)),
                ("regexp_match", raw_call(ExprType::RegexpMatch)),
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
                // array
                ("array_cat", raw_call(ExprType::ArrayCat)),
                ("array_append", raw_call(ExprType::ArrayAppend)),
                ("array_join", raw_call(ExprType::ArrayToString)),
                ("array_prepend", raw_call(ExprType::ArrayPrepend)),
                ("array_to_string", raw_call(ExprType::ArrayToString)),
                ("array_distinct", raw_call(ExprType::ArrayDistinct)),
                ("array_length", raw_call(ExprType::ArrayLength)),
                ("cardinality", raw_call(ExprType::Cardinality)),
                // jsonb
                ("jsonb_object_field", raw_call(ExprType::JsonbAccessInner)),
                ("jsonb_array_element", raw_call(ExprType::JsonbAccessInner)),
                ("jsonb_object_field_text", raw_call(ExprType::JsonbAccessStr)),
                ("jsonb_array_element_text", raw_call(ExprType::JsonbAccessStr)),
                ("jsonb_typeof", raw_call(ExprType::JsonbTypeof)),
                ("jsonb_array_length", raw_call(ExprType::JsonbArrayLength)),
                // Functions that return a constant value
                ("pi", pi()),
                // System information operations.
                (
                    "pg_typeof",
                    guard_by_len(1, raw(|_binder, inputs| {
                        let input = &inputs[0];
                        let v = match input.is_unknown() {
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
                        .catalog
                        .first_valid_schema(
                            &binder.db_name,
                            &binder.search_path,
                            &binder.auth_context.user_name,
                        )
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
                        return Ok(ExprImpl::literal_null(DataType::List {
                            datatype: Box::new(DataType::Varchar),
                        }));
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
                ("session_user", guard_by_len(0, raw(|binder, _inputs| {
                    Ok(ExprImpl::literal_varchar(
                        binder.auth_context.user_name.clone(),
                    ))
                }))),
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
                ("format_type", raw_call(ExprType::FormatType)),
                ("pg_table_is_visible", raw_literal(ExprImpl::literal_bool(true))),
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
                // internal
                ("rw_vnode", raw_call(ExprType::Vnode)),
                // TODO: choose which pg version we should return.
                ("version", raw_literal(ExprImpl::literal_varchar(format!(
                    "PostgreSQL 13.9-RisingWave-{} ({})",
                    RW_VERSION,
                    GIT_SHA
                )))),
                // non-deterministic
                ("now", now()),
                ("current_timestamp", now())
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
            Some(handle) => {
                tracing::info!("get function handle successfully");
                handle(self, inputs)
            }
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
                "Function `Concat` takes at least 1 arguments (0 given)".to_string(),
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
            Err(ErrorCode::BindError("Nullif function must contain 2 arguments".to_string()).into())
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
                | Clause::Filter => {
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
        if self.in_streaming
            && !matches!(
                self.context.clause,
                Some(Clause::Where) | Some(Clause::Having)
            )
        {
            return Err(ErrorCode::InvalidInputSyntax(format!(
                "For creation of materialized views, `NOW()` function is only allowed in `WHERE` and `HAVING`. Found in clause: {:?}",
                self.context.clause
            ))
            .into());
        }
        Ok(())
    }

    fn ensure_aggregate_allowed(&self) -> Result<()> {
        if let Some(clause) = self.context.clause {
            match clause {
                Clause::Where | Clause::Values => {
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
                Clause::Where | Clause::Values => {
                    return Err(ErrorCode::InvalidInputSyntax(format!(
                        "table functions are not allowed in {}",
                        clause
                    ))
                    .into());
                }
                Clause::GroupBy | Clause::Having | Clause::Filter => {}
            }
        }
        Ok(())
    }

    pub(in crate::binder) fn bind_function_expr_arg(
        &mut self,
        arg_expr: FunctionArgExpr,
    ) -> Result<Vec<ExprImpl>> {
        match arg_expr {
            FunctionArgExpr::Expr(expr) => Ok(vec![self.bind_expr(expr)?]),
            FunctionArgExpr::QualifiedWildcard(_) => todo!(),
            FunctionArgExpr::ExprQualifiedWildcard(_, _) => todo!(),
            FunctionArgExpr::Wildcard => Ok(vec![]),
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
