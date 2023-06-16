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
use risingwave_common::error::{ErrorCode, Result, RwError};
use risingwave_common::session_config::USER_NAME_WILD_CARD;
use risingwave_common::types::{DataType, ScalarImpl};
use risingwave_common::{GIT_SHA, RW_VERSION};
use risingwave_expr::agg::AggKind;
use risingwave_expr::function::window::{
    Frame, FrameBound, FrameBounds, FrameExclusion, WindowFuncKind,
};
use risingwave_sqlparser::ast::{
    Function, FunctionArg, FunctionArgExpr, WindowFrameBound, WindowFrameExclusion,
    WindowFrameUnits, WindowSpec,
};

use crate::binder::bind_context::Clause;
use crate::binder::{Binder, BoundQuery, BoundSetExpr};
use crate::expr::{
    AggCall, Expr, ExprImpl, ExprType, FunctionCall, Literal, Now, OrderBy, Subquery, SubqueryKind,
    TableFunction, TableFunctionType, UserDefinedFunction, WindowFunction,
};
use crate::utils::Condition;

impl Binder {
    pub(in crate::binder) fn bind_function(&mut self, f: Function) -> Result<ExprImpl> {
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
        if f.over.is_none() && let Ok(kind) = function_name.parse() {
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
        // TODO: resolve schema name
        if let Some(func) = self.first_valid_schema()?.get_function_by_name_args(
            &function_name,
            &inputs.iter().map(|arg| arg.return_type()).collect_vec(),
        ) {
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
        if matches!(
            kind,
            AggKind::PercentileCont | AggKind::PercentileDisc | AggKind::Mode
        ) {
            if f.within_group.is_none() {
                return Err(ErrorCode::InvalidInputSyntax(format!(
                    "within group is expected for the {}",
                    kind
                ))
                .into());
            }
        } else if f.within_group.is_some() {
            return Err(ErrorCode::InvalidInputSyntax(format!(
                "within group is disallowed for the {}",
                kind
            ))
            .into());
        }
        if kind == AggKind::Mode && !f.args.is_empty() {
            return Err(ErrorCode::InvalidInputSyntax(
                "no arguments are expected in mode agg".to_string(),
            )
            .into());
        }
        self.ensure_aggregate_allowed()?;
        let mut inputs: Vec<ExprImpl> = if f.within_group.is_some() {
            f.within_group
                .iter()
                .map(|x| self.bind_function_expr_arg(FunctionArgExpr::Expr(x.expr.clone())))
                .flatten_ok()
                .try_collect()?
        } else {
            f.args
                .iter()
                .map(|arg| self.bind_function_arg(arg.clone()))
                .flatten_ok()
                .try_collect()?
        };
        if kind == AggKind::PercentileCont {
            inputs[0] = inputs
                .iter()
                .exactly_one()
                .unwrap()
                .clone()
                .cast_implicit(DataType::Float64)?;
        }

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

        if f.distinct && !f.order_by.is_empty() {
            // <https://www.postgresql.org/docs/current/sql-expressions.html#SYNTAX-AGGREGATES:~:text=the%20DISTINCT%20list.-,Note,-The%20ability%20to>
            return Err(ErrorCode::InvalidInputSyntax(
                "DISTINCT and ORDER BY are not supported to appear at the same time now"
                    .to_string(),
            )
            .into());
        }
        let order_by = if f.within_group.is_some() {
            if !f.order_by.is_empty() {
                return Err(ErrorCode::InvalidInputSyntax(format!(
                    "order_by clause outside of within group is disallowed in {}",
                    kind
                ))
                .into());
            }
            OrderBy::new(
                f.within_group
                    .iter()
                    .map(|x| self.bind_order_by_expr(*x.clone()))
                    .try_collect()?,
            )
        } else {
            OrderBy::new(
                f.order_by
                    .into_iter()
                    .map(|e| self.bind_order_by_expr(e))
                    .try_collect()?,
            )
        };
        let direct_args = if matches!(kind, AggKind::PercentileCont | AggKind::PercentileDisc) {
            let args =
                self.bind_function_arg(f.args.into_iter().exactly_one().map_err(|_| {
                    ErrorCode::InvalidInputSyntax(format!("only one arg is expected in {}", kind))
                })?)?;
            if args.len() != 1 || args[0].clone().as_literal().is_none() {
                Err(
                    ErrorCode::InvalidInputSyntax(format!("arg in {} must be constant", kind))
                        .into(),
                )
            } else if let Ok(casted) = args[0]
                .clone()
                .cast_implicit(DataType::Float64)?
                .fold_const()
            {
                if casted
                    .clone()
                    .is_some_and(|x| !(0.0..=1.0).contains(&Into::<f64>::into(*x.as_float64())))
                {
                    Err(ErrorCode::InvalidInputSyntax(format!(
                        "arg in {} must between 0 and 1",
                        kind
                    ))
                    .into())
                } else {
                    Ok::<_, RwError>(vec![Literal::new(casted, DataType::Float64)])
                }
            } else {
                Err(
                    ErrorCode::InvalidInputSyntax(format!("arg in {} must be float64", kind))
                        .into(),
                )
            }
        } else {
            Ok(vec![])
        }?;
        Ok(ExprImpl::AggCall(Box::new(AggCall::new(
            kind,
            inputs,
            f.distinct,
            order_by,
            filter,
            direct_args,
        )?)))
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
                ("string_to_array", raw_call(ExprType::StringToArray)),
                ("encode", raw_call(ExprType::Encode)),
                ("decode", raw_call(ExprType::Decode)),
                ("sha1", raw_call(ExprType::Sha1)),
                ("sha224", raw_call(ExprType::Sha224)),
                ("sha256", raw_call(ExprType::Sha256)),
                ("sha384", raw_call(ExprType::Sha384)),
                ("sha512", raw_call(ExprType::Sha512)),
                // array
                ("array_cat", raw_call(ExprType::ArrayCat)),
                ("array_append", raw_call(ExprType::ArrayAppend)),
                ("array_join", raw_call(ExprType::ArrayToString)),
                ("array_prepend", raw_call(ExprType::ArrayPrepend)),
                ("array_to_string", raw_call(ExprType::ArrayToString)),
                ("array_distinct", raw_call(ExprType::ArrayDistinct)),
                ("array_length", raw_call(ExprType::ArrayLength)),
                ("cardinality", raw_call(ExprType::Cardinality)),
                ("array_remove", raw_call(ExprType::ArrayRemove)),
                ("array_replace", raw_call(ExprType::ArrayReplace)),
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
                ("current_setting", guard_by_len(1, raw(|binder, inputs| {
                    let input = &inputs[0];
                    let ExprImpl::Literal(literal) = input else {
                        return Err(ErrorCode::ExprError(
                            "Only literal is supported in `current_setting`.".into(),
                        )
                        .into());
                    };
                    let Some(ScalarImpl::Utf8(input)) = literal.get_data() else {
                        return Err(ErrorCode::ExprError(
                            "Only string literal is supported in `current_setting`.".into(),
                        )
                        .into());
                    };
                    match binder.session_config.get(input.as_ref()) {
                        Some(setting) => Ok(ExprImpl::literal_varchar(setting.into())),
                        None => Err(ErrorCode::UnrecognizedConfigurationParameter(input.to_string()).into()),
                    }
                }))),
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
                ("pg_tablespace_location", guard_by_len(1, raw_literal(ExprImpl::literal_null(DataType::Varchar)))),
                // internal
                ("rw_vnode", raw_call(ExprType::Vnode)),
                // TODO: choose which pg version we should return.
                ("version", raw_literal(ExprImpl::literal_varchar(format!(
                    "PostgreSQL 8.3-RisingWave-{} ({})",
                    RW_VERSION,
                    GIT_SHA
                )))),
                // non-deterministic
                ("now", now()),
                ("current_timestamp", now()),
                ("proctime", proctime())
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
                | Clause::Filter
                | Clause::From => {
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
                Clause::Where | Clause::Values | Clause::From => {
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
                Clause::GroupBy | Clause::Having | Clause::Filter | Clause::From => {}
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
