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
use std::str::FromStr;
use std::sync::Arc;

use itertools::Itertools;
use risingwave_common::bail_not_implemented;
use risingwave_common::catalog::{INFORMATION_SCHEMA_SCHEMA_NAME, PG_CATALOG_SCHEMA_NAME};
use risingwave_common::types::DataType;
use risingwave_expr::aggregate::AggKind;
use risingwave_expr::window_function::WindowFuncKind;
use risingwave_sqlparser::ast::{self, Function, FunctionArg, FunctionArgExpr, Ident};
use risingwave_sqlparser::parser::ParserError;

use crate::binder::bind_context::Clause;
use crate::binder::{Binder, UdfContext};
use crate::error::{ErrorCode, Result, RwError};
use crate::expr::{
    Expr, ExprImpl, ExprType, FunctionCallWithLambda, InputRef, TableFunction, TableFunctionType,
    UserDefinedFunction,
};

mod aggregate;
mod builtin_scalar;
mod window;

// Defines system functions that without args, ref: https://www.postgresql.org/docs/current/functions-info.html
const SYS_FUNCTION_WITHOUT_ARGS: &[&str] = &[
    "session_user",
    "user",
    "current_user",
    "current_role",
    "current_catalog",
    "current_schema",
    "current_timestamp",
];

pub(super) fn is_sys_function_without_args(ident: &Ident) -> bool {
    SYS_FUNCTION_WITHOUT_ARGS
        .iter()
        .any(|e| ident.real_value().as_str() == *e && ident.quote_style().is_none())
}

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
            .arg_list
            .args
            .iter()
            .map(|arg| self.bind_function_arg(arg.clone()))
            .flatten_ok()
            .try_collect()?;

        // `aggregate:` on a scalar function
        if f.scalar_as_agg {
            let mut scalar_inputs = inputs
                .iter()
                .enumerate()
                .map(|(i, expr)| {
                    InputRef::new(i, DataType::List(Box::new(expr.return_type()))).into()
                })
                .collect_vec();
            let scalar: ExprImpl = if let Ok(schema) = self.first_valid_schema()
                && let Some(func) =
                    schema.get_function_by_name_inputs(&function_name, &mut scalar_inputs)
            {
                if !func.kind.is_scalar() {
                    return Err(ErrorCode::InvalidInputSyntax(
                        "expect a scalar function after `aggregate:`".to_string(),
                    )
                    .into());
                }
                UserDefinedFunction::new(func.clone(), scalar_inputs).into()
            } else {
                self.bind_builtin_scalar_function(
                    &function_name,
                    scalar_inputs,
                    f.arg_list.variadic,
                )?
            };
            return self.bind_aggregate_function(f, AggKind::WrapScalar(scalar.to_expr_proto()));
        }

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
                let Ok(context) =
                    UdfContext::create_udf_context(&f.arg_list.args, &Arc::clone(func))
                else {
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
                        return self.bind_aggregate_function(
                            f,
                            AggKind::UserDefined(func.as_ref().into()),
                        );
                    }
                }
            }
        }

        // agg calls
        if f.over.is_none()
            && let Ok(kind) = function_name.parse()
        {
            return self.bind_aggregate_function(f, AggKind::Builtin(kind));
        }

        if f.arg_list.distinct || !f.arg_list.order_by.is_empty() || f.filter.is_some() {
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

        // file_scan table function
        if function_name.eq_ignore_ascii_case("file_scan") {
            self.ensure_table_function_allowed()?;
            return Ok(TableFunction::new_file_scan(inputs)?.into());
        }
        // table function
        if let Ok(function_type) = TableFunctionType::from_str(function_name.as_str()) {
            self.ensure_table_function_allowed()?;
            return Ok(TableFunction::new(function_type, inputs)?.into());
        }

        self.bind_builtin_scalar_function(function_name.as_str(), inputs, f.arg_list.variadic)
    }

    fn bind_array_transform(&mut self, f: Function) -> Result<ExprImpl> {
        let [array, lambda] =
            <[FunctionArg; 2]>::try_from(f.arg_list.args).map_err(|args| -> RwError {
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
