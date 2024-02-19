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

use fancy_regex::Regex;
use itertools::Itertools;
use pgwire::pg_response::StatementType;
use risingwave_common::catalog::FunctionId;
use risingwave_common::types::DataType;
use risingwave_pb::catalog::function::{Kind, ScalarFunction, TableFunction};
use risingwave_pb::catalog::Function;
use risingwave_sqlparser::ast::{
    CreateFunctionBody, FunctionDefinition, ObjectName, OperateFunctionArg,
};
use risingwave_sqlparser::parser::{Parser, ParserError};

use super::*;
use crate::binder::UdfContext;
use crate::catalog::CatalogError;
use crate::expr::{Expr, ExprImpl, Literal};
use crate::{bind_data_type, Binder};

const DEFAULT_ERR_MSG: &str = "Failed to conduct semantic check";

const PROMPT: &str = "In SQL UDF definition: ";

const SQL_UDF_PATTERN: &str = "[sql udf]";

/// Create a mock `udf_context`, which is used for semantic check
fn create_mock_udf_context(
    arg_types: Vec<DataType>,
    arg_names: Vec<String>,
) -> HashMap<String, ExprImpl> {
    let mut ret: HashMap<String, ExprImpl> = (1..=arg_types.len())
        .map(|i| {
            let mock_expr =
                ExprImpl::Literal(Box::new(Literal::new(None, arg_types[i - 1].clone())));
            (format!("${i}"), mock_expr)
        })
        .collect();

    for (i, arg_name) in arg_names.into_iter().enumerate() {
        let mock_expr = ExprImpl::Literal(Box::new(Literal::new(None, arg_types[i].clone())));
        ret.insert(arg_name, mock_expr);
    }

    ret
}

/// Find the pattern for better hint display
/// return the exact index where the pattern first appears
fn find_target(input: &str, target: &str) -> Option<usize> {
    // Regex pattern to find `target` as a single word without any preceding or following characters
    // here `\b` asserts a word boundary
    let pattern = format!(r"\b{}\b", fancy_regex::escape(target));
    let re = Regex::new(&pattern).unwrap();

    let Ok(Some(ma)) = re.find(input) else {
        return None;
    };

    Some(ma.start())
}

pub async fn handle_create_sql_function(
    handler_args: HandlerArgs,
    or_replace: bool,
    temporary: bool,
    name: ObjectName,
    args: Option<Vec<OperateFunctionArg>>,
    returns: Option<CreateFunctionReturns>,
    params: CreateFunctionBody,
) -> Result<RwPgResponse> {
    if or_replace {
        bail_not_implemented!("CREATE OR REPLACE FUNCTION");
    }

    if temporary {
        bail_not_implemented!("CREATE TEMPORARY FUNCTION");
    }

    let language = "sql".to_string();

    // Just a basic sanity check for `language`
    if !matches!(params.language, Some(lang) if lang.real_value().to_lowercase() == "sql") {
        return Err(ErrorCode::InvalidParameterValue(
            "`language` for sql udf must be `sql`".to_string(),
        )
        .into());
    }

    // SQL udf function supports both single quote (i.e., as 'select $1 + $2')
    // and double dollar (i.e., as $$select $1 + $2$$) for as clause
    let body = match &params.as_ {
        Some(FunctionDefinition::SingleQuotedDef(s)) => s.clone(),
        Some(FunctionDefinition::DoubleDollarDef(s)) => s.clone(),
        None => {
            if params.return_.is_none() {
                return Err(ErrorCode::InvalidParameterValue(
                    "AS or RETURN must be specified".to_string(),
                )
                .into());
            }
            // Otherwise this is a return expression
            // Note: this is a current work around, and we are assuming return sql udf
            // will NOT involve complex syntax, so just reuse the logic for select definition
            format!("select {}", &params.return_.unwrap().to_string())
        }
    };

    // Sanity check for link, this must be none with sql udf function
    if let Some(CreateFunctionUsing::Link(_)) = params.using {
        return Err(ErrorCode::InvalidParameterValue(
            "USING must NOT be specified with sql udf function".to_string(),
        )
        .into());
    };

    // Get return type for the current sql udf function
    let return_type;
    let kind = match returns {
        Some(CreateFunctionReturns::Value(data_type)) => {
            return_type = bind_data_type(&data_type)?;
            Kind::Scalar(ScalarFunction {})
        }
        Some(CreateFunctionReturns::Table(columns)) => {
            if columns.len() == 1 {
                // return type is the original type for single column
                return_type = bind_data_type(&columns[0].data_type)?;
            } else {
                // return type is a struct for multiple columns
                let datatypes = columns
                    .iter()
                    .map(|c| bind_data_type(&c.data_type))
                    .collect::<Result<Vec<_>>>()?;
                let names = columns
                    .iter()
                    .map(|c| c.name.real_value())
                    .collect::<Vec<_>>();
                return_type = DataType::new_struct(datatypes, names);
            }
            Kind::Table(TableFunction {})
        }
        None => {
            return Err(ErrorCode::InvalidParameterValue(
                "return type must be specified".to_string(),
            )
            .into())
        }
    };

    let mut arg_names = vec![];
    let mut arg_types = vec![];
    for arg in args.unwrap_or_default() {
        arg_names.push(arg.name.map_or("".to_string(), |n| n.real_value()));
        arg_types.push(bind_data_type(&arg.data_type)?);
    }

    // resolve database and schema id
    let session = &handler_args.session;
    let db_name = session.database();
    let (schema_name, function_name) = Binder::resolve_schema_qualified_name(db_name, name)?;
    let (database_id, schema_id) = session.get_database_and_schema_id_for_create(schema_name)?;

    // check if function exists
    if (session.env().catalog_reader().read_guard())
        .get_schema_by_id(&database_id, &schema_id)?
        .get_function_by_name_args(&function_name, &arg_types)
        .is_some()
    {
        let name = format!(
            "{function_name}({})",
            arg_types.iter().map(|t| t.to_string()).join(",")
        );
        return Err(CatalogError::Duplicated("function", name).into());
    }

    // Parse function body here
    // Note that the parsing here is just basic syntax / semantic check, the result will NOT be stored
    // e.g., The provided function body contains invalid syntax, return type mismatch, ..., etc.
    let parse_result = Parser::parse_sql(body.as_str());
    if let Err(ParserError::ParserError(err)) | Err(ParserError::TokenizerError(err)) = parse_result
    {
        // Here we just return the original parse error message
        return Err(ErrorCode::InvalidInputSyntax(err).into());
    } else {
        debug_assert!(parse_result.is_ok());

        // Conduct semantic check (e.g., see if the inner calling functions exist, etc.)
        let ast = parse_result.unwrap();
        let mut binder = Binder::new_for_system(session);

        binder
            .udf_context_mut()
            .update_context(create_mock_udf_context(
                arg_types.clone(),
                arg_names.clone(),
            ));

        // Need to set the initial global count to 1
        // otherwise the context will not be probed during the semantic check
        binder.udf_context_mut().incr_global_count();

        if let Ok(expr) = UdfContext::extract_udf_expression(ast) {
            match binder.bind_expr(expr) {
                Ok(expr) => {
                    // Check if the return type mismatches
                    if expr.return_type() != return_type {
                        return Err(ErrorCode::InvalidInputSyntax(format!(
                            "\nreturn type mismatch detected\nexpected: [{}]\nactual: [{}]\nplease adjust your function definition accordingly",
                            return_type,
                            expr.return_type()
                        ))
                        .into());
                    }
                }
                Err(e) => {
                    if let ErrorCode::BindErrorRoot { expr: _, error } = e.inner() {
                        let invalid_msg = error.to_string();

                        // Valid error message for hint display
                        let Some(_) = invalid_msg.as_str().find(SQL_UDF_PATTERN) else {
                            return Err(
                                ErrorCode::InvalidInputSyntax(DEFAULT_ERR_MSG.into()).into()
                            );
                        };

                        // Get the name of the invalid item
                        // We will just display the first one found
                        let invalid_item_name =
                            invalid_msg.split_whitespace().last().unwrap_or("null");

                        // Find the invalid parameter / column
                        let Some(idx) = find_target(body.as_str(), invalid_item_name) else {
                            return Err(
                                ErrorCode::InvalidInputSyntax(DEFAULT_ERR_MSG.into()).into()
                            );
                        };

                        // The exact error position for `^` to point to
                        let position = format!(
                            "{}{}",
                            " ".repeat(idx + PROMPT.len() + 1),
                            "^".repeat(invalid_item_name.len())
                        );

                        return Err(ErrorCode::InvalidInputSyntax(format!(
                            "{}\n{}\n{}`{}`\n{}",
                            DEFAULT_ERR_MSG, invalid_msg, PROMPT, body, position
                        ))
                        .into());
                    } else {
                        // Otherwise return the default error message
                        return Err(ErrorCode::InvalidInputSyntax(DEFAULT_ERR_MSG.into()).into());
                    }
                }
            }
        } else {
            return Err(ErrorCode::InvalidInputSyntax(
                "failed to parse the input query and extract the udf expression,
                please recheck the syntax"
                    .to_string(),
            )
            .into());
        }
    }

    // Create the actual function, will be stored in function catalog
    let function = Function {
        id: FunctionId::placeholder().0,
        schema_id,
        database_id,
        name: function_name,
        kind: Some(kind),
        arg_names,
        arg_types: arg_types.into_iter().map(|t| t.into()).collect(),
        return_type: Some(return_type.into()),
        language,
        identifier: None,
        body: Some(body),
        link: None,
        owner: session.user_id(),
    };

    let catalog_writer = session.catalog_writer()?;
    catalog_writer.create_function(function).await?;

    Ok(PgResponse::empty_result(StatementType::CREATE_FUNCTION))
}
