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
use crate::catalog::CatalogError;
use crate::{bind_data_type, Binder};

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
    // Just a basic sanity check for language
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

    let mut arg_types = vec![];
    for arg in args.unwrap_or_default() {
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
    }

    // Create the actual function, will be stored in function catalog
    let function = Function {
        id: FunctionId::placeholder().0,
        schema_id,
        database_id,
        name: function_name,
        kind: Some(kind),
        arg_types: arg_types.into_iter().map(|t| t.into()).collect(),
        return_type: Some(return_type.into()),
        language,
        identifier: "".to_string(),
        body: Some(body),
        link: "".to_string(),
        owner: session.user_id(),
    };

    let catalog_writer = session.catalog_writer()?;
    catalog_writer.create_function(function).await?;

    Ok(PgResponse::empty_result(StatementType::CREATE_FUNCTION))
}
