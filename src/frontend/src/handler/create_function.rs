// Copyright 2023 Singularity Data
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

use pgwire::pg_response::StatementType;
use risingwave_common::catalog::FunctionId;
use risingwave_pb::catalog::{Function, FunctionArgument};
use risingwave_sqlparser::ast::{
    CreateFunctionBody, DataType, FunctionDefinition, ObjectName, OperateFunctionArg,
};

use super::*;
use crate::{bind_data_type, Binder};

pub async fn handle_create_function(
    handler_args: HandlerArgs,
    or_replace: bool,
    temporary: bool,
    name: ObjectName,
    args: Option<Vec<OperateFunctionArg>>,
    return_type: Option<DataType>,
    params: CreateFunctionBody,
) -> Result<RwPgResponse> {
    if or_replace {
        return Err(ErrorCode::NotImplemented(
            "CREATE OR REPLACE FUNCTION".to_string(),
            None.into(),
        )
        .into());
    }
    if temporary {
        return Err(ErrorCode::NotImplemented(
            "CREATE TEMPORARY FUNCTION".to_string(),
            None.into(),
        )
        .into());
    }
    let language = match params.language {
        Some(lang) => lang.real_value(),
        None => {
            return Err(
                ErrorCode::InvalidParameterValue("LANGUAGE must be specified".to_string()).into(),
            )
        }
    };
    if language != "arrow_flight" {
        return Err(ErrorCode::InvalidParameterValue(
            "LANGUAGE should be one of: arrow_flight".to_string(),
        )
        .into());
    }
    let Some(FunctionDefinition::SingleQuotedDef(flight_server_addr)) = params.as_ else {
        return Err(ErrorCode::InvalidParameterValue(
            "AS must be specified".to_string(),
        )
        .into());
    };
    let Some(return_type) = return_type else {
        return Err(
            ErrorCode::InvalidParameterValue("return type must be specified".to_string()).into(),
        )
    };
    let mut arguments = vec![];
    for arg in args.unwrap_or_default() {
        arguments.push(FunctionArgument {
            name: arg.name.map_or(String::new(), |ident| ident.real_value()),
            r#type: Some(bind_data_type(&arg.data_type)?.into()),
        });
    }

    // resolve database and schema id
    let session = &handler_args.session;
    let db_name = session.database();
    let (schema_name, function_name) = Binder::resolve_schema_qualified_name(db_name, name)?;
    let (database_id, schema_id) = session.get_database_and_schema_id_for_create(schema_name)?;

    let function = Function {
        id: FunctionId::placeholder().0,
        schema_id,
        database_id,
        name: function_name,
        arguments,
        return_type: Some(bind_data_type(&return_type)?.into()),
        language,
        path: flight_server_addr,
        owner: session.user_id(),
    };

    let catalog_writer = session.env().catalog_writer();
    catalog_writer.create_function(function).await?;

    Ok(PgResponse::empty_result(StatementType::CREATE_FUNCTION))
}
