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

use anyhow::anyhow;
use itertools::Itertools;
use pgwire::pg_response::StatementType;
use risingwave_common::catalog::FunctionId;
use risingwave_pb::catalog::function::{Kind, ScalarFunction, TableFunction};
use risingwave_pb::catalog::Function;
use risingwave_sqlparser::ast::{
    CreateFunctionBody, FunctionDefinition, ObjectName, OperateFunctionArg,
};
use risingwave_udf::ArrowFlightUdfClient;

use super::*;
use crate::catalog::CatalogError;
use crate::{bind_data_type, Binder};

pub async fn handle_create_function(
    handler_args: HandlerArgs,
    or_replace: bool,
    temporary: bool,
    name: ObjectName,
    args: Option<Vec<OperateFunctionArg>>,
    returns: Option<CreateFunctionReturns>,
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
        Some(lang) => lang.real_value().to_lowercase(),
        None => {
            return Err(
                ErrorCode::InvalidParameterValue("LANGUAGE must be specified".to_string()).into(),
            )
        }
    };
    if language != "python" {
        return Err(ErrorCode::InvalidParameterValue(
            "LANGUAGE should be one of: python".to_string(),
        )
        .into());
    }
    let Some(FunctionDefinition::SingleQuotedDef(identifier)) = params.as_ else {
        return Err(ErrorCode::InvalidParameterValue(
            "AS must be specified".to_string(),
        )
        .into());
    };
    let Some(CreateFunctionUsing::Link(link)) = params.using else {
        return Err(ErrorCode::InvalidParameterValue(
            "USING must be specified".to_string(),
        )
        .into());
    };
    let mut return_types = vec![];
    let kind = match returns {
        Some(CreateFunctionReturns::Value(data_type)) => {
            let return_type = bind_data_type(&data_type)?;
            return_types.push(return_type.clone());
            Kind::Scalar(ScalarFunction {
                return_type: Some(return_type.into()),
            })
        }
        Some(CreateFunctionReturns::Table(columns)) => {
            for column in columns {
                return_types.push(bind_data_type(&column.data_type)?);
            }
            Kind::Table(TableFunction {
                return_types: return_types.iter().map(|t| t.clone().into()).collect(),
            })
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

    // check the service
    let client = ArrowFlightUdfClient::connect(&link)
        .await
        .map_err(|e| anyhow!(e))?;
    let args = arrow_schema::Schema::new(
        arg_types
            .iter()
            .map(|t| arrow_schema::Field::new("", t.into(), true))
            .collect(),
    );
    let returns = arrow_schema::Schema::new(
        return_types
            .iter()
            .map(|t| arrow_schema::Field::new("", t.into(), true))
            .collect(),
    );
    client
        .check(&identifier, &args, &returns)
        .await
        .map_err(|e| anyhow!(e))?;

    let function = Function {
        id: FunctionId::placeholder().0,
        schema_id,
        database_id,
        name: function_name,
        kind: Some(kind),
        arg_types: arg_types.into_iter().map(|t| t.into()).collect(),
        language,
        identifier,
        link,
        owner: session.user_id(),
    };

    let catalog_writer = session.env().catalog_writer();
    catalog_writer.create_function(function).await?;

    Ok(PgResponse::empty_result(StatementType::CREATE_FUNCTION))
}
