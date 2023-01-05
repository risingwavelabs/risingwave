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

use risingwave_sqlparser::ast::{
    CreateFunctionBody, DataType, FunctionDefinition, ObjectName, OperateFunctionArg,
};

use super::*;

pub fn handle_create_function(
    handle_args: HandlerArgs,
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
    match params.language {
        None => {
            return Err(
                ErrorCode::InvalidParameterValue("LANGUAGE must be specified".to_string()).into(),
            )
        }
        Some(lang) if lang.real_value() != "arrow_flight" => {
            return Err(ErrorCode::InvalidParameterValue(
                "LANGUAGE should be one of: arrow_flight".to_string(),
            )
            .into())
        }
        _ => {}
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
    todo!()
}
