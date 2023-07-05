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

use pgwire::pg_response::StatementType;
use risingwave_common::error::Result;
use risingwave_sqlparser::ast::{Ident, SetVariableValue, Value};

use super::{HandlerArgs, RwPgResponse};

pub async fn handle_alter_system(
    handler_args: HandlerArgs,
    param: Ident,
    value: SetVariableValue,
) -> Result<RwPgResponse> {
    let value = match value {
        SetVariableValue::Literal(Value::DoubleQuotedString(s))
        | SetVariableValue::Literal(Value::SingleQuotedString(s)) => Some(s),
        SetVariableValue::Default => None,
        _ => Some(value.to_string()),
    };
    handler_args
        .session
        .env()
        .meta_client()
        .set_system_param(param.to_string(), value)
        .await?;
    Ok(RwPgResponse::empty_result(StatementType::ALTER_SYSTEM))
}
