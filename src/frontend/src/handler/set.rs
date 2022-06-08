// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use pgwire::pg_response::{PgResponse, StatementType};
use risingwave_common::error::Result;
use risingwave_sqlparser::ast::{Ident, SetVariableValue};

use crate::session::OptimizerContext;

pub(super) fn handle_set(
    context: OptimizerContext,
    name: Ident,
    value: Vec<SetVariableValue>,
) -> Result<PgResponse> {
    let string_val = to_string(&value[0]);
    // Currently store the config variable simply as String -> ConfigEntry(String).
    // In future we can add converter/parser to make the API more robust.
    context.session_ctx.set_config(&name.value, &string_val)?;

    Ok(PgResponse::empty_result(StatementType::SET_OPTION))
}

/// Convert any set variable to String.
/// For example, TRUE -> "TRUE", 1 -> "1".
fn to_string(value: &SetVariableValue) -> String {
    format!("{}", value)
}
