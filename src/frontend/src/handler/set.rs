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
    context.session_ctx.set(&name.value, &string_val);

    Ok(PgResponse::empty_result(StatementType::SET_OPTION))
}

/// Convert any set variable to String.
/// For example, TRUE -> "TRUE", 1 -> "1".
fn to_string(value: &SetVariableValue) -> String {
    format!("{}", value)
}
