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

use itertools::Itertools;
use pgwire::pg_field_descriptor::PgFieldDescriptor;
use pgwire::pg_response::{PgResponse, StatementType};
use pgwire::types::Row;
use risingwave_common::error::{ErrorCode, Result};
use risingwave_common::types::DataType;
use risingwave_sqlparser::ast::{Ident, SetTimeZoneValue, SetVariableValue, Value};

use super::RwPgResponse;
use crate::handler::HandlerArgs;

pub fn handle_set(
    handler_args: HandlerArgs,
    name: Ident,
    value: Vec<SetVariableValue>,
) -> Result<RwPgResponse> {
    // Strip double and single quotes
    let string_vals = value
        .into_iter()
        .map(|v| match v {
            SetVariableValue::Literal(Value::DoubleQuotedString(s))
            | SetVariableValue::Literal(Value::SingleQuotedString(s)) => s,
            _ => v.to_string(),
        })
        .collect_vec();

    // Currently store the config variable simply as String -> ConfigEntry(String).
    // In future we can add converter/parser to make the API more robust.
    // We remark that the name of session parameter is always case-insensitive.
    handler_args
        .session
        .set_config(&name.real_value().to_lowercase(), string_vals)?;

    Ok(PgResponse::empty_result(StatementType::SET_VARIABLE))
}

pub(super) fn handle_set_time_zone(
    handler_args: HandlerArgs,
    value: SetTimeZoneValue,
) -> Result<RwPgResponse> {
    let tz_info = match value {
        SetTimeZoneValue::Local => iana_time_zone::get_timezone()
            .map_err(|e| ErrorCode::InternalError(format!("Failed to get local time zone: {}", e))),
        SetTimeZoneValue::Default => Ok("UTC".to_string()),
        SetTimeZoneValue::Literal(Value::DoubleQuotedString(s))
        | SetTimeZoneValue::Literal(Value::SingleQuotedString(s)) => Ok(s),
        _ => Ok(value.to_string()),
    }?;

    handler_args.session.set_config("timezone", vec![tz_info])?;

    Ok(PgResponse::empty_result(StatementType::SET_VARIABLE))
}

pub(super) async fn handle_show(
    handler_args: HandlerArgs,
    variable: Vec<Ident>,
) -> Result<RwPgResponse> {
    // TODO: Verify that the name used in `show` command is indeed always case-insensitive.
    let name = variable.iter().map(|e| e.real_value()).join(" ");
    if name.eq_ignore_ascii_case("PARAMETERS") {
        return handle_show_system_params(handler_args).await;
    }
    // Show session config.
    let config_reader = handler_args.session.config();
    if name.eq_ignore_ascii_case("ALL") {
        return handle_show_all(handler_args.clone());
    }
    let row = Row::new(vec![Some(config_reader.get(&name)?.into())]);

    Ok(PgResponse::new_for_stream(
        StatementType::SHOW_VARIABLE,
        None,
        vec![row].into(),
        vec![PgFieldDescriptor::new(
            name.to_ascii_lowercase(),
            DataType::Varchar.to_oid(),
            DataType::Varchar.type_len(),
        )],
    ))
}

fn handle_show_all(handler_args: HandlerArgs) -> Result<RwPgResponse> {
    let config_reader = handler_args.session.config();

    let all_variables = config_reader.get_all();

    let rows = all_variables
        .iter()
        .map(|info| {
            Row::new(vec![
                Some(info.name.clone().into()),
                Some(info.setting.clone().into()),
                Some(info.description.clone().into()),
            ])
        })
        .collect_vec();

    Ok(RwPgResponse::new_for_stream(
        StatementType::SHOW_VARIABLE,
        None,
        rows.into(),
        vec![
            PgFieldDescriptor::new(
                "Name".to_string(),
                DataType::Varchar.to_oid(),
                DataType::Varchar.type_len(),
            ),
            PgFieldDescriptor::new(
                "Setting".to_string(),
                DataType::Varchar.to_oid(),
                DataType::Varchar.type_len(),
            ),
            PgFieldDescriptor::new(
                "Description".to_string(),
                DataType::Varchar.to_oid(),
                DataType::Varchar.type_len(),
            ),
        ],
    ))
}

async fn handle_show_system_params(handler_args: HandlerArgs) -> Result<RwPgResponse> {
    let params = handler_args
        .session
        .env()
        .meta_client()
        .get_system_params()
        .await?;
    let rows = params
        .to_kv()
        .into_iter()
        .map(|(k, v)| Row::new(vec![Some(k.into()), Some(v.into())]))
        .collect_vec();

    Ok(RwPgResponse::new_for_stream(
        StatementType::SHOW_VARIABLE,
        None,
        rows.into(),
        vec![
            PgFieldDescriptor::new(
                "Name".to_string(),
                DataType::Varchar.to_oid(),
                DataType::Varchar.type_len(),
            ),
            PgFieldDescriptor::new(
                "Value".to_string(),
                DataType::Varchar.to_oid(),
                DataType::Varchar.type_len(),
            ),
        ],
    ))
}
