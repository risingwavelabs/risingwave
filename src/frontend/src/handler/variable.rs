// Copyright 2025 RisingWave Labs
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

use anyhow::Context;
use itertools::Itertools;
use pgwire::pg_field_descriptor::PgFieldDescriptor;
use pgwire::pg_protocol::ParameterStatus;
use pgwire::pg_response::{PgResponse, StatementType};
use risingwave_common::session_config::{ConfigReporter, SESSION_CONFIG_LIST_SEP};
use risingwave_common::system_param::reader::SystemParamsRead;
use risingwave_common::types::Fields;
use risingwave_sqlparser::ast::{Ident, SetTimeZoneValue, SetVariableValue, Value};

use super::{RwPgResponse, RwPgResponseBuilderExt, fields_to_descriptors};
use crate::error::Result;
use crate::handler::HandlerArgs;

/// convert `SetVariableValue` to string while remove the quotes on literals.
pub(crate) fn set_var_to_param_str(value: &SetVariableValue) -> Option<String> {
    match value {
        SetVariableValue::Single(var) => Some(var.to_string_unquoted()),
        SetVariableValue::List(list) => Some(
            list.iter()
                .map(|var| var.to_string_unquoted())
                .join(SESSION_CONFIG_LIST_SEP),
        ),
        SetVariableValue::Default => None,
    }
}

pub fn handle_set(
    handler_args: HandlerArgs,
    name: Ident,
    value: SetVariableValue,
) -> Result<RwPgResponse> {
    // Strip double and single quotes
    let string_val = set_var_to_param_str(&value);

    let mut status = ParameterStatus::default();

    struct Reporter<'a> {
        status: &'a mut ParameterStatus,
    }

    impl ConfigReporter for Reporter<'_> {
        fn report_status(&mut self, key: &str, new_val: String) {
            if key == "APPLICATION_NAME" {
                self.status.application_name = Some(new_val);
            }
        }
    }

    // Currently store the config variable simply as String -> ConfigEntry(String).
    // In future we can add converter/parser to make the API more robust.
    // We remark that the name of session parameter is always case-insensitive.
    handler_args.session.set_config_report(
        &name.real_value().to_lowercase(),
        string_val,
        Reporter {
            status: &mut status,
        },
    )?;

    Ok(PgResponse::builder(StatementType::SET_VARIABLE)
        .status(status)
        .into())
}

pub(super) fn handle_set_time_zone(
    handler_args: HandlerArgs,
    value: SetTimeZoneValue,
) -> Result<RwPgResponse> {
    let tz_info = match value {
        SetTimeZoneValue::Local => {
            iana_time_zone::get_timezone().context("Failed to get local time zone")
        }
        SetTimeZoneValue::Default => Ok("UTC".to_owned()),
        SetTimeZoneValue::Ident(ident) => Ok(ident.real_value()),
        SetTimeZoneValue::Literal(Value::DoubleQuotedString(s))
        | SetTimeZoneValue::Literal(Value::SingleQuotedString(s)) => Ok(s),
        _ => Ok(value.to_string()),
    }?;

    handler_args.session.set_config("timezone", tz_info)?;

    Ok(PgResponse::empty_result(StatementType::SET_VARIABLE))
}

pub(super) async fn handle_show(
    handler_args: HandlerArgs,
    variable: Vec<Ident>,
) -> Result<RwPgResponse> {
    // TODO: Verify that the name used in `show` command is indeed always case-insensitive.
    let name = variable.iter().map(|e| e.real_value()).join(" ");
    if name.eq_ignore_ascii_case("PARAMETERS") {
        handle_show_system_params(handler_args).await
    } else if name.eq_ignore_ascii_case("ALL") {
        handle_show_all(handler_args.clone())
    } else {
        let config_reader = handler_args.session.config();
        Ok(PgResponse::builder(StatementType::SHOW_VARIABLE)
            .rows([ShowVariableRow {
                name: config_reader.get(&name)?,
            }])
            .into())
    }
}

fn handle_show_all(handler_args: HandlerArgs) -> Result<RwPgResponse> {
    let config_reader = handler_args.session.config();

    let all_variables = config_reader.show_all();

    let rows = all_variables.iter().map(|info| ShowVariableAllRow {
        name: info.name.clone(),
        setting: info.setting.clone(),
        description: info.description.clone(),
    });
    Ok(PgResponse::builder(StatementType::SHOW_VARIABLE)
        .rows(rows)
        .into())
}

async fn handle_show_system_params(handler_args: HandlerArgs) -> Result<RwPgResponse> {
    let params = handler_args
        .session
        .env()
        .meta_client()
        .get_system_params()
        .await?;
    let rows = params
        .get_all()
        .into_iter()
        .map(|info| ShowVariableParamsRow {
            name: info.name.into(),
            value: info.value,
            description: info.description.into(),
            mutable: info.mutable,
        });
    Ok(PgResponse::builder(StatementType::SHOW_VARIABLE)
        .rows(rows)
        .into())
}

pub fn infer_show_variable(name: &str) -> Vec<PgFieldDescriptor> {
    fields_to_descriptors(if name.eq_ignore_ascii_case("ALL") {
        ShowVariableAllRow::fields()
    } else if name.eq_ignore_ascii_case("PARAMETERS") {
        ShowVariableParamsRow::fields()
    } else {
        ShowVariableRow::fields()
    })
}

#[derive(Fields)]
#[fields(style = "Title Case")]
struct ShowVariableRow {
    name: String,
}

#[derive(Fields)]
#[fields(style = "Title Case")]
struct ShowVariableAllRow {
    name: String,
    setting: String,
    description: String,
}

#[derive(Fields)]
#[fields(style = "Title Case")]
struct ShowVariableParamsRow {
    name: String,
    value: String,
    description: String,
    mutable: bool,
}
