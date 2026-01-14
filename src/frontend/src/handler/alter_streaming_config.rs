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

use std::collections::HashMap;

use anyhow::Context;
use pgwire::pg_response::StatementType;
use risingwave_sqlparser::ast::{ObjectName, SqlOption, SqlOptionValue, Value as AstValue};
use serde_json::json;
use toml::Value as TomlValue;
use toml::map::Map as TomlMap;

use crate::error::{Result, bail_invalid_input_syntax};
use crate::handler::alter_utils::resolve_streaming_job_id_for_alter;
use crate::handler::audit_log::record_audit_log;
use crate::handler::{HandlerArgs, RwPgResponse};

/// A diff of a TOML map. `None` means the key should be removed.
type TomlMapDiff = TomlMap<String, Option<TomlValue>>;

fn collect_options(entries: Vec<SqlOption>) -> Result<TomlMapDiff> {
    let mut map = TomlMap::new();

    for SqlOption { name, value } in entries {
        let name = name.real_value();
        if !name.starts_with("streaming.") {
            bail_invalid_input_syntax!(
                "ALTER CONFIG only accepts options starting with `streaming.`"
            );
        }
        let SqlOptionValue::Value(value) = value else {
            bail_invalid_input_syntax!("ALTER CONFIG only accepts value options");
        };

        let value = match value {
            AstValue::Number(n) => {
                let n: TomlValue = n.parse().context("Invalid number for ALTER CONFIG")?;
                Some(n)
            }
            AstValue::SingleQuotedString(s) | AstValue::DoubleQuotedString(s) => {
                Some(TomlValue::String(s))
            }
            AstValue::Boolean(b) => Some(TomlValue::Boolean(b)),
            AstValue::Null => None,
            _ => bail_invalid_input_syntax!("Unsupported value for ALTER CONFIG: {}", value),
        };

        let old = map.insert(name.clone(), value);
        if old.is_some() {
            bail_invalid_input_syntax!("Duplicate option for ALTER CONFIG: {}", name);
        }
    }

    Ok(map)
}

pub async fn handle_alter_streaming_set_config(
    handler_args: HandlerArgs,
    obj_name: ObjectName,
    entries: Vec<SqlOption>,
    stmt_type: StatementType,
) -> Result<RwPgResponse> {
    let session = handler_args.session;

    let obj_name_string = obj_name.to_string();
    let job_id = resolve_streaming_job_id_for_alter(&session, obj_name, stmt_type, "config")?;
    let map_diff = collect_options(entries)?;

    let mut entries_to_add = HashMap::new();
    let mut keys_to_remove = Vec::new();

    for (k, v) in map_diff {
        if let Some(v) = v {
            entries_to_add.insert(k, v.to_string());
        } else {
            keys_to_remove.push(k);
        }
    }

    let audit_set_keys = entries_to_add.keys().cloned().collect::<Vec<_>>();
    let audit_reset_keys = keys_to_remove.clone();
    let catalog_writer = session.catalog_writer()?;
    catalog_writer
        .alter_config(job_id, entries_to_add, keys_to_remove)
        .await?;

    record_audit_log(
        &session,
        "ALTER CONFIG",
        Some("STREAMING_JOB"),
        Some(job_id as u32),
        Some(obj_name_string),
        json!({
            "set_keys": audit_set_keys,
            "reset_keys": audit_reset_keys,
        }),
    )
    .await;

    Ok(RwPgResponse::builder(stmt_type)
        .notice("ALTER CONFIG requires a RECOVER on the specified streaming job to take effect.")
        .into())
}

pub async fn handle_alter_streaming_reset_config(
    handler_args: HandlerArgs,
    obj_name: ObjectName,
    keys: Vec<ObjectName>,
    stmt_type: StatementType,
) -> Result<RwPgResponse> {
    let entries = keys
        .into_iter()
        .map(|k| SqlOption {
            name: k,
            value: SqlOptionValue::null(),
        })
        .collect();

    // Simply delegate to `handle_alter_streaming_set_config` with all values set to `NULL`.
    handle_alter_streaming_set_config(handler_args, obj_name, entries, stmt_type).await
}
