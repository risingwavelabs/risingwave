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
use risingwave_common::id::TableId;
use risingwave_sqlparser::ast::{ObjectName, SqlOption, SqlOptionValue, Value as AstValue};
use toml::Value as TomlValue;
use toml::map::Map as TomlMap;

use crate::error::{Result, bail_invalid_input_syntax};
use crate::handler::alter_utils::resolve_streaming_job_id_for_alter;
use crate::handler::{HandlerArgs, RwPgResponse};

/// A diff of a TOML map. `None` means the key should be removed.
type TomlMapDiff = TomlMap<String, Option<TomlValue>>;

struct CollectedOptions {
    streaming: TomlMapDiff,
    refill_mode: Option<Option<String>>,
}

fn collect_options(entries: Vec<SqlOption>) -> Result<CollectedOptions> {
    let mut streaming = TomlMap::new();
    let mut refill_mode = None;

    for SqlOption { name, value } in entries {
        let name = name.real_value();
        let SqlOptionValue::Value(value) = value else {
            bail_invalid_input_syntax!("ALTER CONFIG only accepts value options");
        };

        if name.starts_with("streaming.") {
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

            let old = streaming.insert(name.clone(), value);
            if old.is_some() {
                bail_invalid_input_syntax!("Duplicate option for ALTER CONFIG: {}", name);
            }
            continue;
        }

        if name == "refill.mode" {
            let value = match value {
                AstValue::SingleQuotedString(s) | AstValue::DoubleQuotedString(s) => {
                    let value = s.to_lowercase();
                    if value != "streaming" && value != "serving" {
                        bail_invalid_input_syntax!(
                            "invalid refill mode: {}, expected streaming or serving",
                            value
                        );
                    }
                    Some(value)
                }
                AstValue::Null => None,
                _ => bail_invalid_input_syntax!("Unsupported value for refill.mode"),
            };

            if refill_mode.replace(value).is_some() {
                bail_invalid_input_syntax!("Duplicate option for ALTER CONFIG: {}", name);
            }
            continue;
        }

        if name.starts_with("refill.") {
            bail_invalid_input_syntax!("Unsupported refill option: {}", name);
        }
        bail_invalid_input_syntax!(
            "ALTER CONFIG only accepts options starting with `streaming.` or `refill.`"
        );
    }

    Ok(CollectedOptions {
        streaming,
        refill_mode,
    })
}

pub async fn handle_alter_streaming_set_config(
    handler_args: HandlerArgs,
    obj_name: ObjectName,
    entries: Vec<SqlOption>,
    stmt_type: StatementType,
) -> Result<RwPgResponse> {
    let session = handler_args.session;

    let job_id = resolve_streaming_job_id_for_alter(&session, obj_name, stmt_type, "config")?;
    let options = collect_options(entries)?;

    let mut entries_to_add = HashMap::new();
    let mut keys_to_remove = Vec::new();

    for (k, v) in options.streaming {
        if let Some(v) = v {
            entries_to_add.insert(k, v.to_string());
        } else {
            keys_to_remove.push(k);
        }
    }

    let mut needs_recover_notice = false;
    let catalog_writer = session.catalog_writer()?;
    if !entries_to_add.is_empty() || !keys_to_remove.is_empty() {
        catalog_writer
            .alter_config(job_id, entries_to_add, keys_to_remove)
            .await?;
        needs_recover_notice = true;
    }

    if let Some(mode) = options.refill_mode {
        catalog_writer
            .alter_table_refill(TableId::new(job_id.as_raw_id()), mode)
            .await?;
    }

    let mut builder = RwPgResponse::builder(stmt_type);
    if needs_recover_notice {
        builder = builder.notice(
            "ALTER CONFIG requires a RECOVER on the specified streaming job to take effect.",
        );
    }
    Ok(builder.into())
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
