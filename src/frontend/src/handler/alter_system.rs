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

use pgwire::pg_response::StatementType;
use risingwave_common::session_config::SessionConfig;
use risingwave_common::system_param::reader::SystemParamsRead;
use risingwave_common::system_param::{NOTICE_BARRIER_INTERVAL_MS, NOTICE_CHECKPOINT_FREQUENCY};
use risingwave_sqlparser::ast::{Ident, SetVariableValue};

use super::variable::set_var_to_param_str;
use super::{HandlerArgs, RwPgResponse};
use crate::error::{ErrorCode, Result};

pub async fn handle_alter_system(
    handler_args: HandlerArgs,
    param: Ident,
    value: SetVariableValue,
) -> Result<RwPgResponse> {
    let value = set_var_to_param_str(&value);
    let param_name = param.to_string();
    let meta_client = handler_args.session.env().meta_client();
    let mut builder = RwPgResponse::builder(StatementType::ALTER_SYSTEM);

    // Currently session params are separated from system params. If the param exist in session params, we set it. Otherwise
    // we try to set it as a system param.
    if SessionConfig::contains_param(&param_name) {
        if SessionConfig::check_no_alter_sys(&param_name)? {
            return Err(ErrorCode::InternalError(format!(
                "session param {} cannot be altered system wide",
                param_name
            ))
            .into());
        }
        meta_client
            .set_session_param(param_name.clone(), value.clone())
            .await?;

        if let Some(value) = value {
            builder = builder.notice(format!(
                "The config {param_name} of the current session has already been set to {value}"
            ));
            handler_args
                .session
                .set_config(param_name.as_str(), value)?;
        } else {
            builder = builder.notice(format!(
                "The config {param_name} of the current session has already been reset to default"
            ));
            handler_args.session.reset_config(param_name.as_str())?;
        }
    } else {
        let params = meta_client.set_system_param(param_name, value).await?;
        if let Some(params) = params {
            if params.barrier_interval_ms() >= NOTICE_BARRIER_INTERVAL_MS {
                builder = builder.notice(
                    format!("Barrier interval is set to {} ms >= {} ms. This can hurt freshness and potentially cause OOM.",
                             params.barrier_interval_ms(), NOTICE_BARRIER_INTERVAL_MS));
            }
            if params.checkpoint_frequency() >= NOTICE_CHECKPOINT_FREQUENCY {
                builder = builder.notice(
                    format!("Checkpoint frequency is set to {} >= {}. This can hurt freshness and potentially cause OOM.",
                             params.checkpoint_frequency(), NOTICE_CHECKPOINT_FREQUENCY));
            }
        }
    }
    Ok(builder.into())
}
