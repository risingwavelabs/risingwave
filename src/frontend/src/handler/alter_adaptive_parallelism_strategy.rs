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
use risingwave_common::system_param::adaptive_parallelism_strategy::parse_strategy;
use risingwave_pb::meta::PbTableParallelism;
use risingwave_pb::meta::table_parallelism::{AdaptiveParallelism, PbParallelism};
use risingwave_sqlparser::ast::{ObjectName, SetVariableValue};

use crate::error::{ErrorCode, Result};
use crate::handler::alter_utils::resolve_streaming_job_id_for_alter;
use crate::handler::util::{LongRunningNotificationAction, execute_with_long_running_notification};
use crate::handler::variable::set_var_to_param_str;
use crate::handler::{HandlerArgs, RwPgResponse};

pub async fn handle_alter_adaptive_parallelism_strategy(
    handler_args: HandlerArgs,
    obj_name: ObjectName,
    strategy: SetVariableValue,
    stmt_type: StatementType,
    deferred: bool,
) -> Result<RwPgResponse> {
    let session = handler_args.session;

    let job_id = resolve_streaming_job_id_for_alter(
        &session,
        obj_name,
        stmt_type,
        "adaptive_parallelism_strategy",
    )?;

    let strategy_string = extract_strategy(strategy)?;

    let mut builder = RwPgResponse::builder(stmt_type);

    let catalog_writer = session.catalog_writer()?;
    execute_with_long_running_notification(
        catalog_writer.alter_parallelism(
            job_id,
            adaptive_table_parallelism(),
            deferred,
            Some(strategy_string),
        ),
        &session,
        "ALTER ADAPTIVE_PARALLELISM_STRATEGY",
        LongRunningNotificationAction::SuggestRecover,
    )
    .await?;

    if deferred {
        builder = builder.notice("DEFERRED is used, please ensure that automatic parallelism control is enabled on the meta, otherwise, the alter will not take effect.".to_owned());
    }

    Ok(builder.into())
}

fn extract_strategy(value: SetVariableValue) -> Result<String> {
    match value {
        SetVariableValue::Default => Ok("DEFAULT".to_owned()),
        other => {
            let raw = set_var_to_param_str(&other).ok_or_else(|| {
                ErrorCode::InvalidInputSyntax(
                    "adaptive_parallelism_strategy must be a single value".to_owned(),
                )
            })?;
            // Validate strategy format.
            let strategy = parse_strategy(&raw).map_err(|e| {
                ErrorCode::InvalidInputSyntax(format!("invalid adaptive_parallelism_strategy: {e}"))
            })?;
            Ok(strategy.to_string())
        }
    }
}

fn adaptive_table_parallelism() -> PbTableParallelism {
    PbTableParallelism {
        parallelism: Some(PbParallelism::Adaptive(AdaptiveParallelism {})),
    }
}
