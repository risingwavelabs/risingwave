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

// Warn user if barrier_interval_ms is set above 5mins.
const NOTICE_BARRIER_INTERVAL_MS: u32 = 300000;
// Warn user if checkpoint_frequency is set above 60.
const NOTICE_CHECKPOINT_FREQUENCY: u64 = 60;

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
    let params = handler_args
        .session
        .env()
        .meta_client()
        .set_system_param(param.to_string(), value)
        .await?;
    let mut builder = RwPgResponse::builder(StatementType::ALTER_SYSTEM);
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
    Ok(builder.into())
}
