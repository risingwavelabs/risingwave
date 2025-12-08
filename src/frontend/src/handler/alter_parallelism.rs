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
use risingwave_pb::meta::table_parallelism::{
    AdaptiveParallelism, FixedParallelism, PbParallelism,
};
use risingwave_pb::meta::{PbTableParallelism, TableParallelism};
use risingwave_sqlparser::ast::{ObjectName, SetVariableValue, SetVariableValueSingle, Value};
use risingwave_sqlparser::keywords::Keyword;
use thiserror_ext::AsReport;

use super::alter_utils::resolve_streaming_job_id_for_alter;
use super::{HandlerArgs, RwPgResponse};
use crate::catalog::FragmentId;
use crate::error::{ErrorCode, Result};

pub async fn handle_alter_parallelism(
    handler_args: HandlerArgs,
    obj_name: ObjectName,
    parallelism: SetVariableValue,
    stmt_type: StatementType,
    deferred: bool,
) -> Result<RwPgResponse> {
    let session = handler_args.session;

    let job_id = resolve_streaming_job_id_for_alter(&session, obj_name, stmt_type, "parallelism")?;

    let target_parallelism = extract_table_parallelism(parallelism)?;

    let mut builder = RwPgResponse::builder(stmt_type);

    let catalog_writer = session.catalog_writer()?;
    catalog_writer
        .alter_parallelism(job_id, target_parallelism, deferred)
        .await?;

    if deferred {
        builder = builder.notice("DEFERRED is used, please ensure that automatic parallelism control is enabled on the meta, otherwise, the alter will not take effect.".to_owned());
    }

    Ok(builder.into())
}

pub async fn handle_alter_fragment_parallelism(
    handler_args: HandlerArgs,
    fragment_ids: Vec<FragmentId>,
    parallelism: SetVariableValue,
) -> Result<RwPgResponse> {
    let session = handler_args.session;
    let target_parallelism = extract_fragment_parallelism(parallelism)?;

    session
        .env()
        .meta_client()
        .alter_fragment_parallelism(fragment_ids, target_parallelism)
        .await?;

    Ok(RwPgResponse::builder(StatementType::ALTER_FRAGMENT).into())
}

fn extract_table_parallelism(parallelism: SetVariableValue) -> Result<TableParallelism> {
    let adaptive_parallelism = PbTableParallelism {
        parallelism: Some(PbParallelism::Adaptive(AdaptiveParallelism {})),
    };

    // If the target parallelism is set to 0/auto/default, we would consider it as auto parallelism.
    let target_parallelism = match parallelism {
        SetVariableValue::Single(SetVariableValueSingle::Ident(ident))
            if ident
                .real_value()
                .eq_ignore_ascii_case(&Keyword::ADAPTIVE.to_string()) =>
        {
            adaptive_parallelism
        }

        SetVariableValue::Default => adaptive_parallelism,
        SetVariableValue::Single(SetVariableValueSingle::Literal(Value::Number(v))) => {
            let fixed_parallelism = v.parse::<u32>().map_err(|e| {
                ErrorCode::InvalidInputSyntax(format!(
                    "target parallelism must be a valid number or adaptive: {}",
                    e.as_report()
                ))
            })?;

            if fixed_parallelism == 0 {
                adaptive_parallelism
            } else {
                PbTableParallelism {
                    parallelism: Some(PbParallelism::Fixed(FixedParallelism {
                        parallelism: fixed_parallelism,
                    })),
                }
            }
        }

        _ => {
            return Err(ErrorCode::InvalidInputSyntax(
                "target parallelism must be a valid number or adaptive".to_owned(),
            )
            .into());
        }
    };

    Ok(target_parallelism)
}

fn extract_fragment_parallelism(parallelism: SetVariableValue) -> Result<Option<TableParallelism>> {
    match parallelism {
        SetVariableValue::Default => Ok(None),
        other => extract_table_parallelism(other).map(Some),
    }
}
