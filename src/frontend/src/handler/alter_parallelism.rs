// Copyright 2024 RisingWave Labs
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
use risingwave_common::bail;
use risingwave_common::hash::VirtualNode;
use risingwave_pb::meta::table_parallelism::{
    AdaptiveParallelism, FixedParallelism, Parallelism, PbParallelism,
};
use risingwave_pb::meta::{PbTableParallelism, TableParallelism};
use risingwave_sqlparser::ast::{ObjectName, SetVariableValue, SetVariableValueSingle, Value};
use risingwave_sqlparser::keywords::Keyword;
use thiserror_ext::AsReport;

use super::{HandlerArgs, RwPgResponse};
use crate::catalog::root_catalog::SchemaPath;
use crate::catalog::table_catalog::TableType;
use crate::catalog::CatalogError;
use crate::error::{ErrorCode, Result};
use crate::Binder;

pub async fn handle_alter_parallelism(
    handler_args: HandlerArgs,
    obj_name: ObjectName,
    parallelism: SetVariableValue,
    stmt_type: StatementType,
    deferred: bool,
) -> Result<RwPgResponse> {
    let session = handler_args.session;
    let db_name = session.database();
    let (schema_name, real_table_name) =
        Binder::resolve_schema_qualified_name(db_name, obj_name.clone())?;
    let search_path = session.config().search_path();
    let user_name = &session.auth_context().user_name;
    let schema_path = SchemaPath::new(schema_name.as_deref(), &search_path, user_name);

    let table_id = {
        let reader = session.env().catalog_reader().read_guard();

        match stmt_type {
            StatementType::ALTER_TABLE
            | StatementType::ALTER_MATERIALIZED_VIEW
            | StatementType::ALTER_INDEX => {
                let (table, schema_name) =
                    reader.get_created_table_by_name(db_name, schema_path, &real_table_name)?;

                match (table.table_type(), stmt_type) {
                    (TableType::Internal, _) => {
                        // we treat internal table as NOT FOUND
                        return Err(
                            CatalogError::NotFound("table", table.name().to_string()).into()
                        );
                    }
                    (TableType::Table, StatementType::ALTER_TABLE)
                    | (TableType::MaterializedView, StatementType::ALTER_MATERIALIZED_VIEW)
                    | (TableType::Index, StatementType::ALTER_INDEX) => {}
                    _ => {
                        return Err(ErrorCode::InvalidInputSyntax(format!(
                            "cannot alter parallelism of {} {} by {}",
                            table.table_type().to_prost().as_str_name(),
                            table.name(),
                            stmt_type,
                        ))
                        .into());
                    }
                }

                session.check_privilege_for_drop_alter(schema_name, &**table)?;
                table.id.table_id()
            }
            StatementType::ALTER_SINK => {
                let (sink, schema_name) =
                    reader.get_sink_by_name(db_name, schema_path, &real_table_name)?;

                session.check_privilege_for_drop_alter(schema_name, &**sink)?;
                sink.id.sink_id()
            }
            _ => bail!(
                "invalid statement type for alter parallelism: {:?}",
                stmt_type
            ),
        }
    };

    let mut target_parallelism = extract_table_parallelism(parallelism)?;

    let available_parallelism = session
        .env()
        .worker_node_manager()
        .list_worker_nodes()
        .iter()
        .filter(|w| w.is_streaming_schedulable())
        .map(|w| w.parallelism)
        .sum::<u32>();
    // TODO(var-vnode): use vnode count from config
    let max_parallelism = VirtualNode::COUNT;

    let mut builder = RwPgResponse::builder(stmt_type);

    match &target_parallelism.parallelism {
        Some(Parallelism::Adaptive(_)) | Some(Parallelism::Auto(_)) => {
            if available_parallelism > max_parallelism as u32 {
                builder = builder.notice(format!("Available parallelism exceeds the maximum parallelism limit, the actual parallelism will be limited to {max_parallelism}"));
            }
        }
        Some(Parallelism::Fixed(FixedParallelism { parallelism })) => {
            if *parallelism > max_parallelism as u32 {
                builder = builder.notice(format!("Provided parallelism exceeds the maximum parallelism limit, resetting to FIXED({max_parallelism})"));
                target_parallelism = PbTableParallelism {
                    parallelism: Some(PbParallelism::Fixed(FixedParallelism {
                        parallelism: max_parallelism as u32,
                    })),
                };
            }
        }
        _ => {}
    };

    let catalog_writer = session.catalog_writer()?;
    catalog_writer
        .alter_parallelism(table_id, target_parallelism, deferred)
        .await?;

    if deferred {
        builder = builder.notice("DEFERRED is used, please ensure that automatic parallelism control is enabled on the meta, otherwise, the alter will not take effect.".to_string());
    }

    Ok(builder.into())
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
                "target parallelism must be a valid number or adaptive".to_string(),
            )
            .into());
        }
    };

    Ok(target_parallelism)
}
