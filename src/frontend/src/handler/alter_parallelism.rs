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
use risingwave_common::error::{ErrorCode, Result};
use risingwave_pb::meta::table_parallelism::{AutoParallelism, FixedParallelism, PbParallelism};
use risingwave_pb::meta::PbTableParallelism;
use risingwave_sqlparser::ast::{ObjectName, SetVariableValue, SetVariableValueSingle, Value};

use super::{HandlerArgs, RwPgResponse};
use crate::catalog::root_catalog::SchemaPath;
use crate::Binder;
pub async fn handle_alter_parallelism(
    handler_args: HandlerArgs,
    obj_name: ObjectName,
    parallelism: SetVariableValue,
    stmt_type: StatementType,
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
            StatementType::ALTER_TABLE | StatementType::ALTER_MATERIALIZED_VIEW => {
                let (table, schema_name) =
                    reader.get_table_by_name(db_name, schema_path, &real_table_name)?;
                session.check_privilege_for_drop_alter(schema_name, &**table)?;
                table.id.table_id()
            }
            StatementType::ALTER_SINK => {
                let (sink, schema_name) =
                    reader.get_sink_by_name(db_name, schema_path, &real_table_name)?;

                session.check_privilege_for_drop_alter(schema_name, &**sink)?;
                sink.id.sink_id()
            }
            _ => unreachable!(),
        }
    };

    let target_parallelism = match parallelism {
        SetVariableValue::Single(SetVariableValueSingle::Ident(ident))
            if ident.real_value() == "auto" || ident.real_value() == "'auto'".to_lowercase() =>
        {
            PbTableParallelism {
                parallelism: Some(PbParallelism::Auto(AutoParallelism {})),
            }
        }
        SetVariableValue::Single(SetVariableValueSingle::Literal(Value::Number(v))) => {
            PbTableParallelism {
                parallelism: Some(PbParallelism::Fixed(FixedParallelism {
                    parallelism: v.parse().unwrap(),
                })),
            }
        }
        _ => {
            return Err(ErrorCode::InvalidInputSyntax(
                "target parallelism must be a number or auto".to_string(),
            )
            .into());
        }
    };

    let catalog_writer = session.catalog_writer()?;
    catalog_writer
        .alter_parallelism(table_id, target_parallelism)
        .await?;

    Ok(RwPgResponse::empty_result(stmt_type))
}
