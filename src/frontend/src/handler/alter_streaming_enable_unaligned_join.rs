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

use pgwire::pg_response::{PgResponse, StatementType};
use risingwave_common::bail;
use risingwave_sqlparser::ast::ObjectName;

use super::{HandlerArgs, RwPgResponse};
use crate::binder::{Binder, Relation};
use crate::catalog::CatalogError;
use crate::error::{ErrorCode, Result};

pub async fn handle_alter_streaming_enable_unaligned_join(
    handler_args: HandlerArgs,
    name: ObjectName,
    enable: bool,
) -> Result<RwPgResponse> {
    if cfg!(debug_assertions) {
        let session = handler_args.session.clone();
        let job_id = {
            let mut binder = Binder::new_for_system(&session);
            Binder::validate_cross_db_reference(&session.database(), &name)?;
            let not_found_err = CatalogError::NotFound("stream job", name.to_string());

            if let Ok(relation) = binder.bind_catalog_relation_by_object_name(name.clone(), true) {
                match relation {
                    Relation::Source(s) => {
                        if s.is_shared() {
                            s.catalog.id
                        } else {
                            bail!(ErrorCode::NotSupported(
                                "source has no unaligned_join".to_owned(),
                                "Please only target materialized views or sinks".to_owned(),
                            ));
                        }
                    }
                    Relation::BaseTable(t) => t.table_catalog.id.table_id,
                    Relation::SystemTable(_t) => {
                        bail!(ErrorCode::NotSupported(
                            "system table has no unaligned_join".to_owned(),
                            "Please only target materialized views or sinks".to_owned(),
                        ));
                    }
                    Relation::Share(_s) => {
                        bail!(ErrorCode::NotSupported(
                            "view has no unaligned_join".to_owned(),
                            "Please only target materialized views or sinks".to_owned(),
                        ));
                    }
                    _ => {
                        // Other relation types (Subquery, Join, etc.) are not directly describable.
                        return Err(not_found_err.into());
                    }
                }
            } else if let Ok(sink) = binder.bind_sink_by_name(name.clone()) {
                sink.sink_catalog.id.sink_id
            } else {
                return Err(not_found_err.into());
            }
        };
        let meta_client = session.env().meta_client();
        meta_client
            .set_sync_log_store_aligned(job_id, !enable)
            .await?;
        Ok(PgResponse::empty_result(
            StatementType::ALTER_MATERIALIZED_VIEW,
        ))
    } else {
        bail!("ALTER STREAMING ENABLE UNALIGNED JOIN is only supported in debug mode");
    }
}
