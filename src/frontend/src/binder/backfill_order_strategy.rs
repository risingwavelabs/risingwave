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

use risingwave_common::catalog::ObjectId;
use risingwave_common::session_config::USER_NAME_WILD_CARD;
use risingwave_pb::common::Uint32Vector;
use risingwave_pb::stream_plan::backfill_order_strategy::Strategy as PbStrategy;
use risingwave_pb::stream_plan::{
    BackfillOrderFixed, BackfillOrderStrategy as PbBackfillOrderStrategy,
};
use risingwave_sqlparser::ast::{BackfillOrderStrategy, ObjectName};

use crate::Binder;
use crate::catalog::CatalogError;
use crate::catalog::schema_catalog::SchemaCatalog;
use crate::error::Result;
use crate::session::SessionImpl;

// FIXME(kwannoel): we can flatten the strategy earlier
/// We don't use query binder directly because its binding rules are different from a query.
/// We only bind tables, materialized views and sources.
/// Queries won't bind duplicate relations in the same query context.
/// But backfill order strategy can have duplicate relations.
pub fn bind_backfill_order_strategy(
    session: &SessionImpl,
    backfill_order_strategy: BackfillOrderStrategy,
) -> Result<PbBackfillOrderStrategy> {
    let pb_strategy = match backfill_order_strategy {
        BackfillOrderStrategy::Auto
        | BackfillOrderStrategy::Default
        | BackfillOrderStrategy::None => None,
        BackfillOrderStrategy::Fixed(orders) => {
            let mut order: HashMap<ObjectId, Uint32Vector> = HashMap::new();
            for (start_name, end_name) in orders {
                let start_relation_id = bind_backfill_relation_id_by_name(session, start_name)?;
                let end_relation_id = bind_backfill_relation_id_by_name(session, end_name)?;
                order
                    .entry(start_relation_id)
                    .or_default()
                    .data
                    .push(end_relation_id);
            }
            Some(PbStrategy::Fixed(BackfillOrderFixed { order }))
        }
    };
    Ok(PbBackfillOrderStrategy {
        strategy: pb_strategy,
    })
}

fn bind_backfill_relation_id_by_name(session: &SessionImpl, name: ObjectName) -> Result<ObjectId> {
    let (db_name, schema_name, rel_name) = Binder::resolve_db_schema_qualified_name(name)?;
    let db_name = db_name.unwrap_or(session.database());

    let reader = session.env().catalog_reader().read_guard();

    match schema_name {
        Some(name) => {
            let schema_catalog = reader.get_schema_by_name(&db_name, &name)?;
            bind_source_or_table(schema_catalog, &rel_name)
        }
        None => {
            let search_path = session.config().search_path();
            for path in search_path.path() {
                let schema_name = if path == USER_NAME_WILD_CARD {
                    &session.user_name()
                } else {
                    path
                };
                if let Ok(schema_catalog) = reader.get_schema_by_name(&db_name, schema_name)
                    && let Ok(relation_id) = bind_source_or_table(schema_catalog, &rel_name)
                {
                    return Ok(relation_id);
                }
            }
            Err(CatalogError::NotFound("table or source", rel_name.to_owned()).into())
        }
    }
}

fn bind_source_or_table(schema_catalog: &SchemaCatalog, name: &String) -> Result<ObjectId> {
    if let Some(table) = schema_catalog.get_created_table_or_any_internal_table_by_name(name) {
        Ok(table.id().table_id)
    } else if let Some(source) = schema_catalog.get_source_by_name(name) {
        Ok(source.id)
    } else {
        Err(CatalogError::NotFound("table or source", name.to_owned()).into())
    }
}
