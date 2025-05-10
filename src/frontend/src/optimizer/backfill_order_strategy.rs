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

use std::collections::{HashMap, HashSet};

use risingwave_common::bail;
use risingwave_common::catalog::ObjectId;
use risingwave_pb::common::Uint32Vector;
use risingwave_pb::stream_plan::backfill_order_strategy::Strategy as PbStrategy;
use risingwave_pb::stream_plan::{
    BackfillOrderFixed, BackfillOrderStrategy as PbBackfillOrderStrategy,
};
use risingwave_sqlparser::ast::{BackfillOrderStrategy, ObjectName};

use crate::catalog::CatalogError;
use crate::catalog::root_catalog::SchemaPath;
use crate::catalog::schema_catalog::SchemaCatalog;
use crate::error::Result;
use crate::optimizer::backfill_order_strategy::auto::plan_auto_strategy;
use crate::optimizer::backfill_order_strategy::fixed::plan_fixed_strategy;
use crate::session::SessionImpl;
use crate::{Binder, PlanRef};

mod auto {
    use risingwave_common::catalog::ObjectId;
    use risingwave_pb::stream_plan::backfill_order_strategy::Strategy as PbStrategy;

    use crate::PlanRef;
    use crate::session::SessionImpl;

    pub(super) fn plan_auto_strategy(session: &SessionImpl, plan: PlanRef) -> PbStrategy {
        todo!()
    }
}

mod fixed {
    use std::collections::HashMap;

    use risingwave_common::bail;
    use risingwave_common::catalog::ObjectId;
    use risingwave_pb::common::Uint32Vector;
    use risingwave_pb::stream_plan::BackfillOrderFixed;
    use risingwave_pb::stream_plan::backfill_order_strategy::Strategy as PbStrategy;
    use risingwave_sqlparser::ast::ObjectName;

    use crate::optimizer::backfill_order_strategy::{bind_backfill_relation_id_by_name, has_cycle};
    use crate::session::SessionImpl;

    pub(super) fn plan_fixed_strategy(
        session: &SessionImpl,
        orders: Vec<(ObjectName, ObjectName)>,
    ) -> crate::error::Result<Option<PbStrategy>> {
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
        if has_cycle(&order) {
            bail!("Backfill order strategy has a cycle");
        }
        Ok(Some(PbStrategy::Fixed(BackfillOrderFixed { order })))
    }
}

// FIXME(kwannoel): we can flatten the strategy earlier
/// We don't use query binder directly because its binding rules are different from a query.
/// We only bind tables, materialized views and sources.
/// Queries won't bind duplicate relations in the same query context.
/// But backfill order strategy can have duplicate relations.
pub fn plan_backfill_order_strategy(
    session: &SessionImpl,
    backfill_order_strategy: BackfillOrderStrategy,
    plan: PlanRef,
) -> Result<PbBackfillOrderStrategy> {
    let pb_strategy = match backfill_order_strategy {
        BackfillOrderStrategy::Default | BackfillOrderStrategy::None => None,
        BackfillOrderStrategy::Auto => Some(plan_auto_strategy(session, plan)),
        BackfillOrderStrategy::Fixed(orders) => plan_fixed_strategy(session, orders)?,
    };
    Ok(PbBackfillOrderStrategy {
        strategy: pb_strategy,
    })
}

/// Check if the backfill order has a cycle.
fn has_cycle(order: &HashMap<ObjectId, Uint32Vector>) -> bool {
    fn dfs(
        node: ObjectId,
        order: &HashMap<ObjectId, Uint32Vector>,
        visited: &mut HashSet<ObjectId>,
        stack: &mut HashSet<ObjectId>,
    ) -> bool {
        if stack.contains(&node) {
            return true; // Cycle detected
        }

        if visited.insert(node) {
            stack.insert(node);
            if let Some(downstreams) = order.get(&node) {
                for neighbor in &downstreams.data {
                    if dfs(*neighbor, order, visited, stack) {
                        return true;
                    }
                }
            }
            stack.remove(&node);
        }
        false
    }

    let mut visited = HashSet::new();
    let mut stack = HashSet::new();
    for &start in order.keys() {
        if dfs(start, order, &mut visited, &mut stack) {
            return true;
        }
    }

    false
}

fn bind_backfill_relation_id_by_name(session: &SessionImpl, name: ObjectName) -> Result<ObjectId> {
    let (db_name, schema_name, rel_name) = Binder::resolve_db_schema_qualified_name(name)?;
    let db_name = db_name.unwrap_or(session.database());

    let reader = session.env().catalog_reader().read_guard();

    match schema_name {
        Some(name) => {
            let schema_catalog = reader.get_schema_by_name(&db_name, &name)?;
            bind_table(schema_catalog, &rel_name)
        }
        None => {
            let search_path = session.config().search_path();
            let user_name = session.user_name();
            let schema_path = SchemaPath::Path(&search_path, &user_name);
            let result: Result<Option<(ObjectId, &str)>> = schema_path.try_find(|schema_name| {
                if let Ok(schema_catalog) = reader.get_schema_by_name(&db_name, schema_name)
                    && let Ok(relation_id) = bind_table(schema_catalog, &rel_name)
                {
                    Ok(Some(relation_id))
                } else {
                    Ok(None)
                }
            });
            if let Some((relation_id, _schema_name)) = result? {
                return Ok(relation_id);
            }
            Err(CatalogError::NotFound("table", rel_name.to_owned()).into())
        }
    }
}

fn bind_table(schema_catalog: &SchemaCatalog, name: &String) -> Result<ObjectId> {
    if let Some(table) = schema_catalog.get_created_table_or_any_internal_table_by_name(name) {
        Ok(table.id().table_id)
    } else {
        Err(CatalogError::NotFound("table", name.to_owned()).into())
    }
    // TODO: support source catalog
    // else if let Some(source) = schema_catalog.get_source_by_name(name) {
    //     Ok(source.id)
    // }
    // Err(CatalogError::NotFound("table or source", name.to_owned()).into())
}
