// Copyright 2026 RisingWave Labs
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

//! Decides whether a materialized view can answer queries, and summarizes its definition.
//!
//! A view is usable only if, at every snapshot, its content equals the batch result of the
//! definition that is summarized here. That takes two things:
//!
//! - The summarized definition is what the view computes. The catalog keeps the SQL only, so
//!   it is bound again, in a context that depends on nothing but the catalog, and the result is
//!   verified against what the catalog recorded when the view was created.
//! - Stream and batch compute the same result for it. Only expressions, aggregates and
//!   operators that are known to behave the same are accepted.
//!
//! Anything that cannot be verified makes the view unusable; it never makes a query fail.

use std::collections::{HashMap, HashSet};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;

use pgwire::net::Address;
use pgwire::pg_server::UserAuthenticator;
use risingwave_common::catalog::{
    ConflictBehavior, DEFAULT_SCHEMA_NAME, DEFAULT_SUPER_USER, DEFAULT_SUPER_USER_ID, Engine,
    INITIAL_TABLE_VERSION_ID,
};
use risingwave_common::id::ObjectId;
use risingwave_common::session_config::SessionConfig;
use risingwave_expr::aggregate::{AggType, PbAggKind};
use risingwave_pb::common::PbObjectType;
use risingwave_sqlparser::ast::{EmitMode, Statement};
use thiserror_ext::AsReport;

use super::extract::BlockExtractor;
use super::summary::BlockSummary;
use crate::binder::Binder;
use crate::catalog::TableCatalog;
use crate::catalog::root_catalog::Catalog;
use crate::expr::{ExprImpl, ExprVisitor};
use crate::handler::HandlerArgs;
use crate::optimizer::plan_node::{LogicalPlanRef as PlanRef, VisitExprsRecursive};
use crate::optimizer::{LogicalOptimizer, OptimizerContext};
use crate::planner::Planner;
use crate::session::{AuthContext, FrontendEnv, SessionImpl};

/// The timezone the definition is bound in. A definition that reads the session timezone is
/// unusable, because the timezone the view was created in is not known here.
const TIMEZONE: &str = "UTC";

/// Why a view is unusable.
pub type Unusable = String;

macro_rules! unusable {
    ($($arg:tt)*) => {
        return Err(format!($($arg)*))
    };
}

/// Summarizes the definition of `view`, or tells why the view cannot be used.
///
/// `catalog` must be the catalog `view` was read from.
pub fn summarize_definition(
    env: &FrontendEnv,
    catalog: &Catalog,
    view: &Arc<TableCatalog>,
) -> Result<BlockSummary, Unusable> {
    // 1. What the catalog alone tells.
    if !view.is_mview() || !view.is_created() {
        unusable!("it is not a created materialized view");
    }
    let Ok(statement) = view.create_sql_ast() else {
        unusable!("its definition cannot be parsed");
    };
    let Statement::CreateView {
        materialized: true,
        query,
        emit_mode,
        with_options,
        ..
    } = &statement
    else {
        unusable!("its definition is not CREATE MATERIALIZED VIEW");
    };
    if matches!(emit_mode, Some(EmitMode::OnWindowClose)) {
        unusable!("it emits on window close");
    }
    if !with_options.is_empty() {
        // Options change how the view is maintained. For example, `refresh_interval_sec`
        // refreshes it from time to time instead of keeping it in sync with its inputs.
        unusable!("it is created with options");
    }
    check_content(view)?;
    let dependencies = check_dependencies(catalog, view)?;

    // 2. Bind the definition in a context that is derived from the catalog only.
    let session = Arc::new(definition_session(env, catalog, view)?);
    let mut binder = Binder::new_for_batch(&session);
    let bound = match binder.bind_query(query) {
        Ok(bound) => bound,
        Err(err) => unusable!("its definition cannot be bound: {}", err.as_report()),
    };

    // 3. What binding tells.
    if binder.is_session_dependent() {
        unusable!("its definition reads the session");
    }
    if !binder.included_udfs().is_empty() {
        // The body of a function can be replaced without any trace in the catalog, and its
        // declared volatility cannot be verified.
        unusable!("its definition calls a user-defined function");
    }
    // Every name in the definition resolved to an object of `dependencies` when the view was
    // created, and no two of them share a name. So if binding sees the same set of objects
    // again, every name resolved to the same object as it did then.
    if *binder.included_relations() != dependencies {
        unusable!("its definition now resolves to other objects than when it was created");
    }
    drop(binder);

    // 4. Plan it, and rewrite it the same way a query is rewritten before it is matched.
    let handler_args = HandlerArgs::new(session, &statement, Arc::from(view.definition.as_str()));
    let context = match handler_args.and_then(OptimizerContext::from_handler_args) {
        Ok(context) => context,
        Err(err) => unusable!("its definition cannot be planned: {}", err.as_report()),
    };
    let plan = match Planner::new_for_batch_dql(context.into()).plan_query(bound) {
        Ok(root) => root.into_unordered_subplan(),
        Err(err) => unusable!("its definition cannot be planned: {}", err.as_report()),
    };
    // Before `now()` is inlined as a constant.
    let mut purity = PurityChecker::default();
    plan.visit_exprs_recursive(&mut purity);
    if purity.impure {
        unusable!("its definition has an expression that is not deterministic");
    }
    let plan = match LogicalOptimizer::normalize_for_match(plan) {
        Ok(plan) => plan,
        Err(err) => unusable!("its definition cannot be planned: {}", err.as_report()),
    };

    // 5. What the plan tells.
    check_aggregates(&plan)?;
    let visible_types = view
        .columns()
        .iter()
        .filter(|column| !column.is_hidden())
        .map(|column| column.data_type().clone());
    if !plan.schema().data_types().into_iter().eq(visible_types) {
        // The same check as for a view: binding may infer other types than it did when the
        // view was created by another version.
        unusable!("its definition now has other column types than the view");
    }

    // 6. Summarize.
    let mut extractor = BlockExtractor::new(TIMEZONE.to_owned());
    let Some(block) = extractor.extract(&plan) else {
        unusable!("its definition has an expression that cannot be compared");
    };
    if extractor.timezone_used() {
        unusable!("its definition depends on the session timezone");
    }
    if block.summary.has_opaque_input() {
        unusable!("its definition has an operator that cannot be compared");
    }
    Ok(block.summary)
}

/// Whether the table holds what it is defined to hold.
fn check_content(table: &TableCatalog) -> Result<(), Unusable> {
    // A table that drops old rows no longer equals its definition, and neither does a view
    // whose input dropped rows after the view read them.
    if table.retention_seconds.is_some()
        || table.clean_watermark_index_in_pk.is_some()
        || !table.clean_watermark_indices.is_empty()
    {
        unusable!("{} drops old rows", table.name);
    }
    // `ALTER MATERIALIZED VIEW ... AS` keeps the rows computed by the old definition, and marks
    // the view to overwrite on conflict because its changes are no longer consistent. So
    // neither the view nor a view computed from it equals its definition anymore.
    if table.is_mview() && table.conflict_behavior != ConflictBehavior::NoCheck {
        unusable!("the definition of {} has been altered", table.name);
    }
    Ok(())
}

/// Checks what the view was recorded to depend on, and returns the relations among it.
fn check_dependencies(
    catalog: &Catalog,
    view: &TableCatalog,
) -> Result<HashSet<ObjectId>, Unusable> {
    let Ok(database) = catalog.get_database_by_id(view.database_id) else {
        unusable!("its database is not found");
    };
    let mut relations = HashSet::new();
    let mut names: HashMap<&str, ObjectId> = HashMap::new();
    for dependency in catalog.object_dependencies_of(view.id.as_object_id()) {
        let id = dependency.referenced_object_id;
        let name = match dependency.referenced_object_type {
            PbObjectType::Table | PbObjectType::Mview => {
                let Ok(table) = catalog.get_any_table_by_id(id.as_table_id()) else {
                    unusable!("a table it depends on is not found");
                };
                if table.database_id != view.database_id {
                    // Only jobs of one database share their snapshots.
                    unusable!("{} is in another database", table.name);
                }
                if table.engine() != Engine::Hummock {
                    unusable!("{} is not stored in Hummock", table.name);
                }
                check_content(table)?;
                // The definition is bound against the columns that the table has now. If the
                // table has ever changed, a name in the definition may resolve differently
                // than when the view was created, without any error. The columns of a
                // materialized view never change.
                if table.is_user_table()
                    && table
                        .version
                        .as_ref()
                        .is_none_or(|version| version.version_id != INITIAL_TABLE_VERSION_ID)
                {
                    unusable!("the schema of {} has been altered", table.name);
                }
                table.name.as_str()
            }
            PbObjectType::View => {
                let Some(dependency_view) = database
                    .iter_schemas()
                    .find_map(|schema| schema.get_view_by_id(id.as_view_id()))
                else {
                    unusable!("a view it depends on is not in its database");
                };
                dependency_view.name.as_str()
            }
            // An index that the stream plan picked. It is not named in the definition.
            PbObjectType::Index => continue,
            // Sources, sinks, functions, secrets and so on.
            _ => unusable!("it depends on an object that is neither a table nor a view"),
        };
        if names.insert(name, id).is_some_and(|other| other != id) {
            unusable!("it depends on two objects named {name}");
        }
        relations.insert(id);
    }
    if relations.is_empty() {
        unusable!("it reads no table");
    }
    Ok(relations)
}

/// A session that is not registered anywhere, in the database of the view and with default
/// settings, so that binding in it depends on the catalog only.
fn definition_session(
    env: &FrontendEnv,
    catalog: &Catalog,
    view: &TableCatalog,
) -> Result<SessionImpl, Unusable> {
    let Ok(database) = catalog.get_database_by_id(view.database_id) else {
        unusable!("its database is not found");
    };
    let Some(schema) = database.get_schema_by_id(view.schema_id) else {
        unusable!("its schema is not found");
    };
    // The search path the view was created with is not recorded. Guess the usual one for its
    // owner, starting with the schema of the view. A wrong guess is caught by the comparison
    // with the recorded dependencies.
    let owner = env
        .user_info_reader()
        .read_guard()
        .get_user_name_by_id(view.owner);
    let search_path = [
        Some(schema.name()),
        owner,
        Some(DEFAULT_SCHEMA_NAME.to_owned()),
    ]
    .into_iter()
    .flatten()
    .collect::<Vec<_>>()
    .join(", ");

    // The built-in super user can always bind the definition. Whoever it is does not change
    // the result, because a definition that reads the session is rejected.
    let session = SessionImpl::new(
        env.clone(),
        AuthContext::new(
            database.name().to_owned(),
            DEFAULT_SUPER_USER.to_owned(),
            DEFAULT_SUPER_USER_ID,
        ),
        UserAuthenticator::None,
        (0, 0),
        Arc::new(Address::Tcp(SocketAddr::new(
            IpAddr::V4(Ipv4Addr::UNSPECIFIED),
            0,
        ))),
        SessionConfig::default(),
    );
    for (key, value) in [
        ("search_path", search_path.as_str()),
        ("timezone", TIMEZONE),
    ] {
        if session.set_config(key, value.to_owned()).is_err() {
            unusable!("its schema or owner has a name that cannot be put in a search path");
        }
    }
    Ok(session)
}

#[derive(Default)]
struct PurityChecker {
    impure: bool,
}

impl ExprVisitor for PurityChecker {
    fn visit_expr(&mut self, expr: &ExprImpl) {
        self.impure |= expr.is_impure();
    }
}

/// Only aggregates whose result is fully determined by the multiset of their input, and that
/// stream and batch compute with the same exact algorithm.
fn check_aggregates(plan: &PlanRef) -> Result<(), Unusable> {
    if let Some(agg) = plan.as_logical_agg() {
        for call in agg.agg_calls() {
            let allowed = matches!(
                call.agg_type,
                AggType::Builtin(
                    PbAggKind::Count
                        | PbAggKind::Sum
                        | PbAggKind::Sum0
                        | PbAggKind::Min
                        | PbAggKind::Max
                )
            );
            if !allowed || !call.order_by.is_empty() {
                unusable!("its definition has an aggregate that is not supported");
            }
        }
    }
    plan.inputs().iter().try_for_each(check_aggregates)
}
