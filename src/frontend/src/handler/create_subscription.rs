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

use std::rc::Rc;

use either::Either;
use pgwire::pg_response::{PgResponse, StatementType};
use risingwave_common::catalog::UserId;
use risingwave_common::error::Result;
use risingwave_pb::stream_plan::stream_fragment_graph::Parallelism;
use risingwave_sqlparser::ast::{CreateSubscriptionStatement, Query};

use super::create_sink::gen_sink_query_from_name;
use super::privilege::resolve_query_privileges;
use super::{HandlerArgs, RwPgResponse};
use crate::catalog::subscription_catalog::SubscriptionCatalog;
use crate::optimizer::RelationCollectorVisitor;
use crate::scheduler::streaming_manager::CreatingStreamingJobInfo;
use crate::session::SessionImpl;
use crate::{
    build_graph, Binder, Explain, OptimizerContext, OptimizerContextRef, PlanRef, Planner,
};

// used to store result of `gen_subscription_plan`
pub struct SubscriptionPlanContext {
    pub query: Box<Query>,
    pub subscription_plan: PlanRef,
    pub subscription_catalog: SubscriptionCatalog,
}

pub fn gen_subscription_plan(
    session: &SessionImpl,
    context: OptimizerContextRef,
    stmt: CreateSubscriptionStatement,
) -> Result<SubscriptionPlanContext> {
    let db_name = session.database();
    let (schema_name, subscription_name) =
        Binder::resolve_schema_qualified_name(db_name, stmt.subscription_name.clone())?;
    let subscription_from_table_name = stmt
        .subscription_from
        .0
        .last()
        .unwrap()
        .real_value()
        .clone();
    let query = Box::new(gen_sink_query_from_name(stmt.subscription_from)?);

    let (database_id, schema_id) =
        session.get_database_and_schema_id_for_create(schema_name.clone())?;

    let definition = context.normalized_sql().to_owned();

    let (dependent_relations, bound) = {
        let mut binder = Binder::new_for_stream(session);
        let bound = binder.bind_query(*query.clone())?;
        (binder.included_relations(), bound)
    };

    let check_items = resolve_query_privileges(&bound);
    session.check_privileges(&check_items)?;

    let with_options = context.with_options().clone();
    let mut plan_root = Planner::new(context).plan_query(bound)?;

    let subscription_plan = plan_root.gen_subscription_plan(
        database_id,
        schema_id,
        dependent_relations.clone(),
        subscription_name,
        definition,
        with_options,
        false,
        db_name.to_string(),
        subscription_from_table_name,
        UserId::new(session.user_id()),
    )?;

    let subscription_catalog = subscription_plan.subscription_catalog();

    let subscription_plan: PlanRef = subscription_plan.into();

    let ctx = subscription_plan.ctx();
    let explain_trace = ctx.is_explain_trace();
    if explain_trace {
        ctx.trace("Create Subscription:");
        ctx.trace(subscription_plan.explain_to_string());
    }

    let dependent_relations =
        RelationCollectorVisitor::collect_with(dependent_relations, subscription_plan.clone());

    let subscription_catalog = subscription_catalog.add_dependent_relations(dependent_relations);
    Ok(SubscriptionPlanContext {
        query,
        subscription_plan,
        subscription_catalog,
    })
}

pub async fn handle_create_subscription(
    handle_args: HandlerArgs,
    stmt: CreateSubscriptionStatement,
) -> Result<RwPgResponse> {
    let session = handle_args.session.clone();

    if let Either::Right(resp) = session.check_relation_name_duplicated(
        stmt.subscription_name.clone(),
        StatementType::CREATE_SUBSCRIPTION,
        stmt.if_not_exists,
    )? {
        return Ok(resp);
    };

    let (subscription_catalog, graph) = {
        let context = Rc::new(OptimizerContext::from_handler_args(handle_args));

        let SubscriptionPlanContext {
            query: _,
            subscription_plan,
            subscription_catalog,
        } = gen_subscription_plan(&session, context.clone(), stmt)?;

        let mut graph = build_graph(subscription_plan)?;

        graph.parallelism =
            session
                .config()
                .streaming_parallelism()
                .map(|parallelism| Parallelism {
                    parallelism: parallelism.get(),
                });

        (subscription_catalog, graph)
    };

    let _job_guard =
        session
            .env()
            .creating_streaming_job_tracker()
            .guard(CreatingStreamingJobInfo::new(
                session.session_id(),
                subscription_catalog.database_id,
                subscription_catalog.schema_id,
                subscription_catalog.name.clone(),
            ));

    let catalog_writer = session.catalog_writer()?;
    catalog_writer
        .create_subscription(subscription_catalog.to_proto(), graph)
        .await?;

    Ok(PgResponse::empty_result(StatementType::CREATE_SUBSCRIPTION))
}
