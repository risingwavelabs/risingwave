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

use std::rc::Rc;

use pgwire::pg_response::{PgResponse, StatementType};
use risingwave_common::catalog::UserId;
use risingwave_sqlparser::ast::CreateSubscriptionStatement;

use super::{HandlerArgs, RwPgResponse};
use crate::catalog::subscription_catalog::{
    SubscriptionCatalog, SubscriptionId, SubscriptionState,
};
use crate::error::Result;
use crate::scheduler::streaming_manager::CreatingStreamingJobInfo;
use crate::session::{DuplicateCheckOutcome, SessionImpl};
use crate::{Binder, OptimizerContext, OptimizerContextRef};

pub fn create_subscription_catalog(
    session: &SessionImpl,
    context: OptimizerContextRef,
    stmt: CreateSubscriptionStatement,
) -> Result<SubscriptionCatalog> {
    let db_name = &session.database();
    let (subscription_schema_name, subscription_name) =
        Binder::resolve_schema_qualified_name(db_name, stmt.subscription_name.clone())?;
    let (table_schema_name, subscription_from_table_name) =
        Binder::resolve_schema_qualified_name(db_name, stmt.subscription_from.clone())?;
    let (table_database_id, table_schema_id) =
        session.get_database_and_schema_id_for_create(table_schema_name.clone())?;
    let (subscription_database_id, subscription_schema_id) =
        session.get_database_and_schema_id_for_create(subscription_schema_name.clone())?;
    let definition = context.normalized_sql().to_owned();
    let dependent_table_id = session
        .get_table_by_name(
            &subscription_from_table_name,
            table_database_id,
            table_schema_id,
        )?
        .id;

    let mut subscription_catalog = SubscriptionCatalog {
        id: SubscriptionId::placeholder(),
        name: subscription_name,
        definition,
        retention_seconds: 0,
        database_id: subscription_database_id,
        schema_id: subscription_schema_id,
        dependent_table_id,
        owner: UserId::new(session.user_id()),
        initialized_at_epoch: None,
        created_at_epoch: None,
        created_at_cluster_version: None,
        initialized_at_cluster_version: None,
        subscription_state: SubscriptionState::Init,
    };

    subscription_catalog.set_retention_seconds(context.with_options())?;

    Ok(subscription_catalog)
}

pub async fn handle_create_subscription(
    handle_args: HandlerArgs,
    stmt: CreateSubscriptionStatement,
) -> Result<RwPgResponse> {
    let session = handle_args.session.clone();

    if let DuplicateCheckOutcome::ExistsAndIgnored(resp) = session.check_relation_name_duplicated(
        stmt.subscription_name.clone(),
        StatementType::CREATE_SUBSCRIPTION,
        stmt.if_not_exists,
    )? {
        return Ok(resp);
    };
    let subscription_catalog = {
        let context = Rc::new(OptimizerContext::from_handler_args(handle_args));
        create_subscription_catalog(&session, context.clone(), stmt)?
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
        .create_subscription(subscription_catalog.to_proto())
        .await?;

    Ok(PgResponse::empty_result(StatementType::CREATE_SUBSCRIPTION))
}
