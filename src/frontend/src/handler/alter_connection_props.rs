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

use std::collections::{BTreeMap, HashSet};

use risingwave_connector::source::kafka::{KAFKA_PROPS_BROKER_KEY, KAFKA_PROPS_BROKER_KEY_ALIAS};
use risingwave_pb::catalog::connection::Info as ConnectionInfo;

use super::RwPgResponse;
use crate::catalog::root_catalog::SchemaPath;
use crate::error::{ErrorCode, Result};
use crate::handler::{HandlerArgs, ObjectName, SqlOption, StatementType};
use crate::utils::{resolve_connection_ref_and_secret_ref, resolve_privatelink_for_alter};
use crate::{Binder, WithOptions};

pub async fn handle_alter_connection_connector_props(
    handler_args: HandlerArgs,
    connection_name: ObjectName,
    alter_props: Vec<SqlOption>,
) -> Result<RwPgResponse> {
    let session = handler_args.session;
    let db_name = &session.database();
    let (schema_name, real_connection_name) =
        Binder::resolve_schema_qualified_name(db_name, &connection_name)?;
    let search_path = session.config().search_path();
    let user_name = &session.user_name();
    let schema_path = SchemaPath::new(schema_name.as_deref(), &search_path, user_name);

    let (connection_id, old_props) = {
        let reader = session.env().catalog_reader().read_guard();
        let (connection, schema_name) =
            reader.get_connection_by_name(db_name, schema_path, &real_connection_name)?;

        session.check_privilege_for_drop_alter(schema_name, &**connection)?;

        tracing::debug!(
            "handle_alter_connection_connector_props triggered: connection_name: {}, connection_id: {}",
            real_connection_name,
            connection.id
        );

        let props: BTreeMap<String, String> = match &connection.info {
            ConnectionInfo::ConnectionParams(params) => params
                .properties
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
            #[expect(deprecated)]
            ConnectionInfo::PrivateLinkService(_) => {
                return Err(ErrorCode::InvalidParameterValue(
                    "Private Link Service connections cannot be altered; please create a new connection".to_owned(),
                ).into());
            }
        };
        (connection.id, props)
    };

    let meta_client = session.env().meta_client();
    let (resolved_with_options, _, connector_conn_ref) = resolve_connection_ref_and_secret_ref(
        WithOptions::try_from(alter_props.as_ref() as &[SqlOption])?,
        &session,
        None,
    )?;
    let (user_set_props, changed_secret_refs) = resolved_with_options.into_parts();

    if connector_conn_ref.is_some() {
        return Err(ErrorCode::InvalidInputSyntax(
            "ALTER CONNECTION CONNECTOR does not support nested CONNECTION references".to_owned(),
        )
        .into());
    }

    // Record which keys the user explicitly touched.
    let touched_set_keys: HashSet<String> = user_set_props.keys().cloned().collect();
    let touched_drop_keys: HashSet<String> = HashSet::new(); // ALTER CONNECTION SET does not support DROP

    // Merge old props with user-set props (user values override).
    let mut merged_props = old_props.clone();
    for (k, v) in &user_set_props {
        merged_props.insert(k.clone(), v.clone());
    }

    // Resolve PrivateLink-related properties and compute effective delta.
    let changed_props = resolve_privatelink_for_alter(
        &old_props,
        &merged_props,
        &touched_set_keys,
        &touched_drop_keys,
    )?;

    meta_client
        .alter_connection_connector_props(
            connection_id.as_raw_id(),
            changed_props,
            changed_secret_refs,
        )
        .await?;

    if user_set_props.contains_key(KAFKA_PROPS_BROKER_KEY)
        || user_set_props.contains_key(KAFKA_PROPS_BROKER_KEY_ALIAS)
    {
        Ok(RwPgResponse::builder(StatementType::ALTER_CONNECTION)
            .notice("changing properties.bootstrap.server may point to a different Kafka cluster and cause data inconsistency".to_owned())
            .into())
    } else {
        Ok(RwPgResponse::empty_result(StatementType::ALTER_CONNECTION))
    }
}
