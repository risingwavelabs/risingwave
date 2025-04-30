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

use std::collections::BTreeMap;

use pgwire::pg_response::{PgResponse, StatementType};
use risingwave_common::system_param::reader::SystemParamsRead;
use risingwave_connector::connector_common::SCHEMA_REGISTRY_CONNECTION_TYPE;
use risingwave_connector::sink::elasticsearch_opensearch::elasticsearch::ES_SINK;
use risingwave_connector::source::enforce_secret_connection;
use risingwave_connector::source::iceberg::ICEBERG_CONNECTOR;
use risingwave_connector::source::kafka::{KAFKA_CONNECTOR, PRIVATELINK_CONNECTION};
use risingwave_pb::catalog::connection_params::ConnectionType;
use risingwave_pb::catalog::{ConnectionParams, PbConnectionParams};
use risingwave_pb::ddl_service::create_connection_request;
use risingwave_pb::secret::SecretRef;
use risingwave_sqlparser::ast::CreateConnectionStatement;

use super::RwPgResponse;
use crate::WithOptions;
use crate::binder::Binder;
use crate::catalog::SecretId;
use crate::catalog::schema_catalog::SchemaCatalog;
use crate::error::ErrorCode::ProtocolError;
use crate::error::{ErrorCode, Result, RwError};
use crate::handler::HandlerArgs;
use crate::session::SessionImpl;
use crate::utils::{resolve_privatelink_in_with_option, resolve_secret_ref_in_with_options};

pub(crate) const CONNECTION_TYPE_PROP: &str = "type";

#[inline(always)]
fn get_connection_property_required(
    with_properties: &mut BTreeMap<String, String>,
    property: &str,
) -> Result<String> {
    with_properties.remove(property).ok_or_else(|| {
        RwError::from(ProtocolError(format!(
            "Required property \"{property}\" is not provided"
        )))
    })
}
fn resolve_create_connection_payload(
    with_properties: WithOptions,
    session: &SessionImpl,
) -> Result<create_connection_request::Payload> {
    if !with_properties.connection_ref().is_empty() {
        return Err(RwError::from(ErrorCode::InvalidParameterValue(
            "Connection reference is not allowed in options in CREATE CONNECTION".to_owned(),
        )));
    }

    let (mut props, secret_refs) =
        resolve_secret_ref_in_with_options(with_properties, session)?.into_parts();
    let connection_type = get_connection_property_required(&mut props, CONNECTION_TYPE_PROP)?;
    let connection_type = match connection_type.as_str() {
        PRIVATELINK_CONNECTION => {
            return Err(RwError::from(ErrorCode::Deprecated(
            "CREATE CONNECTION to Private Link".to_owned(),
            "RisingWave Cloud Portal (Please refer to the doc https://docs.risingwave.com/cloud/create-a-connection/)".to_owned(),
        )));
        }
        KAFKA_CONNECTOR => ConnectionType::Kafka,
        ICEBERG_CONNECTOR => ConnectionType::Iceberg,
        SCHEMA_REGISTRY_CONNECTION_TYPE => ConnectionType::SchemaRegistry,
        ES_SINK => ConnectionType::Elasticsearch,
        _ => {
            return Err(RwError::from(ProtocolError(format!(
                "Connection type \"{connection_type}\" is not supported"
            ))));
        }
    };
    Ok(create_connection_request::Payload::ConnectionParams(
        ConnectionParams {
            connection_type: connection_type as i32,
            properties: props.into_iter().collect(),
            secret_refs: secret_refs.into_iter().collect(),
        },
    ))
}

pub async fn handle_create_connection(
    handler_args: HandlerArgs,
    stmt: CreateConnectionStatement,
) -> Result<RwPgResponse> {
    let session = handler_args.session.clone();
    let db_name = &session.database();
    let (schema_name, connection_name) =
        Binder::resolve_schema_qualified_name(db_name, stmt.connection_name.clone())?;

    if let Err(e) = session.check_connection_name_duplicated(stmt.connection_name) {
        return if stmt.if_not_exists {
            Ok(PgResponse::builder(StatementType::CREATE_CONNECTION)
                .notice(format!(
                    "connection \"{}\" exists, skipping",
                    connection_name
                ))
                .into())
        } else {
            Err(e)
        };
    }
    let (database_id, schema_id) = session.get_database_and_schema_id_for_create(schema_name)?;
    let mut with_properties = handler_args.with_options.clone().into_connector_props();
    resolve_privatelink_in_with_option(&mut with_properties)?;
    let create_connection_payload = resolve_create_connection_payload(with_properties, &session)?;

    let catalog_writer = session.catalog_writer()?;

    if session
        .env()
        .system_params_manager()
        .get_params()
        .load()
        .enforce_secret()
    {
        use risingwave_pb::ddl_service::create_connection_request::Payload::ConnectionParams;
        let ConnectionParams(cp) = &create_connection_payload else {
            unreachable!()
        };
        enforce_secret_connection(
            &cp.connection_type(),
            cp.properties.keys().map(|s| s.as_str()),
        )?;
    }

    catalog_writer
        .create_connection(
            connection_name,
            database_id,
            schema_id,
            session.user_id(),
            create_connection_payload,
        )
        .await?;

    Ok(PgResponse::empty_result(StatementType::CREATE_CONNECTION))
}

pub fn print_connection_params(params: &PbConnectionParams, schema: &SchemaCatalog) -> String {
    let print_secret_ref = |secret_ref: &SecretRef| -> String {
        let secret_name = schema
            .get_secret_by_id(&SecretId::from(secret_ref.secret_id))
            .map(|s| s.name.as_str())
            .unwrap();
        format!(
            "SECRET {} AS {}",
            secret_name,
            secret_ref.get_ref_as().unwrap().as_str_name()
        )
    };
    let deref_secrets = params
        .get_secret_refs()
        .iter()
        .map(|(k, v)| (k.clone(), print_secret_ref(v)));
    let mut props = params.get_properties().clone();
    props.extend(deref_secrets);
    serde_json::to_string(&props).unwrap()
}
