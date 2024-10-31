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

use std::collections::BTreeMap;

use pgwire::pg_response::{PgResponse, StatementType};
use risingwave_connector::source::kafka::PRIVATELINK_CONNECTION;
use risingwave_pb::ddl_service::create_connection_request;
use risingwave_sqlparser::ast::CreateConnectionStatement;

use super::RwPgResponse;
use crate::binder::Binder;
use crate::error::ErrorCode::ProtocolError;
use crate::error::{ErrorCode, Result, RwError};
use crate::handler::HandlerArgs;

pub(crate) const CONNECTION_TYPE_PROP: &str = "type";

#[inline(always)]
fn get_connection_property_required(
    with_properties: &BTreeMap<String, String>,
    property: &str,
) -> Result<String> {
    with_properties
        .get(property)
        .map(|s| s.to_lowercase())
        .ok_or_else(|| {
            RwError::from(ProtocolError(format!(
                "Required property \"{property}\" is not provided"
            )))
        })
}
fn resolve_create_connection_payload(
    with_properties: &BTreeMap<String, String>,
) -> Result<create_connection_request::Payload> {
    let connection_type = get_connection_property_required(with_properties, CONNECTION_TYPE_PROP)?;
    match connection_type.as_str() {
        PRIVATELINK_CONNECTION => Err(RwError::from(ErrorCode::Deprecated(
            "CREATE CONNECTION to Private Link".to_string(),
            "RisingWave Cloud Portal (Please refer to the doc https://docs.risingwave.com/cloud/create-a-connection/)".to_string(),
        ))),
        _ => Err(RwError::from(ProtocolError(format!(
            "Connection type \"{connection_type}\" is not supported"
        )))),
    }
}

pub async fn handle_create_connection(
    handler_args: HandlerArgs,
    stmt: CreateConnectionStatement,
) -> Result<RwPgResponse> {
    let session = handler_args.session.clone();
    let db_name = session.database();
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
    let with_properties = handler_args.with_options.clone().into_connector_props();

    let create_connection_payload = resolve_create_connection_payload(&with_properties)?;

    let catalog_writer = session.catalog_writer()?;
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
