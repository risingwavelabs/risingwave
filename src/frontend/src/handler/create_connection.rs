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

use std::collections::HashMap;

use pgwire::pg_response::{PgResponse, StatementType};
use risingwave_common::error::ErrorCode::ProtocolError;
use risingwave_common::error::{Result, RwError};
use risingwave_connector::source::kafka::PRIVATELINK_CONNECTION;
use risingwave_pb::catalog::connection::private_link_service::PrivateLinkProvider;
use risingwave_pb::ddl_service::create_connection_request;
use risingwave_sqlparser::ast::CreateConnectionStatement;

use super::RwPgResponse;
use crate::binder::Binder;
use crate::handler::HandlerArgs;

pub(crate) const CONNECTION_TYPE_PROP: &str = "type";
pub(crate) const CONNECTION_PROVIDER_PROP: &str = "provider";
pub(crate) const CONNECTION_SERVICE_NAME_PROP: &str = "service.name";

pub(crate) const CLOUD_PROVIDER_MOCK: &str = "mock"; // fake privatelink provider for testing
pub(crate) const CLOUD_PROVIDER_AWS: &str = "aws";

#[inline(always)]
fn get_connection_property_required(
    with_properties: &HashMap<String, String>,
    property: &str,
) -> Result<String> {
    with_properties
        .get(property)
        .map(|s| s.to_lowercase())
        .ok_or(RwError::from(ProtocolError(format!(
            "Required property \"{property}\" was not provided"
        ))))
}

fn resolve_private_link_properties(
    with_properties: &HashMap<String, String>,
) -> Result<create_connection_request::PrivateLink> {
    let provider =
        match get_connection_property_required(with_properties, CONNECTION_PROVIDER_PROP)?.as_str()
        {
            CLOUD_PROVIDER_MOCK => PrivateLinkProvider::Mock,
            CLOUD_PROVIDER_AWS => PrivateLinkProvider::Aws,
            provider => {
                return Err(RwError::from(ProtocolError(format!(
                    "Unsupported privatelink provider {}",
                    provider
                ))));
            }
        };
    match provider {
        PrivateLinkProvider::Mock => Ok(create_connection_request::PrivateLink {
            provider: provider.into(),
            service_name: String::new(),
        }),
        PrivateLinkProvider::Aws => {
            let service_name =
                get_connection_property_required(with_properties, CONNECTION_SERVICE_NAME_PROP)?;
            Ok(create_connection_request::PrivateLink {
                provider: provider.into(),
                service_name,
            })
        }
        PrivateLinkProvider::Unspecified => Err(RwError::from(ProtocolError(
            "Privatelink provider unspecified".to_string(),
        ))),
    }
}

fn resolve_create_connection_payload(
    with_properties: &HashMap<String, String>,
) -> Result<create_connection_request::Payload> {
    let connection_type = get_connection_property_required(with_properties, CONNECTION_TYPE_PROP)?;
    let create_connection_payload = match connection_type.as_str() {
        PRIVATELINK_CONNECTION => create_connection_request::Payload::PrivateLink(
            resolve_private_link_properties(with_properties)?,
        ),
        _ => {
            return Err(RwError::from(ProtocolError(format!(
                "Connection type \"{connection_type}\" is not supported"
            ))));
        }
    };
    Ok(create_connection_payload)
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
            Ok(PgResponse::empty_result_with_notice(
                StatementType::CREATE_CONNECTION,
                format!("connection \"{}\" exists, skipping", connection_name),
            ))
        } else {
            Err(e)
        };
    }
    let (database_id, schema_id) = session.get_database_and_schema_id_for_create(schema_name)?;
    let with_properties = handler_args
        .with_options
        .inner()
        .clone()
        .into_iter()
        .collect();

    let create_connection_payload = resolve_create_connection_payload(&with_properties)?;

    let catalog_writer = session.env().catalog_writer();
    catalog_writer
        .create_connection(
            connection_name,
            database_id,
            schema_id,
            create_connection_payload,
        )
        .await?;

    Ok(PgResponse::empty_result(StatementType::CREATE_CONNECTION))
}
