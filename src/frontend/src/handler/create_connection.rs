use std::collections::HashMap;

use pgwire::pg_response::{PgResponse, StatementType};
use risingwave_common::error::ErrorCode::ProtocolError;
use risingwave_common::error::{Result, RwError};
use risingwave_connector::source::kafka::PRIVATELINK_CONNECTION;
use risingwave_pb::ddl_service::create_connection_request;
use risingwave_sqlparser::ast::CreateConnectionStatement;
use serde_json;

use super::RwPgResponse;
use crate::binder::Binder;
use crate::handler::HandlerArgs;

pub(crate) const CONNECTION_TYPE_PROP: &str = "type";
pub(crate) const CONNECTION_PROVIDER_PROP: &str = "provider";
pub(crate) const CONNECTION_SERVICE_NAME_PROP: &str = "service.name";
pub(crate) const CONNECTION_AVAIL_ZONE_PROP: &str = "availability.zones";

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
    let provider = get_connection_property_required(with_properties, CONNECTION_PROVIDER_PROP)?;
    let service_name =
        get_connection_property_required(with_properties, CONNECTION_SERVICE_NAME_PROP)?;
    let availability_zones_str =
        get_connection_property_required(with_properties, CONNECTION_AVAIL_ZONE_PROP)?;
    let availability_zones: Vec<String> =
        serde_json::from_str(&availability_zones_str).map_err(|e| {
            RwError::from(ProtocolError(format!(
                "Can not parse {}: {}",
                CONNECTION_AVAIL_ZONE_PROP, e
            )))
        })?;
    Ok(create_connection_request::PrivateLink {
        provider,
        service_name,
        availability_zones,
    })
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

    session.check_relation_name_duplicated(stmt.connection_name.clone())?;

    Binder::resolve_connection_name(stmt.connection_name)?;

    let with_properties = handler_args
        .with_options
        .inner()
        .clone()
        .into_iter()
        .collect();

    let create_connection_payload = resolve_create_connection_payload(&with_properties)?;

    let catalog_writer = session.env().catalog_writer();
    catalog_writer
        .create_connection(create_connection_payload)
        .await?;

    Ok(PgResponse::empty_result(StatementType::CREATE_CONNECTION))
}
