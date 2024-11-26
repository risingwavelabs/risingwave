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

use risingwave_common::types::Fields;
use risingwave_frontend_macro::system_catalog;
use risingwave_pb::catalog::connection::Info;

use crate::catalog::system_catalog::SysCatalogReaderImpl;
use crate::error::Result;
use crate::handler::create_connection::print_connection_params;

#[derive(Fields)]
struct RwConnection {
    #[primary_key]
    id: i32,
    name: String,
    schema_id: i32,
    owner: i32,
    type_: String,
    provider: String,
    acl: Vec<String>,
    connection_params: String,
}

#[system_catalog(table, "rw_catalog.rw_connections")]
fn read_rw_connections(reader: &SysCatalogReaderImpl) -> Result<Vec<RwConnection>> {
    let catalog_reader = reader.catalog_reader.read_guard();
    let schemas = catalog_reader.iter_schemas(&reader.auth_context.database)?;

    // todo: redesign the internal table for connection params
    Ok(schemas
        .flat_map(|schema| {
            schema.iter_connections().map(|conn| {
                let mut rw_connection = RwConnection {
                    id: conn.id as i32,
                    name: conn.name.clone(),
                    schema_id: schema.id() as i32,
                    owner: conn.owner as i32,
                    type_: conn.connection_type().into(),
                    provider: "".to_string(),
                    acl: vec![],
                    connection_params: "".to_string(),
                };
                if let Some(ref params) = conn.connection_params {
                    rw_connection.connection_params = print_connection_params(params, schema);
                } else if matches!(conn.info, Info::PrivateLinkService(_)) {
                    // make sure the connection is private link service or it can lead to panic
                    rw_connection.provider = conn.provider().into();
                }

                rw_connection
            })
        })
        .collect())
}
