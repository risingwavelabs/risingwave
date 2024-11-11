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

use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;

use anyhow::{anyhow, Context};
use axum::body::Bytes;
use axum::extract::{Extension, Path};
use axum::http::{HeaderMap, Method, StatusCode};
use axum::routing::post;
use axum::Router;
use pgwire::net::Address;
use pgwire::pg_server::SessionManager;
use risingwave_common::secret::LocalSecretManager;
use risingwave_sqlparser::ast::{Expr, ObjectName};
use tokio::net::TcpListener;
use tower::ServiceBuilder;
use tower_http::add_extension::AddExtensionLayer;
use tower_http::compression::CompressionLayer;
use tower_http::cors::{self, CorsLayer};

use crate::handler::handle;
use crate::webhook::utils::{err, Result};
mod utils;

#[derive(Clone)]
pub struct WebhookService {
    pub webhook_addr: SocketAddr,
    // pub compute_clients: ComputeClientPool,
}
pub type Service = Arc<WebhookService>;

pub(super) mod handlers {

    use std::net::Ipv4Addr;

    use risingwave_pb::catalog::WebhookSourceInfo;
    use risingwave_sqlparser::ast::{Query, SetExpr, Statement, Value, Values};
    use utils::verify_signature;

    use super::*;
    use crate::catalog::root_catalog::SchemaPath;
    use crate::session::SESSION_MANAGER;

    pub async fn handle_post_request(
        Extension(_srv): Extension<Service>,
        headers: HeaderMap,
        Path((user, database, schema, table)): Path<(String, String, String, String)>,
        body: Bytes,
    ) -> Result<()> {
        println!(
            "WKXLOG receive something: {:?}, \ndatabase: {}, \ntable: {}, \nheaders: {:?}",
            body, database, table, headers
        );
        let session_mgr = SESSION_MANAGER
            .get()
            .expect("session manager has been initialized");

        let dummy_addr = Address::Tcp(SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)),
            5691, // port of meta
        ));

        // TODO(kexiang): optimize this
        // get a session object for the corresponding user and database
        let session = session_mgr
            .connect(database.as_str(), user.as_str(), Arc::new(dummy_addr))
            .map_err(|e| {
                err(
                    anyhow!(e).context(format!(
                        "failed to create session for database: {}, user: {}",
                        database, user
                    )),
                    StatusCode::UNAUTHORIZED,
                )
            })?;

        let WebhookSourceInfo {
            secret_ref,
            header_key,
            signature_expr,
        } = {
            let search_path = session.config().search_path();
            let user_name = user.as_str();
            let schema_path = SchemaPath::new(Some(schema.as_str()), &search_path, user_name);

            let reader = session.env().catalog_reader().read_guard();
            let (table, _schema) = reader
                .get_any_table_by_name(database.as_str(), schema_path, &table)
                .map_err(|e| err(e, StatusCode::NOT_FOUND))?;

            table
                .webhook_info
                .as_ref()
                .ok_or_else(|| {
                    err(
                        anyhow!("Table {:?} is not connected to wehbook source", table),
                        StatusCode::METHOD_NOT_ALLOWED,
                    )
                })?
                .clone()
        };

        let secret_string = LocalSecretManager::global()
            .fill_secret(secret_ref.unwrap())
            .map_err(|e| err(e, StatusCode::NOT_FOUND))?;

        println!("WKXLOG header_key: {:?}", header_key);

        let signature = headers
            .get(header_key)
            .ok_or_else(|| {
                err(
                    anyhow!("Signature not found in the header"),
                    StatusCode::BAD_REQUEST,
                )
            })?
            .as_bytes();
        println!("WKXLOG signature: {:?}", signature);
        println!("WKXLOG secret_string: {:?}", secret_string);

        let is_valid = verify_signature(
            secret_string.as_bytes(),
            body.as_ref(),
            signature_expr.unwrap(),
            signature,
        )
        .await?;
        println!("WKXLOG is_valid: {:?}", is_valid);

        if !is_valid {
            return Err(err(
                anyhow!("Signature verification failed"),
                StatusCode::UNAUTHORIZED,
            ));
        }

        let payload = String::from_utf8(body.to_vec()).map_err(|e| {
            err(
                anyhow!(e).context("Failed to parse body"),
                StatusCode::BAD_REQUEST,
            )
        })?;

        let insert_stmt = Statement::Insert {
            table_name: ObjectName::from(vec![table.as_str().into()]),
            columns: vec![],
            source: Box::new(Query {
                with: None,
                body: SetExpr::Values(Values(vec![vec![Expr::Value(Value::SingleQuotedString(
                    payload,
                ))]])),
                order_by: vec![],
                limit: None,
                offset: None,
                fetch: None,
            }),
            returning: vec![],
        };

        let _rsp = handle(session, insert_stmt, Arc::from(""), vec![])
            .await
            .map_err(|e| anyhow!("failed to insert: {:?}", e))?;

        Ok(())
    }
}

impl WebhookService {
    pub async fn serve(self) -> anyhow::Result<()> {
        use handlers::*;
        let srv = Arc::new(self);

        let cors_layer = CorsLayer::new()
            .allow_origin(cors::Any)
            .allow_methods(vec![Method::POST]);

        let api_router = Router::new()
            .route("/:user/:database/:schema/:table", post(handle_post_request))
            .layer(
                ServiceBuilder::new()
                    .layer(AddExtensionLayer::new(srv.clone()))
                    .into_inner(),
            )
            .layer(cors_layer);

        let app = Router::new()
            .nest("/message", api_router)
            .layer(CompressionLayer::new());

        let listener = TcpListener::bind(&srv.webhook_addr)
            .await
            .context("failed to bind dashboard address")?;
        axum::serve(listener, app)
            .await
            .context("failed to serve dashboard service")?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    #[tokio::test]
    #[ignore]
    async fn test_exchange_client() -> anyhow::Result<()> {
        let addr = SocketAddr::from(([127, 0, 0, 1], 8080));
        let service = crate::webhook::WebhookService { webhook_addr: addr };
        service.serve().await?;
        Ok(())
    }
}
