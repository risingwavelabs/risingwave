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

use anyhow::{anyhow, Context as _, Result};
use axum::body::Bytes;
use axum::extract::{Extension, Path};
use axum::http::{HeaderMap, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use pgwire::net::{Address, AddressRef};
use pgwire::pg_server::SessionManager;
use risingwave_sqlparser::ast::{Expr, ObjectName};
use serde::{Deserialize, Serialize, Serializer};
use tokio::net::TcpListener;
use tower::ServiceBuilder;
use tower_http::add_extension::AddExtensionLayer;
use tower_http::compression::CompressionLayer;
use tower_http::cors::{self, CorsLayer};

use crate::handler::{handle, query};

#[derive(Clone)]
pub struct WebhookService {
    pub webhook_addr: SocketAddr,
    // pub prometheus_selector: String,
    // pub metadata_manager: MetadataManager,
    // pub compute_clients: ComputeClientPool,
}

pub type Service = Arc<WebhookService>;

#[derive(Debug, Serialize, Deserialize)]
pub struct Payload {
    value: String,
}

pub(super) mod handlers {

    use std::net::Ipv4Addr;

    use axum::Json;
    use risingwave_common::bail;
    use risingwave_sqlparser::ast::{Query, SetExpr, Statement, Value, Values};
    use serde_json::json;
    use thiserror_ext::AsReport;

    use super::*;
    use crate::handler::HandlerArgs;
    use crate::session::SESSION_MANAGER;

    pub struct WebhookError(anyhow::Error);
    pub type Result<T> = std::result::Result<T, WebhookError>;

    pub fn err(err: impl Into<anyhow::Error>) -> WebhookError {
        WebhookError(err.into())
    }

    impl From<anyhow::Error> for WebhookError {
        fn from(value: anyhow::Error) -> Self {
            WebhookError(value)
        }
    }

    impl IntoResponse for WebhookError {
        fn into_response(self) -> axum::response::Response {
            let mut resp = Json(json!({
                "error": self.0.to_report_string(),
            }))
            .into_response();
            *resp.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
            resp
        }
    }

    pub async fn handle_empty_request(
        Extension(srv): Extension<Service>,
        headers: HeaderMap,
        body: String,
        // Json(payload): Json<String>,
    ) -> Result<()> {
        println!(
            "WKXLOG handle_empty_request receive headers: {:?}, body: {:?}",
            headers, body
        );
        handle_post_request(
            Extension(srv),
            Path((String::from("dev"), String::from("github"))),
            Json(Payload { value: body }),
        )
        .await
    }

    pub async fn handle_post_request(
        Extension(srv): Extension<Service>,
        Path((database, table)): Path<(String, String)>,
        Json(payload): Json<Payload>,
    ) -> Result<()> {
        println!(
            "WKXLOG receive something: {:?}, database: {}, table: {}",
            payload, database, table
        );
        let session_mgr = SESSION_MANAGER
            .get()
            .expect("session manager has been initialized");

        let dummy_addr = Address::Tcp(SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)),
            5691, // port of meta
        ));

        let owner = "root";
        // get a session object for the corresponding user and database

        let session = session_mgr
            .connect(database.as_str(), owner, Arc::new(dummy_addr))
            .map_err(|e| {
                anyhow!(
                    "failed to create dummy session for database: {}, owner: {}, error: {}",
                    database,
                    owner,
                    e
                )
            })?;

        // let new_columns = table_change.columns.into_iter().map(|c| c.into()).collect();
        // let table_name = ObjectName::from(vec![table_name.as_str().into()]);
        // let (new_table_definition, original_catalog) =
        //     get_new_table_definition_for_cdc_table(&session, table_name.clone(), new_columns)
        //         .await?;
        // let (_, table, graph, col_index_mapping, job_type) = get_replace_table_plan(
        //     &session,
        //     table_name,
        //     new_table_definition,
        //     &original_catalog,
        //     None,
        // )
        // .await?;

        let insert_stmt = Statement::Insert {
            table_name: ObjectName::from(vec![table.as_str().into()]),
            columns: vec![],
            source: Box::new(Query {
                with: None,
                body: SetExpr::Values(Values(vec![vec![Expr::Value(Value::SingleQuotedString(
                    payload.value.clone(),
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

        if payload.value.is_empty() {
            bail!(
                "payload: {:?} not found in {:?}.{:?}",
                payload,
                database,
                table
            )
        } else {
            println!(
                "WKXLOG Received payload: {:?} in {:?}.{:?}",
                payload.value, database, table
            );
        }

        Ok(())
    }
}

impl WebhookService {
    pub async fn serve(self) -> Result<()> {
        use handlers::*;
        let srv = Arc::new(self);

        // tracing_subscriber::registry()
        //     .with(tracing_subscriber::fmt::layer())
        //     .init();

        let cors_layer = CorsLayer::new()
            .allow_origin(cors::Any)
            .allow_methods(vec![Method::POST]);

        let api_router = Router::new()
            .route("/", post(handle_empty_request))
            .route("/:database/:table", post(handle_post_request))
            .layer(
                ServiceBuilder::new()
                    .layer(AddExtensionLayer::new(srv.clone()))
                    .into_inner(),
            )
            .layer(cors_layer);

        // let dashboard_router = risingwave_meta_dashboard::router();

        let app = Router::new()
            // .fallback_service(dashboard_router)
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

    use serde_json::Value;

    #[tokio::test]
    async fn test_exchange_client() -> anyhow::Result<()> {
        let addr = SocketAddr::from(([127, 0, 0, 1], 8080));
        println!("WKXLOG Starting webhook service at: {:?}", addr);
        let service = crate::webhook::WebhookService { webhook_addr: addr };
        service.serve().await?;
        // let _task = tokio::spawn(service.serve());
        // let res = task.await.unwrap();
        // res.unwrap();
        Ok(())
    }
}
