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

use std::collections::{HashMap, VecDeque};
use std::env;
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
use risingwave_sqlparser::ast::{Expr, ObjectName};
use tokio::net::TcpListener;
use tokio::sync::{Notify, RwLock, Semaphore};
use tokio::time::interval;
use tower::ServiceBuilder;
use tower_http::add_extension::AddExtensionLayer;
use tower_http::compression::CompressionLayer;
use tower_http::cors::{self, CorsLayer};

use crate::handler::handle;
use crate::session::{SessionImpl, SESSION_MANAGER};
use crate::webhook::utils::{err, Result};
mod utils;
use std::time::Duration;

// We always use the `root` user to connect to the database to allow the webhook service to access all tables.
const USER: &str = "root";
const REFRESH_INTERVAL: Duration = Duration::from_secs(5);
const DEFAULT_SESSION_POOL_SIZE: usize = 10;

pub type Service = Arc<WebhookService>;

struct SessionGuard {
    service: Service,
    database: String,
    session: Option<Arc<SessionImpl>>,
}

impl SessionGuard {
    fn new(service: Arc<WebhookService>, database: String, session: Arc<SessionImpl>) -> Self {
        Self {
            service,
            database,
            session: Some(session),
        }
    }
}

impl Drop for SessionGuard {
    fn drop(&mut self) {
        if let Some(session) = self.session.take() {
            let service = self.service.clone();
            let database = self.database.clone();

            // Release session in a separate async task to avoid blocking
            tokio::spawn(async move {
                service.release_session(database, session).await;
            });
        }
    }
}

#[derive(Clone)]
pub struct WebhookService {
    webhook_addr: SocketAddr,
    session_pools: Arc<RwLock<HashMap<String, DatabaseSessionPool>>>,
    session_refresh_notify: Arc<Notify>,
    max_pool_size: usize,
}

struct DatabaseSessionPool {
    pool: VecDeque<Arc<SessionImpl>>,
    semaphore: Arc<Semaphore>,
}

impl WebhookService {
    pub async fn serve(self) -> anyhow::Result<()> {
        let srv = Arc::new(self);
        let cors_layer = CorsLayer::new()
            .allow_origin(cors::Any)
            .allow_methods(vec![Method::POST]);

        let api_router: Router = Router::new()
            .route(
                "/:database/:schema/:table",
                post(handlers::handle_post_request),
            )
            .layer(
                ServiceBuilder::new()
                    .layer(AddExtensionLayer::new(srv.clone()))
                    .into_inner(),
            )
            .layer(cors_layer);

        let app: Router = Router::new()
            .nest("/webhook", api_router)
            .layer(CompressionLayer::new());

        let listener = TcpListener::bind(&srv.webhook_addr)
            .await
            .context("Failed to bind webhook address")?;

        #[cfg(not(madsim))]
        axum::serve(listener, app)
            .await
            .context("Failed to serve webhook service")?;

        Ok(())
    }

    pub fn new(webhook_addr: SocketAddr) -> Self {
        let max_pool_size = env::var("RW_SESSION_POOL_SIZE")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_SESSION_POOL_SIZE);

        println!("Max session pool size set to: {}", max_pool_size);

        let session_pools = Arc::new(RwLock::new(HashMap::new()));
        let session_refresh_notify = Arc::new(Notify::new());

        let service = Self {
            webhook_addr,
            session_pools: session_pools.clone(),
            session_refresh_notify: session_refresh_notify.clone(),
            max_pool_size,
        };

        tokio::spawn(service.clone().refresh_sessions());

        service
    }

    /// Acquire a session from the pool or create a new one if needed.
    /// Each database has its own semaphore to limit the total number of sessions.
    pub async fn acquire_session(&self, database: String) -> Arc<SessionImpl> {
        let semaphore = {
            let pools_guard = self.session_pools.read().await;
            pools_guard
                .get(&database)
                .map(|db_pool| db_pool.semaphore.clone())
        };

        // If no pool exists for the database, create a new one with a semaphore
        let semaphore = match semaphore {
            Some(semaphore) => semaphore,
            None => {
                let mut pools_guard = self.session_pools.write().await;
                let db_pool =
                    pools_guard
                        .entry(database.clone())
                        .or_insert_with(|| DatabaseSessionPool {
                            pool: VecDeque::new(),
                            semaphore: Arc::new(Semaphore::new(self.max_pool_size)),
                        });
                db_pool.semaphore.clone()
            }
        };

        // Wait for semaphore permission to ensure we don't exceed max session limit
        let _permit = semaphore
            .acquire()
            .await
            .expect("Failed to acquire semaphore");

        // Try to reuse an existing session from the pool
        {
            let mut pools_guard = self.session_pools.write().await;
            if let Some(db_pool) = pools_guard.get_mut(&database) {
                if let Some(session) = db_pool.pool.pop_front() {
                    return session;
                }
            }
        }

        // No available session, create a new one
        let mut pools_guard = self.session_pools.write().await;
        // let db_pool = pools_guard
        //     .entry(database.clone())
        //     .or_insert_with(|| DatabaseSessionPool {
        //         pool: VecDeque::new(),
        //         semaphore: Arc::new(Semaphore::new(self.max_pool_size)),
        //     });

        let session_mgr = SESSION_MANAGER
            .get()
            .expect("Session manager must be initialized");
        let dummy_addr = Address::Tcp(SocketAddr::new(IpAddr::V4([0, 0, 0, 0].into()), 5691));

        let new_session = session_mgr
            .connect(&database, USER, Arc::new(dummy_addr))
            .expect("Failed to create session");

        // db_pool.pool.push_back(new_session.clone());
        new_session
    }

    /// Return the session to the pool, discard if the pool is full.
    pub async fn release_session(&self, database: String, session: Arc<SessionImpl>) {
        let mut pools_guard = self.session_pools.write().await;
        if let Some(db_pool) = pools_guard.get_mut(&database) {
            if db_pool.pool.len() < self.max_pool_size {
                db_pool.pool.push_back(session);
            } else {
                println!("Session pool for {} is full, dropping session.", database);
            }

            // Release semaphore permit, indicating session is available
            db_pool.semaphore.add_permits(1);
        }
    }

    /// Periodically refresh each database's pool to ensure at least one session exists.
    pub async fn refresh_sessions(self) {
        let mut interval = interval(REFRESH_INTERVAL);

        loop {
            interval.tick().await;
            let mut pools_guard = self.session_pools.write().await;

            for (database, db_pool) in pools_guard.iter_mut() {
                if db_pool.pool.len() < 1 {
                    let session_mgr = SESSION_MANAGER
                        .get()
                        .expect("Session manager not initialized");
                    let dummy_addr =
                        Address::Tcp(SocketAddr::new(IpAddr::V4([0, 0, 0, 0].into()), 5691));

                    if let Ok(session) = session_mgr.connect(database, USER, Arc::new(dummy_addr)) {
                        db_pool.pool.push_back(session);
                        println!("Refreshed session for database: {}", database);
                    }
                }
            }
            self.session_refresh_notify.notify_waiters();
        }
    }
}

pub(super) mod handlers {
    use risingwave_pb::catalog::WebhookSourceInfo;
    use risingwave_sqlparser::ast::{Query, SetExpr, Statement, Value, Values};
    use utils::{header_map_to_json, verify_signature};

    use super::*;
    use crate::catalog::root_catalog::SchemaPath;

    pub async fn handle_post_request(
        Extension(service): Extension<Service>,
        headers: HeaderMap,
        Path((database, schema, table)): Path<(String, String, String)>,
        body: Bytes,
    ) -> Result<()> {
        let session = service.acquire_session(database.clone()).await;

        // Wrap session in a guard to ensure automatic release
        let _session_guard = SessionGuard::new(service.clone(), database.clone(), session.clone());

        let WebhookSourceInfo {
            secret_ref: _secret_ref,
            signature_expr,
        } = {
            let search_path = session.config().search_path();
            let schema_path = SchemaPath::new(Some(schema.as_str()), &search_path, USER);

            let reader = session.env().catalog_reader().read_guard();
            let (table_catalog, _schema) = reader
                .get_any_table_by_name(database.as_str(), schema_path, &table)
                .map_err(|e| err(e, StatusCode::NOT_FOUND))?;

            table_catalog
                .webhook_info
                .as_ref()
                .ok_or_else(|| {
                    err(
                        anyhow!("Table `{}` is not with webhook source", table),
                        StatusCode::FORBIDDEN,
                    )
                })?
                .clone()
        };

        // let secret_string = LocalSecretManager::global()
        //     .fill_secret(secret_ref.unwrap())
        //     .map_err(|e| err(e, StatusCode::NOT_FOUND))?;
        let secret_string = String::from("TEST_WEBHOOK");

        // Once limitation here is that the key is no longer case-insensitive, users must user the lowercase key when defining the webhook source table.
        let headers_jsonb = header_map_to_json(&headers);

        let is_valid = verify_signature(
            headers_jsonb,
            secret_string.as_str(),
            body.as_ref(),
            signature_expr.unwrap(),
        )
        .await?;

        if !is_valid {
            return Err(err(
                anyhow!("Signature verification failed"),
                StatusCode::UNAUTHORIZED,
            ));
        }

        let payload = String::from_utf8(body.to_vec()).map_err(|e| {
            err(
                anyhow!(e).context("Failed to parse body"),
                StatusCode::UNPROCESSABLE_ENTITY,
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

        let _rsp = handle(session.clone(), insert_stmt, Arc::from(""), vec![])
            .await
            .map_err(|e| {
                err(
                    anyhow!(e).context("Failed to insert into target table"),
                    StatusCode::INTERNAL_SERVER_ERROR,
                )
            })?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    #[tokio::test]
    #[ignore]
    async fn test_webhook_server() -> anyhow::Result<()> {
        let addr = SocketAddr::from(([127, 0, 0, 1], 4560));
        let service = crate::webhook::WebhookService::new(addr);
        service.serve().await?;
        Ok(())
    }
}
