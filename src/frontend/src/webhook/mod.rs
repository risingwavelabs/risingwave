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

use std::net::SocketAddr;
use std::ops::Index;
use std::sync::Arc;
use std::sync::atomic::AtomicU32;

use anyhow::{Context, anyhow};
use axum::body::Bytes;
use axum::extract::{Extension, Path, Query};
use axum::http::{HeaderMap, Method, StatusCode};
use axum::routing::{get, post};
use axum::{Json, Router};
use futures::StreamExt;
use pgwire::pg_server::SessionManager;
use pgwire::types::Row;
use risingwave_common::array::{Array, ArrayBuilder, DataChunk};
use risingwave_common::catalog::DEFAULT_DATABASE_NAME;
use risingwave_common::secret::LocalSecretManager;
use risingwave_common::types::{DataType, JsonbVal, Scalar};
use risingwave_pb::catalog::WebhookSourceInfo;
use risingwave_pb::task_service::{FastInsertRequest, FastInsertResponse};
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tokio::sync::{Mutex, OnceCell};
use tokio::time::{Duration, sleep};
use tower::ServiceBuilder;
use tower_http::add_extension::AddExtensionLayer;
use tower_http::compression::CompressionLayer;
use tower_http::cors::{self, CorsLayer};

use crate::webhook::utils::{Result, err};
mod utils;
use risingwave_rpc_client::ComputeClient;
use scopeguard::guard;

use crate::handler::RwPgResponse;
use crate::records_demo::{DEMO_SCHEMA_NAME, DEMO_TABLE_NAME};
use crate::session::{SESSION_MANAGER, SessionImpl};

pub type Service = Arc<WebhookService>;

// We always use the `root` user to connect to the database to allow the webhook service to access all tables.
const USER: &str = "root";

#[derive(Clone)]
pub struct FastInsertContext {
    pub webhook_source_info: WebhookSourceInfo,
    pub fast_insert_request: FastInsertRequest,
    pub compute_client: ComputeClient,
}

pub struct WebhookService {
    webhook_addr: SocketAddr,
    counter: AtomicU32,
    demo_bootstrap: OnceCell<()>,
    demo_append_lock: Mutex<()>,
}

pub(super) mod handlers {
    use jsonbb::Value;
    use risingwave_common::array::JsonbArrayBuilder;
    use risingwave_common::session_config::SearchPath;
    use risingwave_pb::catalog::WebhookSourceInfo;
    use risingwave_pb::task_service::fast_insert_response;
    use utils::{header_map_to_json, verify_signature};

    use super::*;
    use crate::catalog::root_catalog::SchemaPath;
    use crate::scheduler::choose_fast_insert_client;

    pub async fn handle_post_request(
        Extension(srv): Extension<Service>,
        headers: HeaderMap,
        Path((database, schema, table)): Path<(String, String, String)>,
        body: Bytes,
    ) -> Result<()> {
        let request_id = srv
            .counter
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let FastInsertContext {
            webhook_source_info,
            mut fast_insert_request,
            compute_client,
        } = acquire_table_info(request_id, &database, &schema, &table).await?;

        let WebhookSourceInfo {
            signature_expr,
            secret_ref,
            wait_for_persistence: _,
            is_batched,
        } = webhook_source_info;

        let is_valid = if let Some(signature_expr) = signature_expr {
            let secret_string = if let Some(secret_ref) = secret_ref {
                LocalSecretManager::global()
                    .fill_secret(secret_ref)
                    .map_err(|e| err(e, StatusCode::NOT_FOUND))?
            } else {
                String::new()
            };

            // Once limitation here is that the key is no longer case-insensitive, users must user the lowercase key when defining the webhook source table.
            let headers_jsonb = header_map_to_json(&headers);

            // verify the signature
            verify_signature(
                headers_jsonb,
                secret_string.as_str(),
                body.as_ref(),
                signature_expr,
            )
            .await?
        } else {
            true
        };

        if !is_valid {
            return Err(err(
                anyhow!("Signature verification failed"),
                StatusCode::UNAUTHORIZED,
            ));
        }

        let data_chunk = generate_data_chunk(is_batched, &body)?;

        // fill the data_chunk
        fast_insert_request.data_chunk = Some(data_chunk.to_protobuf());
        // execute on the compute node
        let res = execute(fast_insert_request, compute_client).await?;

        if res.status == fast_insert_response::Status::Succeeded as i32 {
            Ok(())
        } else {
            Err(err(
                anyhow!("Failed to fast insert: {}", res.error_message),
                StatusCode::INTERNAL_SERVER_ERROR,
            ))
        }
    }

    #[derive(Deserialize)]
    pub struct DemoAppendRequest {
        body: String,
    }

    #[derive(Deserialize)]
    pub struct DemoReadQuery {
        seq_num: Option<String>,
        limit: Option<u32>,
    }

    #[derive(Serialize)]
    pub struct DemoAppendResponse {
        seq_num: String,
        ts_ms: i64,
        body: String,
    }

    #[derive(Serialize)]
    pub struct DemoRecord {
        seq_num: String,
        ts_ms: i64,
        body: String,
    }

    #[derive(Serialize)]
    pub struct DemoReadResponse {
        records: Vec<DemoRecord>,
    }

    #[derive(Serialize)]
    pub struct DemoTailResponse {
        seq_num: String,
        ts_ms: i64,
    }

    pub async fn handle_demo_append(
        Extension(srv): Extension<Service>,
        Json(req): Json<DemoAppendRequest>,
    ) -> Result<Json<DemoAppendResponse>> {
        srv.ensure_demo_table().await?;
        let _guard = srv.demo_append_lock.lock().await;
        let session = create_demo_session()?;

        let session_mgr = SESSION_MANAGER
            .get()
            .expect("session manager has been initialized");
        let _session_guard = guard(session.clone(), |session| {
            session_mgr.end_session(&session);
        });

        let prev_seq_num = load_demo_tail_row(session.clone())
            .await?
            .map(|row| row_i64(&row, 0))
            .transpose()?
            .unwrap_or(0);

        let insert_sql = format!(
            "INSERT INTO {DEMO_SCHEMA_NAME}.{DEMO_TABLE_NAME} (body, ts_ms) VALUES ({body}, (extract(epoch from now()) * 1000)::bigint)",
            body = quote_sql_literal(&req.body),
        );
        run_sql(session.clone(), &insert_sql).await?;
        run_sql(session.clone(), "FLUSH").await?;

        let row = wait_for_demo_row(session, prev_seq_num).await?;

        Ok(Json(DemoAppendResponse {
            seq_num: row_text(&row, 0)?,
            ts_ms: row_i64(&row, 1)?,
            body: row_text(&row, 2)?,
        }))
    }

    pub async fn handle_demo_read(
        Extension(srv): Extension<Service>,
        Query(query): Query<DemoReadQuery>,
    ) -> Result<Json<DemoReadResponse>> {
        srv.ensure_demo_table().await?;
        let session = create_demo_session()?;

        let session_mgr = SESSION_MANAGER
            .get()
            .expect("session manager has been initialized");
        let _session_guard = guard(session.clone(), |session| {
            session_mgr.end_session(&session);
        });

        let seq_num = query
            .seq_num
            .as_deref()
            .unwrap_or("0")
            .parse::<i64>()
            .map_err(|e| {
                err(
                    anyhow!(e).context("invalid seq_num"),
                    StatusCode::BAD_REQUEST,
                )
            })?;
        let limit = query.limit.unwrap_or(100).clamp(1, 1000);
        let rows = run_sql_rows(
            session,
            &format!(
                "SELECT CAST(_row_id AS bigint), ts_ms, body FROM {DEMO_SCHEMA_NAME}.{DEMO_TABLE_NAME} \
                 WHERE CAST(_row_id AS bigint) >= {seq_num} ORDER BY _row_id LIMIT {limit}"
            ),
        )
        .await?;

        let records = rows
            .into_iter()
            .map(|row| {
                Ok(DemoRecord {
                    seq_num: row_text(&row, 0)?,
                    ts_ms: row_i64(&row, 1)?,
                    body: row_text(&row, 2)?,
                })
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(Json(DemoReadResponse { records }))
    }

    pub async fn handle_demo_tail(
        Extension(srv): Extension<Service>,
    ) -> Result<Json<DemoTailResponse>> {
        srv.ensure_demo_table().await?;
        let session = create_demo_session()?;

        let session_mgr = SESSION_MANAGER
            .get()
            .expect("session manager has been initialized");
        let _session_guard = guard(session.clone(), |session| {
            session_mgr.end_session(&session);
        });

        let rows = run_sql_rows(
            session,
            &format!(
                "SELECT CAST(_row_id AS bigint), ts_ms FROM {DEMO_SCHEMA_NAME}.{DEMO_TABLE_NAME} ORDER BY _row_id DESC LIMIT 1"
            ),
        )
        .await?;

        let response = if let Some(row) = rows.into_iter().next() {
            DemoTailResponse {
                seq_num: row_text(&row, 0)?,
                ts_ms: row_i64(&row, 1)?,
            }
        } else {
            DemoTailResponse {
                seq_num: "0".to_owned(),
                ts_ms: 0,
            }
        };

        Ok(Json(response))
    }

    fn generate_data_chunk(is_batched: bool, body: &Bytes) -> Result<DataChunk> {
        let mut builder = JsonbArrayBuilder::with_type(1, DataType::Jsonb);

        if !is_batched {
            // Use builder to obtain a single column & single row DataChunk
            let json_value = Value::from_text(body).map_err(|e| {
                err(
                    anyhow!(e).context("Failed to parse body"),
                    StatusCode::UNPROCESSABLE_ENTITY,
                )
            })?;

            let jsonb_val = JsonbVal::from(json_value);
            builder.append(Some(jsonb_val.as_scalar_ref()));

            Ok(DataChunk::new(vec![builder.finish().into_ref()], 1))
        } else {
            let rows: Vec<_> = body.split(|&b| b == b'\n').collect();

            for row in &rows {
                let json_value = Value::from_text(row).map_err(|e| {
                    err(
                        anyhow!(e).context("Failed to parse body"),
                        StatusCode::UNPROCESSABLE_ENTITY,
                    )
                })?;
                let jsonb_val = JsonbVal::from(json_value);

                builder.append(Some(jsonb_val.as_scalar_ref()));
            }

            Ok(DataChunk::new(
                vec![builder.finish().into_ref()],
                rows.len(),
            ))
        }
    }

    async fn acquire_table_info(
        request_id: u32,
        database: &String,
        schema: &String,
        table: &String,
    ) -> Result<FastInsertContext> {
        let session_mgr = SESSION_MANAGER
            .get()
            .expect("session manager has been initialized");

        let frontend_env = session_mgr.env();

        let search_path = SearchPath::default();
        let schema_path = SchemaPath::new(Some(schema.as_str()), &search_path, USER);

        let (webhook_source_info, table_id, version_id, row_id_index) = {
            let reader = frontend_env.catalog_reader().read_guard();
            let (table_catalog, _schema) = reader
                .get_any_table_by_name(database.as_str(), schema_path, table)
                .map_err(|e| err(e, StatusCode::NOT_FOUND))?;

            let webhook_source_info = table_catalog
                .webhook_info
                .as_ref()
                .ok_or_else(|| {
                    err(
                        anyhow!("Table `{}` is not with webhook source", table),
                        StatusCode::FORBIDDEN,
                    )
                })?
                .clone();
            (
                webhook_source_info,
                table_catalog.id(),
                table_catalog.version_id().expect("table must be versioned"),
                table_catalog.row_id_index.map(|idx| idx as u32),
            )
        };

        let fast_insert_request = FastInsertRequest {
            table_id,
            table_version_id: version_id,
            column_indices: vec![0],
            // leave the data_chunk empty for now
            data_chunk: None,
            row_id_index,
            request_id,
            wait_for_persistence: webhook_source_info.wait_for_persistence,
        };

        let compute_client = choose_fast_insert_client(table_id, frontend_env, request_id)
            .await
            .unwrap();

        Ok(FastInsertContext {
            webhook_source_info,
            fast_insert_request,
            compute_client,
        })
    }

    async fn execute(
        request: FastInsertRequest,
        client: ComputeClient,
    ) -> Result<FastInsertResponse> {
        let response = client.fast_insert(request).await.map_err(|e| {
            err(
                anyhow!(e).context("Failed to execute on compute node"),
                StatusCode::INTERNAL_SERVER_ERROR,
            )
        })?;
        Ok(response)
    }

    pub(super) fn create_demo_session() -> Result<Arc<SessionImpl>> {
        let session_mgr = SESSION_MANAGER
            .get()
            .expect("session manager has been initialized");
        let database_id = {
            let reader = session_mgr.env().catalog_reader().read_guard();
            reader
                .get_database_by_name(DEFAULT_DATABASE_NAME)
                .map_err(|e| err(e, StatusCode::INTERNAL_SERVER_ERROR))?
                .id()
        };
        session_mgr
            .create_dummy_session(database_id)
            .map_err(|e| err(e, StatusCode::INTERNAL_SERVER_ERROR))
    }

    pub(super) async fn run_sql(session: Arc<SessionImpl>, sql: &str) -> Result<RwPgResponse> {
        session
            .run_statement(sql.into(), vec![])
            .await
            .map_err(|e| err(anyhow!(e), StatusCode::INTERNAL_SERVER_ERROR))
    }

    async fn run_sql_rows(session: Arc<SessionImpl>, sql: &str) -> Result<Vec<Row>> {
        let mut rsp = run_sql(session, sql).await?;
        collect_rows(&mut rsp).await
    }

    async fn load_demo_tail_row(session: Arc<SessionImpl>) -> Result<Option<Row>> {
        let rows = run_sql_rows(
            session,
            &format!(
                "SELECT CAST(_row_id AS bigint), ts_ms, body FROM {DEMO_SCHEMA_NAME}.{DEMO_TABLE_NAME} \
                 ORDER BY _row_id DESC LIMIT 1"
            ),
        )
        .await?;
        Ok(rows.into_iter().next())
    }

    async fn wait_for_demo_row(session: Arc<SessionImpl>, prev_seq_num: i64) -> Result<Row> {
        for _ in 0..50 {
            let rows = run_sql_rows(
                session.clone(),
                &format!(
                    "SELECT CAST(_row_id AS bigint), ts_ms, body FROM {DEMO_SCHEMA_NAME}.{DEMO_TABLE_NAME} \
                     WHERE CAST(_row_id AS bigint) > {prev_seq_num} ORDER BY _row_id DESC LIMIT 1"
                ),
            )
            .await?;
            if let Some(row) = rows.into_iter().next() {
                return Ok(row);
            }
            sleep(Duration::from_millis(20)).await;
        }

        Err(err(
            anyhow!("demo append did not produce a visible row"),
            StatusCode::INTERNAL_SERVER_ERROR,
        ))
    }

    async fn collect_rows(rsp: &mut RwPgResponse) -> Result<Vec<Row>> {
        let mut rows = Vec::new();
        while let Some(row_set) = rsp.values_stream().next().await {
            rows.extend(row_set.map_err(|e| err(anyhow!(e), StatusCode::INTERNAL_SERVER_ERROR))?);
        }
        Ok(rows)
    }

    fn row_text(row: &Row, idx: usize) -> Result<String> {
        let bytes = row.index(idx).as_ref().ok_or_else(|| {
            err(
                anyhow!("missing column {idx} in demo SQL response"),
                StatusCode::INTERNAL_SERVER_ERROR,
            )
        })?;
        std::str::from_utf8(bytes)
            .map(str::to_owned)
            .map_err(|e| err(anyhow!(e), StatusCode::INTERNAL_SERVER_ERROR))
    }

    fn row_i64(row: &Row, idx: usize) -> Result<i64> {
        row_text(row, idx)?
            .parse::<i64>()
            .map_err(|e| err(anyhow!(e), StatusCode::INTERNAL_SERVER_ERROR))
    }

    pub(super) fn quote_sql_literal(value: &str) -> String {
        format!("'{}'", value.replace('\'', "''"))
    }
}

impl WebhookService {
    pub fn new(webhook_addr: SocketAddr) -> Self {
        Self {
            webhook_addr,
            counter: AtomicU32::new(0),
            demo_bootstrap: OnceCell::const_new(),
            demo_append_lock: Mutex::new(()),
        }
    }

    async fn ensure_demo_table(&self) -> Result<()> {
        self.demo_bootstrap
            .get_or_try_init(|| async {
                let session = handlers::create_demo_session()?;
                let session_mgr = SESSION_MANAGER
                    .get()
                    .expect("session manager has been initialized");
                let _session_guard = guard(session.clone(), |session| {
                    session_mgr.end_session(&session);
                });

                handlers::run_sql(
                    session.clone(),
                    &format!("CREATE SCHEMA IF NOT EXISTS {DEMO_SCHEMA_NAME}"),
                )
                .await?;
                handlers::run_sql(
                    session,
                    &format!(
                        "CREATE TABLE IF NOT EXISTS {DEMO_SCHEMA_NAME}.{DEMO_TABLE_NAME} (body varchar, ts_ms bigint) APPEND ONLY"
                    ),
                )
                .await?;
                Ok::<(), utils::WebhookError>(())
            })
            .await?;
        Ok(())
    }

    pub async fn serve(self) -> anyhow::Result<()> {
        use handlers::*;
        let srv = Arc::new(self);

        let webhook_cors_layer = CorsLayer::new()
            .allow_origin(cors::Any)
            .allow_methods(vec![Method::POST]);
        let demo_cors_layer = CorsLayer::new()
            .allow_origin(cors::Any)
            .allow_methods(vec![Method::GET, Method::POST]);

        let webhook_router: Router = Router::new()
            .route("/:database/:schema/:table", post(handle_post_request))
            .layer(
                ServiceBuilder::new()
                    .layer(AddExtensionLayer::new(srv.clone()))
                    .into_inner(),
            )
            .layer(webhook_cors_layer);

        let demo_router: Router = Router::new()
            .route("/records", post(handle_demo_append).get(handle_demo_read))
            .route("/records/tail", get(handle_demo_tail))
            .layer(
                ServiceBuilder::new()
                    .layer(AddExtensionLayer::new(srv.clone()))
                    .into_inner(),
            )
            .layer(demo_cors_layer);

        let app: Router = Router::new()
            .nest("/webhook", webhook_router)
            .nest("/demo", demo_router)
            .layer(CompressionLayer::new());

        let listener = TcpListener::bind(&srv.webhook_addr)
            .await
            .context("Failed to bind dashboard address")?;

        #[cfg(not(madsim))]
        axum::serve(listener, app)
            .await
            .context("Failed to serve dashboard service")?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use risingwave_common::catalog::DEFAULT_DATABASE_NAME;
    use risingwave_common::hash::VnodeCountCompat;

    use super::handlers::quote_sql_literal;
    use crate::test_utils::LocalFrontend;

    #[test]
    fn test_quote_sql_literal() {
        assert_eq!(quote_sql_literal("demo'value"), "'demo''value'");
    }

    #[tokio::test]
    async fn test_demo_table_uses_singleton_distribution() {
        let frontend = LocalFrontend::new(Default::default()).await;
        frontend
            .run_sql("CREATE SCHEMA rw_records_demo")
            .await
            .unwrap();
        frontend
            .run_sql(
                "CREATE TABLE rw_records_demo.records (body varchar, ts_ms bigint) APPEND ONLY",
            )
            .await
            .unwrap();

        let session = frontend.session_ref();
        let catalog_reader = session.env().catalog_reader().read_guard();
        let schema_path = crate::catalog::root_catalog::SchemaPath::Name("rw_records_demo");
        let (table, _) = catalog_reader
            .get_created_table_by_name(DEFAULT_DATABASE_NAME, schema_path, "records")
            .unwrap();

        assert!(table.distribution_key().is_empty());
        assert_eq!(table.to_prost().vnode_count(), 1);
    }

    #[tokio::test]
    #[ignore]
    async fn test_webhook_server() -> anyhow::Result<()> {
        let addr = SocketAddr::from(([127, 0, 0, 1], 4560));
        let service = crate::webhook::WebhookService::new(addr);
        service.serve().await?;
        Ok(())
    }
}
