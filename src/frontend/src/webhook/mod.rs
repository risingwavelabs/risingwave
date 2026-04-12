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
use std::sync::Arc;
use std::sync::atomic::AtomicU32;

use anyhow::{Context, anyhow};
use axum::Router;
use axum::body::Bytes;
use axum::extract::{Extension, Path};
use axum::http::{HeaderMap, Method, StatusCode};
use axum::routing::post;
use risingwave_common::array::{Array, ArrayBuilder, ArrayRef, DataChunk};
use risingwave_common::catalog::TableId;
use risingwave_common::secret::LocalSecretManager;
use risingwave_common::types::{DataType, JsonbVal, Scalar};
use risingwave_pb::catalog::WebhookSourceInfo;
use risingwave_pb::task_service::{FastInsertRequest, FastInsertResponse};
use tokio::net::TcpListener;
use tower::ServiceBuilder;
use tower_http::add_extension::AddExtensionLayer;
use tower_http::compression::CompressionLayer;
use tower_http::cors::{self, CorsLayer};

use crate::webhook::utils::{Result, err};
mod utils;
use risingwave_rpc_client::ComputeClient;

pub type Service = Arc<WebhookService>;

// We always use the `root` user to connect to the database to allow the webhook service to access all tables.
const USER: &str = "root";

/// Metadata for an INCLUDE header column, used to extract header values at request time.
///
/// At request time, the header value is looked up via `HeaderMap::get`, which is
/// **case-insensitive** per HTTP semantics. The column is always produced as VARCHAR;
/// if the header is absent from the request, the column value will be NULL.
#[derive(Clone)]
pub struct HeaderColumnInfo {
    /// The HTTP header name to look up (from `AdditionalColumnHeader.inner_field`).
    pub header_name: String,
}

/// Resolved table metadata needed to fast-insert a webhook request.
#[derive(Clone)]
pub struct WebhookTableMeta {
    pub webhook_source_info: WebhookSourceInfo,
    pub table_id: TableId,
    pub version_id: u64,
    pub row_id_index: Option<u32>,
    /// Maps each DataChunk column position to its table column index.
    /// Layout: `[body_col, header_col_0, header_col_1, ...]`.
    pub column_indices: Vec<u32>,
    /// Ordered list of INCLUDE header columns; positions align with
    /// `column_indices[1..]`.
    pub header_columns: Vec<HeaderColumnInfo>,
}

#[derive(Clone)]
pub struct FastInsertContext {
    pub table_meta: WebhookTableMeta,
    pub fast_insert_request: FastInsertRequest,
    pub compute_client: ComputeClient,
}

pub struct WebhookService {
    webhook_addr: SocketAddr,
    counter: AtomicU32,
}

pub(super) mod handlers {
    use jsonbb::Value;
    use risingwave_common::array::{JsonbArrayBuilder, Utf8ArrayBuilder};
    use risingwave_common::session_config::SearchPath;
    use risingwave_pb::catalog::WebhookSourceInfo;
    use risingwave_pb::plan_common::additional_column::ColumnType as AdditionalColumnType;
    use risingwave_pb::task_service::fast_insert_response;
    use utils::{header_map_to_json, verify_signature};

    use super::*;
    use crate::catalog::root_catalog::SchemaPath;
    use crate::scheduler::choose_fast_insert_client;
    use crate::session::SESSION_MANAGER;

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
            table_meta,
            mut fast_insert_request,
            compute_client,
        } = acquire_table_info(request_id, &database, &schema, &table).await?;

        let WebhookSourceInfo {
            signature_expr,
            secret_ref,
            wait_for_persistence: _,
            is_batched,
        } = table_meta.webhook_source_info;

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

        let data_chunk = generate_data_chunk(is_batched, &body, &headers, &table_meta.header_columns)?;

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

    /// Build a `DataChunk` from the webhook request body and headers.
    ///
    /// Columns are ordered as: `[body_jsonb, header_col_0, header_col_1, ...]`.
    /// Header columns are always VARCHAR; if the header is absent the value is NULL.
    fn generate_data_chunk(
        is_batched: bool,
        body: &Bytes,
        headers: &HeaderMap,
        header_columns: &[HeaderColumnInfo],
    ) -> Result<DataChunk> {
        let mut columns: Vec<ArrayRef> = Vec::new();

        if !is_batched {
            // Build JSONB body column (single row)
            let mut jsonb_builder = JsonbArrayBuilder::with_type(1, DataType::Jsonb);
            let json_value = Value::from_text(body).map_err(|e| {
                err(
                    anyhow!(e).context("Failed to parse body"),
                    StatusCode::UNPROCESSABLE_ENTITY,
                )
            })?;
            let jsonb_val = JsonbVal::from(json_value);
            jsonb_builder.append(Some(jsonb_val.as_scalar_ref()));
            columns.push(jsonb_builder.finish().into_ref());

            // Build VARCHAR columns for each INCLUDE header
            for hdr in header_columns {
                let mut varchar_builder = Utf8ArrayBuilder::new(1);
                let value = headers.get(&hdr.header_name).and_then(|v| v.to_str().ok());
                varchar_builder.append(value);
                columns.push(varchar_builder.finish().into_ref());
            }

            Ok(DataChunk::new(columns, 1))
        } else {
            let rows: Vec<_> = body.split(|&b| b == b'\n').collect();
            let num_rows = rows.len();

            // Build JSONB body column (multiple rows)
            let mut jsonb_builder = JsonbArrayBuilder::with_type(num_rows, DataType::Jsonb);
            for row in &rows {
                let json_value = Value::from_text(row).map_err(|e| {
                    err(
                        anyhow!(e).context("Failed to parse body"),
                        StatusCode::UNPROCESSABLE_ENTITY,
                    )
                })?;
                let jsonb_val = JsonbVal::from(json_value);
                jsonb_builder.append(Some(jsonb_val.as_scalar_ref()));
            }
            columns.push(jsonb_builder.finish().into_ref());

            // Build VARCHAR columns for each INCLUDE header
            // (same value replicated across all rows — headers are per-request)
            for hdr in header_columns {
                let mut varchar_builder = Utf8ArrayBuilder::new(num_rows);
                let value = headers.get(&hdr.header_name).and_then(|v| v.to_str().ok());
                for _ in 0..num_rows {
                    varchar_builder.append(value);
                }
                columns.push(varchar_builder.finish().into_ref());
            }

            Ok(DataChunk::new(columns, num_rows))
        }
    }

    async fn acquire_table_info(
        request_id: u32,
        database: &str,
        schema: &str,
        table: &str,
    ) -> Result<FastInsertContext> {
        let session_mgr = SESSION_MANAGER
            .get()
            .expect("session manager has been initialized");

        let frontend_env = session_mgr.env();

        let search_path = SearchPath::default();
        let schema_path = SchemaPath::new(Some(schema), &search_path, USER);

        let table_meta = {
            let reader = frontend_env.catalog_reader().read_guard();
            let (table_catalog, _schema) = reader
                .get_any_table_by_name(database, schema_path, table)
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

            // Build column_indices and header_columns from the catalog.
            // The DataChunk will have: [body_jsonb, header_col_0, header_col_1, ...]
            // and column_indices maps each chunk position to its table column index.
            let mut body_col_idx: Option<u32> = None;
            let mut hdr_col_indices = Vec::new();
            let mut hdr_cols = Vec::new();

            for (table_col_idx, col) in table_catalog.columns().iter().enumerate() {
                if col.is_hidden() {
                    continue;
                }
                match &col.column_desc.additional_column.column_type {
                    Some(AdditionalColumnType::HeaderInner(header_inner)) => {
                        hdr_col_indices.push(table_col_idx as u32);
                        hdr_cols.push(HeaderColumnInfo {
                            header_name: header_inner.inner_field.clone(),
                        });
                    }
                    None => {
                        // User-defined column (the body JSONB column).
                        // Webhook tables must have exactly one user-defined body column.
                        if body_col_idx.is_some() {
                            return Err(err(
                                anyhow!(
                                    "Webhook table `{}` has multiple user-defined columns; expected exactly one body column",
                                    table
                                ),
                                StatusCode::INTERNAL_SERVER_ERROR,
                            ));
                        }
                        body_col_idx = Some(table_col_idx as u32);
                    }
                    _ => {
                        // Other additional column types — skip for now.
                    }
                }
            }

            let body_col_idx = body_col_idx.ok_or_else(|| {
                err(
                    anyhow!(
                        "Webhook table `{}` has no visible body column",
                        table
                    ),
                    StatusCode::INTERNAL_SERVER_ERROR,
                )
            })?;

            // column_indices layout: [body, header_0, header_1, ...]
            let mut col_indices = Vec::with_capacity(1 + hdr_col_indices.len());
            col_indices.push(body_col_idx);
            col_indices.extend(hdr_col_indices);

            WebhookTableMeta {
                webhook_source_info,
                table_id: table_catalog.id(),
                version_id: table_catalog.version_id().expect("table must be versioned"),
                row_id_index: table_catalog.row_id_index.map(|idx| idx as u32),
                column_indices: col_indices,
                header_columns: hdr_cols,
            }
        };

        let fast_insert_request = FastInsertRequest {
            table_id: table_meta.table_id,
            table_version_id: table_meta.version_id,
            column_indices: table_meta.column_indices.clone(),
            // leave the data_chunk empty for now
            data_chunk: None,
            row_id_index: table_meta.row_id_index,
            request_id,
            wait_for_persistence: table_meta.webhook_source_info.wait_for_persistence,
        };

        let compute_client = choose_fast_insert_client(table_meta.table_id, frontend_env, request_id)
            .await
            .unwrap();

        Ok(FastInsertContext {
            table_meta,
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
}

impl WebhookService {
    pub fn new(webhook_addr: SocketAddr) -> Self {
        Self {
            webhook_addr,
            counter: AtomicU32::new(0),
        }
    }

    pub async fn serve(self) -> anyhow::Result<()> {
        use handlers::*;
        let srv = Arc::new(self);

        let cors_layer = CorsLayer::new()
            .allow_origin(cors::Any)
            .allow_methods(vec![Method::POST]);

        let api_router: Router = Router::new()
            .route("/{database}/{schema}/{table}", post(handle_post_request))
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

    #[tokio::test]
    #[ignore]
    async fn test_webhook_server() -> anyhow::Result<()> {
        let addr = SocketAddr::from(([127, 0, 0, 1], 4560));
        let service = crate::webhook::WebhookService::new(addr);
        service.serve().await?;
        Ok(())
    }
}
