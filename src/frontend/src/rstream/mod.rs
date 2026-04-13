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

use std::sync::Arc;
use std::sync::atomic::AtomicU32;

use axum::Router;
use axum::response::Response;
use axum::routing::{delete, get, post};
use tower::ServiceBuilder;
use tower_http::add_extension::AddExtensionLayer;

pub mod auth;
mod handlers;
pub mod types;

use self::types::RStreamError;

/// Axum middleware that extracts a bearer token, authenticates it, and
/// inserts [`auth::AuthenticatedUser`] into request extensions.
async fn auth_middleware(
    headers: axum::http::HeaderMap,
    mut request: axum::extract::Request,
    next: axum::middleware::Next,
) -> std::result::Result<Response, RStreamError> {
    let token = auth::extract_bearer_token(&headers)?;
    let user = auth::authenticate_token(&token)?;
    request.extensions_mut().insert(user);
    Ok(next.run(request).await)
}

/// Build the Axum router for rstream API endpoints.
///
/// The returned router should be nested under `/v1` on the main HTTP server.
pub fn rstream_router(counter: Arc<AtomicU32>) -> Router {
    // Data routes — require bearer token authentication
    let data_routes = Router::new()
        .route("/streams", post(handlers::handle_create_stream))
        .route("/streams", get(handlers::handle_list_streams))
        .route("/streams/{name}", get(handlers::handle_get_stream))
        .route("/streams/{name}", delete(handlers::handle_delete_stream))
        .route(
            "/streams/{name}/records",
            post(handlers::handle_append_records).get(handlers::handle_read_records),
        )
        .layer(axum::middleware::from_fn(auth_middleware));

    // Token management routes — require admin secret (or open in dev mode)
    let token_routes = Router::new()
        .route("/tokens", post(handlers::handle_create_token))
        .route("/tokens", get(handlers::handle_list_tokens))
        .route("/tokens/{token}", delete(handlers::handle_delete_token));

    Router::new()
        .merge(data_routes)
        .merge(token_routes)
        .layer(
            ServiceBuilder::new()
                .layer(AddExtensionLayer::new(counter))
                .into_inner(),
        )
}
