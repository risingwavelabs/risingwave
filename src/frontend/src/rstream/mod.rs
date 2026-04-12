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
use axum::routing::{delete, get, post};
use tower::ServiceBuilder;
use tower_http::add_extension::AddExtensionLayer;

mod handlers;
pub mod types;

/// Build the Axum router for rstream API endpoints.
///
/// The returned router should be nested under `/v1` on the main HTTP server.
pub fn rstream_router(counter: Arc<AtomicU32>) -> Router {
    Router::new()
        .route("/streams", post(handlers::handle_create_stream))
        .route("/streams", get(handlers::handle_list_streams))
        .route("/streams/{name}", get(handlers::handle_get_stream))
        .route("/streams/{name}", delete(handlers::handle_delete_stream))
        .route(
            "/streams/{name}/records",
            post(handlers::handle_append_records).get(handlers::handle_read_records),
        )
        .layer(
            ServiceBuilder::new()
                .layer(AddExtensionLayer::new(counter))
                .into_inner(),
        )
}
