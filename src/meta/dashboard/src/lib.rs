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

#![allow(clippy::doc_markdown)]

use axum::Router;

mod embed;
mod proxy;

/// The router for the dashboard.
///
/// Based on the configuration, it will either serve the assets embedded in the binary,
/// or proxy all requests to the latest static files built and hosted on GitHub.
///
/// - For development and CI builds, the requests to the dashboard will be proxied. This
///   is to reduce the build time and eliminate the dependency on `node`, so that the
///   developer experience can be better.
///
/// - For release builds, the assets will built in the build script and embedded in the
///   binary. This is to make the deployment easier and the dashboard more reliable
///   without relying on external or remote resources.
///
/// If you're going to develop with the dashboard, see `dashboard/README.md` for more
/// details.
pub fn router() -> Router {
    // We use `cfg!` instead of `#[cfg]` here to ensure both branches can be checked
    // by the compiler no matter which one is actually used.
    if cfg!(dashboard_built) {
        tracing::info!("using embedded dashboard assets");
        embed::router()
    } else {
        tracing::info!("using proxied dashboard assets");
        proxy::router()
    }
}
