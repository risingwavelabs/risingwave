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

use axum::Router;

mod embed;
mod proxy;

/// The router for the dashboard.
///
/// Based on the configuration, it will either serve the assets embedded in the binary,
/// or proxy all requests to the latest static files built and hosted on GitHub.
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
