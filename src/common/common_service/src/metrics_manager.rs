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

use std::collections::HashSet;
use std::ops::Deref;
use std::sync::OnceLock;

use axum::Extension;
use axum::body::Body;
use axum::handler::{Handler, HandlerWithoutStateExt};
use axum::response::{IntoResponse, Response};
use axum_extra::extract::Query as ExtraQuery;
use prometheus::{Encoder, Registry, TextEncoder};
use risingwave_common::monitor::GLOBAL_METRICS_REGISTRY;
use serde::Deserialize;
use thiserror_ext::AsReport;
use tokio::net::TcpListener;
use tower_http::add_extension::AddExtensionLayer;
use tower_http::compression::CompressionLayer;
use tracing::{error, info, warn};

/// The filter for metrics scrape handler. See [`MetricsManager::metrics`] for more details.
#[derive(Debug, Deserialize)]
struct Filter {
    #[serde(default)]
    include: HashSet<String>,
    #[serde(default)]
    exclude: HashSet<String>,
}

pub struct MetricsManager;

impl MetricsManager {
    pub fn boot_metrics_service(listen_addr: String) {
        static METRICS_SERVICE_LISTEN_ADDR: OnceLock<String> = OnceLock::new();
        let new_listen_addr = listen_addr.clone();
        let current_listen_addr = METRICS_SERVICE_LISTEN_ADDR.get_or_init(|| {
            let listen_addr_clone = listen_addr.clone();
            #[cfg(not(madsim))] // no need in simulation test
            tokio::spawn(async move {
                info!(
                    "Prometheus listener for Prometheus is set up on http://{}",
                    listen_addr
                );

                let service = Self::metrics
                    .layer(AddExtensionLayer::new(
                        GLOBAL_METRICS_REGISTRY.deref().clone(),
                    ))
                    .layer(CompressionLayer::new())
                    .into_make_service();

                let serve_future =
                    axum::serve(TcpListener::bind(&listen_addr).await.unwrap(), service);
                if let Err(err) = serve_future.await {
                    error!(error = %err.as_report(), "metrics service exited with error");
                }
            });
            listen_addr_clone
        });
        if new_listen_addr != *current_listen_addr {
            warn!(
                "unable to listen port {} for metrics service. Currently listening on {}",
                new_listen_addr, current_listen_addr
            );
        }
    }

    /// Gather metrics from the global registry and encode them in the Prometheus text format.
    ///
    /// The handler accepts the following query parameters to filter metrics. Note that `include`
    /// and `exclude` should not be used together.
    ///
    /// - `/metrics`                            (without filter)
    /// - `/metrics?include=foo`                (include one metric)
    /// - `/metrics?include=foo&include=bar`    (include multiple metrics)
    /// - `/metrics?exclude=foo&exclude=bar`    (include all but foo and bar)
    ///
    /// One can specify parameters by configuring Prometheus scrape config like below:
    /// ```yaml
    /// - job_name: compute-node
    ///   params:
    ///     include: ["foo", "bar"]
    /// ```
    #[expect(clippy::unused_async, reason = "required by service_fn")]
    async fn metrics(
        ExtraQuery(Filter { include, exclude }): ExtraQuery<Filter>,
        Extension(registry): Extension<Registry>,
    ) -> impl IntoResponse {
        let mut mf = registry.gather();

        // Filter metrics by name.
        // TODO: can we avoid gathering them all?
        if !include.is_empty() && !exclude.is_empty() {
            return Response::builder()
                .status(400)
                .body("should not specify both include and exclude".into())
                .unwrap();
        } else if !include.is_empty() {
            mf.retain(|fam| include.contains(fam.name()));
        } else if !exclude.is_empty() {
            mf.retain(|fam| !exclude.contains(fam.name()));
        }

        let encoder = TextEncoder::new();
        let mut buffer = vec![];
        encoder.encode(&mf, &mut buffer).unwrap();

        Response::builder()
            .header(axum::http::header::CONTENT_TYPE, encoder.format_type())
            .body(Body::from(buffer))
            .unwrap()
    }
}
