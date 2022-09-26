// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::net::SocketAddr;
use std::sync::Arc;

use hyper::{Body, Request, Response};
use prometheus::{Encoder, Registry, TextEncoder};
use tower::make::Shared;
use tower::ServiceBuilder;
use tower_http::add_extension::AddExtensionLayer;
use tracing::info;

pub struct MetricsManager {}

impl MetricsManager {
    pub fn boot_metrics_service(listen_addr: String, registry: Arc<Registry>) {
        tokio::spawn(async move {
            info!(
                "Prometheus listener for Prometheus is set up on http://{}",
                listen_addr
            );
            let listen_socket_addr: SocketAddr = listen_addr.parse().unwrap();
            let service = ServiceBuilder::new()
                .layer(AddExtensionLayer::new(registry))
                .service_fn(Self::metrics_service);
            let serve_future = hyper::Server::bind(&listen_socket_addr).serve(Shared::new(service));
            if let Err(err) = serve_future.await {
                eprintln!("server error: {}", err);
            }
        });
    }

    #[expect(clippy::unused_async, reason = "required by service_fn")]
    async fn metrics_service(req: Request<Body>) -> Result<Response<Body>, hyper::Error> {
        let registry = req.extensions().get::<Arc<Registry>>().unwrap();
        let encoder = TextEncoder::new();
        let mut buffer = vec![];
        let mf = registry.gather();
        encoder.encode(&mf, &mut buffer).unwrap();
        let response = Response::builder()
            .header(hyper::header::CONTENT_TYPE, encoder.format_type())
            .body(Body::from(buffer))
            .unwrap();

        Ok(response)
    }
}
