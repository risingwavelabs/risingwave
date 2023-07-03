// Copyright 2023 RisingWave Labs
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

use std::task::{Context, Poll};

use futures::Future;
use hyper::Body;
use risingwave_common::util::tracing::TracingContext;
use tower::{Layer, Service};
use tracing::Instrument;

/// A layer that decorates the inner service with [`TracingExtract`].
#[derive(Clone, Default)]
pub struct TracingExtractLayer {
    _private: (),
}

impl TracingExtractLayer {
    pub fn new() -> Self {
        Self::default()
    }
}

impl<S> Layer<S> for TracingExtractLayer {
    type Service = TracingExtract<S>;

    fn layer(&self, service: S) -> Self::Service {
        TracingExtract { inner: service }
    }
}

/// A service wrapper that extracts the [`TracingContext`] from the HTTP headers and uses it to
/// create a new tracing span for the request handler, if one exists.
///
/// See also `TracingInject` in the `rpc_client` crate.
#[derive(Clone)]
pub struct TracingExtract<S> {
    inner: S,
}

impl<S> Service<hyper::Request<Body>> for TracingExtract<S>
where
    S: Service<hyper::Request<Body>> + Clone + Send + 'static,
    S::Future: Send + 'static,
{
    type Error = S::Error;
    type Response = S::Response;

    type Future = impl Future<Output = Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: hyper::Request<Body>) -> Self::Future {
        // This is necessary because tonic internally uses `tower::buffer::Buffer`.
        // See https://github.com/tower-rs/tower/issues/547#issuecomment-767629149
        // for details on why this is necessary
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);

        async move {
            let span = if let Some(tracing_context) =
                TracingContext::from_http_headers(req.headers())
            {
                let span = tracing::info_span!(
                    "grpc_serve",
                    "otel.name" = req.uri().path(),
                    uri = %req.uri()
                );
                tracing_context.attach(span)
            } else {
                tracing::Span::none() // if there's no parent span, disable tracing for this request
            };

            inner.call(req).instrument(span).await
        }
    }
}
