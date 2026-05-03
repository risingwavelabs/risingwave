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

use fastrace::future::FutureExt as _;
use futures::Future;
use risingwave_common::util::tracing::TracingContext;
use tonic::body::Body;
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

#[cfg(test)]
mod tests {
    use std::sync::{Arc, LazyLock, Mutex};
    use std::time::Duration;

    use fastrace::collector::{Config, Reporter, SpanContext, SpanRecord};
    use tower::{Layer as _, Service as _, ServiceExt as _, service_fn};

    use super::*;

    static FASTRACE_REPORTER_LOCK: LazyLock<Mutex<()>> = LazyLock::new(Mutex::default);

    #[derive(Clone)]
    struct CapturingReporter {
        spans: Arc<Mutex<Vec<SpanRecord>>>,
    }

    impl Reporter for CapturingReporter {
        fn report(&mut self, mut spans: Vec<SpanRecord>) {
            self.spans.lock().unwrap().append(&mut spans);
        }
    }

    #[tokio::test]
    async fn tracing_extract_uses_fastrace_parent_on_async_handler_future() {
        let _guard = FASTRACE_REPORTER_LOCK.lock().unwrap();
        let spans = Arc::new(Mutex::new(Vec::new()));
        fastrace::set_reporter(
            CapturingReporter {
                spans: spans.clone(),
            },
            Config::default().report_interval(Duration::from_millis(1)),
        );

        let parent = fastrace::Span::root("client_root", SpanContext::random());
        let parent_context = TracingContext::from_fastrace_span(&parent);
        let parent_span_id = parent_context.to_fastrace_span_context().unwrap().span_id;

        let mut req = http::Request::builder()
            .uri("/test.Service/Call")
            .body(Body::empty())
            .unwrap();
        req.headers_mut().extend(parent_context.to_http_headers());

        let service = service_fn(|_req: http::Request<Body>| async move {
            let child = fastrace::Span::enter_with_local_parent("handler_child");
            drop(child);
            Ok::<_, std::convert::Infallible>(http::Response::new(Body::empty()))
        });
        let mut service = TracingExtractLayer::new().layer(service);

        service.ready().await.unwrap().call(req).await.unwrap();
        drop(parent);
        fastrace::flush();

        let spans = spans.lock().unwrap().clone();
        let grpc_span = spans
            .iter()
            .find(|span| span.name == "grpc_serve")
            .expect("grpc_serve span not collected");
        assert_eq!(grpc_span.parent_id, parent_span_id);

        let child_span = spans
            .iter()
            .find(|span| span.name == "handler_child")
            .expect("handler_child span not collected");
        assert_eq!(child_span.parent_id, grpc_span.span_id);
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

impl<S> Service<http::Request<Body>> for TracingExtract<S>
where
    S: Service<http::Request<Body>> + Clone + Send + 'static,
    S::Future: Send + 'static,
{
    type Error = S::Error;
    type Response = S::Response;

    type Future = impl Future<Output = Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: http::Request<Body>) -> Self::Future {
        // This is necessary because tonic internally uses `tower::buffer::Buffer`.
        // See https://github.com/tower-rs/tower/issues/547#issuecomment-767629149
        // for details on why this is necessary
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);

        async move {
            let uri = req.uri().to_string();
            let span = match TracingContext::from_http_headers(req.headers()) {
                Some(tracing_context) => {
                    let span = tracing::info_span!(
                        "grpc_serve",
                        "otel.name" = req.uri().path(),
                        uri = %req.uri()
                    );
                    let fastrace_span = tracing_context
                        .root_span("grpc_serve")
                        .with_property(|| ("otel.name", req.uri().path().to_owned()))
                        .with_property(|| ("uri", uri.clone()));

                    return inner
                        .call(req)
                        .instrument(tracing_context.attach(span))
                        .in_span(fastrace_span)
                        .await;
                }
                _ => {
                    tracing::Span::none() // if there's no parent span, disable tracing for this request
                }
            };

            inner.call(req).instrument(span).await
        }
    }
}
