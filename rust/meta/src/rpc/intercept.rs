use std::sync::Arc;
use std::task::{Context, Poll};

use hyper::Body;
use tower::{Layer, Service};

use super::metrics::MetaMetrics;

#[derive(Clone)]
pub struct MetricsMiddlewareLayer {
    metrics: Arc<MetaMetrics>,
}

impl MetricsMiddlewareLayer {
    pub fn new(metrics: Arc<MetaMetrics>) -> Self {
        Self { metrics }
    }
}

impl<S> Layer<S> for MetricsMiddlewareLayer {
    type Service = MetricsMiddleware<S>;

    fn layer(&self, service: S) -> Self::Service {
        MetricsMiddleware {
            inner: service,
            metrics: self.metrics.clone(),
        }
    }
}

#[derive(Clone)]
pub struct MetricsMiddleware<S> {
    inner: S,
    metrics: Arc<MetaMetrics>,
}

impl<S> Service<hyper::Request<Body>> for MetricsMiddleware<S>
where
    S: Service<hyper::Request<Body>> + Clone + Send + 'static,
    S::Future: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = futures::future::BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: hyper::Request<Body>) -> Self::Future {
        // This is necessary because tonic internally uses `tower::buffer::Buffer`.
        // See https://github.com/tower-rs/tower/issues/547#issuecomment-767629149
        // for details on why this is necessary
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);

        let metrics = self.metrics.clone();

        // TODO: use GAT to avoid static future and Box::pin
        Box::pin(async move {
            let path = req.uri().path();
            let timer = metrics
                .grpc_latency
                .with_label_values(&[path])
                .start_timer();

            let response = inner.call(req).await?;

            timer.observe_duration();

            Ok(response)
        })
    }
}
