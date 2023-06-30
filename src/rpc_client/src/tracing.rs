use std::task::{Context, Poll};

use futures::Future;
use risingwave_common::util::tracing::TracingContext;
use tower::{Layer, Service};

#[derive(Clone, Default)]
pub struct TracingInjectLayer {
    _private: (),
}

impl TracingInjectLayer {
    #[allow(dead_code)]
    pub fn new() -> Self {
        Self::default()
    }
}

impl<S> Layer<S> for TracingInjectLayer {
    type Service = TracingInject<S>;

    fn layer(&self, service: S) -> Self::Service {
        TracingInject { inner: service }
    }
}

#[derive(Clone, Debug)]
pub struct TracingInject<S> {
    inner: S,
}

impl<S, B> Service<hyper::Request<B>> for TracingInject<S>
where
    S: Service<hyper::Request<B>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    B: hyper::body::HttpBody, // tonic `Channel` uses `BoxBody` instead of `hyper::Body`
{
    type Error = S::Error;
    type Response = S::Response;

    type Future = impl Future<Output = Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut req: hyper::Request<B>) -> Self::Future {
        // This is necessary because tonic internally uses `tower::buffer::Buffer`.
        // See https://github.com/tower-rs/tower/issues/547#issuecomment-767629149
        // for details on why this is necessary
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);

        async move {
            let headers = TracingContext::from_current_span().to_http_headers();
            req.headers_mut().extend(headers);
            inner.call(req).await
        }
    }
}

pub type Channel = TracingInject<tonic::transport::Channel>;

#[easy_ext::ext(TracingInjectedChannelExt)]
impl tonic::transport::Channel {
    pub fn tracing_injected(self) -> Channel {
        TracingInject { inner: self }
    }
}
