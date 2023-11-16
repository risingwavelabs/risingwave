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
use risingwave_common::util::tracing::TracingContext;
use tower::{Layer, Service};

/// A layer that decorates the inner service with [`TracingInject`].
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

/// A service wrapper that injects the [`TracingContext`] obtained from the current tracing span
/// into the HTTP headers of the request.
///
/// See also `TracingExtract` in the `common_service` crate.
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

/// A wrapper around tonic's `Channel` that injects the [`TracingContext`] obtained from the current
/// tracing span when making gRPC requests.
#[cfg(not(madsim))]
pub type Channel = TracingInject<tonic::transport::Channel>;
#[cfg(madsim)]
pub type Channel = tonic::transport::Channel;

/// An extension trait for tonic's `Channel` that wraps it in a [`TracingInject`] service.
#[easy_ext::ext(TracingInjectedChannelExt)]
impl tonic::transport::Channel {
    /// Wraps the channel in a [`TracingInject`] service, so that the [`TracingContext`] obtained
    /// from the current tracing span is injected into the HTTP headers of the request.
    ///
    /// The server can then extract the [`TracingContext`] from the HTTP headers with the
    /// `TracingExtract` middleware.
    pub fn tracing_injected(self) -> Channel {
        #[cfg(not(madsim))]
        return TracingInject { inner: self };
        #[cfg(madsim)]
        return self;
    }
}
