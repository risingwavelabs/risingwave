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

use std::task::{Context, Poll};

use futures::Future;
use risingwave_common::util::tracing::TracingContext;
use tonic::body::BoxBody;
use tower::Service;

/// A service wrapper that injects the [`TracingContext`] obtained from the current tracing span
/// into the HTTP headers of the request.
///
/// See also `TracingExtract` in the `common_service` crate.
#[derive(Clone, Debug)]
pub struct TracingInjectChannel {
    inner: tonic::transport::Channel,
}

#[cfg(not(madsim))]
impl Service<http::Request<BoxBody>> for TracingInjectChannel {
    type Error = tonic::transport::Error;
    type Response = http::Response<BoxBody>;

    type Future = impl Future<Output = Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut req: http::Request<BoxBody>) -> Self::Future {
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
pub type Channel = TracingInjectChannel;
#[cfg(madsim)]
pub type Channel = tonic::transport::Channel;

/// An extension trait for tonic's `Channel` that wraps it into a [`TracingInjectChannel`].
#[easy_ext::ext(TracingInjectedChannelExt)]
impl tonic::transport::Channel {
    /// Wraps the channel into a [`TracingInjectChannel`], so that the [`TracingContext`] obtained
    /// from the current tracing span is injected into the HTTP headers of the request.
    ///
    /// The server can then extract the [`TracingContext`] from the HTTP headers with the
    /// `TracingExtract` middleware.
    pub fn tracing_injected(self) -> Channel {
        #[cfg(not(madsim))]
        return TracingInjectChannel { inner: self };
        #[cfg(madsim)]
        return self;
    }
}
