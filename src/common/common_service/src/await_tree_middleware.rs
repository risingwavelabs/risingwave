// Copyright 2026 RisingWave Labs
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

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll};

use either::Either;
use futures::Future;
use tonic::body::Body;
use tower::{Layer, Service};

/// Manages the await-trees of `gRPC` requests that are currently served by a node.
pub type AwaitTreeRegistryRef = await_tree::Registry;

/// Await-tree key type for `gRPC` calls.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct GrpcCall {
    pub desc: String,
}

#[derive(Clone)]
pub struct AwaitTreeMiddlewareLayer {
    registry: Option<AwaitTreeRegistryRef>,
}

impl AwaitTreeMiddlewareLayer {
    pub fn new(registry: AwaitTreeRegistryRef) -> Self {
        Self {
            registry: Some(registry),
        }
    }

    pub fn new_optional(registry: Option<AwaitTreeRegistryRef>) -> Self {
        Self { registry }
    }
}

impl<S> Layer<S> for AwaitTreeMiddlewareLayer {
    type Service = AwaitTreeMiddleware<S>;

    fn layer(&self, service: S) -> Self::Service {
        AwaitTreeMiddleware {
            inner: service,
            registry: self.registry.clone(),
            next_id: Default::default(),
        }
    }
}

#[derive(Clone)]
pub struct AwaitTreeMiddleware<S> {
    inner: S,
    registry: Option<AwaitTreeRegistryRef>,
    next_id: Arc<AtomicU64>,
}

impl<S> Service<http::Request<Body>> for AwaitTreeMiddleware<S>
where
    S: Service<http::Request<Body>> + Clone,
{
    type Error = S::Error;
    type Response = S::Response;

    type Future = impl Future<Output = Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: http::Request<Body>) -> Self::Future {
        let Some(registry) = self.registry.clone() else {
            return Either::Left(self.inner.call(req));
        };

        // This is necessary because tonic internally uses `tower::buffer::Buffer`.
        // See https://github.com/tower-rs/tower/issues/547#issuecomment-767629149
        // for details on why this is necessary
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);

        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let desc = if let Some(authority) = req.uri().authority() {
            format!("{authority} - {id}")
        } else {
            format!("?? - {id}")
        };
        let key = GrpcCall { desc };

        Either::Right(async move {
            let root = registry.register(key, req.uri().path());

            root.instrument(inner.call(req)).await
        })
    }
}

#[cfg(not(madsim))]
impl<S: tonic::server::NamedService> tonic::server::NamedService for AwaitTreeMiddleware<S> {
    const NAME: &'static str = S::NAME;
}
