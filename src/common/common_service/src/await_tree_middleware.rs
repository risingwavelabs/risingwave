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

use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll};

use either::Either;
use futures::Future;
use tonic::body::Body;
use tower::{Layer, Service};

/// Manages the await-trees of `gRPC` requests that are currently served by a node.
pub type AwaitTreeRegistryRef = await_tree::Registry;

static NEXT_GRPC_CALL_ID: AtomicU64 = AtomicU64::new(0);

/// Await-tree key type for `gRPC` calls.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct GrpcCall {
    pub desc: String,
}

impl GrpcCall {
    /// Creates a key with an ID shared by all middleware layers and registries in this process.
    pub fn new(desc: impl Into<String>) -> Self {
        let id = NEXT_GRPC_CALL_ID.fetch_add(1, Ordering::Relaxed);
        Self {
            desc: format!("{} - {id}", desc.into()),
        }
    }
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
        }
    }
}

#[derive(Clone)]
pub struct AwaitTreeMiddleware<S> {
    inner: S,
    registry: Option<AwaitTreeRegistryRef>,
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

        let desc = req
            .uri()
            .authority()
            .map_or("??", |authority| authority.as_str());
        let key = GrpcCall::new(desc);

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

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::convert::Infallible;
    use std::sync::Arc;

    use tokio::sync::Barrier;
    use tower::{ServiceExt as _, service_fn};

    use super::*;

    fn pending_service(
        entered: Arc<Barrier>,
        release: Arc<Barrier>,
    ) -> impl Service<http::Request<Body>, Response = http::Response<Body>, Error = Infallible> + Clone
    {
        service_fn(move |_| {
            let entered = entered.clone();
            let release = release.clone();
            async move {
                entered.wait().await;
                release.wait().await;
                Ok(http::Response::new(Body::empty()))
            }
        })
    }

    fn request(path: &'static str) -> http::Request<Body> {
        http::Request::builder()
            .uri(format!("http://127.0.0.1{path}"))
            .body(Body::empty())
            .unwrap()
    }

    #[tokio::test]
    async fn grpc_call_keys_are_unique_across_layers() {
        let registry = await_tree::Registry::new(await_tree::Config::default());
        let entered = Arc::new(Barrier::new(3));
        let release = Arc::new(Barrier::new(3));

        let service_1 = AwaitTreeMiddlewareLayer::new(registry.clone())
            .layer(pending_service(entered.clone(), release.clone()));
        let service_2 = AwaitTreeMiddlewareLayer::new(registry.clone())
            .layer(pending_service(entered.clone(), release.clone()));

        let verify = async {
            entered.wait().await;
            assert_eq!(registry.collect::<GrpcCall>().len(), 2);
            release.wait().await;
        };
        let (result_1, result_2, ()) = tokio::join!(
            service_1.oneshot(request("/task_service.TaskService/CreateTask")),
            service_2.oneshot(request("/task_service.BatchExchangeService/GetData")),
            verify,
        );
        result_1.unwrap();
        result_2.unwrap();
    }

    #[tokio::test]
    async fn grpc_call_keys_are_unique_across_registries() {
        let registry_1 = await_tree::Registry::new(await_tree::Config::default());
        let registry_2 = await_tree::Registry::new(await_tree::Config::default());
        let entered = Arc::new(Barrier::new(3));
        let release = Arc::new(Barrier::new(3));

        let service_1 = AwaitTreeMiddlewareLayer::new(registry_1.clone())
            .layer(pending_service(entered.clone(), release.clone()));
        let service_2 = AwaitTreeMiddlewareLayer::new(registry_2.clone())
            .layer(pending_service(entered.clone(), release.clone()));

        let verify = async {
            entered.wait().await;
            let merged: BTreeMap<_, _> = registry_1
                .collect::<GrpcCall>()
                .into_iter()
                .chain(registry_2.collect::<GrpcCall>())
                .map(|(key, _)| (key.desc, ()))
                .collect();
            assert_eq!(merged.len(), 2);
            release.wait().await;
        };
        let (result_1, result_2, ()) = tokio::join!(
            service_1.oneshot(request("/task_service.TaskService/CreateTask")),
            service_2.oneshot(request("/stream_service.StreamService/UpdateActors")),
            verify,
        );
        result_1.unwrap();
        result_2.unwrap();
    }
}
