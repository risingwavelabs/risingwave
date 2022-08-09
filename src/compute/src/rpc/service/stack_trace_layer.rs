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

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use async_stack_trace::{SpanValue, StackTraceManager};
use futures::Future;
use hyper::Body;
use tokio::sync::Mutex;
use tower::{Layer, Service};

pub type GrpcStackTraceManager = Arc<Mutex<StackTraceManager<u64>>>;

#[derive(Clone)]
pub struct StackTraceLayer {
    manager: GrpcStackTraceManager,
}

impl StackTraceLayer {
    pub fn new(manager: GrpcStackTraceManager) -> Self {
        Self { manager }
    }
}

impl<S> Layer<S> for StackTraceLayer {
    type Service = StackTrace<S>;

    fn layer(&self, service: S) -> Self::Service {
        StackTrace {
            inner: service,
            manager: self.manager.clone(),
            next_id: Default::default(),
        }
    }
}

#[derive(Clone)]
pub struct StackTrace<S> {
    inner: S,

    manager: GrpcStackTraceManager,

    next_id: Arc<AtomicU64>,
}

impl<S> Service<hyper::Request<Body>> for StackTrace<S>
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

        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let manager = self.manager.clone();

        async move {
            let sender = manager.lock().await.register(id);
            let root_span: SpanValue = format!("{}:{}", req.uri().path(), id).into();

            sender
                .trace(
                    inner.call(req),
                    root_span,
                    false,
                    Duration::from_millis(100),
                )
                .await
        }
    }
}
