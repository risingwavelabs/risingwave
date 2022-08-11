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

use std::sync::Arc;

use risingwave_pb::monitor_service::monitor_service_server::MonitorService;
use risingwave_pb::monitor_service::{StackTraceRequest, StackTraceResponse};
use risingwave_stream::task::LocalStreamManager;
use tonic::{Request, Response, Status};

#[derive(Clone)]
pub struct MonitorServiceImpl {
    stream_mgr: Arc<LocalStreamManager>,
    grpc_stack_trace_mgr: GrpcStackTraceManagerRef,
}

impl MonitorServiceImpl {
    pub fn new(
        stream_mgr: Arc<LocalStreamManager>,
        grpc_stack_trace_mgr: GrpcStackTraceManagerRef,
    ) -> Self {
        Self {
            stream_mgr,
            grpc_stack_trace_mgr,
        }
    }
}

#[async_trait::async_trait]
impl MonitorService for MonitorServiceImpl {
    #[cfg_attr(coverage, no_coverage)]
    async fn stack_trace(
        &self,
        request: Request<StackTraceRequest>,
    ) -> Result<Response<StackTraceResponse>, Status> {
        let _req = request.into_inner();

        let actor_traces = self
            .stream_mgr
            .get_actor_traces()
            .into_iter()
            .map(|(k, v)| (k, v.to_string()))
            .collect();

        let grpc_traces = self
            .grpc_stack_trace_mgr
            .lock()
            .await
            .get_all()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();

        Ok(Response::new(StackTraceResponse {
            actor_traces,
            grpc_traces,
        }))
    }
}

pub use grpc_middleware::*;

pub mod grpc_middleware {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;
    use std::task::{Context, Poll};
    use std::time::Duration;

    use async_stack_trace::{SpanValue, StackTraceManager};
    use futures::Future;
    use hyper::Body;
    use risingwave_common::util::env_var::ENABLE_ASYNC_STACK_TRACE;
    use tokio::sync::Mutex;
    use tower::{Layer, Service};

    /// Manages the stack trace of all `gRPC` requests that are currently served by the compute node
    pub type GrpcStackTraceManagerRef = Arc<Mutex<StackTraceManager<u64>>>;

    #[derive(Clone)]
    pub struct StackTraceMiddlewareLayer {
        manager: GrpcStackTraceManagerRef,
    }

    impl StackTraceMiddlewareLayer {
        pub fn new(manager: GrpcStackTraceManagerRef) -> Self {
            Self { manager }
        }
    }

    impl<S> Layer<S> for StackTraceMiddlewareLayer {
        type Service = StackTraceMiddleware<S>;

        fn layer(&self, service: S) -> Self::Service {
            StackTraceMiddleware {
                inner: service,
                manager: self.manager.clone(),
                next_id: Default::default(),
            }
        }
    }

    #[derive(Clone)]
    pub struct StackTraceMiddleware<S> {
        inner: S,
        manager: GrpcStackTraceManagerRef,
        next_id: Arc<AtomicU64>,
    }

    impl<S> Service<hyper::Request<Body>> for StackTraceMiddleware<S>
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
                    .optional_trace(
                        inner.call(req),
                        root_span,
                        false,
                        Duration::from_millis(100),
                        *ENABLE_ASYNC_STACK_TRACE,
                    )
                    .await
            }
        }
    }
}
