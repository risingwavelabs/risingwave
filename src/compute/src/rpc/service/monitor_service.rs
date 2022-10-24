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
use std::time::Duration;

use risingwave_pb::monitor_service::monitor_service_server::MonitorService;
use risingwave_pb::monitor_service::{
    ProfilingRequest, ProfilingResponse, StackTraceRequest, StackTraceResponse,
};
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

        let rpc_traces = self
            .grpc_stack_trace_mgr
            .lock()
            .await
            .get_all()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();

        Ok(Response::new(StackTraceResponse {
            actor_traces,
            rpc_traces,
        }))
    }

    #[cfg_attr(coverage, no_coverage)]
    async fn profiling(
        &self,
        request: Request<ProfilingRequest>,
    ) -> Result<Response<ProfilingResponse>, Status> {
        if std::env::var("RW_PROFILE_PATH").is_ok() {
            return Err(Status::internal(
                "Profiling is already running by setting RW_PROFILE_PATH",
            ));
        }
        let time = request.into_inner().get_sleep_s();
        let guard = pprof::ProfilerGuardBuilder::default()
            .blocklist(&["libc", "libgcc", "pthread", "vdso"])
            .build()
            .unwrap();
        tokio::time::sleep(Duration::from_secs(time)).await;
        // let buf = SharedWriter::new(vec![]);
        let mut buf = vec![];
        match guard.report().build() {
            Ok(report) => {
                report.flamegraph(&mut buf).unwrap();
                tracing::info!("succeed to generate flamegraph");
                Ok(Response::new(ProfilingResponse { result: buf }))
            }
            Err(err) => {
                tracing::warn!("failed to generate flamegraph: {}", err);
                Err(Status::internal(err.to_string()))
            }
        }
    }
}

pub use grpc_middleware::*;

pub mod grpc_middleware {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;
    use std::task::{Context, Poll};

    use async_stack_trace::{SpanValue, StackTraceManager, TraceConfig};
    use futures::Future;
    use hyper::Body;
    use tokio::sync::Mutex;
    use tower::layer::util::Identity;
    use tower::util::Either;
    use tower::{Layer, Service};

    /// Manages the stack trace of `gRPC` requests that are currently served by the compute node.
    pub type GrpcStackTraceManagerRef = Arc<Mutex<StackTraceManager<u64>>>;

    #[derive(Clone)]
    pub struct StackTraceMiddlewareLayer {
        manager: GrpcStackTraceManagerRef,
        config: TraceConfig,
    }
    pub type OptionalStackTraceMiddlewareLayer = Either<StackTraceMiddlewareLayer, Identity>;

    impl StackTraceMiddlewareLayer {
        pub fn new(manager: GrpcStackTraceManagerRef, config: TraceConfig) -> Self {
            Self { manager, config }
        }

        pub fn new_optional(
            optional: Option<(GrpcStackTraceManagerRef, TraceConfig)>,
        ) -> OptionalStackTraceMiddlewareLayer {
            if let Some((manager, config)) = optional {
                Either::A(Self::new(manager, config))
            } else {
                Either::B(Identity::new())
            }
        }
    }

    impl<S> Layer<S> for StackTraceMiddlewareLayer {
        type Service = StackTraceMiddleware<S>;

        fn layer(&self, service: S) -> Self::Service {
            StackTraceMiddleware {
                inner: service,
                manager: self.manager.clone(),
                config: self.config.clone(),
                next_id: Default::default(),
            }
        }
    }

    #[derive(Clone)]
    pub struct StackTraceMiddleware<S> {
        inner: S,
        manager: GrpcStackTraceManagerRef,
        config: TraceConfig,
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
            let config = self.config.clone();

            async move {
                let sender = manager.lock().await.register(id);
                let root_span: SpanValue = format!("{}:{}", req.uri().path(), id).into();

                sender.trace(inner.call(req), root_span, config).await
            }
        }
    }
}
