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

use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use itertools::Itertools;
use risingwave_common::config::ServerConfig;
use risingwave_common::heap_profiling::{
    self, AUTO_DUMP_MID_NAME, COLLAPSED_SUFFIX, MANUALLY_DUMP_MID_NAME,
};
use risingwave_pb::monitor_service::monitor_service_server::MonitorService;
use risingwave_pb::monitor_service::{
    AnalyzeHeapRequest, AnalyzeHeapResponse, HeapProfilingRequest, HeapProfilingResponse,
    ListHeapProfilingRequest, ListHeapProfilingResponse, ProfilingRequest, ProfilingResponse,
    StackTraceRequest, StackTraceResponse,
};
use risingwave_stream::task::LocalStreamManager;
use tonic::{Request, Response, Status};

#[derive(Clone)]
pub struct MonitorServiceImpl {
    stream_mgr: Arc<LocalStreamManager>,
    grpc_await_tree_reg: Option<AwaitTreeRegistryRef>,
    server_config: ServerConfig,
}

impl MonitorServiceImpl {
    pub fn new(
        stream_mgr: Arc<LocalStreamManager>,
        grpc_await_tree_reg: Option<AwaitTreeRegistryRef>,
        server_config: ServerConfig,
    ) -> Self {
        Self {
            stream_mgr,
            grpc_await_tree_reg,
            server_config,
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
            .await
            .into_iter()
            .map(|(k, v)| (k, v.to_string()))
            .collect();

        let rpc_traces = if let Some(m) = &self.grpc_await_tree_reg {
            m.lock()
                .await
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect()
        } else {
            Default::default()
        };

        Ok(Response::new(StackTraceResponse {
            actor_traces,
            rpc_traces,
            compaction_task_traces: Default::default(),
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

    #[cfg_attr(coverage, no_coverage)]
    async fn heap_profiling(
        &self,
        request: Request<HeapProfilingRequest>,
    ) -> Result<Response<HeapProfilingResponse>, Status> {
        use std::ffi::CStr;
        use std::fs::create_dir_all;
        use std::path::PathBuf;

        use tikv_jemalloc_ctl;

        if !cfg!(target_os = "linux") {
            return Err(Status::unimplemented(
                "heap profiling is only implemented on Linux",
            ));
        }

        if !tikv_jemalloc_ctl::opt::prof::read().unwrap() {
            return Err(Status::failed_precondition(
                "Jemalloc profiling is not enabled on the node. Try start the node with `MALLOC_CONF=prof:true`",
            ));
        }

        let time_prefix = chrono::Local::now().format("%Y-%m-%d-%H-%M-%S").to_string();
        let file_name = format!("{}.{}\0", time_prefix, MANUALLY_DUMP_MID_NAME);
        let arg_dir = request.into_inner().get_dir().clone();
        let dir = PathBuf::from(if arg_dir.is_empty() {
            &self.server_config.heap_profiling.dir
        } else {
            &arg_dir
        });
        create_dir_all(&dir)?;

        let file_path_buf = dir.join(file_name);
        let file_path = file_path_buf
            .to_str()
            .ok_or_else(|| Status::internal("The file dir is not a UTF-8 String"))?;

        let file_path_str = Box::leak(file_path.to_string().into_boxed_str());
        let file_path_bytes = unsafe { file_path_str.as_bytes_mut() };
        let file_path_ptr = file_path_bytes.as_mut_ptr();
        let response = if let Err(e) = tikv_jemalloc_ctl::prof::dump::write(
            CStr::from_bytes_with_nul(file_path_bytes).unwrap(),
        ) {
            tracing::warn!("Manually Jemalloc dump heap file failed! {:?}", e);
            Err(Status::internal(e.to_string()))
        } else {
            Ok(Response::new(HeapProfilingResponse {}))
        };
        let _ = unsafe { Box::from_raw(file_path_ptr) };
        response
    }

    #[cfg_attr(coverage, no_coverage)]
    async fn list_heap_profiling(
        &self,
        _request: Request<ListHeapProfilingRequest>,
    ) -> Result<Response<ListHeapProfilingResponse>, Status> {
        let dump_dir = self.server_config.heap_profiling.dir.clone();
        let auto_dump_files_name: Vec<_> = fs::read_dir(dump_dir.clone())?
            .map(|entry| {
                let entry = entry?;
                Ok::<_, Status>(entry.file_name().to_string_lossy().to_string())
            })
            .filter(|name| {
                if let Ok(name) = name {
                    name.contains(AUTO_DUMP_MID_NAME) && !name.ends_with(COLLAPSED_SUFFIX)
                } else {
                    true
                }
            })
            .try_collect()?;
        let manually_dump_files_name: Vec<_> = fs::read_dir(dump_dir.clone())?
            .map(|entry| {
                let entry = entry?;
                Ok::<_, Status>(entry.file_name().to_string_lossy().to_string())
            })
            .filter(|name| {
                if let Ok(name) = name {
                    name.contains(MANUALLY_DUMP_MID_NAME) && !name.ends_with(COLLAPSED_SUFFIX)
                } else {
                    true
                }
            })
            .try_collect()?;

        Ok(Response::new(ListHeapProfilingResponse {
            dir: dump_dir,
            name_auto: auto_dump_files_name,
            name_manually: manually_dump_files_name,
        }))
    }

    #[cfg_attr(coverage, no_coverage)]
    async fn analyze_heap(
        &self,
        request: Request<AnalyzeHeapRequest>,
    ) -> Result<Response<AnalyzeHeapResponse>, Status> {
        let dumped_path_str = request.into_inner().get_path().clone();
        let collapsed_path_str = format!("{}.{}", dumped_path_str, COLLAPSED_SUFFIX);
        let collapsed_path = Path::new(&collapsed_path_str);

        // run jeprof if the target was not analyzed before
        if !collapsed_path.exists() {
            heap_profiling::jeprof::run(dumped_path_str, collapsed_path_str.clone()).await?;
        }

        let file = fs::read(Path::new(&collapsed_path_str))?;
        Ok(Response::new(AnalyzeHeapResponse { result: file }))
    }
}

pub use grpc_middleware::*;

pub mod grpc_middleware {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;
    use std::task::{Context, Poll};

    use either::Either;
    use futures::Future;
    use hyper::Body;
    use tokio::sync::Mutex;
    use tonic::transport::NamedService;
    use tower::{Layer, Service};

    /// Manages the await-trees of `gRPC` requests that are currently served by the compute node.
    pub type AwaitTreeRegistryRef = Arc<Mutex<await_tree::Registry<u64>>>;

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

    impl<S> Service<hyper::Request<Body>> for AwaitTreeMiddleware<S>
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
            let Some(registry) = self.registry.clone() else {
                return Either::Left(self.inner.call(req));
            };

            // This is necessary because tonic internally uses `tower::buffer::Buffer`.
            // See https://github.com/tower-rs/tower/issues/547#issuecomment-767629149
            // for details on why this is necessary
            let clone = self.inner.clone();
            let mut inner = std::mem::replace(&mut self.inner, clone);

            let id = self.next_id.fetch_add(1, Ordering::SeqCst);

            Either::Right(async move {
                let root = registry
                    .lock()
                    .await
                    .register(id, format!("{}:{}", req.uri().path(), id));

                root.instrument(inner.call(req)).await
            })
        }
    }

    impl<S: NamedService> NamedService for AwaitTreeMiddleware<S> {
        const NAME: &'static str = S::NAME;
    }
}
