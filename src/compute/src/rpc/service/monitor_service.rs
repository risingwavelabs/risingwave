// Copyright 2025 RisingWave Labs
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

use std::collections::{BTreeMap, HashMap};
use std::ffi::CString;
use std::fs;
use std::path::Path;
use std::time::Duration;

use foyer::{HybridCache, TracingOptions};
use itertools::Itertools;
use prometheus::core::Collector;
use prometheus::proto::Metric;
use risingwave_common::config::{MetricLevel, ServerConfig};
use risingwave_common_heap_profiling::{AUTO_DUMP_SUFFIX, COLLAPSED_SUFFIX, MANUALLY_DUMP_SUFFIX};
use risingwave_hummock_sdk::HummockSstableObjectId;
use risingwave_jni_core::jvm_runtime::dump_jvm_stack_traces;
use risingwave_pb::monitor_service::monitor_service_server::MonitorService;
use risingwave_pb::monitor_service::{
    AnalyzeHeapRequest, AnalyzeHeapResponse, ChannelStats, FragmentStats, GetProfileStatsRequest,
    GetProfileStatsResponse, GetStreamingStatsRequest, GetStreamingStatsResponse,
    HeapProfilingRequest, HeapProfilingResponse, ListHeapProfilingRequest,
    ListHeapProfilingResponse, ProfilingRequest, ProfilingResponse, RelationStats,
    StackTraceRequest, StackTraceResponse, TieredCacheTracingRequest, TieredCacheTracingResponse,
};
use risingwave_rpc_client::error::ToTonicStatus;
use risingwave_storage::hummock::compactor::await_tree_key::Compaction;
use risingwave_storage::hummock::{Block, Sstable, SstableBlockIndex};
use risingwave_stream::executor::monitor::global_streaming_metrics;
use risingwave_stream::task::LocalStreamManager;
use risingwave_stream::task::await_tree_key::{Actor, BarrierAwait};
use thiserror_ext::AsReport;
use tonic::{Code, Request, Response, Status};

type MetaCache = HybridCache<HummockSstableObjectId, Box<Sstable>>;
type BlockCache = HybridCache<SstableBlockIndex, Box<Block>>;

#[derive(Clone)]
pub struct MonitorServiceImpl {
    stream_mgr: LocalStreamManager,
    server_config: ServerConfig,
    meta_cache: Option<MetaCache>,
    block_cache: Option<BlockCache>,
}

impl MonitorServiceImpl {
    pub fn new(
        stream_mgr: LocalStreamManager,
        server_config: ServerConfig,
        meta_cache: Option<MetaCache>,
        block_cache: Option<BlockCache>,
    ) -> Self {
        Self {
            stream_mgr,
            server_config,
            meta_cache,
            block_cache,
        }
    }
}

#[async_trait::async_trait]
impl MonitorService for MonitorServiceImpl {
    #[cfg_attr(coverage, coverage(off))]
    async fn stack_trace(
        &self,
        request: Request<StackTraceRequest>,
    ) -> Result<Response<StackTraceResponse>, Status> {
        let _req = request.into_inner();

        let actor_traces = if let Some(reg) = self.stream_mgr.await_tree_reg() {
            reg.collect::<Actor>()
                .into_iter()
                .map(|(k, v)| (k.0, v.to_string()))
                .collect()
        } else {
            Default::default()
        };

        let barrier_traces = if let Some(reg) = self.stream_mgr.await_tree_reg() {
            reg.collect::<BarrierAwait>()
                .into_iter()
                .map(|(k, v)| (k.prev_epoch, v.to_string()))
                .collect()
        } else {
            Default::default()
        };

        let rpc_traces = if let Some(reg) = self.stream_mgr.await_tree_reg() {
            reg.collect::<GrpcCall>()
                .into_iter()
                .map(|(k, v)| (k.desc, v.to_string()))
                .collect()
        } else {
            Default::default()
        };

        let compaction_task_traces = if let Some(hummock) =
            self.stream_mgr.env.state_store().as_hummock()
            && let Some(m) = hummock.compaction_await_tree_reg()
        {
            m.collect::<Compaction>()
                .into_iter()
                .map(|(k, v)| (format!("{k:?}"), v.to_string()))
                .collect()
        } else {
            Default::default()
        };

        let barrier_worker_state = self.stream_mgr.inspect_barrier_state().await?;

        let jvm_stack_traces = match dump_jvm_stack_traces() {
            Ok(None) => None,
            Err(err) => Some(err.as_report().to_string()),
            Ok(Some(stack_traces)) => Some(stack_traces),
        };

        Ok(Response::new(StackTraceResponse {
            actor_traces,
            rpc_traces,
            compaction_task_traces,
            inflight_barrier_traces: barrier_traces,
            barrier_worker_state: BTreeMap::from_iter([(
                self.stream_mgr.env.worker_id(),
                barrier_worker_state,
            )]),
            jvm_stack_traces: match jvm_stack_traces {
                Some(stack_traces) => {
                    BTreeMap::from_iter([(self.stream_mgr.env.worker_id(), stack_traces)])
                }
                None => BTreeMap::new(),
            },
        }))
    }

    #[cfg_attr(coverage, coverage(off))]
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
                tracing::warn!(error = %err.as_report(), "failed to generate flamegraph");
                Err(err.to_status(Code::Internal, "monitor"))
            }
        }
    }

    #[cfg_attr(coverage, coverage(off))]
    async fn heap_profiling(
        &self,
        request: Request<HeapProfilingRequest>,
    ) -> Result<Response<HeapProfilingResponse>, Status> {
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

        let time_prefix = chrono::Local::now().format("%Y-%m-%d-%H-%M-%S");
        let file_name = format!("{}.{}", time_prefix, MANUALLY_DUMP_SUFFIX);
        let arg_dir = request.into_inner().dir;
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
        let file_path_c =
            CString::new(file_path).map_err(|_| Status::internal("0 byte in file path"))?;

        // FIXME(yuhao): `unsafe` here because `jemalloc_dump_mib.write` requires static lifetime
        if let Err(e) =
            tikv_jemalloc_ctl::prof::dump::write(unsafe { &*(file_path_c.as_c_str() as *const _) })
        {
            tracing::warn!("Manually Jemalloc dump heap file failed! {:?}", e);
            Err(Status::internal(e.to_string()))
        } else {
            tracing::info!("Manually Jemalloc dump heap file created: {}", file_path);
            Ok(Response::new(HeapProfilingResponse {}))
        }
    }

    #[cfg_attr(coverage, coverage(off))]
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
                    name.contains(AUTO_DUMP_SUFFIX) && !name.ends_with(COLLAPSED_SUFFIX)
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
                    name.contains(MANUALLY_DUMP_SUFFIX) && !name.ends_with(COLLAPSED_SUFFIX)
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

    #[cfg_attr(coverage, coverage(off))]
    async fn analyze_heap(
        &self,
        request: Request<AnalyzeHeapRequest>,
    ) -> Result<Response<AnalyzeHeapResponse>, Status> {
        let dumped_path_str = request.into_inner().get_path().clone();
        let collapsed_path_str = format!("{}.{}", dumped_path_str, COLLAPSED_SUFFIX);
        let collapsed_path = Path::new(&collapsed_path_str);

        // run jeprof if the target was not analyzed before
        if !collapsed_path.exists() {
            risingwave_common_heap_profiling::jeprof::run(
                dumped_path_str,
                collapsed_path_str.clone(),
            )
            .await
            .map_err(|e| e.to_status(Code::Internal, "monitor"))?;
        }

        let file = fs::read(Path::new(&collapsed_path_str))?;
        Ok(Response::new(AnalyzeHeapResponse { result: file }))
    }

    async fn get_profile_stats(
        &self,
        request: Request<GetProfileStatsRequest>,
    ) -> Result<Response<GetProfileStatsResponse>, Status> {
        let metrics = global_streaming_metrics(MetricLevel::Info);
        let executor_ids = &request.into_inner().executor_ids;
        let stream_node_output_row_count = metrics
            .mem_stream_node_output_row_count
            .collect(executor_ids);
        let stream_node_output_blocking_duration_ms = metrics
            .mem_stream_node_output_blocking_duration_ms
            .collect(executor_ids);
        Ok(Response::new(GetProfileStatsResponse {
            stream_node_output_row_count,
            stream_node_output_blocking_duration_ms,
        }))
    }

    #[cfg_attr(coverage, coverage(off))]
    async fn get_streaming_stats(
        &self,
        _request: Request<GetStreamingStatsRequest>,
    ) -> Result<Response<GetStreamingStatsResponse>, Status> {
        let metrics = global_streaming_metrics(MetricLevel::Info);

        fn collect<T: Collector>(m: &T) -> Vec<Metric> {
            m.collect().into_iter().next().unwrap().take_metric()
        }

        // Must ensure the label exists and can be parsed into `T`
        fn get_label<T: std::str::FromStr>(metric: &Metric, label: &str) -> T {
            metric
                .get_label()
                .iter()
                .find(|lp| lp.name() == label)
                .unwrap()
                .value()
                .parse::<T>()
                .ok()
                .unwrap()
        }

        let actor_output_buffer_blocking_duration_ns =
            collect(&metrics.actor_output_buffer_blocking_duration_ns);
        let actor_count = collect(&metrics.actor_count);

        let actor_count: HashMap<_, _> = actor_count
            .iter()
            .map(|m| {
                let fragment_id: u32 = get_label(m, "fragment_id");
                let count = m.get_gauge().value() as u32;
                (fragment_id, count)
            })
            .collect();

        let mut fragment_stats: HashMap<u32, FragmentStats> = HashMap::new();
        for (&fragment_id, &actor_count) in &actor_count {
            fragment_stats.insert(
                fragment_id,
                FragmentStats {
                    actor_count,
                    current_epoch: 0,
                },
            );
        }

        let actor_current_epoch = collect(&metrics.actor_current_epoch);
        for m in &actor_current_epoch {
            let fragment_id: u32 = get_label(m, "fragment_id");
            let epoch = m.get_gauge().value() as u64;
            if let Some(s) = fragment_stats.get_mut(&fragment_id) {
                s.current_epoch = if s.current_epoch == 0 {
                    epoch
                } else {
                    u64::min(s.current_epoch, epoch)
                }
            } else {
                warn!(
                    fragment_id = fragment_id,
                    "Miss corresponding actor count metrics"
                );
            }
        }

        let mut relation_stats: HashMap<u32, RelationStats> = HashMap::new();
        let mview_current_epoch = collect(&metrics.materialize_current_epoch);
        for m in &mview_current_epoch {
            let table_id: u32 = get_label(m, "table_id");
            let epoch = m.get_gauge().value() as u64;
            if let Some(s) = relation_stats.get_mut(&table_id) {
                s.current_epoch = if s.current_epoch == 0 {
                    epoch
                } else {
                    u64::min(s.current_epoch, epoch)
                };
                s.actor_count += 1;
            } else {
                relation_stats.insert(
                    table_id,
                    RelationStats {
                        actor_count: 1,
                        current_epoch: epoch,
                    },
                );
            }
        }

        let mut channel_stats: BTreeMap<String, ChannelStats> = BTreeMap::new();

        for metric in actor_output_buffer_blocking_duration_ns {
            let fragment_id: u32 = get_label(&metric, "fragment_id");
            let downstream_fragment_id: u32 = get_label(&metric, "downstream_fragment_id");

            let key = format!("{}_{}", fragment_id, downstream_fragment_id);
            let channel_stat = channel_stats.entry(key).or_insert_with(|| ChannelStats {
                actor_count: 0,
                output_blocking_duration: 0.,
                recv_row_count: 0,
                send_row_count: 0,
            });

            // When metrics level is Debug, `actor_id` will be removed to reduce metrics.
            // See `src/common/metrics/src/relabeled_metric.rs`
            channel_stat.actor_count += if get_label::<String>(&metric, "actor_id").is_empty() {
                actor_count[&fragment_id]
            } else {
                1
            };
            channel_stat.output_blocking_duration += metric.get_counter().value();
        }

        let actor_output_row_count = collect(&metrics.actor_out_record_cnt);
        for metric in actor_output_row_count {
            let fragment_id: u32 = get_label(&metric, "fragment_id");

            // Find out and write to all downstream channels
            let key_prefix = format!("{}_", fragment_id);
            let key_range_end = format!("{}`", fragment_id); // '`' is next to `_`
            for (_, s) in channel_stats.range_mut(key_prefix..key_range_end) {
                s.send_row_count += metric.get_counter().value() as u64;
            }
        }

        let actor_input_row_count = collect(&metrics.actor_in_record_cnt);
        for metric in actor_input_row_count {
            let upstream_fragment_id: u32 = get_label(&metric, "upstream_fragment_id");
            let fragment_id: u32 = get_label(&metric, "fragment_id");

            let key = format!("{}_{}", upstream_fragment_id, fragment_id);
            if let Some(s) = channel_stats.get_mut(&key) {
                s.recv_row_count += metric.get_counter().value() as u64;
            }
        }

        let channel_stats = channel_stats.into_iter().collect();
        Ok(Response::new(GetStreamingStatsResponse {
            channel_stats,
            fragment_stats,
            relation_stats,
        }))
    }

    #[cfg_attr(coverage, coverage(off))]
    async fn tiered_cache_tracing(
        &self,
        request: Request<TieredCacheTracingRequest>,
    ) -> Result<Response<TieredCacheTracingResponse>, Status> {
        let req = request.into_inner();

        tracing::info!("Update tiered cache tracing config: {req:?}");

        if let Some(cache) = &self.meta_cache {
            if req.enable {
                cache.enable_tracing();
            } else {
                cache.disable_tracing();
            }
            let mut options = TracingOptions::new();
            if let Some(threshold) = req.record_hybrid_insert_threshold_ms {
                options = options
                    .with_record_hybrid_insert_threshold(Duration::from_millis(threshold as _));
            }
            if let Some(threshold) = req.record_hybrid_get_threshold_ms {
                options =
                    options.with_record_hybrid_get_threshold(Duration::from_millis(threshold as _));
            }
            if let Some(threshold) = req.record_hybrid_obtain_threshold_ms {
                options = options
                    .with_record_hybrid_obtain_threshold(Duration::from_millis(threshold as _));
            }
            if let Some(threshold) = req.record_hybrid_remove_threshold_ms {
                options = options
                    .with_record_hybrid_remove_threshold(Duration::from_millis(threshold as _));
            }
            if let Some(threshold) = req.record_hybrid_fetch_threshold_ms {
                options = options
                    .with_record_hybrid_fetch_threshold(Duration::from_millis(threshold as _));
            }
            cache.update_tracing_options(options);
        }

        if let Some(cache) = &self.block_cache {
            if req.enable {
                cache.enable_tracing();
            } else {
                cache.disable_tracing();
            }
            let mut options = TracingOptions::new();
            if let Some(threshold) = req.record_hybrid_insert_threshold_ms {
                options = options
                    .with_record_hybrid_insert_threshold(Duration::from_millis(threshold as _));
            }
            if let Some(threshold) = req.record_hybrid_get_threshold_ms {
                options =
                    options.with_record_hybrid_get_threshold(Duration::from_millis(threshold as _));
            }
            if let Some(threshold) = req.record_hybrid_obtain_threshold_ms {
                options = options
                    .with_record_hybrid_obtain_threshold(Duration::from_millis(threshold as _));
            }
            if let Some(threshold) = req.record_hybrid_remove_threshold_ms {
                options = options
                    .with_record_hybrid_remove_threshold(Duration::from_millis(threshold as _));
            }
            if let Some(threshold) = req.record_hybrid_fetch_threshold_ms {
                options = options
                    .with_record_hybrid_fetch_threshold(Duration::from_millis(threshold as _));
            }
            cache.update_tracing_options(options);
        }

        Ok(Response::new(TieredCacheTracingResponse::default()))
    }
}

pub use grpc_middleware::*;

pub mod grpc_middleware {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::task::{Context, Poll};

    use either::Either;
    use futures::Future;
    use tonic::body::BoxBody;
    use tower::{Layer, Service};

    /// Manages the await-trees of `gRPC` requests that are currently served by the compute node.
    pub type AwaitTreeRegistryRef = await_tree::Registry;

    /// Await-tree key type for gRPC calls.
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

    impl<S> Service<http::Request<BoxBody>> for AwaitTreeMiddleware<S>
    where
        S: Service<http::Request<BoxBody>> + Clone,
    {
        type Error = S::Error;
        type Response = S::Response;

        type Future = impl Future<Output = Result<Self::Response, Self::Error>>;

        fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            self.inner.poll_ready(cx)
        }

        fn call(&mut self, req: http::Request<BoxBody>) -> Self::Future {
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
}
