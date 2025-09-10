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

use risingwave_pb::compactor::compactor_service_server::CompactorService;
use risingwave_pb::compactor::{
    DispatchCompactionTaskRequest, DispatchCompactionTaskResponse, EchoRequest, EchoResponse,
};
use risingwave_pb::monitor_service::monitor_service_server::MonitorService;
use risingwave_pb::monitor_service::{
    AnalyzeHeapRequest, AnalyzeHeapResponse, GetProfileStatsRequest, GetProfileStatsResponse,
    GetStreamingStatsRequest, GetStreamingStatsResponse, HeapProfilingRequest,
    HeapProfilingResponse, ListHeapProfilingRequest, ListHeapProfilingResponse, ProfilingRequest,
    ProfilingResponse, StackTraceRequest, StackTraceResponse, TieredCacheTracingRequest,
    TieredCacheTracingResponse,
};
use risingwave_storage::hummock::compactor::CompactionAwaitTreeRegRef;
use risingwave_storage::hummock::compactor::await_tree_key::Compaction;
use tokio::sync::mpsc;
use tonic::{Request, Response, Status};

#[derive(Default)]
pub struct CompactorServiceImpl {
    sender: Option<mpsc::UnboundedSender<Request<DispatchCompactionTaskRequest>>>,
}
impl CompactorServiceImpl {
    pub fn new(sender: mpsc::UnboundedSender<Request<DispatchCompactionTaskRequest>>) -> Self {
        Self {
            sender: Some(sender),
        }
    }
}
#[async_trait::async_trait]
impl CompactorService for CompactorServiceImpl {
    async fn echo(&self, _request: Request<EchoRequest>) -> Result<Response<EchoResponse>, Status> {
        Ok(Response::new(EchoResponse {}))
    }

    async fn dispatch_compaction_task(
        &self,
        request: Request<DispatchCompactionTaskRequest>,
    ) -> Result<Response<DispatchCompactionTaskResponse>, Status> {
        match &self.sender.as_ref() {
            Some(sender) => {
                sender
                    .send(request)
                    .expect("DispatchCompactionTaskRequest should be able to send");
            }
            None => {
                tracing::error!(
                    "fail to send DispatchCompactionTaskRequest, sender has not been initialized."
                );
            }
        }
        Ok(Response::new(DispatchCompactionTaskResponse {
            status: None,
        }))
    }
}

pub struct MonitorServiceImpl {
    await_tree_reg: Option<CompactionAwaitTreeRegRef>,
}

impl MonitorServiceImpl {
    pub fn new(await_tree_reg: Option<CompactionAwaitTreeRegRef>) -> Self {
        Self { await_tree_reg }
    }
}

#[async_trait::async_trait]
impl MonitorService for MonitorServiceImpl {
    async fn stack_trace(
        &self,
        _request: Request<StackTraceRequest>,
    ) -> Result<Response<StackTraceResponse>, Status> {
        let compaction_task_traces = match &self.await_tree_reg {
            None => Default::default(),
            Some(await_tree_reg) => await_tree_reg
                .collect::<Compaction>()
                .into_iter()
                .map(|(k, v)| (format!("{k:?}"), v.to_string()))
                .collect(),
        };
        Ok(Response::new(StackTraceResponse {
            compaction_task_traces,
            ..Default::default()
        }))
    }

    async fn profiling(
        &self,
        _request: Request<ProfilingRequest>,
    ) -> Result<Response<ProfilingResponse>, Status> {
        Err(Status::unimplemented(
            "CPU profiling unimplemented in compactor",
        ))
    }

    async fn heap_profiling(
        &self,
        _request: Request<HeapProfilingRequest>,
    ) -> Result<Response<HeapProfilingResponse>, Status> {
        Err(Status::unimplemented(
            "Heap profiling unimplemented in compactor",
        ))
    }

    async fn list_heap_profiling(
        &self,
        _request: Request<ListHeapProfilingRequest>,
    ) -> Result<Response<ListHeapProfilingResponse>, Status> {
        Err(Status::unimplemented(
            "Heap profiling unimplemented in compactor",
        ))
    }

    async fn analyze_heap(
        &self,
        _request: Request<AnalyzeHeapRequest>,
    ) -> Result<Response<AnalyzeHeapResponse>, Status> {
        Err(Status::unimplemented(
            "Heap profiling unimplemented in compactor",
        ))
    }

    async fn get_streaming_stats(
        &self,
        _request: Request<GetStreamingStatsRequest>,
    ) -> Result<Response<GetStreamingStatsResponse>, Status> {
        Err(Status::unimplemented(
            "Get Back Pressure unimplemented in compactor",
        ))
    }

    async fn tiered_cache_tracing(
        &self,
        _: Request<TieredCacheTracingRequest>,
    ) -> Result<Response<TieredCacheTracingResponse>, Status> {
        Err(Status::unimplemented(
            "Tiered Cache Tracing unimplemented in compactor",
        ))
    }

    async fn get_profile_stats(
        &self,
        _request: Request<GetProfileStatsRequest>,
    ) -> Result<Response<GetProfileStatsResponse>, Status> {
        Err(Status::unimplemented(
            "Get Profile Stats unimplemented in compactor",
        ))
    }
}
