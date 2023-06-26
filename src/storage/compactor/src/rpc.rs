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

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;
use risingwave_pb::compactor::compactor_service_server::CompactorService;
use risingwave_pb::compactor::{EchoRequest, EchoResponse};
use risingwave_pb::monitor_service::monitor_service_server::MonitorService;
use risingwave_pb::monitor_service::{
    ProfilingRequest, ProfilingResponse, StackTraceRequest, StackTraceResponse,
};
use tonic::{Request, Response, Status};

#[derive(Default)]
pub struct CompactorServiceImpl {}

#[async_trait::async_trait]
impl CompactorService for CompactorServiceImpl {
    async fn echo(&self, _request: Request<EchoRequest>) -> Result<Response<EchoResponse>, Status> {
        Ok(Response::new(EchoResponse {}))
    }
}

pub struct MonitorServiceImpl {
    await_tree_reg: Option<Arc<RwLock<await_tree::Registry<String>>>>,
}

impl MonitorServiceImpl {
    pub fn new(await_tree_reg: Option<Arc<RwLock<await_tree::Registry<String>>>>) -> Self {
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
            None => HashMap::default(),
            Some(await_tree_reg) => await_tree_reg
                .read()
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        };
        Ok(Response::new(StackTraceResponse {
            actor_traces: Default::default(),
            rpc_traces: Default::default(),
            compaction_task_traces,
        }))
    }

    async fn profiling(
        &self,
        _request: Request<ProfilingRequest>,
    ) -> Result<Response<ProfilingResponse>, Status> {
        Err(Status::unimplemented(
            "profiling unimplemented in compactor",
        ))
    }
}
